//! Request/reply messaging over two Orbit rings.
//!
//! RPC differs from [`crate::event::OrbitEventBus`]: an event is a
//! broadcast fact, while an RPC request is addressed to one fleet node
//! and carries a correlation id that a reply must echo.
//!
//! Application request and reply types do not implement
//! [`crate::OrbitTyped`]. A [`Lane`] declares exactly two
//! physical ring kinds for an RPC domain: one request segment and one reply
//! segment. Each segment contains one writer lane per fleet node. Typed codecs
//! and handler registration belong above this raw byte protocol.

use std::collections::HashMap;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use bytes::{BufMut, Bytes, BytesMut};

use crate::error::{Error, Result};
use crate::fleet::FleetLaneCursor;
use crate::{Fleet, NetId64, NodeId, OrbitEpoch, OrbitTyped, RingSpec};

const PROTOCOL_VERSION: u8 = 1;
const FRAME_KIND_REQUEST: u8 = 1;
const FRAME_KIND_REPLY: u8 = 1;
const REQUEST_HEADER_LEN: usize = 1 + 2 + 2 + 4;
const REPLY_HEADER_LEN: usize = 1 + 1 + 8 + 4;

/// Physical request/reply lanes for one RPC domain.
///
/// A domain normally defines one zero-sized marker type implementing
/// this trait. Individual methods carried by that domain are identified
/// by their method name inside the request payload; they do not allocate
/// more SHM segments.
pub trait Lane: Send + Sync + 'static {
    const REQUEST_RING_KIND: u8;
    const REQUEST_RING_SPEC: RingSpec;
    const REPLY_RING_KIND: u8;
    const REPLY_RING_SPEC: RingSpec;
}

struct RpcRequestRecord<L>(PhantomData<L>);

impl<L> Clone for RpcRequestRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: Lane> OrbitTyped for RpcRequestRecord<L> {
    const KIND: u8 = L::REQUEST_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        L::REQUEST_RING_SPEC.capacity,
        L::REQUEST_RING_SPEC.payload_capacity,
    );
}

struct RpcReplyRecord<L>(PhantomData<L>);

impl<L> Clone for RpcReplyRecord<L> {
    fn clone(&self) -> Self {
        Self(PhantomData)
    }
}

impl<L: Lane> OrbitTyped for RpcReplyRecord<L> {
    const KIND: u8 = L::REPLY_RING_KIND;
    const RING_SPEC: RingSpec = RingSpec::per_node(
        L::REPLY_RING_SPEC.capacity,
        L::REPLY_RING_SPEC.payload_capacity,
    );
}

/// Caller-owned position in one RPC request stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RequestCursor {
    inner: FleetLaneCursor,
}

/// Caller-owned position in one RPC reply stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplyCursor {
    inner: FleetLaneCursor,
}

/// One decoded RPC request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Incoming {
    /// Correlation id minted by the request ring write.
    pub id: NetId64,
    pub from: NodeId,
    pub target: NodeId,
    pub method: String,
    pub payload: Bytes,
    pub timestamp_ms: u64,
}

/// Terminal outcome reported by an RPC handler.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Outcome {
    Completed = 1,
    Rejected = 2,
    Failed = 3,
}

impl Outcome {
    fn from_wire(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Completed),
            2 => Some(Self::Rejected),
            3 => Some(Self::Failed),
            _ => None,
        }
    }
}

/// One decoded RPC reply.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Response {
    pub id: NetId64,
    pub request_id: NetId64,
    pub from: NodeId,
    pub outcome: Outcome,
    pub payload: Bytes,
    pub timestamp_ms: u64,
}

/// Result of advancing an RPC request cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RequestPoll {
    pub requests: Vec<Incoming>,
    pub lagged: u64,
}

impl RequestPoll {
    pub fn is_empty(&self) -> bool {
        self.requests.is_empty() && self.lagged == 0
    }
}

/// Result of advancing an RPC reply cursor.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReplyPoll {
    pub replies: Vec<Response>,
    pub lagged: u64,
}

impl ReplyPoll {
    pub fn is_empty(&self) -> bool {
        self.replies.is_empty() && self.lagged == 0
    }
}

/// Raw fleet RPC transport. Cheap to clone.
pub struct Endpoint<L: Lane> {
    fleet: Arc<Fleet>,
    _lane: PhantomData<L>,
}

impl<L: Lane> Clone for Endpoint<L> {
    fn clone(&self) -> Self {
        Self {
            fleet: self.fleet.clone(),
            _lane: PhantomData,
        }
    }
}

impl<L: Lane> Endpoint<L> {
    pub fn new(fleet: Arc<Fleet>) -> Self {
        assert_ne!(
            L::REQUEST_RING_KIND,
            L::REPLY_RING_KIND,
            "RPC request and reply ring kinds must differ"
        );
        Self {
            fleet,
            _lane: PhantomData,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.fleet.node_id()
    }

    /// Start after every request currently retained in the lane.
    pub fn request_cursor_at_head(&self) -> RequestCursor {
        RequestCursor {
            inner: self.fleet.lane_cursor_at_head::<RpcRequestRecord<L>>(),
        }
    }

    /// Replay request history still retained by the ring.
    pub fn request_cursor_from_start(&self) -> RequestCursor {
        RequestCursor {
            inner: self.fleet.lane_cursor_from_start::<RpcRequestRecord<L>>(),
        }
    }

    /// Start after every reply currently retained in the lane.
    pub fn reply_cursor_at_head(&self) -> ReplyCursor {
        ReplyCursor {
            inner: self.fleet.lane_cursor_at_head::<RpcReplyRecord<L>>(),
        }
    }

    /// Replay reply history still retained by the ring.
    pub fn reply_cursor_from_start(&self) -> ReplyCursor {
        ReplyCursor {
            inner: self.fleet.lane_cursor_from_start::<RpcReplyRecord<L>>(),
        }
    }

    /// Clear both RPC lanes during owner-controlled boot.
    ///
    /// Do not call while fleet peers are publishing.
    pub fn reset_rings(&self) -> Result<()> {
        self.fleet
            .reset_ring::<RpcRequestRecord<L>>()
            .map_err(Error::Io)?;
        self.fleet
            .reset_ring::<RpcReplyRecord<L>>()
            .map_err(Error::Io)
    }

    /// Publish one request addressed to a fleet node.
    pub fn publish_request(
        &self,
        target: NodeId,
        method: &str,
        payload: impl AsRef<[u8]>,
    ) -> Result<NetId64> {
        let timestamp_ms = OrbitEpoch::now().as_unix_ms();
        let frame = encode_request::<L>(target, method.as_bytes(), payload.as_ref())?;
        Ok(self
            .fleet
            .publish::<RpcRequestRecord<L>>(FRAME_KIND_REQUEST, timestamp_ms, frame))
    }

    /// Publish a terminal reply for `request`.
    pub fn reply(
        &self,
        request: &Incoming,
        outcome: Outcome,
        payload: impl AsRef<[u8]>,
    ) -> Result<NetId64> {
        self.publish_reply(request.id, outcome, payload)
    }

    /// Publish a terminal reply for a known correlation id.
    pub fn publish_reply(
        &self,
        request_id: NetId64,
        outcome: Outcome,
        payload: impl AsRef<[u8]>,
    ) -> Result<NetId64> {
        let timestamp_ms = OrbitEpoch::now().as_unix_ms();
        let frame = encode_reply::<L>(request_id, outcome, payload.as_ref())?;
        Ok(self
            .fleet
            .publish::<RpcReplyRecord<L>>(FRAME_KIND_REPLY, timestamp_ms, frame))
    }

    /// Poll requests addressed to this node.
    ///
    /// The cursor advances past requests for other nodes as well; every
    /// node must therefore own an independent request cursor.
    pub fn poll_requests(&self, cursor: &mut RequestCursor) -> RequestPoll {
        let ring_poll = self
            .fleet
            .poll_lanes::<RpcRequestRecord<L>>(&mut cursor.inner);
        let mut lagged = ring_poll.loss.total();
        let mut requests = Vec::new();

        for frame in ring_poll.frames {
            if frame.kind != FRAME_KIND_REQUEST {
                lagged = lagged.saturating_add(1);
                continue;
            }
            let Some(decoded) = decode_request(&frame.payload) else {
                lagged = lagged.saturating_add(1);
                continue;
            };
            if decoded.target != self.node_id() {
                continue;
            }
            requests.push(Incoming {
                id: frame.id,
                from: NodeId::new(frame.id.node()),
                target: decoded.target,
                method: decoded.method.to_owned(),
                payload: decoded.payload,
                timestamp_ms: frame.ver,
            });
        }

        RequestPoll { requests, lagged }
    }

    /// Poll every decoded reply across the node-owned reply lanes.
    pub fn poll_replies(&self, cursor: &mut ReplyCursor) -> ReplyPoll {
        let ring_poll = self
            .fleet
            .poll_lanes::<RpcReplyRecord<L>>(&mut cursor.inner);
        let mut lagged = ring_poll.loss.total();
        let mut replies = Vec::new();

        for frame in ring_poll.frames {
            if frame.kind != FRAME_KIND_REPLY {
                lagged = lagged.saturating_add(1);
                continue;
            }
            let Some(decoded) = decode_reply::<L>(&frame.payload) else {
                lagged = lagged.saturating_add(1);
                continue;
            };
            replies.push(Response {
                id: frame.id,
                request_id: decoded.request_id,
                from: NodeId::new(frame.id.node()),
                outcome: decoded.outcome,
                payload: decoded.payload,
                timestamp_ms: frame.ver,
            });
        }

        ReplyPoll { replies, lagged }
    }
}

struct PendingState {
    reply: Option<Response>,
    waker: Option<Waker>,
}

struct PendingReply {
    target: NodeId,
    state: Mutex<PendingState>,
}

impl PendingReply {
    fn new(target: NodeId) -> Self {
        Self {
            target,
            state: Mutex::new(PendingState {
                reply: None,
                waker: None,
            }),
        }
    }

    fn complete(&self, reply: Response) {
        let waker = {
            let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
            state.reply = Some(reply);
            state.waker.take()
        };
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

struct RpcClientInner<L: Lane> {
    rpc: Endpoint<L>,
    reply_cursor: Mutex<ReplyCursor>,
    pending: Mutex<HashMap<NetId64, Arc<PendingReply>>>,
}

/// Correlates replies with in-process callers.
///
/// This type does not create a thread or depend on an async runtime.
/// The embedding runtime must drive [`Self::poll_replies`] from one
/// long-running task. Each matched reply wakes the corresponding
/// [`Call`] future.
pub struct Client<L: Lane> {
    inner: Arc<RpcClientInner<L>>,
}

impl<L: Lane> Clone for Client<L> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<L: Lane> Client<L> {
    pub fn new(rpc: Endpoint<L>) -> Self {
        let reply_cursor = rpc.reply_cursor_at_head();
        Self {
            inner: Arc::new(RpcClientInner {
                rpc,
                reply_cursor: Mutex::new(reply_cursor),
                pending: Mutex::new(HashMap::new()),
            }),
        }
    }

    /// Publish a request and wait for its correlated reply.
    pub async fn send(
        &self,
        target: NodeId,
        method: &str,
        payload: impl AsRef<[u8]>,
    ) -> Result<Response> {
        Ok(self.start(target, method, payload)?.await)
    }

    /// Publish a request and return its awaitable reply handle without
    /// awaiting it yet.
    ///
    /// Registering the pending call and publishing are serialized with
    /// reply dispatch so a very fast peer cannot reply before the local
    /// correlation entry becomes visible.
    pub fn start(
        &self,
        target: NodeId,
        method: &str,
        payload: impl AsRef<[u8]>,
    ) -> Result<Call<L>> {
        let pending = Arc::new(PendingReply::new(target));
        let mut calls = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
        let request_id = self.inner.rpc.publish_request(target, method, payload)?;
        calls.insert(request_id, pending.clone());
        drop(calls);

        Ok(Call {
            request_id,
            pending,
            client: Arc::downgrade(&self.inner),
            completed: false,
        })
    }

    /// Advance the reply lane and wake every matching local call.
    ///
    /// Replies for another node or another client instance remain in
    /// the returned poll as unmatched observations, but do not complete
    /// a local future.
    pub fn poll_replies(&self) -> ReplyPoll {
        let mut cursor = self
            .inner
            .reply_cursor
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let poll = self.inner.rpc.poll_replies(&mut cursor);
        drop(cursor);

        for reply in &poll.replies {
            if reply.request_id.node() != self.inner.rpc.node_id().get() {
                continue;
            }
            let pending = {
                let mut calls = self.inner.pending.lock().unwrap_or_else(|e| e.into_inner());
                if calls
                    .get(&reply.request_id)
                    .is_some_and(|pending| pending.target == reply.from)
                {
                    calls.remove(&reply.request_id)
                } else {
                    None
                }
            };
            if let Some(pending) = pending {
                pending.complete(reply.clone());
            }
        }

        poll
    }

    pub fn pending_len(&self) -> usize {
        self.inner
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }
}

/// Awaitable handle for one RPC reply.
///
/// Dropping this future removes its local correlation entry. Timeouts
/// remain runtime policy: wrap the future with the embedding runtime's
/// timeout primitive.
#[must_use = "an RPC call does nothing useful unless it is awaited"]
pub struct Call<L: Lane> {
    request_id: NetId64,
    pending: Arc<PendingReply>,
    client: Weak<RpcClientInner<L>>,
    completed: bool,
}

impl<L: Lane> Call<L> {
    pub fn request_id(&self) -> NetId64 {
        self.request_id
    }
}

impl<L: Lane> Future for Call<L> {
    type Output = Response;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.pending.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(reply) = state.reply.take() {
            drop(state);
            self.completed = true;
            return Poll::Ready(reply);
        }
        if state
            .waker
            .as_ref()
            .is_none_or(|waker| !waker.will_wake(cx.waker()))
        {
            state.waker = Some(cx.waker().clone());
        }
        Poll::Pending
    }
}

impl<L: Lane> Drop for Call<L> {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        let Some(client) = self.client.upgrade() else {
            return;
        };
        let mut calls = client.pending.lock().unwrap_or_else(|e| e.into_inner());
        if calls
            .get(&self.request_id)
            .is_some_and(|entry| Arc::ptr_eq(entry, &self.pending))
        {
            calls.remove(&self.request_id);
        }
    }
}

struct DecodedRequest {
    target: NodeId,
    method: String,
    payload: Bytes,
}

struct DecodedReply {
    request_id: NetId64,
    outcome: Outcome,
    payload: Bytes,
}

fn encode_request<L: Lane>(target: NodeId, method: &[u8], payload: &[u8]) -> Result<Bytes> {
    let total = REQUEST_HEADER_LEN
        .saturating_add(method.len())
        .saturating_add(payload.len());
    if method.len() > u16::MAX as usize
        || payload.len() > u32::MAX as usize
        || total > L::REQUEST_RING_SPEC.payload_capacity
    {
        return Err(Error::RpcFrameTooLarge {
            frame: "request",
            method_len: method.len(),
            payload_len: payload.len(),
            max_payload: L::REQUEST_RING_SPEC.payload_capacity,
        });
    }

    let mut buf = BytesMut::with_capacity(total);
    buf.put_u8(PROTOCOL_VERSION);
    buf.put_u16_le(target.get());
    buf.put_u16_le(method.len() as u16);
    buf.put_u32_le(payload.len() as u32);
    buf.put_slice(method);
    buf.put_slice(payload);
    Ok(buf.freeze())
}

fn decode_request(payload: &Bytes) -> Option<DecodedRequest> {
    if payload.len() < REQUEST_HEADER_LEN || payload[0] != PROTOCOL_VERSION {
        return None;
    }
    let target = NodeId::new(u16::from_le_bytes(payload[1..3].try_into().ok()?));
    let method_len = u16::from_le_bytes(payload[3..5].try_into().ok()?) as usize;
    let payload_len = u32::from_le_bytes(payload[5..9].try_into().ok()?) as usize;
    let method_start = REQUEST_HEADER_LEN;
    let method_end = method_start.checked_add(method_len)?;
    let payload_end = method_end.checked_add(payload_len)?;
    if payload_end != payload.len() {
        return None;
    }
    let method = std::str::from_utf8(&payload[method_start..method_end])
        .ok()?
        .to_owned();
    Some(DecodedRequest {
        target,
        method,
        payload: payload.slice(method_end..payload_end),
    })
}

fn encode_reply<L: Lane>(request_id: NetId64, outcome: Outcome, payload: &[u8]) -> Result<Bytes> {
    let total = REPLY_HEADER_LEN.saturating_add(payload.len());
    if payload.len() > u32::MAX as usize || total > L::REPLY_RING_SPEC.payload_capacity {
        return Err(Error::RpcFrameTooLarge {
            frame: "reply",
            method_len: 0,
            payload_len: payload.len(),
            max_payload: L::REPLY_RING_SPEC.payload_capacity,
        });
    }

    let mut buf = BytesMut::with_capacity(total);
    buf.put_u8(PROTOCOL_VERSION);
    buf.put_u8(outcome as u8);
    buf.put_u64_le(request_id.raw());
    buf.put_u32_le(payload.len() as u32);
    buf.put_slice(payload);
    Ok(buf.freeze())
}

fn decode_reply<L: Lane>(payload: &Bytes) -> Option<DecodedReply> {
    if payload.len() < REPLY_HEADER_LEN || payload[0] != PROTOCOL_VERSION {
        return None;
    }
    let outcome = Outcome::from_wire(payload[1])?;
    let request_id = NetId64::from_raw(u64::from_le_bytes(payload[2..10].try_into().ok()?));
    if request_id.kind() != L::REQUEST_RING_KIND {
        return None;
    }
    let payload_len = u32::from_le_bytes(payload[10..14].try_into().ok()?) as usize;
    let payload_end = REPLY_HEADER_LEN.checked_add(payload_len)?;
    if payload_end != payload.len() {
        return None;
    }
    Some(DecodedReply {
        request_id,
        outcome,
        payload: payload.slice(REPLY_HEADER_LEN..payload_end),
    })
}
