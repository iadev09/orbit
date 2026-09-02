//! Ring buffers — orbit-rs's runtime substrate.
//!
//! > *"orbit runtime yani ring"* — the place where the fleet's
//! > shared state actually lives at the lowest level. Higher-level
//! > shapes (`OrbitCache`, future metrics/event substrates, etc.)
//! > reduce to *one or more rings*.
//!
//! ## Shape
//!
//! One [`Ring`] per [`OrbitTyped`] kind. A ring is a fixed-size
//! circular log of [`Frame`]s. Writers append (atomic
//! `fetch_add` on the head); readers walk the log by counter
//! position. When `head` overflows the capacity, the oldest slot
//! is overwritten — readers that fell behind see the loss as a
//! "missing counter".
//!
//! The frame layout mirrors the `nwd1` seed (see VISION §13):
//!
//! ```text
//! ┌──────────┬──────┬──────┬─────────────┐
//! │ id (8)   │ kind │ ver  │ payload (N) │
//! │ NetId64  │  u8  │ u64  │   bytes     │
//! └──────────┴──────┴──────┴─────────────┘
//! ```
//!
//! Two `kind` bytes coexist on the wire and they mean different
//! things (intentional, two-axis encoding):
//!
//! - `frame.id.kind()` — *which Rust type* (the data shape).
//! - `frame.kind`      — *which message class* (state / event /
//!   command / ack / invalidate / …). V0 leaves this open at `0`;
//!   concrete classes appear when subscriber semantics arrive.
//!
//! ## V0 backing
//!
//! `RwLock<Option<Frame>>` per slot — simple, correct, slow. V1
//! swaps the slot storage for a SHM-backed lock-free layout with
//! per-slot sequence numbers (publishing involves a write to the
//! seq, then the body, then the seq again, like LMAX Disruptor).
//! The [`Ring`] public API is V1-stable; this file is the only
//! place that changes.
//!
//! ## Who mints
//!
//! Writes go through [`Ring::write`], which mints the [`NetId64`]
//! atomically with the slot reservation (the COUNTER part *is*
//! the slot's write position). NetId64s are therefore minted
//! **server-side, by the writer process**. Browsers / external
//! clients receive ids; they do not generate them.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use bytes::Bytes;

use crate::NodeId;
use crate::OrbitTyped;
use crate::id::NetId64;

pub mod cursor;
#[cfg(unix)]
pub mod shm;

/// Physical policy for one [`OrbitTyped`] ring lane.
///
/// The policy is part of the lane's wire contract: every process that
/// uses the same `OrbitTyped::KIND` must declare the same values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RingSpec {
    /// Number of slots retained by the circular ring.
    pub capacity: usize,
    /// Maximum payload bytes stored inline in each slot.
    pub payload_capacity: usize,
}

impl RingSpec {
    pub const fn new(capacity: usize, payload_capacity: usize) -> Self {
        Self {
            capacity,
            payload_capacity,
        }
    }

    pub(crate) fn assert_valid(self) {
        assert!(self.capacity > 0, "ring capacity must be > 0");
        assert!(
            self.capacity.is_power_of_two(),
            "ring capacity must be a power of two"
        );
        assert!(
            self.payload_capacity <= u32::MAX as usize,
            "ring payload capacity must fit in u32"
        );
    }
}

/// One record in a ring — the on-wire shape (mirrors `nwd1::Frame`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Frame {
    pub id: NetId64,
    pub kind: u8,
    pub ver: u64,
    pub payload: Bytes,
}

/// A fixed-capacity, fleet-wide append-only log keyed on KIND byte.
///
/// V0 is single-process; V1 is SHM-backed. The API is the same.
pub struct Ring {
    /// The KIND this ring carries — equals `T::KIND` for the
    /// `OrbitTyped` value-shape it's storing.
    kind: u8,
    /// Number of slots; constant for the ring's lifetime.
    capacity: usize,
    /// Maximum inline payload bytes for this ring lane.
    payload_capacity: usize,
    /// Monotonic write counter. Always increases; slot index is
    /// `(write_pos - 1) % capacity` after a `fetch_add`.
    write_pos: AtomicU64,
    /// Slot storage. Each slot is `None` until the first write
    /// lands; afterwards holds the most recent frame to occupy
    /// that position. Wraparound silently overwrites.
    slots: Vec<RwLock<Option<Frame>>>,
}

impl Ring {
    /// Create the process-local ring declared by `T::RING_SPEC`.
    pub fn new<T: OrbitTyped>() -> Self {
        let spec = T::RING_SPEC;
        spec.assert_valid();
        let capacity = spec.capacity;
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(RwLock::new(None));
        }
        Self {
            kind: T::KIND,
            capacity,
            payload_capacity: spec.payload_capacity,
            write_pos: AtomicU64::new(0),
            slots,
        }
    }

    /// The KIND byte this ring carries (equals `T::KIND`).
    pub fn kind(&self) -> u8 {
        self.kind
    }

    /// Total slot count — fixed at construction.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Maximum inline payload bytes for this ring lane.
    pub fn payload_capacity(&self) -> usize {
        self.payload_capacity
    }

    pub fn spec(&self) -> RingSpec {
        RingSpec::new(self.capacity, self.payload_capacity)
    }

    /// Monotonic claim head — number of counters reserved by writers.
    pub fn head(&self) -> u64 {
        self.write_pos.load(Ordering::Acquire)
    }

    /// Append a frame. Atomically reserves the next counter, mints
    /// the [`NetId64`], and writes the frame into the corresponding
    /// slot. Returns the minted id.
    ///
    /// `frame_kind` is the message class byte (V0: pass `0`).
    /// `ver` is the version / tick at write time (V0: caller's
    /// choice).
    pub fn write(&self, node_id: NodeId, frame_kind: u8, ver: u64, payload: Bytes) -> NetId64 {
        assert!(
            payload.len() <= self.payload_capacity,
            "payload {} > ring payload capacity {}",
            payload.len(),
            self.payload_capacity
        );
        let counter = self.write_pos.fetch_add(1, Ordering::AcqRel);
        let id = NetId64::make(self.kind, node_id.get(), counter);
        let slot_idx = (counter as usize) % self.capacity;
        let frame = Frame {
            id,
            kind: frame_kind,
            ver,
            payload,
        };
        let mut guard = self.slots[slot_idx].write().expect("ring slot poisoned");
        *guard = Some(frame);
        id
    }

    /// Read the slot that the given [`NetId64`]'s counter points at.
    ///
    /// Returns:
    /// - `Some(frame)` if the slot's stored id matches the queried id
    ///   exactly (the slot has not been overwritten by a later writer).
    /// - `None` if the slot is empty, has wrapped past, or holds a
    ///   different id than the one asked for.
    pub fn read(&self, id: NetId64) -> Option<Frame> {
        if id.kind() != self.kind {
            return None;
        }
        let slot_idx = (id.counter() as usize) % self.capacity;
        let guard = self.slots[slot_idx].read().expect("ring slot poisoned");
        match &*guard {
            Some(f) if f.id == id => Some(f.clone()),
            _ => None,
        }
    }

    /// Read the most recent frame, regardless of who wrote it.
    /// Useful for "what's the current state?" — ignores
    /// counter-by-counter walking.
    pub fn read_head(&self) -> Option<Frame> {
        let head = self.head();
        if head == 0 {
            return None;
        }
        let slot_idx = ((head - 1) as usize) % self.capacity;
        self.slots[slot_idx]
            .read()
            .expect("ring slot poisoned")
            .clone()
    }

    /// Read whatever frame currently occupies the slot at
    /// `counter % capacity`, regardless of which counter is
    /// stored in it. Used by walking readers that need slot-by-slot
    /// access without knowing the writer's `NetId64` ahead of time.
    ///
    /// Returns `None` if the slot is empty.
    pub fn read_at(&self, counter: u64) -> Option<Frame> {
        let slot_idx = (counter as usize) % self.capacity;
        self.slots[slot_idx]
            .read()
            .expect("ring slot poisoned")
            .clone()
    }

    pub(crate) fn read_state_at(&self, counter: u64) -> cursor::RingRead {
        match self.read_at(counter) {
            Some(frame) if frame.id.counter() == counter => cursor::RingRead::Ready(frame),
            Some(frame) if frame.id.counter() > counter => cursor::RingRead::Unavailable,
            Some(_) | None => cursor::RingRead::Pending,
        }
    }

    /// Clear all slots and reset the claim head to zero.
    ///
    /// Intended for owner-controlled boot-time cleanup. Do not call
    /// while other threads are publishing to this ring.
    pub fn reset(&self) {
        for slot in &self.slots {
            *slot.write().expect("ring slot poisoned") = None;
        }
        self.write_pos.store(0, Ordering::Release);
    }
}

impl cursor::RingFrameSource for Ring {
    fn kind(&self) -> u8 {
        Ring::kind(self)
    }

    fn head(&self) -> u64 {
        Ring::head(self)
    }

    fn capacity(&self) -> usize {
        Ring::capacity(self)
    }

    fn read_at(&self, counter: u64) -> Option<Frame> {
        Ring::read_at(self, counter)
    }

    fn read_state_at(&self, counter: u64) -> cursor::RingRead {
        Ring::read_state_at(self, counter)
    }
}

#[cfg(unix)]
impl cursor::RingFrameSource for shm::ShmRing {
    fn kind(&self) -> u8 {
        shm::ShmRing::kind(self)
    }

    fn head(&self) -> u64 {
        shm::ShmRing::head(self)
    }

    fn capacity(&self) -> usize {
        shm::ShmRing::capacity(self)
    }

    fn read_at(&self, counter: u64) -> Option<Frame> {
        shm::ShmRing::read_at(self, counter)
    }

    fn read_state_at(&self, counter: u64) -> cursor::RingRead {
        shm::ShmRing::read_state_at(self, counter)
    }
}

impl std::fmt::Debug for Ring {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Ring")
            .field("kind", &self.kind)
            .field("capacity", &self.capacity)
            .field("payload_capacity", &self.payload_capacity)
            .field("head", &self.head())
            .finish()
    }
}

/// Type-keyed registry of rings. A `Fleet` holds one of these and
/// hands out `Arc<Ring>` per `OrbitTyped` kind on demand.
pub(crate) struct RingRegistry {
    rings: dashmap::DashMap<u8, Arc<Ring>>,
}

impl RingRegistry {
    pub fn new() -> Self {
        Self {
            rings: dashmap::DashMap::new(),
        }
    }

    /// Get-or-create the ring declared by `T`.
    pub fn get_or_create<T: OrbitTyped>(&self) -> Arc<Ring> {
        let ring = self
            .rings
            .entry(T::KIND)
            .or_insert_with(|| Arc::new(Ring::new::<T>()))
            .clone();
        assert_eq!(
            ring.spec(),
            T::RING_SPEC,
            "OrbitTyped KIND {} was reused with a different ring spec",
            T::KIND
        );
        ring
    }

    /// Look up a ring by KIND byte (e.g. when only the id is known).
    pub fn lookup(&self, kind: u8) -> Option<Arc<Ring>> {
        self.rings.get(&kind).map(|e| e.clone())
    }
}
