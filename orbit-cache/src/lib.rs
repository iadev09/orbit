//! Fleet-coherent local cache over Orbit shared-memory rings.
//!
//! Values live in a bounded process-local L1. A dedicated mutation ring
//! propagates `Put`, `Delete`, and `Reset`; a separate addressable payload
//! ring carries `Put` bytes without coupling cache traffic to the generic
//! Orbit event bus.

mod error;
mod layout;
mod local;
mod protocol;
mod transport;

use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use orbit_rs::{Fleet, RingLoss};

pub use error::{Error, Result};
pub use layout::{
    CACHE_MUTATION_RING_KIND, CACHE_MUTATION_RING_SPEC, CACHE_PAYLOAD_RING_KIND,
    CACHE_PAYLOAD_RING_SPEC, CacheLayout, DefaultCacheLayout,
};
pub use local::{CacheEntry, CacheRead, LocalCache};
pub use protocol::{CacheMutation, CacheRevision, PayloadRef};
pub use transport::{CacheMutationCursor, CacheMutationPoll, CacheTransport};

#[cfg(any(target_os = "linux", target_os = "freebsd"))]
pub use orbit_rs::RingEventFd;

pub const DEFAULT_L1_CAPACITY: usize = 10_000;

/// Result of applying all currently visible cache mutations to the local L1.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CachePoll {
    pub observed: usize,
    pub applied: usize,
    pub ignored: usize,
    pub payload_unavailable: Vec<Bytes>,
    pub loss: RingLoss,
    pub malformed: u64,
    pub resync_required: bool,
}

impl CachePoll {
    pub fn is_empty(&self) -> bool {
        self.observed == 0 && self.loss.is_empty() && self.malformed == 0
    }
}

/// One process' cache handle. Clones share the same L1 and mutation cursor.
#[derive(Clone)]
pub struct Cache<L: CacheLayout = DefaultCacheLayout> {
    transport: CacheTransport<L>,
    local: LocalCache,
    cursor: Arc<Mutex<CacheMutationCursor>>,
}

impl<L: CacheLayout> Cache<L> {
    /// Start with an empty coherent L1 and subscribe only to future
    /// mutations. A normal local miss may be recovered from a backing store by
    /// the embedding layer.
    pub fn new(fleet: Arc<Fleet>, l1_capacity: NonZeroUsize) -> Result<Self> {
        let transport = CacheTransport::<L>::new(fleet)?;
        let cursor = transport.cursor_at_head();
        Ok(Self {
            transport,
            local: LocalCache::new(l1_capacity),
            cursor: Arc::new(Mutex::new(cursor)),
        })
    }

    /// Replay all mutation history still retained by the ring. This is useful
    /// for tests and diagnostic tools; a production cold start should normally
    /// hydrate from its backing store and then follow future mutations.
    pub fn replay_retained(fleet: Arc<Fleet>, l1_capacity: NonZeroUsize) -> Result<Self> {
        let transport = CacheTransport::<L>::new(fleet)?;
        let cursor = transport.cursor_from_start();
        Ok(Self {
            transport,
            local: LocalCache::new(l1_capacity),
            cursor: Arc::new(Mutex::new(cursor)),
        })
    }

    pub fn with_default_capacity(fleet: Arc<Fleet>) -> Result<Self> {
        Self::new(
            fleet,
            NonZeroUsize::new(DEFAULT_L1_CAPACITY).expect("default L1 capacity is non-zero"),
        )
    }

    pub fn read(&self, key: &[u8]) -> CacheRead {
        self.local.read(key)
    }

    pub fn validate_put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.transport.validate_put(key, value)
    }

    pub fn validate_key(&self, key: &[u8]) -> Result<()> {
        self.transport.validate_key(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8], ttl: Option<Duration>) -> Result<CacheRevision> {
        let mutation = self.transport.publish_put(key, value, ttl)?;
        let CacheMutation::Put {
            revision,
            expires_at_ms,
            ..
        } = mutation
        else {
            unreachable!("publish_put returned a non-put mutation")
        };
        self.local
            .install_local(key, Bytes::copy_from_slice(value), revision, expires_at_ms);
        Ok(revision)
    }

    pub fn delete(&self, key: &[u8]) -> Result<CacheRevision> {
        let mutation = self.transport.publish_delete(key)?;
        let CacheMutation::Delete { revision, .. } = mutation else {
            unreachable!("publish_delete returned a non-delete mutation")
        };
        self.local.install_missing(key, revision);
        Ok(revision)
    }

    pub fn reset(&self) -> Result<CacheRevision> {
        let mutation = self.transport.publish_reset()?;
        let CacheMutation::Reset { revision } = mutation else {
            unreachable!("publish_reset returned a non-reset mutation")
        };
        self.local.reset_local(revision);
        Ok(revision)
    }

    /// Reset cache transport during an owner-controlled, quiescent boot and
    /// align this handle's cursor and empty L1 with the new ring generation.
    pub fn reset_transport(&self) -> Result<()> {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        self.transport.reset_rings()?;
        *cursor = self.transport.cursor_at_head();
        self.local.reset_after_transport_reset();
        Ok(())
    }

    /// Drain every committed mutation currently visible and apply it to this
    /// process' L1.
    pub fn poll(&self) -> CachePoll {
        let raw = {
            let mut cursor = self
                .cursor
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            self.transport.poll(&mut cursor)
        };
        let observed = raw.mutations.len();
        if raw.requires_full_resync() {
            self.local.require_resync();
        }

        let mut result = CachePoll {
            observed,
            loss: raw.loss,
            malformed: raw.malformed,
            ..CachePoll::default()
        };
        for mutation in raw.mutations {
            match self.local.apply(&self.transport, mutation) {
                local::ApplyOutcome::Applied => result.applied += 1,
                local::ApplyOutcome::Ignored => result.ignored += 1,
                local::ApplyOutcome::PayloadUnavailable { key } => {
                    result.payload_unavailable.push(key);
                }
            }
        }
        result.resync_required = !self.local.is_coherent();
        result
    }

    /// Discard uncertain L1 state and resume after the current mutation heads.
    ///
    /// Call this only when a higher layer can satisfy ordinary misses from an
    /// authoritative source or L2. Mutations whose revisions were allocated
    /// before this boundary are ignored even if a delayed writer commits them
    /// after the new cursor snapshot.
    pub fn recover_from_backing(&self) {
        let mut cursor = self
            .cursor
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *cursor = self.transport.cursor_at_head();
        let revision_floor = self.transport.current_revision_sequence();
        self.local.recover_from_backing(revision_floor);
    }

    pub fn local(&self) -> &LocalCache {
        &self.local
    }

    pub fn transport(&self) -> &CacheTransport<L> {
        &self.transport
    }

    #[cfg(any(target_os = "linux", target_os = "freebsd"))]
    pub fn event_fd(&self) -> Result<RingEventFd> {
        self.transport.event_fd()
    }
}
