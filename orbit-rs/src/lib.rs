//! # `orbit-rs` — fleet-aware shared memory
//!
//! ## What this crate is
//!
//! Fleet-shared same-host runtime storage between workers.
//!
//! Every process in the fleet is an equal member. There is no master,
//! no worker — only peers. Whatever role distinction matters to the
//! embedder is the embedder's concern; not Orbit's.
//!
//! ## Status — first light (V0)
//!
//! V0 ships fixed-capacity append logs and current-state tables with POSIX
//! shared-memory backing on Unix. Semantic layers choose the shape that
//! matches their data: history belongs in rings; current leases do not.

pub mod cache;
pub mod contest;
pub mod epoch;
pub mod error;
pub mod event;
pub mod fleet;
pub mod orbital;
pub mod ring;
pub mod rpc;
#[cfg(unix)]
pub mod shm;

pub mod id {
    //! Re-export of the standalone `netid64` primitive.

    pub use netid64::{NetId64, ParseNetId64Error};
}

#[cfg(unix)]
pub mod ring_shm {
    //! Compatibility module for the pre-`ring::shm` layout.
    pub use crate::ring::shm::*;
}

#[cfg(unix)]
pub use cache::{
    CACHE_PAYLOAD_MAX, CACHE_RING_KIND, CACHE_RING_SPEC, OrbitCache, OrbitCacheEntry,
    OrbitCacheRead,
};
pub use contest::fence::{Fence, FenceToken};
pub use contest::guard::{
    CONTEST_STATE_CAPACITY, CONTEST_STATE_KIND, CONTEST_STATE_PAYLOAD_MAX, Claim, Contest,
    ContestOwner, ContestSubject, ContestType, Guard, Holder,
};
pub use epoch::OrbitEpoch;
pub use error::{Error, Result};
pub use event::{
    EVENT_PAYLOAD_MAX, EVENT_RING_KIND, EVENT_RING_SPEC, OrbitEvent, OrbitEventBus,
    OrbitEventCursor, OrbitEventPoll,
};
pub use fleet::{Fleet, NodeId};
pub use id::{NetId64, ParseNetId64Error};
pub use orbital::Orbital;
#[cfg(any(target_os = "linux", target_os = "freebsd"))]
pub use ring::RingEventFd;
pub use ring::cursor::{RingCursor, RingFrameSource, RingLoss, RingPoll, RingRead, poll_ring};
pub use ring::{Frame, Ring, RingSpec, RingTopology};

/// Marker for a type that has a stable wire identity across the fleet.
///
/// Required bounds:
/// - `Clone` — values may be delivered to multiple subscribers / nodes.
/// - `Send + Sync + 'static` — values cross thread / process boundaries.
pub trait OrbitTyped: Clone + Send + Sync + 'static {
    /// Stable wire identifier. Hand-picked in V0; build.rs-generated later.
    const KIND: u8;

    /// Ring layout and retention policy for this wire kind.
    ///
    /// This is part of the cross-process wire contract. Reusing a KIND
    /// with a different spec is an error.
    const RING_SPEC: RingSpec;
}
