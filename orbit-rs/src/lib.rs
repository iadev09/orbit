//! # `orbit-rs` — fleet-aware shared-memory rings
//!
//! ## What this crate is
//!
//! Fleet-shared, network-aware ring storage — the same-host tier between
//! workers
//!
//! Every process in the fleet is an equal member. There is no master,
//! no worker — only peers. Whatever role distinction matters to the
//! embedder is the embedder's concern; not Orbit's.
//!
//! ## Status — first light (V0)
//!
//! V0 ships the ring API surface plus POSIX shared-memory backing on
//! Unix. Higher-level data shapes should build on rings/cache directly
//! instead of adding one-off shared cells here.

pub mod cache;
pub mod contest;
pub mod error;
pub mod event;
pub mod fleet;
pub mod orbital;
pub mod ring;
#[cfg(unix)]
pub mod shm;
pub mod tick;

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
pub use cache::{CACHE_PAYLOAD_MAX, OrbitCache, OrbitCacheEntry, OrbitCacheRead};
pub use contest::fence::{Fence, FenceToken};
pub use contest::guard::{
    CONTEST_FRAME_KIND_CLAIM, CONTEST_FRAME_KIND_RELEASE, CONTEST_FRAME_KIND_RENEW,
    CONTEST_PAYLOAD_MAX, CONTEST_RING_KIND, Claim, Contest, ContestOwner, ContestRecord,
    ContestSubject, ContestType, Guard, Holder,
};
pub use error::{Error, Result};
pub use event::{
    EVENT_PAYLOAD_MAX, EVENT_RING_KIND, OrbitEvent, OrbitEventBus, OrbitEventCursor, OrbitEventPoll,
};
pub use fleet::{
    DEFAULT_RING_CAPACITY, FLEET_HEARTBEAT_RING_KIND, Fleet, FleetHeartbeat, FleetHeartbeatRecord,
    FleetHeartbeatSnapshot, NodeId,
};
pub use id::{NetId64, ParseNetId64Error};
pub use orbital::Orbital;
pub use ring::cursor::{RingCursor, RingFrameSource, RingLoss, RingPoll, poll_ring};
pub use ring::{Frame, Ring};
pub use tick::OrbitEpoch;

/// Marker for a type that has a stable wire identity across the fleet.
///
/// Required bounds:
/// - `Clone` — values may be delivered to multiple subscribers / nodes.
/// - `Send + Sync + 'static` — values cross thread / process boundaries.
pub trait OrbitTyped: Clone + Send + Sync + 'static {
    /// Stable wire identifier. Hand-picked in V0; build.rs-generated later.
    const KIND: u8;
}
