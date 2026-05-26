//! # `orbit-rs` — fleet-aware shared-memory rings
//!
//! See the repository `VISION.md` for the broader design direction.
//!
//! ## What this crate is
//!
//! Fleet-shared, network-aware ring storage — the same-host tier between
//! APCu (per-worker) and Redis (cross-host).
//!
//! ```text
//! local var       — register-fast,    no sharing
//! APCu            — process-fast,     per-worker
//! Orbit           — fleet-shared,     same-host    ← here
//! Redis           — cluster-shared,   cross-host
//! PostgreSQL      — durable,          cross-time
//! ```
//!
//! ## What this crate is NOT
//!
//! - **Not bound to any application framework.** The crate exposes
//!   runtime primitives; framework-specific wiring belongs in adapter
//!   crates above it.
//! - **Not a service.** Orbit has its own fleet handle, but no
//!   application lifecycle.
//! - **Not a database or message broker.** The crate exposes rings and
//!   small substrates such as `OrbitCache`; product-facing services live
//!   in adapters above it.
//!
//! ## Three Musketeers
//!
//! > *Hepimiz birimiz, birimiz hepimiz. — Dumas*
//! >
//! > *All for one, one for all.*
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
pub mod typed;

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
pub use contest::guard::{
    CONTEST_FRAME_KIND_CLAIM, CONTEST_FRAME_KIND_RELEASE, CONTEST_PAYLOAD_MAX, CONTEST_RING_KIND,
    Claim, Contest, ContestOwner, ContestRecord, ContestSubject, ContestType, Guard, Holder,
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
pub use typed::OrbitTyped;
