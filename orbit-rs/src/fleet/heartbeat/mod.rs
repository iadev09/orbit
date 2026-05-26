//! Fleet-level heartbeat over an Orbit ring.
//!
//! This is an Orbit substrate signal: it shows that a node is still
//! publishing into the shared fleet fabric. It is not process
//! supervision and does not replace a runtime's own worker-health
//! mechanism.

mod record;
mod sample;
mod snapshot;

pub use record::{FLEET_HEARTBEAT_FRAME_KIND, FLEET_HEARTBEAT_RING_KIND, FleetHeartbeatRecord};
pub use sample::FleetHeartbeat;
pub(crate) use sample::latest_heartbeats;
pub use snapshot::FleetHeartbeatSnapshot;
pub(crate) use snapshot::heartbeat_snapshot;
