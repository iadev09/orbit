use crate::OrbitTyped;

/// Core Orbit heartbeat ring.
///
/// The value intentionally sits next to the event ring range, away from
/// cache and metrics families that already occupy the 200-211 area.
pub const FLEET_HEARTBEAT_RING_KIND: u8 = 221;

/// Message-class byte for a heartbeat sample inside the heartbeat ring.
pub const FLEET_HEARTBEAT_FRAME_KIND: u8 = 1;

#[derive(Clone, Copy, Debug)]
pub struct FleetHeartbeatRecord;

impl OrbitTyped for FleetHeartbeatRecord {
    const KIND: u8 = FLEET_HEARTBEAT_RING_KIND;
}
