use std::collections::BTreeSet;
use std::time::Duration;

use crate::NodeId;
use crate::fleet::Fleet;
use crate::fleet::heartbeat::sample::{FleetHeartbeat, latest_heartbeats};
use crate::tick::OrbitEpoch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetHeartbeatSnapshot {
    pub now: OrbitEpoch,
    pub max_age: Duration,
    pub latest: Vec<FleetHeartbeat>,
    pub missing_node_ids: Vec<NodeId>,
    pub stale_node_ids: Vec<NodeId>,
}

pub(crate) fn heartbeat_snapshot(
    fleet: &Fleet,
    now: OrbitEpoch,
    max_age: Duration,
) -> FleetHeartbeatSnapshot {
    let latest = latest_heartbeats(fleet);
    let seen: BTreeSet<u16> = latest.iter().map(|sample| sample.node_id.get()).collect();
    let missing_node_ids = (0..u16::from(fleet.fleet_size()))
        .filter(|id| !seen.contains(id))
        .map(NodeId::new)
        .collect();
    let stale_node_ids = latest
        .iter()
        .filter(|sample| !sample.is_fresh_at(now, max_age))
        .map(|sample| sample.node_id)
        .collect();

    FleetHeartbeatSnapshot {
        now,
        max_age,
        latest,
        missing_node_ids,
        stale_node_ids,
    }
}
