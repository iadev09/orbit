use std::collections::BTreeMap;
use std::time::Duration;

use crate::NodeId;
use crate::fleet::Fleet;
use crate::fleet::heartbeat::record::{
    FLEET_HEARTBEAT_FRAME_KIND, FLEET_HEARTBEAT_RING_KIND, FleetHeartbeatRecord,
};
use crate::id::NetId64;
use crate::ring::Frame;
use crate::tick::OrbitEpoch;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FleetHeartbeat {
    pub id: NetId64,
    pub node_id: NodeId,
    pub captured_at: OrbitEpoch,
}

impl FleetHeartbeat {
    pub fn counter(&self) -> u64 {
        self.id.counter()
    }

    pub fn age_at(&self, now: OrbitEpoch) -> Duration {
        self.captured_at.age_at(now)
    }

    pub fn is_fresh_at(&self, now: OrbitEpoch, max_age: Duration) -> bool {
        self.captured_at.is_fresh_at(now, max_age)
    }

    pub(crate) fn from_frame(frame: Frame) -> Option<Self> {
        if frame.id.kind() != FLEET_HEARTBEAT_RING_KIND {
            return None;
        }
        if frame.kind != FLEET_HEARTBEAT_FRAME_KIND {
            return None;
        }

        Some(Self {
            id: frame.id,
            node_id: NodeId::new(frame.id.node()),
            captured_at: OrbitEpoch::from_unix_ms(frame.ver),
        })
    }
}

pub(crate) fn latest_heartbeats(fleet: &Fleet) -> Vec<FleetHeartbeat> {
    let head = fleet.head::<FleetHeartbeatRecord>();
    if head == 0 {
        return Vec::new();
    }

    let capacity = fleet.ring_capacity::<FleetHeartbeatRecord>() as u64;
    let walk_count = head.min(capacity);
    let expected_nodes = fleet.fleet_size() as usize;
    let mut by_node = BTreeMap::new();

    for offset in 0..walk_count {
        let counter = head - 1 - offset;
        let Some(frame) = fleet.read_at::<FleetHeartbeatRecord>(counter) else {
            if counter == 0 {
                break;
            }
            continue;
        };
        if frame.id.counter() != counter {
            if counter == 0 {
                break;
            }
            continue;
        }
        let Some(sample) = FleetHeartbeat::from_frame(frame) else {
            if counter == 0 {
                break;
            }
            continue;
        };

        by_node.entry(sample.node_id.get()).or_insert(sample);
        if expected_nodes > 0 && by_node.len() >= expected_nodes {
            break;
        }
        if counter == 0 {
            break;
        }
    }

    by_node.into_values().collect()
}
