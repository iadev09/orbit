use std::time::Duration;

use orbit_rs::{Fleet, NodeId, OrbitEpoch};

#[test]
fn heartbeat_keeps_latest_sample_for_node() {
    let fleet = Fleet::join_as("hb", 2, NodeId::new(1)).expect("join fleet");

    let first = fleet.publish_heartbeat_at(OrbitEpoch::from_unix_ms(100));
    let second = fleet.publish_heartbeat_at(OrbitEpoch::from_unix_ms(250));

    assert_eq!(first.node_id, NodeId::new(1));
    assert_eq!(second.node_id, NodeId::new(1));

    let latest = fleet.latest_heartbeats();
    assert_eq!(latest.len(), 1);
    assert_eq!(latest[0].node_id, NodeId::new(1));
    assert_eq!(latest[0].captured_at, OrbitEpoch::from_unix_ms(250));
    assert_eq!(latest[0].counter(), second.counter());
}

#[test]
fn heartbeat_snapshot_marks_missing_and_stale_nodes() {
    let fleet = Fleet::join_as("hb", 3, NodeId::new(2)).expect("join fleet");
    fleet.publish_heartbeat_at(OrbitEpoch::from_unix_ms(1_000));

    let fresh =
        fleet.heartbeat_snapshot_at(OrbitEpoch::from_unix_ms(1_500), Duration::from_millis(750));
    assert_eq!(fresh.missing_node_ids, vec![NodeId::new(0), NodeId::new(1)]);
    assert!(fresh.stale_node_ids.is_empty());
    assert_eq!(
        fresh.latest[0].age_at(fresh.now),
        Duration::from_millis(500)
    );

    let stale =
        fleet.heartbeat_snapshot_at(OrbitEpoch::from_unix_ms(2_000), Duration::from_millis(750));
    assert_eq!(stale.stale_node_ids, vec![NodeId::new(2)]);
}

#[test]
fn orbit_epoch_age_is_saturating() {
    let captured = OrbitEpoch::from_unix_ms(2_000);
    let earlier_now = OrbitEpoch::from_unix_ms(1_000);

    assert_eq!(captured.age_at(earlier_now), Duration::ZERO);
}
