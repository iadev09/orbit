//! End-to-end Contest arbitration through independent SHM fleet handles.

#![cfg(unix)]

use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use orbit_rs::{
    CONTEST_RING_SPEC, Claim, Contest, ContestRecord, ContestType, Fleet, NodeId, RingTopology,
};

struct SharedSubject;

impl ContestType for SharedSubject {
    const KIND: u8 = 1;
}

fn fresh_name() -> &'static str {
    use std::sync::atomic::{AtomicU64, Ordering};

    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF;
    Box::leak(format!("c{pid:04x}{nonce:08x}{counter:02x}").into_boxed_str())
}

#[test]
fn simultaneous_shm_claims_choose_exactly_one_earliest_holder() {
    assert_eq!(CONTEST_RING_SPEC.topology, RingTopology::SharedOrdered);

    let name = fresh_name();
    let first_fleet =
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("first process fleet handle"));
    let second_fleet =
        Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(1)).expect("second process fleet handle"));
    let cleanup_fleet = first_fleet.clone();
    let barrier = Arc::new(Barrier::new(3));

    let spawn_claim = |fleet: Arc<Fleet>, owner: &'static str, barrier: Arc<Barrier>| {
        std::thread::spawn(move || {
            let contest = Contest::new(fleet);
            barrier.wait();
            contest
                .try_claim_at::<SharedSubject>(
                    "same-subject",
                    owner,
                    Duration::from_secs(30),
                    1_000,
                )
                .expect("SHM contest claim")
        })
    };

    let first = spawn_claim(first_fleet, "node:0", barrier.clone());
    let second = spawn_claim(second_fleet, "node:1", barrier.clone());
    barrier.wait();

    // Keep both returned Claims alive until both writers have observed the
    // ring. Dropping the winning Guard earlier would publish a release and
    // legitimately allow the other contender to become a later winner.
    let first = first.join().expect("first claimant thread");
    let second = second.join().expect("second claimant thread");

    let (winner, follower) = match (&first, &second) {
        (Claim::Claimed(winner), Claim::YieldTo(follower))
        | (Claim::YieldTo(follower), Claim::Claimed(winner)) => (winner, follower),
        _ => panic!("exactly one simultaneous claimant must win"),
    };
    assert_eq!(follower.claim_id, winner.claim_id());
    assert_eq!(winner.claim_id().counter(), 0);

    drop((first, second));
    cleanup_fleet
        .shm_ring::<ContestRecord>()
        .expect("contest ring")
        .unlink()
        .expect("unlink contest ring and lock file");
}
