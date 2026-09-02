//! End-to-end Contest arbitration through independent SHM fleet handles.

#![cfg(unix)]

use std::sync::{Arc, Barrier};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};
use orbit_rs::{CONTEST_STATE_CAPACITY, Claim, Contest, ContestType, Fleet, NodeId};

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

fn wait_child(pid: nix::unistd::Pid) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => return code,
            Ok(WaitStatus::Signaled(_, signal, _)) => {
                panic!("child killed by signal {signal:?}");
            }
            Ok(WaitStatus::StillAlive) => {
                if std::time::Instant::now() >= deadline {
                    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL);
                    panic!("child timed out");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(other) => panic!("unexpected child status: {other:?}"),
            Err(error) => panic!("waitpid failed: {error}"),
        }
    }
}

#[test]
fn simultaneous_shm_claims_choose_exactly_one_earliest_holder() {
    assert_eq!(CONTEST_STATE_CAPACITY, 1024);

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

    // Keep both returned Claims alive until both callers have observed the
    // state. Dropping the winning Guard earlier would release the slot and
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
    Contest::new(cleanup_fleet)
        .unlink()
        .expect("unlink contest state and lock file");
}

#[test]
fn child_process_observes_parent_holder_without_ring_replay() {
    let name = fresh_name();
    let parent_fleet = Arc::new(Fleet::join_shm_as(name, 2, NodeId::new(0)).expect("parent fleet"));
    let parent_contest = Contest::new(parent_fleet);
    let Claim::Claimed(parent_guard) = parent_contest
        .try_claim_at::<SharedSubject>(
            "cross-process-subject",
            "parent",
            Duration::from_secs(30),
            1_000,
        )
        .expect("parent claim")
    else {
        panic!("parent must claim fresh subject");
    };
    let parent_claim_id = parent_guard.claim_id();

    match unsafe { fork() }.expect("fork failed") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            drop(parent_guard);
            parent_contest.unlink().expect("unlink contest state");
            assert_eq!(code, 0, "child reported failure (exit code {code})");
        }
        ForkResult::Child => {
            let child_fleet = match Fleet::join_shm_as(name, 2, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(11),
            };
            let child_contest = Contest::new(child_fleet);
            match child_contest.try_claim_at::<SharedSubject>(
                "cross-process-subject",
                "child",
                Duration::from_secs(30),
                1_001,
            ) {
                Ok(Claim::YieldTo(holder))
                    if holder.claim_id == parent_claim_id && holder.owner.as_str() == "parent" =>
                {
                    std::process::exit(0);
                }
                Ok(_) => std::process::exit(12),
                Err(_) => std::process::exit(13),
            }
        }
    }
}
