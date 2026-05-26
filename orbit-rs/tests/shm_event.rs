//! Cross-process OrbitEventBus via Fleet::join_shm.
//!
//! This is the smallest proof that Orbit events are real cross-process
//! pulses: one process publishes a topic/payload event into the SHM
//! ring, another process advances its own cursor and sees it.

#![cfg(unix)]

use std::sync::Arc;
use std::time::Duration;

use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};
use orbit_rs::ring_shm::ShmRing;
use orbit_rs::{EVENT_RING_KIND, Fleet, NodeId, OrbitEventBus};

fn fresh_name() -> &'static str {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let pid_short = std::process::id() & 0xFFFF;
    let s = format!("ev{pid_short:04x}{n}");
    Box::leak(s.into_boxed_str())
}

fn wait_child(pid: nix::unistd::Pid) -> i32 {
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => return code,
            Ok(WaitStatus::Signaled(_, sig, _)) => {
                panic!("child killed by signal {:?}", sig);
            }
            Ok(WaitStatus::StillAlive) => {
                if std::time::Instant::now() >= deadline {
                    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL);
                    panic!("child timed out");
                }
                std::thread::sleep(Duration::from_millis(20));
            }
            Ok(other) => panic!("unexpected child status: {:?}", other),
            Err(e) => panic!("waitpid failed: {e}"),
        }
    }
}

fn cleanup_event_ring(name: &str) {
    if let Ok(ring) = ShmRing::open_or_create(name, EVENT_RING_KIND, 16) {
        let _ = ring.unlink();
    }
}

#[test]
fn child_publishes_parent_polls_event() {
    let name = fresh_name();
    let parent_fleet =
        Arc::new(Fleet::join_shm_as(name, 2, 16, NodeId::new(0)).expect("parent join_shm"));
    let parent_bus = OrbitEventBus::new(parent_fleet);
    let mut cursor = parent_bus.cursor_at_head();

    match unsafe { fork() }.expect("fork") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            assert_eq!(code, 0, "child reported failure (exit {code})");

            let poll = parent_bus.poll(&mut cursor);
            cleanup_event_ring(name);

            assert_eq!(poll.lagged, 0);
            assert_eq!(poll.events.len(), 1);
            let event = &poll.events[0];
            println!(
                "parent saw OrbitEvent id={} topic={} payload={}",
                event.id,
                event.topic,
                String::from_utf8_lossy(&event.payload)
            );
            assert_eq!(event.id.node(), 1);
            assert_eq!(event.topic, "test.child_published");
            assert_eq!(event.payload, b"hello-from-child");
        }
        ForkResult::Child => {
            let child_fleet = match Fleet::join_shm_as(name, 2, 16, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(11),
            };
            let child_bus = OrbitEventBus::new(child_fleet);
            if child_bus
                .publish("test.child_published", b"hello-from-child")
                .is_err()
            {
                std::process::exit(12);
            }
            std::process::exit(0);
        }
    }
}

#[test]
fn parent_publishes_child_polls_event() {
    let name = fresh_name();
    let parent_fleet =
        Arc::new(Fleet::join_shm_as(name, 2, 16, NodeId::new(0)).expect("parent join_shm"));
    let parent_bus = OrbitEventBus::new(parent_fleet);
    let id = parent_bus
        .publish("test.parent_published", b"hello-from-parent")
        .expect("publish");

    match unsafe { fork() }.expect("fork") {
        ForkResult::Parent { child } => {
            let code = wait_child(child);
            cleanup_event_ring(name);
            assert_eq!(code, 0, "child reported failure (exit {code})");
            println!("child accepted OrbitEvent id={id} topic=test.parent_published");
        }
        ForkResult::Child => {
            let child_fleet = match Fleet::join_shm_as(name, 2, 16, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(21),
            };
            let child_bus = OrbitEventBus::new(child_fleet);
            let mut cursor = child_bus.cursor_from_start();
            let poll = child_bus.poll(&mut cursor);
            if poll.lagged != 0 || poll.events.len() != 1 {
                std::process::exit(22);
            }
            let event = &poll.events[0];
            if event.topic != "test.parent_published" {
                std::process::exit(23);
            }
            if event.payload != b"hello-from-parent" {
                std::process::exit(24);
            }
            if event.id.node() != 0 {
                std::process::exit(25);
            }
            std::process::exit(0);
        }
    }
}
