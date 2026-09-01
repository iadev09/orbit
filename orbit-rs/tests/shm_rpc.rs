//! Cross-process request/reply proof for Orbit RPC.

#![cfg(unix)]

use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::task::{Context, Poll, Wake, Waker};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nix::sys::wait::{WaitStatus, waitpid};
use nix::unistd::{ForkResult, fork};
use orbit_rs::ring_shm::ShmRing;
use orbit_rs::{Fleet, NodeId, OrbitRpc, OrbitRpcClient, OrbitRpcLane, OrbitRpcOutcome, RingSpec};

struct TestRpcLane;

impl OrbitRpcLane for TestRpcLane {
    const REQUEST_RING_KIND: u8 = 223;
    const REQUEST_RING_SPEC: RingSpec = RingSpec::new(16, 256);
    const REPLY_RING_KIND: u8 = 224;
    const REPLY_RING_SPEC: RingSpec = RingSpec::new(16, 128);
}

fn fresh_name() -> &'static str {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid_short = std::process::id() & 0xFFFF;
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before Unix epoch")
        .subsec_nanos();
    let n = COUNTER.fetch_add(1, Ordering::Relaxed) & 0xFF;
    let name = format!("r{pid_short:04x}{nonce:08x}{n:02x}");
    Box::leak(name.into_boxed_str())
}

fn wait_child(pid: nix::unistd::Pid) -> i32 {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        match waitpid(pid, Some(nix::sys::wait::WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::Exited(_, code)) => return code,
            Ok(WaitStatus::Signaled(_, signal, _)) => {
                panic!("child killed by signal {signal:?}");
            }
            Ok(WaitStatus::StillAlive) => {
                if Instant::now() >= deadline {
                    let _ = nix::sys::signal::kill(pid, nix::sys::signal::Signal::SIGKILL);
                    panic!("child timed out");
                }
                thread::sleep(Duration::from_millis(10));
            }
            Ok(other) => panic!("unexpected child status: {other:?}"),
            Err(error) => panic!("waitpid failed: {error}"),
        }
    }
}

fn cleanup(name: &str) {
    for (kind, spec) in [
        (
            TestRpcLane::REQUEST_RING_KIND,
            TestRpcLane::REQUEST_RING_SPEC,
        ),
        (TestRpcLane::REPLY_RING_KIND, TestRpcLane::REPLY_RING_SPEC),
    ] {
        if let Ok(ring) = ShmRing::open_or_create(name, kind, spec) {
            let _ = ring.unlink();
        }
    }
}

struct ThreadWake(thread::Thread);

impl Wake for ThreadWake {
    fn wake(self: Arc<Self>) {
        self.0.unpark();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.unpark();
    }
}

fn block_on_timeout<F: Future>(future: F, timeout: Duration) -> F::Output {
    let deadline = Instant::now() + timeout;
    let waker = Waker::from(Arc::new(ThreadWake(thread::current())));
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);

    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        let now = Instant::now();
        assert!(now < deadline, "RPC future timed out");
        thread::park_timeout(deadline.saturating_duration_since(now));
    }
}

#[test]
fn handler_replies_complete_the_matching_send_futures() {
    let name = fresh_name();
    let parent_fleet =
        Arc::new(Fleet::join_shm_as(name, 3, NodeId::new(0)).expect("parent joins RPC fleet"));
    let parent_rpc = OrbitRpc::<TestRpcLane>::new(parent_fleet);
    parent_rpc.reset_rings().expect("reset RPC rings");
    let client = OrbitRpcClient::new(parent_rpc);

    match unsafe { fork() }.expect("fork") {
        ForkResult::Child => {
            let child_fleet = match Fleet::join_shm_as(name, 3, NodeId::new(1)) {
                Ok(fleet) => Arc::new(fleet),
                Err(_) => std::process::exit(11),
            };
            let rpc = OrbitRpc::<TestRpcLane>::new(child_fleet);
            let mut cursor = rpc.request_cursor_from_start();
            let deadline = Instant::now() + Duration::from_secs(5);
            let mut requests = Vec::new();

            while requests.len() < 3 && Instant::now() < deadline {
                let poll = rpc.poll_requests(&mut cursor);
                if poll.lagged != 0 {
                    std::process::exit(12);
                }
                requests.extend(poll.requests);
                thread::sleep(Duration::from_millis(1));
            }
            if requests.len() != 3
                || requests[0].method != "test.echo"
                || requests[0].payload != "first"
                || requests[1].method != "test.echo"
                || requests[1].payload != "second"
                || requests[2].method != "test.echo"
                || requests[2].payload != "third"
            {
                std::process::exit(13);
            }

            // Reply in reverse order to prove correlation is by request id,
            // not by the order in which callers await their futures.
            if rpc
                .reply(&requests[2], OrbitRpcOutcome::Completed, "reply:third")
                .is_err()
            {
                std::process::exit(14);
            }
            if rpc
                .reply(&requests[1], OrbitRpcOutcome::Completed, "reply:second")
                .is_err()
            {
                std::process::exit(15);
            }
            if rpc
                .reply(&requests[0], OrbitRpcOutcome::Completed, "reply:first")
                .is_err()
            {
                std::process::exit(16);
            }
            std::process::exit(0);
        }
        ForkResult::Parent { child } => {
            let stop = Arc::new(AtomicBool::new(false));
            let poller_stop = stop.clone();
            let poller_client = client.clone();
            let poller = thread::spawn(move || {
                while !poller_stop.load(Ordering::Acquire) {
                    poller_client.poll_replies();
                    thread::sleep(Duration::from_millis(1));
                }
            });

            let first = client
                .start(NodeId::new(1), "test.echo", "first")
                .expect("publish first request");
            let second = client
                .start(NodeId::new(1), "test.echo", "second")
                .expect("publish second request");
            assert_eq!(client.pending_len(), 2);

            // Knowing the correlation id is not enough: only the node
            // addressed by the request may complete the call.
            let rogue_fleet = Arc::new(
                Fleet::join_shm_as(name, 3, NodeId::new(2)).expect("rogue joins RPC fleet"),
            );
            OrbitRpc::<TestRpcLane>::new(rogue_fleet)
                .publish_reply(
                    first.request_id(),
                    OrbitRpcOutcome::Completed,
                    "reply:rogue",
                )
                .expect("publish reply from wrong node");
            let sending_client = client.clone();

            let (first_reply, second_reply, third_reply) = block_on_timeout(
                async move {
                    let third_reply = sending_client
                        .send(NodeId::new(1), "test.echo", "third")
                        .await
                        .expect("send third request");
                    (first.await, second.await, third_reply)
                },
                Duration::from_secs(5),
            );

            stop.store(true, Ordering::Release);
            poller.join().expect("reply poller joins");
            let child_code = wait_child(child);
            cleanup(name);

            assert_eq!(child_code, 0, "child reported failure");
            assert_eq!(first_reply.outcome, OrbitRpcOutcome::Completed);
            assert_eq!(first_reply.payload, "reply:first");
            assert_eq!(first_reply.from, NodeId::new(1));
            assert_eq!(second_reply.outcome, OrbitRpcOutcome::Completed);
            assert_eq!(second_reply.payload, "reply:second");
            assert_eq!(third_reply.outcome, OrbitRpcOutcome::Completed);
            assert_eq!(third_reply.payload, "reply:third");
            assert_eq!(client.pending_len(), 0);
        }
    }
}
