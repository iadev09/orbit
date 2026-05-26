//! ShmRing smoke tests — V1 substrate, single-process for now.
//!
//! Each test uses a unique fleet name to avoid SHM segment
//! collisions across parallel runs. Stale segments left over from a
//! crashed test can be cleaned manually with
//! `rm /dev/shm/orbit-shmtest-*-$(id -u)` (Linux) or
//! `ls /tmp/orbit-shmtest-*` plus appropriate cleanup on macOS.

#![cfg(unix)]

use bytes::Bytes;
use orbit_rs::NodeId;
use orbit_rs::ring_shm::ShmRing;

/// macOS limits POSIX SHM names to PSHMNAMLEN (31 chars). Keep
/// fleet names short — the full segment name is
/// `/orbit-{fleet}-{kind}-{uid}` and even with a 5-digit UID and a
/// 3-digit kind we only have ~14 chars budget for the fleet name.
fn fresh_name() -> String {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let pid_short = std::process::id() & 0xFFFF;
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("ts{pid_short:04x}{n}")
}

#[test]
fn open_or_create_creates_first_then_attaches() {
    let name = fresh_name();
    let r1 = ShmRing::open_or_create(&name, 7, 16).unwrap();
    assert!(r1.created(), "first opener creates");
    let r2 = ShmRing::open_or_create(&name, 7, 16).unwrap();
    assert!(!r2.created(), "second opener attaches");
    let _ = r1.unlink();
}

#[test]
fn write_then_read_returns_same_frame() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 7, 16).unwrap();
    let id = ring
        .write(NodeId::new(3), 0, 42, Bytes::from_static(b"hello"))
        .unwrap();
    assert_eq!(id.kind(), 7);
    assert_eq!(id.node(), 3);
    assert_eq!(id.counter(), 0);

    let frame = ring.read(id).unwrap();
    assert_eq!(frame.id, id);
    assert_eq!(frame.kind, 0);
    assert_eq!(frame.ver, 42);
    assert_eq!(&frame.payload[..], b"hello");

    let _ = ring.unlink();
}

#[test]
fn head_advances_with_writes() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 5, 16).unwrap();
    assert_eq!(ring.head(), 0);
    ring.write(NodeId::new(0), 0, 0, Bytes::from_static(b"a"))
        .unwrap();
    ring.write(NodeId::new(0), 0, 0, Bytes::from_static(b"b"))
        .unwrap();
    assert_eq!(ring.head(), 2);
    let _ = ring.unlink();
}

#[test]
fn read_head_returns_latest() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 5, 16).unwrap();
    assert!(ring.read_head().is_none());

    ring.write(NodeId::new(0), 0, 0, Bytes::from_static(b"first"))
        .unwrap();
    let last = ring
        .write(NodeId::new(0), 0, 0, Bytes::from_static(b"second"))
        .unwrap();

    let frame = ring.read_head().unwrap();
    assert_eq!(frame.id, last);
    assert_eq!(&frame.payload[..], b"second");
    let _ = ring.unlink();
}

#[test]
fn wraparound_overwrites_old_slots() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 5, 4).unwrap();
    let mut ids = Vec::new();
    for i in 0..6 {
        let id = ring
            .write(NodeId::new(0), 0, 0, Bytes::from(vec![i]))
            .unwrap();
        ids.push(id);
    }
    // First two slots overwritten by writes 4 and 5 (capacity = 4).
    assert!(ring.read(ids[0]).is_none());
    assert!(ring.read(ids[1]).is_none());
    for id in &ids[2..] {
        assert!(ring.read(*id).is_some(), "id {id} should still be live");
    }
    let _ = ring.unlink();
}

#[test]
fn two_handles_same_segment_share_state() {
    let name = fresh_name();
    let writer = ShmRing::open_or_create(&name, 9, 16).unwrap();
    let reader = ShmRing::open_or_create(&name, 9, 16).unwrap();
    assert!(writer.created());
    assert!(!reader.created());

    let id = writer
        .write(NodeId::new(1), 0, 0, Bytes::from_static(b"shared"))
        .unwrap();

    // Reader sees what writer wrote — the whole point of SHM.
    let frame = reader.read(id).unwrap();
    assert_eq!(&frame.payload[..], b"shared");
    let _ = writer.unlink();
}

#[test]
fn payload_too_large_returns_error() {
    let name = fresh_name();
    let ring = ShmRing::open_or_create(&name, 3, 16).unwrap();
    let huge = Bytes::from(vec![0u8; orbit_rs::ring_shm::PAYLOAD_MAX + 1]);
    assert!(ring.write(NodeId::new(0), 0, 0, huge).is_err());
    let _ = ring.unlink();
}
