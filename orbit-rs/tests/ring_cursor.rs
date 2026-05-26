use bytes::Bytes;
use orbit_rs::{
    Fleet, Frame, NetId64, NodeId, OrbitTyped, Ring, RingCursor, RingFrameSource, poll_ring,
};

#[derive(Clone, Debug)]
struct CursorRecord;

impl OrbitTyped for CursorRecord {
    const KIND: u8 = 31;
}

#[derive(Clone, Debug)]
struct SmallWindowRecord;

impl OrbitTyped for SmallWindowRecord {
    const KIND: u8 = 32;
}

#[test]
fn cursor_from_start_reads_available_history() {
    let ring = Ring::new::<CursorRecord>(4);
    ring.write(NodeId::ZERO, 0, 0, Bytes::from_static(b"a"));
    ring.write(NodeId::ZERO, 0, 0, Bytes::from_static(b"b"));

    let mut cursor = RingCursor::from_start();
    let poll = poll_ring(&ring, &mut cursor);

    assert_eq!(poll.loss.total(), 0);
    assert_eq!(poll.frames.len(), 2);
    assert_eq!(&poll.frames[0].payload[..], b"a");
    assert_eq!(&poll.frames[1].payload[..], b"b");
    assert_eq!(cursor.next_counter(), 2);
    assert!(poll_ring(&ring, &mut cursor).is_empty());
}

#[test]
fn cursor_at_head_only_reads_future_frames() {
    let fleet = Fleet::join("cursor_head", 1).expect("fleet");
    fleet.publish::<CursorRecord>(0, 0, Bytes::from_static(b"old"));
    let mut cursor = fleet.cursor_at_head::<CursorRecord>();
    fleet.publish::<CursorRecord>(0, 0, Bytes::from_static(b"new"));

    let poll = fleet.poll_ring::<CursorRecord>(&mut cursor);

    assert_eq!(poll.loss.total(), 0);
    assert_eq!(poll.frames.len(), 1);
    assert_eq!(&poll.frames[0].payload[..], b"new");
    assert_eq!(cursor.next_counter(), 2);
}

#[test]
fn lagged_cursor_reports_overwritten_window() {
    let fleet = Fleet::join("cursor_lag", 1).expect("fleet");
    fleet.ring_with_capacity::<SmallWindowRecord>(2);
    let mut cursor = fleet.cursor_from_start::<SmallWindowRecord>();

    fleet.publish::<SmallWindowRecord>(0, 0, Bytes::from_static(b"one"));
    fleet.publish::<SmallWindowRecord>(0, 0, Bytes::from_static(b"two"));
    fleet.publish::<SmallWindowRecord>(0, 0, Bytes::from_static(b"three"));

    let poll = fleet.poll_ring::<SmallWindowRecord>(&mut cursor);

    assert_eq!(poll.loss.overwritten, 1);
    assert_eq!(poll.loss.unavailable, 0);
    assert_eq!(poll.frames.len(), 2);
    assert_eq!(&poll.frames[0].payload[..], b"two");
    assert_eq!(&poll.frames[1].payload[..], b"three");
    assert_eq!(cursor.next_counter(), 3);
}

#[test]
fn missing_or_unexpected_slots_report_unavailable() {
    struct SparseSource {
        frames: Vec<Option<Frame>>,
    }

    impl RingFrameSource for SparseSource {
        fn kind(&self) -> u8 {
            CursorRecord::KIND
        }

        fn head(&self) -> u64 {
            3
        }

        fn capacity(&self) -> usize {
            3
        }

        fn read_at(&self, counter: u64) -> Option<Frame> {
            self.frames.get(counter as usize).cloned().flatten()
        }
    }

    let source = SparseSource {
        frames: vec![
            Some(Frame {
                id: NetId64::make(CursorRecord::KIND, 0, 0),
                kind: 0,
                ver: 0,
                payload: Bytes::from_static(b"ok"),
            }),
            None,
            Some(Frame {
                id: NetId64::make(CursorRecord::KIND, 0, 99),
                kind: 0,
                ver: 0,
                payload: Bytes::from_static(b"wrapped"),
            }),
        ],
    };
    let mut cursor = RingCursor::from_start();

    let poll = poll_ring(&source, &mut cursor);

    assert_eq!(poll.loss.overwritten, 0);
    assert_eq!(poll.loss.unavailable, 2);
    assert_eq!(poll.frames.len(), 1);
    assert_eq!(&poll.frames[0].payload[..], b"ok");
    assert_eq!(cursor.next_counter(), 3);
}
