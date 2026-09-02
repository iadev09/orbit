use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use orbit_cache::{
    Cache, CacheLayout, CacheMutation, CacheRead, CacheTransport, Error, PayloadRef,
};
use orbit_rs::{Fleet, RingSpec};

struct TinyLayout;

impl CacheLayout for TinyLayout {
    const MUTATION_RING_KIND: u8 = 40;
    const MUTATION_RING_SPEC: RingSpec = RingSpec::per_node(8, 128);
    const PAYLOAD_RING_KIND: u8 = 41;
    const PAYLOAD_RING_SPEC: RingSpec = RingSpec::per_node(4, 4);
}

fn capacity(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("test cache capacity is non-zero")
}

#[test]
fn put_payload_spans_slots_and_peer_applies_the_descriptor() {
    let fleet = Arc::new(Fleet::join("cache_chunks", 1).expect("fleet"));
    let publisher = Cache::<TinyLayout>::new(fleet.clone(), capacity(8)).expect("publisher");
    let peer = Cache::<TinyLayout>::new(fleet, capacity(8)).expect("peer");

    publisher
        .put(b"greeting", b"hello-world", None)
        .expect("put");
    let poll = peer.poll();

    assert_eq!(poll.observed, 1);
    assert_eq!(poll.applied, 1);
    assert!(poll.payload_unavailable.is_empty());
    let CacheRead::Hit(entry) = peer.read(b"greeting") else {
        panic!("peer must install the put in its local L1");
    };
    assert_eq!(&entry.value[..], b"hello-world");
}

#[test]
fn mutation_contains_a_payload_reference_not_the_value_bytes() {
    let fleet = Arc::new(Fleet::join("cache_ref", 1).expect("fleet"));
    let transport = CacheTransport::<TinyLayout>::new(fleet.clone()).expect("transport");
    let mut cursor = transport.cursor_at_head();

    transport
        .publish_put(b"key", b"eight888", None)
        .expect("put");
    let poll = transport.poll(&mut cursor);

    let [CacheMutation::Put { payload, .. }] = poll.mutations.as_slice() else {
        panic!("one put descriptor expected");
    };
    assert_eq!(payload.chunk_count, 2);
    assert_eq!(payload.value_len, 8);
    assert_eq!(
        transport.read_payload(*payload).as_deref(),
        Some(&b"eight888"[..])
    );
}

#[test]
fn delete_and_reset_propagate_through_the_mutation_ring() {
    let fleet = Arc::new(Fleet::join("cache_delete", 1).expect("fleet"));
    let publisher = Cache::<TinyLayout>::new(fleet.clone(), capacity(8)).expect("publisher");
    let peer = Cache::<TinyLayout>::new(fleet, capacity(8)).expect("peer");

    publisher.put(b"a", b"A", None).expect("put a");
    publisher.put(b"b", b"B", None).expect("put b");
    peer.poll();
    assert!(matches!(peer.read(b"a"), CacheRead::Hit(_)));
    assert!(matches!(peer.read(b"b"), CacheRead::Hit(_)));

    publisher.delete(b"a").expect("delete a");
    peer.poll();
    assert_eq!(peer.read(b"a"), CacheRead::Miss);
    assert!(matches!(peer.read(b"b"), CacheRead::Hit(_)));

    publisher.reset().expect("reset");
    peer.poll();
    assert_eq!(peer.read(b"a"), CacheRead::Miss);
    assert_eq!(peer.read(b"b"), CacheRead::Miss);
}

#[test]
fn overwritten_payload_is_reported_and_never_misread() {
    let fleet = Arc::new(Fleet::join("cache_payload_wrap", 1).expect("fleet"));
    let publisher = Cache::<TinyLayout>::new(fleet.clone(), capacity(8)).expect("publisher");
    let peer = Cache::<TinyLayout>::new(fleet, capacity(8)).expect("peer");

    publisher.put(b"old", b"abcdefgh", None).expect("old put");
    publisher.put(b"new-1", b"1111", None).expect("new put 1");
    publisher.put(b"new-2", b"2222", None).expect("new put 2");
    publisher.put(b"new-3", b"3333", None).expect("new put 3");

    let poll = peer.poll();
    assert_eq!(
        poll.payload_unavailable,
        vec![bytes::Bytes::from_static(b"old")]
    );
    assert_eq!(peer.read(b"old"), CacheRead::Miss);
    assert!(matches!(peer.read(b"new-3"), CacheRead::Hit(_)));
}

#[test]
fn mutation_lag_disables_local_hits_until_resync() {
    let fleet = Arc::new(Fleet::join("cache_mutation_wrap", 1).expect("fleet"));
    let publisher = Cache::<TinyLayout>::new(fleet.clone(), capacity(8)).expect("publisher");
    let peer = Cache::<TinyLayout>::new(fleet, capacity(8)).expect("peer");

    for index in 0..9 {
        publisher
            .delete(format!("key-{index}").as_bytes())
            .expect("delete");
    }
    let poll = peer.poll();

    assert_eq!(poll.loss.overwritten, 1);
    assert!(poll.resync_required);
    assert_eq!(peer.read(b"anything"), CacheRead::ResyncRequired);
}

#[test]
fn backing_recovery_resubscribes_with_an_empty_coherent_l1() {
    let fleet = Arc::new(Fleet::join("cache_recover", 1).expect("fleet"));
    let publisher = Cache::<TinyLayout>::new(fleet.clone(), capacity(8)).expect("publisher");
    let peer = Cache::<TinyLayout>::new(fleet, capacity(8)).expect("peer");

    for index in 0..9 {
        publisher
            .delete(format!("key-{index}").as_bytes())
            .expect("delete");
    }
    assert!(peer.poll().resync_required);

    peer.recover_from_backing();

    assert_eq!(peer.read(b"backing-key"), CacheRead::Miss);
    publisher
        .put(b"future", b"visible", None)
        .expect("future put");
    assert_eq!(peer.poll().applied, 1);
    assert!(matches!(peer.read(b"future"), CacheRead::Hit(_)));
}

#[test]
fn ttl_expiry_becomes_a_local_miss() {
    let fleet = Arc::new(Fleet::join("cache_ttl", 1).expect("fleet"));
    let cache = Cache::<TinyLayout>::new(fleet, capacity(8)).expect("cache");
    cache
        .put(b"short", b"value", Some(Duration::from_millis(1)))
        .expect("put");

    std::thread::sleep(Duration::from_millis(3));
    assert_eq!(cache.read(b"short"), CacheRead::Miss);
}

#[test]
fn value_larger_than_the_payload_lane_is_rejected_before_publication() {
    let fleet = Arc::new(Fleet::join("cache_large", 1).expect("fleet"));
    let transport = CacheTransport::<TinyLayout>::new(fleet).expect("transport");
    let mut cursor = transport.cursor_at_head();

    let error = transport
        .publish_put(b"key", &[0; 17], None)
        .expect_err("value must exceed the four-slot lane");
    assert!(matches!(
        error,
        Error::ValueTooLarge {
            value_len: 17,
            max: 16
        }
    ));
    assert!(transport.poll(&mut cursor).is_empty());
}

#[test]
fn empty_values_still_have_one_addressable_payload_frame() {
    let fleet = Arc::new(Fleet::join("cache_empty", 1).expect("fleet"));
    let transport = CacheTransport::<TinyLayout>::new(fleet).expect("transport");
    let CacheMutation::Put { payload, .. } = transport
        .publish_put(b"empty", b"", None)
        .expect("empty put")
    else {
        panic!("put expected");
    };

    assert_eq!(
        payload,
        PayloadRef {
            first_id: payload.first_id,
            payload_version: payload.payload_version,
            chunk_count: 1,
            value_len: 0,
        }
    );
    assert_eq!(transport.read_payload(payload).as_deref(), Some(&b""[..]));
}
