use std::sync::Arc;
use std::time::Duration;

use orbit_rs::{CACHE_PAYLOAD_MAX, Fleet, OrbitCache, OrbitCacheRead};

#[test]
fn cache_put_get_roundtrip() {
    let fleet = Arc::new(Fleet::join("cache", 1).unwrap());
    let cache = OrbitCache::new(fleet);

    cache.put("answer", b"forty-two", None).unwrap();

    assert_eq!(cache.get("answer").as_deref(), Some(&b"forty-two"[..]));
}

#[test]
fn newest_put_wins() {
    let fleet = Arc::new(Fleet::join("cache", 1).unwrap());
    let cache = OrbitCache::new(fleet);

    cache.put("k", b"old", None).unwrap();
    cache.put("k", b"new", None).unwrap();

    assert_eq!(cache.get("k").as_deref(), Some(&b"new"[..]));
}

#[test]
fn delete_shadows_older_put() {
    let fleet = Arc::new(Fleet::join("cache", 1).unwrap());
    let cache = OrbitCache::new(fleet);

    cache.put("k", b"value", None).unwrap();
    let delete_id = cache.delete("k").unwrap();

    assert_eq!(cache.get("k"), None);
    assert_eq!(cache.get_entry("k").epoch(), Some(delete_id.counter()));
}

#[test]
fn prefixes_isolate_keys() {
    let fleet = Arc::new(Fleet::join("cache", 1).unwrap());
    let a = OrbitCache::with_prefix(fleet.clone(), "a:");
    let b = OrbitCache::with_prefix(fleet, "b:");

    a.put("shared", b"A", None).unwrap();
    b.put("shared", b"B", None).unwrap();

    assert_eq!(a.get("shared").as_deref(), Some(&b"A"[..]));
    assert_eq!(b.get("shared").as_deref(), Some(&b"B"[..]));
}

#[test]
fn ttl_expiry_is_a_miss() {
    let fleet = Arc::new(Fleet::join("cache", 1).unwrap());
    let cache = OrbitCache::new(fleet);

    cache
        .put("short", b"value", Some(Duration::from_millis(1)))
        .unwrap();
    std::thread::sleep(Duration::from_millis(3));

    assert!(matches!(
        cache.get_entry("short"),
        OrbitCacheRead::Miss { epoch: Some(_) }
    ));
}

#[test]
fn oversized_frame_is_rejected() {
    let fleet = Arc::new(Fleet::join("cache", 1).unwrap());
    let cache = OrbitCache::new(fleet);
    let value = vec![b'x'; CACHE_PAYLOAD_MAX];

    assert!(cache.put("too-large", &value, None).is_err());
}
