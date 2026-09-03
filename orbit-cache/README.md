# orbit-cache

`orbit-cache` keeps named, bounded byte-cache stores in every process and
propagates cache mutations between sibling processes through one shared Orbit
cache connection.

It is intended for a same-host worker fleet where every worker should serve
hot reads from its own RAM while observing `Put`, `Delete`, and `Reset`
operations made by its peers.

```text
worker 0: models L1 + responses L1 --\
worker 1: models L1 + responses L1 ---- one mutation ring + payload ring
worker 2: models L1 + responses L1 --/
```

The rings are transport. Values served by reads live in the process-local L1;
the payload ring only retains newly published bytes long enough for peers to
copy them into their own L1.

## What It Provides

- a bounded process-local LRU cache;
- multiple named stores with independent process-local L1s;
- fleet-wide `Put`, `Delete`, and `Reset` propagation;
- values split across consecutive payload slots;
- deterministic last-write-wins ordering across writer lanes;
- TTL expiry at the local read boundary;
- explicit detection of mutation loss, malformed frames, and overwritten
  payloads;
- native mutation readiness on Linux and FreeBSD.

It does not provide persistence, serialization, concrete backing-store
drivers, an async runtime, or an authoritative source of truth. It also does
not implement increment/decrement or leases; those belong to Orbit state and
coordination primitives.

## Basic Use

Every fleet process joins with a distinct `NodeId` and constructs the same
cache layout:

```rust
use std::sync::Arc;
use std::time::Duration;

use orbit_cache::{Cache, CacheRead};
use orbit_rs::{Fleet, NodeId};

let fleet = Arc::new(Fleet::join_shm_as(
    "clv",
    3,
    NodeId::new(1),
)?);
let cache = Cache::new(fleet)?;
let store = cache.open_default_store()?;

store.put(b"user:42", b"encoded value", Some(Duration::from_secs(60)))?;

match store.read(b"user:42") {
    CacheRead::Hit(entry) => assert_eq!(&entry.value[..], b"encoded value"),
    CacheRead::Miss => { /* recover from the authoritative source or L2 */ }
    CacheRead::ResyncRequired => { /* rebuild local coherence */ }
}

# Ok::<(), Box<dyn std::error::Error>>(())
```

The publisher updates that store's L1 immediately. Each sibling process must
drive the connection's `cache.poll()` once to dispatch remote mutations to all
registered stores:

```rust
let result = cache.poll();

if result.resync_required {
    // Only when ordinary misses can fall through to a backing store:
    cache.recover_from_backing();
}

for address in result.payload_unavailable {
    // This value left the bounded payload window before it was copied.
    // Recover (address.store, address.key) from that store's backing.
}
```

On Linux and FreeBSD, `cache.event_fd()` can be registered with the runtime's
native I/O driver. Drain it when readable, then call `cache.poll()`. The fd is
a readiness signal, not the cache data itself, and several publications may
coalesce into one wakeup. On other targets, call `poll()` from a bounded
fallback interval.

## Storage and Ordering

A `Put` is committed in this order:

```text
1. Split and publish value bytes to the payload ring.
2. Allocate a fleet-wide mutation sequence.
3. Publish Put { store, key, TTL, PayloadRef } to the mutation ring.
4. Notify mutation-ring listeners.
```

`CacheRevision.sequence` comes from one SHM-backed `AtomicU64` shared by all
mutation writer lanes. It decides which mutation is newer. The accompanying
`NetId64` identifies the frame through its ring kind, writer node, and
writer-local counter; it is not the fleet-wide time order.

Readers use exact payload frame ids and validate every chunk. If any referenced
slot has wrapped, the cache reports the key as unavailable instead of reading
the replacement frame as though it were the requested value.

## Default Limits

| Resource | Default |
| --- | ---: |
| Local L1 | 10,000 entries per logical store and process |
| Mutation ring kind | 200 |
| Mutation retention | 1,024 frames per node lane |
| Mutation slot payload | 1,024 bytes |
| Payload ring kind | 201 |
| Payload retention | 1,024 frames per node lane |
| Payload slot bytes | 4,096 bytes |
| Store name + key | 983 bytes |
| Maximum store name | 982 bytes |
| Maximum key in `default` store | 976 bytes |
| Maximum value | 4 MiB |

The 4 MiB value limit is the complete payload lane, not a recommended regular
entry size. One value of that size replaces the entire payload retention
window for its writer. Choose ring dimensions for expected value size,
publication rate, reader latency, and fleet size.

Ring capacity is not cache key capacity. The ring bounds how far a consumer may
lag behind; each store's process-local L1 independently bounds how many keys
remain hot. Store names are encoded separately from keys, so the same key may
hold different values in different logical stores.

## Custom Layout

Applications may define a different pair of ring kinds and sizes:

```rust
use orbit_cache::CacheLayout;
use orbit_rs::RingSpec;

struct AppCacheLayout;

impl CacheLayout for AppCacheLayout {
    const MUTATION_RING_KIND: u8 = 230;
    const MUTATION_RING_SPEC: RingSpec = RingSpec::per_node(2_048, 512);
    const PAYLOAD_RING_KIND: u8 = 231;
    const PAYLOAD_RING_SPEC: RingSpec = RingSpec::per_node(4_096, 8_192);
}
```

Ring kinds and specs are wire contracts. Every process joining the same cache
domain must use identical values, and kind numbers must not collide with other
Orbit types in that fleet.

## Benchmark

The SHM benchmark runs four independent writer lanes against the same key. It
measures publication throughput, then verifies outside the timed section that
all fleet-wide sequences are unique and that an observer L1 converges on the
value with the greatest sequence. Writer threads use distinct `Fleet` handles
and `NodeId` lanes; process startup is not part of the measurement.

```sh
cargo bench -p orbit-cache --bench multi_writer
```
