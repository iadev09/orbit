# orbit-cache - Architecture

`orbit-cache` is a framework-neutral, fleet-coherent byte cache built on
`orbit-rs`. Each process opens one physical cache connection and may register
multiple named logical stores on it. Every store keeps the values it serves in
an independent bounded local L1. Orbit rings distribute addressed mutations
and temporarily carry newly stored value bytes between peers.

## Role

The crate owns:

- the `Put`, `Delete`, and `Reset` mutation protocol,
- a dedicated notified mutation ring,
- a separate addressable payload ring,
- caller-owned mutation cursors,
- named logical stores with independent bounded process-local L1 state,
- detection of mutation lag and overwritten payloads.

It does not own application configuration, provider registration,
application-specific value envelopes, concrete backing-store drivers, a
durable backing store, or an async runtime. Embedders drive `Cache::poll`
after waiting on the mutation ring's readiness fd, or on a fallback timer
where native readiness is unavailable.

The cache is process-local data with fleet-wide coherence. Shared memory is a
bounded transport, not the authoritative cache heap:

```text
authoritative source / optional per-store L2
                    |
                    v
       named process-local L1 <--- cache mutations --- sibling processes
```

A process exit discards that process' L1. A new process starts empty and
subscribes to future mutations. Retained ring history can be replayed for
tests and diagnostics, but it is not a complete snapshot contract.

## Two Rings, One Order

Every cache mutation is published to the mutation ring. `Put` carries a
`PayloadRef`; `Delete` and `Reset` carry no value bytes.

```text
payload ring:   [chunk 0][chunk 1]... -> PayloadRef
mutation ring:  Put { store, key, expiry, PayloadRef }
```

The payload ring is not a second event stream and readers never merge its
physical order with the mutation ring. A reader consumes the mutation ring in
semantic revision order and looks up payload chunks directly by their
`NetId64`s. The writer commits the complete payload batch before publishing
`Put`.

Payload frames are deliberately not notification-enabled. Only a committed
mutation can make their bytes relevant to readers.

## Connection and Logical Stores

`Cache` represents one physical connection: one mutation ring, one payload
ring, one cursor, and one readiness source per process. `Cache::open_store`
returns a named `Store`. Store names are part of every mutation address, so
`models/42` and `responses/42` are unrelated entries even though both use the
same physical rings.

Every `Store` owns its own `LocalCache` and may use a different L1 capacity or
backing store in the embedding layer. Opening a name again within the same
process returns a handle to the existing L1; its first registered capacity
wins. The store registry must be established before the inbound driver starts
so configuration is consistent across peers.

One connection means one shared lag boundary. If ring loss makes the missing
mutation's store unknowable, every registered store becomes
`ResyncRequired`; the embedding layer recovers each from its own backing.
Unknown store mutations are reported and ignored rather than being inserted
into another store.

## Local L1

The L1 contains raw bytes plus the revision and optional expiry of each local
entry. It is bounded and non-authoritative. Eviction is always allowed: a
missing L1 entry is a normal miss for a higher layer to recover from its
backing store.

Delete, expiry, and unavailable payloads retain a local missing marker long
enough to prevent an older delayed mutation from resurrecting stale data.
When such metadata is evicted, the L1 advances a conservative revision floor;
older mutations may then be ignored. Ignoring an old mutation can cost a hit,
but must never serve stale bytes.

If a referenced payload has already been overwritten, the key becomes a local
miss and its `(store, key)` address is returned in
`CachePoll::payload_unavailable`. The higher layer may recover that entry from
the store's authoritative source or L2.

If the mutation cursor reports lost or malformed frames, the L1 clears all
values and becomes `ResyncRequired`. A backing-store adapter may call
`Cache::recover_from_backing`: the shared cursor moves to the current lane
heads, every registered L1 becomes an empty coherent cache, and ordinary
misses fall through to the relevant backing store. The recovery boundary also
records the current shared mutation sequence, so an older writer that commits
late cannot repopulate stale bytes. Without backing stores the caller must
leave the cache in
`ResyncRequired` rather than silently accepting an incomplete view.

## Revisions

The mutation ring owns one SHM-backed `AtomicU64` semantic-version allocator
shared by every writer lane. Writers allocate immediately before publishing
the mutation and carry that sequence in `Frame::ver`. The sequence and
mutation `NetId64` form `CacheRevision`:

```text
CacheRevision.sequence
  fleet-wide mutation order; primary last-write-wins key

CacheRevision.mutation_id
  [ring kind | writer node | writer-local ring counter]
  frame identity and deterministic tie-breaker
```

Two writers can use the same lane-local counter because their node fields are
different. The shared sequence, not the raw `NetId64`, establishes which
mutation is newer. A delayed older mutation is ignored even when it becomes
visible after a newer mutation.

This gives every reader the same order across per-node writer lanes without
treating wall-clock time as a unique version. It is a cache convergence order,
not a database transaction sequence. Wall-clock milliseconds are used only
for TTL expiry.

An authoritative backing store may later supply a stronger version. That
boundary belongs in this crate, while concrete storage and application
adapters belong above it.

## Layout

`CacheLayout` declares the two stable ring kinds and their independent
`RingSpec`s. `DefaultCacheLayout` uses one per-node mutation lane and one
per-node payload lane. Applications with a different memory or burst budget
may define another layout, but every fleet peer must use the same layout.

The default wire allocation reserves kind `200` for mutations and kind `201`
for payload chunks. Embedders that compose Orbit with a different fleet-wide
kind registry may provide another `CacheLayout`; every process in that fleet
must use the same allocation.

Values may span consecutive payload slots. Orbit's batch publish primitive
holds one lane reservation across all chunks and exposes the new lane head
only after the complete batch commits. A value larger than the entire payload
lane is rejected explicitly.

The default layout is:

| Surface | Topology | Capacity | Payload per slot |
| --- | --- | ---: | ---: |
| Mutation ring | per node | 1,024 | 1,024 bytes |
| Payload ring | per node | 1,024 | 4,096 bytes |
| Local L1 | per logical store and process | 10,000 entries | variable |

With the default protocol header, store name and key share 983 bytes. For the
seven-byte `default` store name, the largest key is 976 bytes. The hard value
limit is 4 MiB because one value may occupy at most the complete payload lane.
That is a validity bound, not a recommended object size: a 4 MiB write
consumes the lane's entire retained payload window.

Store names and keys must both be non-empty. A store name may use at most 982
of the shared bytes so every accepted store can still address a one-byte key.

Ring capacity is not the number of cache keys. It is the burst window retained
for a reader that has not yet consumed mutations or payload descriptors. The
number of resident keys is bounded independently by the process-local L1.

## Lifecycle and Driving

`Cache::new` creates a connection and positions its cursor after all currently
committed mutations. `Cache::open_store` creates an empty coherent L1.
`Cache::replay_retained` starts at counter zero and replays only the history
still present in the bounded rings.

`Cache::reset_transport` is the owner-controlled boot primitive. It resets both
rings only while peers are quiescent, then realigns the connection cursor and
all registered L1s with the new generation. Runtime cache clearing uses
`Store::reset`, which publishes a store-scoped `Reset` mutation instead.

Publishing through `Store::put`, `delete`, or `reset` applies the mutation to
that publisher store's L1 immediately. Other processes observe it when their
owner drives the shared `Cache::poll`.

On Linux and FreeBSD, `Cache::event_fd` creates a process-local readiness
bridge for the mutation ring. Readiness means only that the ring changed;
consumers drain the fd and then call `Cache::poll`. Notifications may coalesce.
The payload ring has no notification fd. Other targets use a caller-owned
fallback interval.

Clones of `Cache` share one store registry and one cursor. Clones of `Store`
share that store's L1. The embedding runtime should therefore create one
connection per process cache domain and run one inbound poll driver for it,
not one driver per logical store.

## Invariants

- Value bytes are committed before the corresponding `Put` mutation.
- The mutation ring is the only semantic mutation order.
- The shared mutation sequence, not time or `NetId64`, decides last write.
- The payload ring is addressed by `PayloadRef`; it is never cursor-consumed.
- Payload overwrite produces a reported miss, never a mismatched value.
- Cache traffic never shares capacity or readiness with the generic Orbit
  event bus.
- One cache mutation ring means one readiness listener per subscribing
  process, not one listener per key or logical store.
- Mutation identity is `(store, key)`; resets affect only their addressed
  logical store.
- L1 eviction may reduce hit rate but may not make stale bytes observable.
- Lost or malformed mutations disable L1 hits until explicit resynchronization.
- `increment`, `decrement`, and leases are not cache mutations; they belong to
  atomic-state and coordination primitives.
