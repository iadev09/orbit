# orbit-rs

`orbit-rs` is the primitive Orbit crate: a same-host runtime substrate
for recent facts shared by sibling processes.

It provides type-keyed bounded rings, optional POSIX shared-memory backing,
fleet membership, cursor traversal, and native notification primitives.

`orbit-rs` is framework-agnostic. Application lifecycle and runtime
policy belong above this crate.

It is not a database, message broker, service process, global registry,
or durable log.

## Model

```text
Fleet
  process-local handle into a named fleet

OrbitTyped
  Rust type -> stable KIND byte -> ring family

Ring / Frame
  fixed-capacity append surface with bounded payloads

netid64::NetId64
  runtime-bound id carried by every frame

Semantic layers
  orbit-cache, orbit-event, orbit-lock, orbit-metrics
```

Frame identifiers come from the external
[`netid64`](https://github.com/iadev09/netid64) crate. Orbit uses that
runtime-bound identifier format; it does not define the identifier type
itself.

## Fleet

`Fleet` is the per-process handle into Orbit.

```text
Fleet::join(...)
  -> process-local rings

Fleet::join_shm(...)
  -> POSIX shared-memory surfaces visible to sibling processes
```

A fleet has a name, a `NodeId`, an expected fleet size, and shared
surface registries. Every `OrbitTyped::KIND` maps to its own ring and
declares its own `RingSpec { capacity, payload_capacity, topology }`.
Role hierarchy is
outside the crate: master, worker, standalone process, or sibling tool
can all join the same fleet if the embedder gives them compatible
configuration.

On Unix, shared-memory ring names are derived from:

```text
/orbit-{fleet}-{kind}-{uid}
```

## Rings

A ring is one or more fixed-capacity circular append lanes. The topology
defines how writers publish:

- `Shared` uses one lock-free multi-writer reservation counter;
- `PerNode` gives every fleet member an independent counter and disjoint
  slots;
- `SharedOrdered` uses one globally ordered counter, serializes the short
  write section with a process-recoverable kernel lock, and advances its
  head only after the slot commits.

Frames are written into:

```text
counter % capacity
```

`PerNode` is a membership contract: only one active process may publish as
a given `NodeId`. Concurrent tasks inside that process are serialized locally.
An embedder that replaces a process must fully stop the old incarnation before
reusing its node id; workloads that genuinely have multiple writers use
`Shared` topology instead.

`SharedOrdered` is for algorithms whose correctness depends on one total
order. On Unix its advisory lock uses a state-free companion file at
`/tmp/orbit-{fleet}-{kind}-{uid}.lock`; all frame data remains in SHM. If a
writer process dies, the kernel releases the lock. A pre-head partial write
is then reused by the next writer instead of leaving a permanent hole.

The frame shape is:

```text
id: NetId64 | frame kind: u8 | version: u64 | payload: bytes
```

Two kind values are intentionally present:

```text
id.kind()
  the OrbitTyped value family, used to choose the ring

frame.kind
  the message/opcode class inside that ring
```

Readers must tolerate wraparound. If a reader asks for a counter whose
slot has already been overwritten, the frame is missing by design. For a
per-node ring, `capacity` is the retained message count per node lane.

## Typed Rings

`OrbitTyped` is the type-to-ring contract. A Rust type declares a stable
`KIND`, and that `KIND` selects the ring family used for its frames.

```rust
use orbit_rs::{OrbitTyped, RingSpec};

#[repr(C)]
#[derive(Clone, Copy)]
struct WorkerLoad {
    busy: u32,
    idle: u32,
}

impl OrbitTyped for WorkerLoad {
    const KIND: u8 = 12;
    const RING_SPEC: RingSpec = RingSpec::new(1024, 8);
}
```

Callers do not pass ring names through the API. They pass their own type.
Every worker built from the same program must know the same `KIND` and
`RING_SPEC`, so
`Fleet::publish::<WorkerLoad>` and `Fleet::read_head::<WorkerLoad>` meet
on the same ring.

`OrbitTyped` selects the ring; it does not serialize application values.
Semantic layers encode their own payloads before `Fleet::publish::<T>` and
decode frames returned by the fleet. Cache, event, lock, and metrics therefore
own their wire formats without a primitive-level serializer choice.

## Cache Layer

Cache semantics live in the separate `orbit-cache` crate. It uses Orbit's
generic ring, cursor, shared-version, batch-publication, and notification
primitives to maintain a process-local L1 through dedicated mutation and
addressable payload rings.

`orbit-rs` deliberately does not expose a key/value cache API. This keeps the
primitive crate independent of cache eviction, TTL, resynchronization, and
payload-retention policy.

## Event Layer

Topic framing, event cursors, and event bus semantics live in the separate
[`orbit-event`](../orbit-event/README.md) crate. `orbit-rs` exposes the per-node
ring traversal and notification primitives that support it without knowing
about topics or application events.

Run the Linux suite natively on an Apple Silicon development host with:

```sh
docker build --platform linux/arm64 -t orbit-rs-linux-arm64 .
docker run --rm --platform linux/arm64 orbit-rs-linux-arm64
```

On existing native Linux and FreeBSD checkouts, run the platform guard plus
the shared smoke suite with:

```sh
just smoke-linux
just smoke-freebsd
```

Both commands delegate to `just smoke`. Gitea Actions targets the native Rust
runner registered by each Gitea instance and runs that shared recipe on every
push.

Typed dispatch, application lifecycle hooks, acknowledgements, durable replay,
and consumer groups belong above the primitive and raw event layers.

## Dependency Boundary

Application lifecycle, typed runtime adapters, handlers, process
supervision, and product policy should live above `orbit-rs`.
