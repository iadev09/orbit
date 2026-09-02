# orbit-rs

`orbit-rs` is the primitive Orbit crate: a same-host runtime substrate
for recent facts shared by sibling processes.

It provides type-keyed bounded rings, a bounded current-state table for
Contest leases, optional POSIX shared-memory backing, fleet membership,
and small reusable substrates for cache, events, and RPC.

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

Contest state
  fixed-capacity current lease table keyed by typed subject

netid64::NetId64
  runtime-bound id carried by every frame

Substrates
  cache, event bus, RPC, contest, typed POD values
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
Contest uses one separate fleet-scoped state surface. Role hierarchy is
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

`Orbital<T>` is the fixed-size value helper:

```text
T: OrbitTyped + bytemuck::Pod
  -> Orbital<T>::store(value)
  -> Fleet::publish::<T>
  -> ring selected by T::KIND
  -> Orbital<T>::load()
  -> T
```

This path is for small structs whose byte layout is known and stable.
Variable-length records use explicit encoders in layers such as cache,
event, contest, and metrics.

## Cache

`OrbitCache` is a byte-oriented cache primitive over one dedicated ring.
It does not choose a serializer and it does not know application values.

Each mutation is one frame:

```text
put    key bytes, value bytes, optional expiry
delete key bytes
reset  prefix bytes
```

Reads walk backward from the ring head. The newest matching frame wins:

- `put` returns a value unless expired;
- `delete` shadows older puts;
- `reset` shadows older entries inside the cache prefix.

Values are inline and bounded by the ring payload size. Larger object
caches should be built above this primitive, for example with a ring as
mutation log plus a separate shared arena.

## Event Bus

`OrbitEventBus` is an append-only event stream over one Orbit SHM segment
with one writer lane per fleet node.
Events are not cache entries and not metrics snapshots:

```text
cache   asks: what is the newest value for this key?
metrics asks: what is the newest sample for each node/key?
events  ask: which frames appeared since my cursor?
```

All topics deliberately share the same segment. The topic is carried
inside the frame payload, so adding a new event type does not allocate
another shared-memory segment. Subscribers keep one cursor per node lane;
there is no total order across producing nodes.

Each subscriber owns its own cursor. Polling advances that cursor across
all frames, including frames later filtered out by topic. If a subscriber
falls behind the fixed ring window, the poll result reports lag.

```text
OrbitEventBus::publish(topic, payload)
  -> topic/payload/timestamp frame
  -> publisher's node lane
  -> OrbitEventCursor
  -> poll() / poll_topic()
```

On Linux and FreeBSD, an SHM-backed event bus can also create a process-local
`RingEventFd`. A committed publish increments a shared generation and wakes
every listener through Linux futex or FreeBSD umtx. Each listener converts that
broadcast into readiness on its own native eventfd, which can be registered
with epoll/kqueue or an async runtime. The fd contains no event data and is not
a delivery counter: consumers drain it, then bulk-poll the ring with their own
cursor. Several publishes may coalesce into one readiness wake without losing
resident ring frames.

The eventfd is deliberately not shared between fleet processes. Eventfd reads
consume its counter, which would turn a shared descriptor into load-balancing
rather than broadcast delivery. Other Unix targets continue to use
caller-owned polling until they gain a native notification backend.

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

Typed dispatch, application lifecycle hooks, acknowledgements, durable
replay, and consumer groups belong above this primitive.

## Contest

`Contest` is not a race primitive. It turns simultaneous interest in the
same typed subject into a small claim/yield protocol.

```text
claim typed subject
  -> hash directly to its current state slot
  -> free/expired subject receives Guard
  -> active subject returns YieldTo(holder)
  -> renew updates the same slot
  -> dropping Guard tombstones the matching slot
```

Contest uses one fixed-capacity, open-addressed current-state table. A
claim, renewal, or release takes a short process-recoverable lock and
touches the subject's probe chain; the lock is not held while the winner
performs guarded work. Operations do not scan or reconstruct ring history.

`CONTEST_STATE_CAPACITY` is the maximum number of simultaneously resident
subjects, not a history length. Renewing a long-lived lease keeps the same
claim id and fencing token even if arbitrarily many unrelated subjects are
claimed and released. A full table fails explicitly instead of evicting a
live lease.

A subject is a caller-defined `ContestType::KIND` plus a label. The
owner label is only for observation; Orbit does not interpret it.

The important bias is that followers yield. Orbit does not serialize a
queue or make the holder faster. It lets one peer carry a typed subject
while others can observe who carries it and back off.

TTL handles abandoned claims. Releasing is tied to the `Guard` lifetime,
so normal Rust scope becomes the release boundary for successful work.

## RPC

`rpc::Lane` declares one request segment and one reply segment for an RPC
domain. Each segment contains one writer lane per fleet node. Individual
request and reply messages are carried as method plus payload bytes; they
do not implement `OrbitTyped` and do not allocate their own SHM segments.

`rpc::Client::send(...).await` completes from the reply correlated by
the request frame's `NetId64` and the addressed node. Orbit does not
start a hidden thread or choose an async runtime: the embedding runtime
owns one reply-polling task and drives `rpc::Client::poll_replies`.
Handler registration, codecs, timeouts, retries, and authorization
remain adapter policy.

## Dependency Boundary

Application lifecycle, typed runtime adapters, handlers, process
supervision, and product policy should live above `orbit-rs`.
