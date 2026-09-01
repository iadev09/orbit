# orbit-rs

`orbit-rs` is the primitive Orbit crate: a same-host runtime substrate
for recent facts shared by sibling processes.

It provides type-keyed bounded rings, optional POSIX shared-memory
backing, fleet membership, and small reusable substrates for cache,
events, and contest/claim coordination.

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
  -> POSIX shared-memory rings visible to sibling processes
```

A fleet has a name, a `NodeId`, an expected fleet size, and one ring
registry. Every `OrbitTyped::KIND` maps to its own ring and declares its
own `RingSpec { capacity, payload_capacity }`. Role hierarchy
is outside the crate: master, worker, standalone process, or sibling
tool can all join the same fleet if the embedder gives them compatible
configuration.

On Unix, shared-memory ring names are derived from:

```text
/orbit-{fleet}-{kind}-{uid}
```

## Rings

A ring is a fixed-capacity circular append surface. Writers reserve a
monotonic counter, mint a `NetId64`, and write a frame into:

```text
counter % capacity
```

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
slot has already been overwritten, the frame is missing by design.

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

`OrbitEventBus` is an append-only event stream over one Orbit ring.
Events are not cache entries and not metrics snapshots:

```text
cache   asks: what is the newest value for this key?
metrics asks: what is the newest sample for each node/key?
events  ask: which frames appeared since my cursor?
```

All topics deliberately share the same event ring. The topic is carried
inside the frame payload, so adding a new event type does not allocate
another shared-memory segment.

Each subscriber owns its own cursor. Polling advances that cursor across
all frames, including frames later filtered out by topic. If a subscriber
falls behind the fixed ring window, the poll result reports lag.

```text
OrbitEventBus::publish(topic, payload)
  -> topic/payload/timestamp frame
  -> shared event ring
  -> OrbitEventCursor
  -> poll() / poll_topic()
```

Typed dispatch, application lifecycle hooks, acknowledgements, durable
replay, and consumer groups belong above this primitive.

## Contest

`Contest` is not a race primitive. It turns simultaneous interest in the
same typed subject into a small claim/yield protocol.

```text
claim typed subject
  -> observe active claims
  -> earliest active claimant receives Guard
  -> later claimants receive YieldTo(holder)
  -> dropping Guard publishes release
```

Every peer may publish a claim. The earliest active claim receives a
drop-released `Guard`; later claimants receive `YieldTo(holder)`.

A subject is a caller-defined `ContestType::KIND` plus a label. The
owner label is only for observation; Orbit does not interpret it.

The important bias is that followers yield. Orbit does not serialize a
queue or make the holder faster. It lets one peer carry a typed subject
while others can observe who carries it and back off.

TTL handles abandoned claims. Releasing is tied to the `Guard` lifetime,
so normal Rust scope becomes the release boundary for successful work.

## RPC

`rpc::Lane` declares one request ring and one reply ring for an RPC
domain. Individual request and reply messages are carried as method plus
payload bytes; they do not implement `OrbitTyped` and do not allocate
their own SHM segments.

`rpc::Client::send(...).await` completes from the reply correlated by
the request frame's `NetId64` and the addressed node. Orbit does not
start a hidden thread or choose an async runtime: the embedding runtime
owns one reply-polling task and drives `rpc::Client::poll_replies`.
Handler registration, codecs, timeouts, retries, and authorization
remain adapter policy.

## Dependency Boundary

Application lifecycle, typed runtime adapters, handlers, process
supervision, and product policy should live above `orbit-rs`.
