# orbit-event

`orbit-event` is Orbit's fleet-shared topic event stream. It builds event
semantics on top of the rings, cursors, and notification primitives exposed by
`orbit-rs`.

One event ring is shared by all topics. Each fleet node owns one writer lane,
and every subscriber owns an independent cursor. Events remain available only
while they are inside the ring's retained window.

```rust
use std::sync::Arc;

use orbit_event::OrbitEventBus;
use orbit_rs::Fleet;

let fleet = Arc::new(Fleet::join("example", 1)?);
let bus = OrbitEventBus::new(fleet);
let mut cursor = bus.cursor_at_head();

bus.publish("worker.ready", b"worker-1")?;

let poll = bus.poll(&mut cursor);
assert_eq!(poll.events[0].topic, "worker.ready");
assert_eq!(poll.events[0].payload, b"worker-1");
# Ok::<(), Box<dyn std::error::Error>>(())
```

## Semantics

An event frame contains a topic, opaque payload bytes, and a millisecond
timestamp. The bus does not select a serializer or understand application
event types.

Polling advances the subscriber's cursor across all observed frames. A topic
filter changes which events are returned, not which frames the cursor consumes.
If a subscriber falls behind the fixed ring window, `OrbitEventPoll::lagged`
reports the lost counters.

This crate provides:

- `OrbitEventBus` for publishing and polling;
- `OrbitEventCursor` for subscriber-owned progress;
- `OrbitEvent` and `OrbitEventPoll` for decoded raw events;
- the stable event ring kind, layout, and payload limit.

It does not provide acknowledgement, durable replay, consumer groups, typed
application dispatch, or network transport.

## Readiness

On Linux and FreeBSD, an SHM-backed bus can create a process-local
`RingEventFd`. A publish commits the frame before notifying the shared ring
generation. Readiness may coalesce several publishes, so consumers drain the fd
and then poll the ring; the fd itself carries no event data.

Other targets use caller-owned polling.

## Crate Boundary

`orbit-rs` owns fleet membership, ring storage, cursor traversal, loss
accounting, and native notification. `orbit-event` owns topic framing and event
stream behavior. Runtime lifecycle bridges and typed codecs belong in the
embedding application or framework.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the wire and ownership contracts.
