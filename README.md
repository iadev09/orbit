# Orbit

Orbit is a Rust workspace for runtime-local, fleet-aware shared-memory
primitives.

The repository is intentionally a small monorepo: the crates live in the
same GitHub project, but publish to crates.io as separate packages.

Repository: <https://github.com/iadev09/orbit>

Orbit is for state that should be visible across sibling processes on
the same host without turning into a database, broker, or application
runtime. It uses bounded ring buffers, stable type ids, and small binary
frames so readers can observe recent facts cheaply and tolerate
overwrite.

The main use case is a multi-process runtime: many workers, one host,
shared operational state. Orbit gives those workers a common substrate
instead of forcing every runtime fact through sockets, files, external
stores, or per-worker scraping.

Typical use cases:

- process fleet heartbeats;
- small runtime cache facts;
- local event streams;
- contest/claim coordination for duplicate work;
- metrics snapshots collected from multiple workers;
- compact counters and lease/lock metadata in adapters above the raw
  rings.

## Crates

- `orbit-rs`: the primitive layer. It owns fleet membership,
  type-keyed rings, POSIX shared-memory backing, cache/event/contest
  substrates, heartbeat records, and cursor/loss accounting.
- `orbit-metrics`: a metrics snapshot layer built on top of `orbit-rs`.
  It is a use-case crate, not part of the primitive core.

`orbit-rs` uses [`netid64`](https://github.com/iadev09/netid64) for
runtime-bound frame identifiers.

## orbit-metrics Use Case

`orbit-metrics` models periodic measurements as compact snapshots over
Orbit rings.

Hot paths should update process-local counters or atomics. A background
publisher captures a compact snapshot and writes it to an Orbit ring.
Collectors then read the ring and keep the newest valid sample per node
or per metric key.

```text
worker-local counters
  -> compact snapshot
  -> OrbitMetricPublisher<T>
  -> orbit-rs ring
  -> OrbitMetricCollector<T>
  -> newest sample per node/key
```

This is useful for worker health, runtime gauges, aggregation decisions,
and dashboards where stale samples should be ignored instead of replayed.

## Layout

```text
Cargo.toml              workspace only; not published as a crate
orbit-rs/               crates.io package: orbit-rs
orbit-metrics/          crates.io package: orbit-metrics
```

`orbit-rs` must remain framework-agnostic. Application lifecycle,
runtime adapters, handlers, and product policy live above the primitive
layer.
