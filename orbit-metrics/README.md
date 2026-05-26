# orbit-metrics

`orbit-metrics` provides metrics snapshot collection on top of
`orbit-rs` rings.

It is for periodic runtime measurements where readers usually want the
newest valid sample, not a replayable history.

The intended shape is a multi-worker runtime where workers should not
each expose their own metrics endpoint. Workers publish compact snapshots
into Orbit; an aggregator folds fresh samples into one visible view.

## Model

Hot paths should update process-local counters, gauges, atomics, or
small structs. A background task captures those values into a compact
snapshot and publishes the snapshot into an Orbit ring.

Collectors walk the ring backwards and keep the newest decodable sample
per node or per logical metric key.

```text
local counters/gauges
  -> compact snapshot
  -> OrbitMetricPublisher<T>
  -> orbit-rs ring for T::KIND
  -> OrbitMetricCollector<T>
  -> latest_by_node() / latest_by_key()
```

This keeps the hot path local and cheap. Orbit is only touched by the
publisher task that captures a bounded snapshot.

## Core Types

```text
OrbitMetricSnapshot
  Trait implemented by a compact metrics record.

OrbitMetricKeyedSnapshot
  Optional trait for row-like metric families with a logical key.

OrbitMetricFamily<T>
  Typed metrics family bound to an orbit-rs Fleet.

OrbitMetricPublisher<T>
  Writes encoded snapshots into the ring.

OrbitMetricCollector<T>
  Reads newest valid samples from the ring.

OrbitMetricSample<T>
  Decoded snapshot plus the NetId64 that carried it.
```

## Semantics

- newest valid sample per node wins;
- newest valid sample per metric key wins for keyed families;
- malformed old frames are ignored by collectors;
- stale samples can be filtered by timestamp;
- aggregation is left to the caller.

This crate does not sum counters, render Prometheus output, choose a
serialization format, supervise workers, or define application policy.

## Aggregation Pattern

Use one metric family per transport shape.

Fixed-width scalar families can usually be encoded as one compact
snapshot per worker. Dynamic or labeled rows should use a keyed metric
family so collectors can keep the newest row per key.

```text
multi-worker mode:
  worker local state
    -> OrbitMetricPublisher<T>
    -> aggregator collector
    -> merged output

standalone mode:
  local state
    -> local output
```

The point is not to make metrics durable. The point is to expose current
runtime state without requiring external systems to scrape every worker
process.

## Example Shape

```rust
use orbit_metrics::OrbitTyped;
use orbit_metrics::OrbitMetricSnapshot;

struct WorkerSnapshot {
    node: u16,
    captured_at: u64,
    requests: u64,
}

impl OrbitTyped for WorkerSnapshot {
    const KIND: u8 = 42;
}

impl OrbitMetricSnapshot for WorkerSnapshot {
    const FAMILY: &'static str = "worker";

    fn node_id(&self) -> u16 {
        self.node
    }

    fn captured_at_unix_secs(&self) -> u64 {
        self.captured_at
    }

    fn encode(&self) -> Result<Vec<u8>, String> {
        let mut out = Vec::with_capacity(18);
        out.extend_from_slice(&self.node.to_le_bytes());
        out.extend_from_slice(&self.captured_at.to_le_bytes());
        out.extend_from_slice(&self.requests.to_le_bytes());
        Ok(out)
    }

    fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() != 18 {
            return Err("invalid worker snapshot length".into());
        }

        Ok(Self {
            node: u16::from_le_bytes(bytes[0..2].try_into().unwrap()),
            captured_at: u64::from_le_bytes(bytes[2..10].try_into().unwrap()),
            requests: u64::from_le_bytes(bytes[10..18].try_into().unwrap()),
        })
    }
}
```

## Use Cases

- worker health snapshots;
- runtime gauges;
- aggregation inputs where stale data should be ignored;
- dashboards that need the latest row per node or key;
- compact operational metrics shared across sibling processes.
