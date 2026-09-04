# orbit-rustls

`orbit-rustls` provides same-host, fleet-shared runtime state for rustls over
Orbit shared memory. The current session-store surface requires `std` and a
Unix target.

Its first integration is a fleet-wide implementation of
`rustls::server::StoresServerSessions`. rustls generates the session keys,
encodes the session values, and validates them when they are read back.
`orbit-rustls` never interprets those bytes; it provides bounded storage, TTL,
domain isolation, and an atomic single-use `take` across processes.

## How it relates to rustls

The relationship has two layers:

- `session` directly depends on rustls and implements its
  `StoresServerSessions` trait;
- the underlying SHM table is only indirectly related to rustls. It imports no
  rustls types and stores the adapter's opaque key/value bytes.

The stored representation is rustls-private state, not a TLS RFC wire format.
It must not be assumed compatible with another TLS implementation or with a
future rustls release. A deployment that preserves SHM across binary upgrades
should include a compatibility epoch in its session domain.

No rustls struct, enum, reference, or trait object is placed in SHM. A rustls
upgrade therefore does not create an SHM ABI or type-layout boundary; only
opaque byte arrays cross it. This avoids compiler-invisible layout UB, but it
does not make old encoded values compatible. Stale values must still be
isolated with a new domain epoch or removed during the upgrade.

## Storage semantics

The server-session store is a fixed-capacity current-state table, not a ring.
It has no cursor, notification fd, or background task. A TLS handshake performs
synchronous keyed access, and a cache miss simply causes a full handshake.

rustls uses `take` for TLS 1.3 stateful ticket resumption and requires every
returned value to be reliably deleted. This implementation removes the entry
atomically under the shared process lock, so only one fleet process can consume
a ticket.

This is not a browser or general application-session store. Those uses usually
need reusable reads, explicit durability, larger values, and different eviction
guarantees.

## Scope

Server sessions are the first surface, not the crate's permanent limit.
Additional rustls-facing shared state, such as versioned resolver snapshots or
encrypted credential material, can live here once its security, ownership, and
compatibility contracts are explicit. Generic SHM and ring mechanics remain in
`orbit-rs`.
