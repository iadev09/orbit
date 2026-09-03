# orbit-lock

`orbit-lock` provides keyed, fleet-shared locks over Orbit shared memory.

The primitive stores one current record per active `LockKey`:

```text
LockKey -> LockOwner + acquired deadline + fencing token + revision
```

Acquisition, owner-matched renewal, and owner-matched release are atomic
across same-host fleet processes. Every call is one immediate attempt;
blocking and retry loops belong to the caller using the primitive. A separate
notified ring publishes successful transitions and exposes native readiness
for caller-owned coordination policies.

`LockKey` and cache keys are unrelated namespaces. Cache deletion or reset
never changes lock state, and lock expiry or release never changes cache data.

See [ARCHITECTURE.md](ARCHITECTURE.md) for the complete contract.
