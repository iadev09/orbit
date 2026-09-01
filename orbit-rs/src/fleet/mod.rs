//! `Fleet` — the per-process handle that represents membership in
//! a fleet of peers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use dashmap::DashMap;

use crate::OrbitTyped;
use crate::error::{Error, Result};
use crate::id::NetId64;
#[cfg(unix)]
use crate::ring::shm::{ShmRing, ShmRingRegistry};
use crate::ring::{Frame, Ring, RingRegistry};

mod cursor;

/// A node's slot inside the fleet — assigned at `join` time.
///
/// V0: assigned monotonically by a process-local counter (always
/// `0` for a single-process test). V1+: assigned by the SHM-backed
/// fleet header so peers see consistent values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub struct NodeId(pub u16);

impl NodeId {
    pub const ZERO: Self = Self(0);

    pub const fn new(value: u16) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

impl std::fmt::Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "node:{}", self.0)
    }
}

/// Per-process handle into the fleet. Cheap to clone — the inner
/// state is `Arc`-shared.
#[derive(Clone)]
pub struct Fleet {
    inner: Arc<FleetInner>,
}

struct FleetInner {
    name: &'static str,
    fleet_size: u8,
    node_id: NodeId,
    /// Per-KIND counter for `next_id` calls that don't go through a
    /// ring (i.e. when the caller wants a fleet-unique id without
    /// allocating a ring slot). V0: process-local atomic. V1: still
    /// here, parallel to the ring's own write-position.
    id_counters: DashMap<u8, Arc<AtomicU64>>,
    /// Per-KIND ring buffers — orbit's runtime substrate. Either
    /// in-process for unit-test / single-process use, or POSIX SHM
    /// for real cross-process visibility (V1, master+worker fleet).
    backing: RingBacking,
}

/// Backing storage for the fleet's ring buffers — chosen at
/// `Fleet::join` / `Fleet::join_shm` time and frozen for the
/// fleet's lifetime.
enum RingBacking {
    /// Process-local DashMap of `Ring` instances. No cross-process
    /// visibility — peers running other processes do not see this
    /// fleet's writes. Useful for unit tests and embedded scenarios.
    InMemory(RingRegistry),
    /// POSIX-SHM-backed `ShmRing` instances. Multiple processes
    /// joining the same fleet name share the same kernel-level
    /// memory; writes from one are visible to all immediately.
    #[cfg(unix)]
    Shm(ShmRingRegistry),
}

impl Fleet {
    /// Join (or create) a fleet under `name` with `fleet_size` total
    /// expected members. In V0 every call returns a fresh local
    /// fleet; the backing slot table is process-local.
    pub fn join(name: &'static str, fleet_size: u8) -> Result<Self> {
        Self::join_as(name, fleet_size, NodeId::ZERO)
    }

    /// Join (or create) a process-local fleet with an explicit node id.
    pub fn join_as(name: &'static str, fleet_size: u8, node_id: NodeId) -> Result<Self> {
        if fleet_size == 0 {
            return Err(Error::EmptyFleet);
        }
        Ok(Self {
            inner: Arc::new(FleetInner {
                name,
                fleet_size,
                node_id,
                id_counters: DashMap::new(),
                backing: RingBacking::InMemory(RingRegistry::new()),
            }),
        })
    }

    /// Join (or create) a fleet whose ring storage is backed by
    /// POSIX shared memory. Multiple processes calling this with
    /// the same `name` share the same kernel-level
    /// segments — the fleet sees each other's writes.
    ///
    /// Each `OrbitTyped` kind gets its own SHM segment whose layout is
    /// declared by `OrbitTyped::RING_SPEC`.
    ///
    /// Cross-process naming: segments are `/orbit-{name}-{kind}-{uid}`.
    /// macOS limits POSIX SHM names to 31 chars (PSHMNAMLEN); a
    /// short fleet name is required there.
    #[cfg(unix)]
    pub fn join_shm(name: &'static str, fleet_size: u8) -> Result<Self> {
        Self::join_shm_as(name, fleet_size, NodeId::ZERO)
    }

    /// Join (or create) a SHM-backed fleet with an explicit node id.
    #[cfg(unix)]
    pub fn join_shm_as(name: &'static str, fleet_size: u8, node_id: NodeId) -> Result<Self> {
        if fleet_size == 0 {
            return Err(Error::EmptyFleet);
        }
        Ok(Self {
            inner: Arc::new(FleetInner {
                name,
                fleet_size,
                node_id,
                id_counters: DashMap::new(),
                backing: RingBacking::Shm(ShmRingRegistry::new(name)),
            }),
        })
    }

    pub fn name(&self) -> &'static str {
        self.inner.name
    }

    pub fn fleet_size(&self) -> u8 {
        self.inner.fleet_size
    }

    pub fn node_id(&self) -> NodeId {
        self.inner.node_id
    }

    /// Mint a fresh fleet-unique [`NetId64`] for type `T` *without*
    /// publishing anything. Use this when the caller only needs the
    /// id (e.g. minting an id to attach to data being persisted to
    /// DB before going through the ring).
    ///
    /// For most use cases prefer [`Fleet::publish`] — it mints AND
    /// stores in a single atomic step.
    pub fn next_id<T: OrbitTyped>(&self) -> NetId64 {
        let counter_arc = self
            .inner
            .id_counters
            .entry(T::KIND)
            .or_insert_with(|| Arc::new(AtomicU64::new(0)))
            .clone();
        let counter = counter_arc.fetch_add(1, Ordering::Relaxed);
        NetId64::make(T::KIND, self.node_id().get(), counter)
    }

    /// True when this fleet's ring storage is backed by POSIX SHM
    /// (visible across processes). False for in-memory fleets.
    pub fn is_shm(&self) -> bool {
        #[cfg(unix)]
        {
            matches!(self.inner.backing, RingBacking::Shm(_))
        }
        #[cfg(not(unix))]
        {
            false
        }
    }

    /// Get-or-create the in-memory ring for type `T`. Only valid
    /// for fleets created via [`Fleet::join`]; SHM-backed fleets
    /// should use [`Fleet::shm_ring`] instead.
    ///
    /// # Panics
    ///
    /// Panics if called on a SHM-backed fleet.
    pub fn ring<T: OrbitTyped>(&self) -> Arc<Ring> {
        match &self.inner.backing {
            RingBacking::InMemory(r) => r.get_or_create::<T>(),
            #[cfg(unix)]
            RingBacking::Shm(_) => {
                panic!("Fleet::ring called on SHM-backed fleet — use Fleet::shm_ring instead");
            }
        }
    }

    /// Get-or-create the SHM ring for type `T`. Only valid on fleets
    /// created via [`Fleet::join_shm`].
    ///
    /// # Errors
    ///
    /// Returns an `io::Error` if the SHM segment cannot be opened
    /// (permissions, name too long, etc.).
    ///
    /// # Panics
    ///
    /// Panics if called on an in-memory fleet.
    #[cfg(unix)]
    pub fn shm_ring<T: OrbitTyped>(&self) -> std::io::Result<Arc<ShmRing>> {
        match &self.inner.backing {
            RingBacking::Shm(r) => r.get_or_create_for::<T>(),
            RingBacking::InMemory(_) => {
                panic!("Fleet::shm_ring called on in-memory fleet — use Fleet::ring instead");
            }
        }
    }

    /// Publish a payload to the ring for type `T`. Mints a
    /// [`NetId64`] (atomic with slot reservation), writes the
    /// [`Frame`] to the appropriate ring (in-memory or SHM), and
    /// returns the id.
    ///
    /// # Panics
    ///
    /// Panics if the ring cannot be opened or the payload exceeds
    /// `T::RING_SPEC.payload_capacity`. Ring failures are
    /// operator-visible, not silently ignored.
    pub fn publish<T: OrbitTyped>(&self, frame_kind: u8, ver: u64, payload: Bytes) -> NetId64 {
        match &self.inner.backing {
            RingBacking::InMemory(r) => {
                let ring = r.get_or_create::<T>();
                ring.write(self.node_id(), frame_kind, ver, payload)
            }
            #[cfg(unix)]
            RingBacking::Shm(r) => {
                let ring = r
                    .get_or_create_for::<T>()
                    .expect("SHM ring open failed — fleet unusable");
                ring.write(self.node_id(), frame_kind, ver, payload)
                    .expect("SHM ring write failed")
            }
        }
    }

    /// Look up a previously-published frame by its id. Returns the
    /// frame if its slot still holds the same id (i.e. the ring has
    /// not wrapped past it).
    pub fn read(&self, id: NetId64) -> Option<Frame> {
        match &self.inner.backing {
            RingBacking::InMemory(r) => r.lookup(id.kind())?.read(id),
            #[cfg(unix)]
            RingBacking::Shm(r) => r.lookup(id.kind())?.read(id),
        }
    }

    /// Read the most recent frame for type `T` (head - 1 in the ring).
    /// Returns `None` if no write has happened yet.
    pub fn read_head<T: OrbitTyped>(&self) -> Option<Frame> {
        match &self.inner.backing {
            RingBacking::InMemory(r) => {
                let ring = r.get_or_create::<T>();
                ring.read_head()
            }
            #[cfg(unix)]
            RingBacking::Shm(r) => {
                let ring = r.get_or_create_for::<T>().ok()?;
                ring.read_head()
            }
        }
    }

    /// Current head (write position) for type `T`'s ring. Lazily
    /// creates / attaches the ring on first access — important for
    /// cross-process readers, where a child process may need to
    /// attach to a SHM segment a peer already populated. Returns 0
    /// when the ring is fresh / no writes have happened.
    pub fn head<T: OrbitTyped>(&self) -> u64 {
        match &self.inner.backing {
            RingBacking::InMemory(r) => r.get_or_create::<T>().head(),
            #[cfg(unix)]
            RingBacking::Shm(r) => r
                .get_or_create_for::<T>()
                .map(|ring| ring.head())
                .unwrap_or(0),
        }
    }

    /// Read whatever frame currently occupies the slot at
    /// `counter % capacity` for type `T`'s ring. Lazily attaches
    /// the ring on first access (same rationale as [`Fleet::head`]).
    /// Returns `None` if the slot is empty/torn or attach fails.
    ///
    /// Used by walking readers; for typed handle-based reads,
    /// prefer [`Fleet::read`].
    pub fn read_at<T: OrbitTyped>(&self, counter: u64) -> Option<Frame> {
        match &self.inner.backing {
            RingBacking::InMemory(r) => r.get_or_create::<T>().read_at(counter),
            #[cfg(unix)]
            RingBacking::Shm(r) => r.get_or_create_for::<T>().ok()?.read_at(counter),
        }
    }

    /// Capacity of the ring for type `T`. Lazily attaches the ring
    /// on first access. Falls back to `T::RING_SPEC.capacity` when
    /// SHM attach fails.
    pub fn ring_capacity<T: OrbitTyped>(&self) -> usize {
        match &self.inner.backing {
            RingBacking::InMemory(r) => r.get_or_create::<T>().capacity(),
            #[cfg(unix)]
            RingBacking::Shm(r) => r
                .get_or_create_for::<T>()
                .map(|ring| ring.capacity())
                .unwrap_or(T::RING_SPEC.capacity),
        }
    }

    /// Clear the ring for `T` and reset its write head to zero.
    ///
    /// This is an owner-side boot cleanup primitive. It is safe for
    /// runtime state such as events and periodic metrics when the
    /// embedding application calls it before peer processes begin
    /// publishing. It is not a coordination protocol; callers must not
    /// reset a ring while other fleet members are actively writing it.
    pub fn reset_ring<T: OrbitTyped>(&self) -> std::io::Result<()> {
        match &self.inner.backing {
            RingBacking::InMemory(r) => {
                r.get_or_create::<T>().reset();
                Ok(())
            }
            #[cfg(unix)]
            RingBacking::Shm(r) => {
                r.get_or_create_for::<T>()?.reset();
                Ok(())
            }
        }
    }
}

impl std::fmt::Debug for Fleet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Fleet")
            .field("name", &self.inner.name)
            .field("fleet_size", &self.inner.fleet_size)
            .field("node_id", &self.inner.node_id)
            .field("id_counters", &self.inner.id_counters.len())
            .finish()
    }
}
