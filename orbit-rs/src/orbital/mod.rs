//! `Orbital<T>` — typed wrapper over the ring buffer substrate.
//!
//! The most ergonomic way to put a value into the fleet and read the
//! latest from anywhere. Internally:
//!
//! - `store(value)` → `bytemuck::bytes_of(&value)` → `Bytes` →
//!   `Fleet::publish::<T>` (mints a [`NetId64`], writes Frame to ring).
//! - `load()` → `Fleet::ring::<T>::read_head()` → bytemuck cast → `T`.
//!
//! ## Constraints
//!
//! `T: OrbitTyped + bytemuck::Pod + bytemuck::Zeroable` — V0 only
//! supports plain-old-data types (fixed size, `#[repr(C)]`, no
//! pointers, no padding holes). String / Vec / nested heap data
//! goes through a different encoder (a future `OrbitEncoded` trait
//! with explicit `to_bytes` / `from_bytes` methods); blanket impls
//! over Pod will keep the simple case ergonomic.
//!
//! ## Why Pod for V0
//!
//! - Zero-cost transformation (memcpy of fixed size).
//! - Compile-time validated layout.
//! - FFI-safe — same bytes work in C / over the wire.
//! - Fits SHM substrate cleanly (V1 will memmap the same bytes).

use std::marker::PhantomData;
use std::sync::Arc;

use bytemuck::Pod;
use bytes::Bytes;

use crate::OrbitTyped;
use crate::fleet::Fleet;
use crate::id::NetId64;

/// Typed handle into the fleet's ring for type `T`.
///
/// Cheap to clone (holds an `Arc<Fleet>` + a phantom). Multiple
/// handles to the same `T` share the same underlying ring.
#[derive(Clone)]
pub struct Orbital<T: OrbitTyped + Pod> {
    fleet: Arc<Fleet>,
    _t: PhantomData<T>,
}

impl<T: OrbitTyped + Pod> Orbital<T> {
    /// Bind to the fleet for type `T`. The ring for `T::KIND` is
    /// created lazily on first publish/read.
    pub fn new(fleet: Arc<Fleet>) -> Self {
        Self {
            fleet,
            _t: PhantomData,
        }
    }

    /// Publish a new value. Returns the [`NetId64`] minted by the
    /// fleet's ring for this write — the COUNTER part is the slot
    /// position (V0: process-local, V1: SHM ring index).
    pub fn store(&self, value: T) -> NetId64 {
        let payload = Bytes::copy_from_slice(bytemuck::bytes_of(&value));
        self.fleet.publish::<T>(0, 0, payload)
    }

    /// Read the most recent value from the ring. Returns `None` when
    /// no write has happened yet, or when the most recent frame's
    /// payload doesn't decode as `T` (size mismatch — should not
    /// occur unless someone published the wrong shape under this
    /// `T::KIND`).
    pub fn load(&self) -> Option<T> {
        let frame = self.fleet.read_head::<T>()?;
        bytemuck::try_from_bytes::<T>(&frame.payload).ok().copied()
    }

    /// Read by [`NetId64`] — returns the value if its slot has not
    /// been overwritten yet (ring still holds it).
    pub fn load_by_id(&self, id: NetId64) -> Option<T> {
        let frame = self.fleet.read(id)?;
        bytemuck::try_from_bytes::<T>(&frame.payload).ok().copied()
    }

    /// The fleet handle this `Orbital` is bound to. Cheap clone.
    pub fn fleet(&self) -> &Arc<Fleet> {
        &self.fleet
    }
}
