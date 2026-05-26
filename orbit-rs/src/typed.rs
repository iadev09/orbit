//! `OrbitTyped` — marker trait that gives a Rust type a stable wire kind.
//!
//! This module is a **placeholder** in V0. The full design (see
//! VISION.md §7.3) gives every `OrbitTyped` type a `KIND: u8` byte
//! that is stable across the fleet because the same binary runs
//! across the fleet — the byte is assigned at compile time by
//! `build.rs` so no runtime registry is needed.
//!
//! V0 keeps the trait surface but does not yet enforce KIND uniqueness
//! or generate it from `build.rs`. Implementors hand-pick a value;
//! collisions are caught only at runtime if/when the OrbitBus or
//! NetId64 layers are wired up. The build-time path arrives once
//! the first cross-process consumer needs it.

/// Marker for a type that has a stable wire identity across the fleet.
///
/// V0 contract: implement `KIND` as any unique `u8` for now. A future
/// `build.rs` step will replace hand-picked values with a generated
/// table, with collision detection at compile time.
///
/// Required bounds:
/// - `Clone` — values may be delivered to multiple subscribers / nodes.
/// - `Send + Sync + 'static` — values cross thread / process boundaries.
pub trait OrbitTyped: Clone + Send + Sync + 'static {
    /// Stable wire identifier. Hand-picked in V0; build.rs-generated later.
    const KIND: u8;
}
