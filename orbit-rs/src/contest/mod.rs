//! Small coordination primitives built on Orbit shared memory.
//!
//! These primitives do not own application policy. They provide
//! reusable fleet-visible shapes over the runtime substrate.

pub mod fence;
pub mod guard;
pub(crate) mod state;
