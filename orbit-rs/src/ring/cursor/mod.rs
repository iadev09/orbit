//! Cursor and walking primitives for Orbit rings.
//!
//! This module owns the generic "walk counters from cursor toward head"
//! semantics shared by event streams, cache compaction, metrics, and
//! future ring-backed substrates. Semantic layers decode frames after
//! this layer has handled wraparound, in-flight slots, loss, and cursor
//! advance.

mod poll;
mod source;
mod state;
mod walk;

pub use poll::{RingLoss, RingPoll};
pub use source::{RingFrameSource, RingRead};
pub use state::RingCursor;
pub use walk::poll_ring;
