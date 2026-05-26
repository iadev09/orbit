//! Tick and epoch helpers for Orbit frame versions.
//!
//! Ring counters answer "where in this ring did the write land?".
//! An [`OrbitEpoch`] answers "when did this sample claim to be
//! captured?". Keeping that distinction explicit lets semantic layers
//! use `Frame::ver` consistently without making the ring itself know
//! about clocks.

mod epoch;

pub use epoch::OrbitEpoch;
