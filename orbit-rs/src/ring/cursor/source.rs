use crate::ring::Frame;

/// Result of reading one logical counter from a ring source.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RingRead {
    /// The expected counter is fully committed and safe to consume.
    Ready(Frame),
    /// The counter has been claimed but may still be in flight.
    /// A cursor must stay on this counter and retry later.
    Pending,
    /// The counter can no longer be read from this slot.
    Unavailable,
}

/// Read-only source that can be walked by [`super::RingCursor`].
///
/// Implementors expose only the ring facts needed by the generic walker:
/// the ring kind, current head, fixed capacity, and counter-addressed
/// frame reads.
pub trait RingFrameSource {
    /// The `OrbitTyped::KIND` carried by this ring.
    fn kind(&self) -> u8;

    /// Monotonic visible head. Depending on topology, this counts counters
    /// reserved by writers or frames committed by writers.
    fn head(&self) -> u64;

    /// Fixed slot count for this ring.
    fn capacity(&self) -> usize;

    /// Read the frame currently occupying `counter % capacity`.
    fn read_at(&self, counter: u64) -> Option<Frame>;

    /// Classify the logical counter currently addressed by
    /// `counter % capacity`.
    ///
    /// Sources that can distinguish an in-flight claim should override
    /// this method. The compatibility default preserves the previous
    /// `read_at` behavior for external implementations.
    fn read_state_at(&self, counter: u64) -> RingRead {
        self.read_at(counter)
            .map_or(RingRead::Unavailable, RingRead::Ready)
    }
}
