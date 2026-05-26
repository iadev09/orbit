use crate::ring::Frame;

/// Read-only source that can be walked by [`super::RingCursor`].
///
/// Implementors expose only the ring facts needed by the generic walker:
/// the ring kind, current head, fixed capacity, and counter-addressed
/// frame reads.
pub trait RingFrameSource {
    /// The `OrbitTyped::KIND` carried by this ring.
    fn kind(&self) -> u8;

    /// Monotonic write head: number of writes ever published.
    fn head(&self) -> u64;

    /// Fixed slot count for this ring.
    fn capacity(&self) -> usize;

    /// Read the frame currently occupying `counter % capacity`.
    fn read_at(&self, counter: u64) -> Option<Frame>;
}
