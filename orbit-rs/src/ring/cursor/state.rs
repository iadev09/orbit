/// Caller-owned position in a ring walk.
///
/// The cursor stores the next counter a reader should attempt. Different
/// subscribers keep independent cursors over the same ring.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RingCursor {
    next_counter: u64,
}

impl RingCursor {
    /// Start at counter 0 and replay whatever ring history is still
    /// available.
    pub const fn from_start() -> Self {
        Self { next_counter: 0 }
    }

    /// Start from a known next counter.
    pub const fn from_counter(next_counter: u64) -> Self {
        Self { next_counter }
    }

    /// The next counter this cursor will read.
    pub const fn next_counter(self) -> u64 {
        self.next_counter
    }

    pub(crate) fn set_next_counter(&mut self, next_counter: u64) {
        self.next_counter = next_counter;
    }
}
