use crate::ring::Frame;

/// Counters skipped while walking a ring.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RingLoss {
    /// Counters older than the current ring window.
    pub overwritten: u64,
    /// Counters inside the readable window whose slot was definitively
    /// unavailable, wrapped, corrupt, or carried an unexpected frame id.
    pub unavailable: u64,
}

impl RingLoss {
    pub const fn total(self) -> u64 {
        self.overwritten.saturating_add(self.unavailable)
    }

    pub const fn is_empty(self) -> bool {
        self.overwritten == 0 && self.unavailable == 0
    }
}

/// Result of walking a cursor toward a ring claim head.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RingPoll {
    pub frames: Vec<Frame>,
    pub loss: RingLoss,
    pub from_counter: u64,
    pub to_counter: u64,
}

impl RingPoll {
    pub fn is_empty(&self) -> bool {
        self.frames.is_empty() && self.loss.is_empty()
    }
}
