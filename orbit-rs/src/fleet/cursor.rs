use std::marker::PhantomData;

use crate::fleet::Fleet;
use crate::ring::Frame;
use crate::ring::cursor::{RingCursor, RingFrameSource, RingPoll, poll_ring};
use crate::typed::OrbitTyped;

struct FleetRingSource<'a, T: OrbitTyped> {
    fleet: &'a Fleet,
    _t: PhantomData<T>,
}

impl<'a, T: OrbitTyped> FleetRingSource<'a, T> {
    fn new(fleet: &'a Fleet) -> Self {
        Self {
            fleet,
            _t: PhantomData,
        }
    }
}

impl<T: OrbitTyped> RingFrameSource for FleetRingSource<'_, T> {
    fn kind(&self) -> u8 {
        T::KIND
    }

    fn head(&self) -> u64 {
        self.fleet.head::<T>()
    }

    fn capacity(&self) -> usize {
        self.fleet.ring_capacity::<T>()
    }

    fn read_at(&self, counter: u64) -> Option<Frame> {
        self.fleet.read_at::<T>(counter)
    }
}

impl Fleet {
    /// Cursor that starts after the latest frame currently published for
    /// `T`. Useful for subscribers that only want future frames.
    pub fn cursor_at_head<T: OrbitTyped>(&self) -> RingCursor {
        RingCursor::from_counter(self.head::<T>())
    }

    /// Cursor that starts at counter 0 for `T`.
    pub const fn cursor_from_start<T: OrbitTyped>(&self) -> RingCursor {
        let _ = self;
        let _ = PhantomData::<T>;
        RingCursor::from_start()
    }

    /// Walk `cursor` over the ring for `T`, advancing it to the current
    /// head and reporting skipped counters as [`crate::ring::cursor::RingLoss`].
    pub fn poll_ring<T: OrbitTyped>(&self, cursor: &mut RingCursor) -> RingPoll {
        poll_ring(&FleetRingSource::<T>::new(self), cursor)
    }
}
