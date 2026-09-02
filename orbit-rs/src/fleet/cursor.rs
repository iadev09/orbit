use std::marker::PhantomData;

use crate::OrbitTyped;
use crate::fleet::Fleet;
use crate::ring::Frame;
use crate::ring::cursor::{RingCursor, RingFrameSource, RingPoll, RingRead, poll_ring};

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

    fn read_state_at(&self, counter: u64) -> RingRead {
        self.fleet.read_state_at::<T>(counter)
    }
}

impl Fleet {
    /// Cursor that starts after every counter currently claimed for `T`.
    /// Useful for subscribers that only want future writes.
    pub fn cursor_at_head<T: OrbitTyped>(&self) -> RingCursor {
        RingCursor::from_counter(self.head::<T>())
    }

    /// Cursor that starts at counter 0 for `T`.
    pub const fn cursor_from_start<T: OrbitTyped>(&self) -> RingCursor {
        let _ = self;
        let _ = PhantomData::<T>;
        RingCursor::from_start()
    }

    /// Walk `cursor` toward the current claim head for `T`, stopping at
    /// an in-flight counter and reporting definitive losses as
    /// [`crate::ring::cursor::RingLoss`].
    pub fn poll_ring<T: OrbitTyped>(&self, cursor: &mut RingCursor) -> RingPoll {
        poll_ring(&FleetRingSource::<T>::new(self), cursor)
    }
}
