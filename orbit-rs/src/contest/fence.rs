//! Fencing tokens for contest tenures (Kleppmann).
//!
//! A lease alone cannot protect a shared resource: a holder can stall past
//! its TTL, lose the tenure to a new holder, then resume and write stale
//! data. A fencing token closes that gap. Every tenure carries a
//! monotonically non-decreasing token ([`crate::contest::Guard::fence_token`]);
//! the protected resource records the highest token it has honored and
//! rejects any write carrying a lower one, so a superseded holder that wakes
//! up late is fenced out even if mutual exclusion momentarily slipped.
//!
//! The token is the per-ring claim counter. Successive winners always carry
//! a strictly higher counter (a new winner's claim is published after the
//! previous winner's), so the fence is sound. The 40-bit ring counter is the
//! horizon; see FINDINGS "CONTEST (claude ultra)".

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// A monotonic fencing token identifying one contest tenure.
///
/// Obtain it from [`crate::contest::Guard::fence_token`] and pass it with
/// every write to the protected resource. Reconstruct one from a persisted
/// `u64` with [`FenceToken::new`] when the resource keeps its high-water
/// mark across restarts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct FenceToken(u64);

impl FenceToken {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    pub const fn get(self) -> u64 {
        self.0
    }
}

impl fmt::Display for FenceToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "fence:{}", self.0)
    }
}

/// Resource-side high-water mark that enforces fencing.
///
/// Place one in front of the resource a contest subject protects. Call
/// [`Fence::admit`] with the writer's [`FenceToken`] before applying its
/// write, and apply the write only when it returns `true`.
#[derive(Debug, Default)]
pub struct Fence {
    high_water: AtomicU64,
}

impl Fence {
    /// A fresh fence that has honored no writes yet.
    pub const fn new() -> Self {
        Self {
            high_water: AtomicU64::new(0),
        }
    }

    /// A fence restored from a persisted high-water mark (resource restart).
    pub const fn with_high_water(token: u64) -> Self {
        Self {
            high_water: AtomicU64::new(token),
        }
    }

    /// Admit a write fenced by `token`. Returns `true` and advances the
    /// high-water mark iff `token` is at least every previously-admitted
    /// token; returns `false` for a stale token from a superseded holder.
    ///
    /// Atomic and lock-free; safe to call concurrently from many writers.
    pub fn admit(&self, token: FenceToken) -> bool {
        let prev = self.high_water.fetch_max(token.get(), Ordering::AcqRel);
        token.get() >= prev
    }

    /// The highest token honored so far.
    pub fn high_water(&self) -> u64 {
        self.high_water.load(Ordering::Acquire)
    }
}

#[cfg(test)]
mod tests {
    use super::{Fence, FenceToken};

    #[test]
    fn admits_monotonic_and_rejects_stale() {
        let fence = Fence::new();
        assert!(fence.admit(FenceToken::new(5))); // first holder
        assert!(fence.admit(FenceToken::new(5))); // same tenure re-writes
        assert!(fence.admit(FenceToken::new(9))); // newer tenure takes over
        assert!(!fence.admit(FenceToken::new(7))); // stale: 7 < hwm 9
        assert!(!fence.admit(FenceToken::new(5))); // stale superseded holder
        assert_eq!(fence.high_water(), 9);
    }

    #[test]
    fn restored_fence_keeps_rejecting_below_persisted_mark() {
        let fence = Fence::with_high_water(42);
        assert!(!fence.admit(FenceToken::new(41)));
        assert!(fence.admit(FenceToken::new(42)));
        assert!(fence.admit(FenceToken::new(100)));
    }
}
