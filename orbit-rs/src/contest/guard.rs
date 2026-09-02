//! Contest coordination over one fleet-shared current-state table.
//!
//! `Contest` is not a race primitive. It turns simultaneous interest in
//! the same typed subject into a small Claim/Yield protocol: every peer
//! may attempt a claim, the active claimant receives a drop-released
//! [`Guard`], and later claimants receive `YieldTo(holder)`.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use crate::contest::fence::FenceToken;
pub use crate::contest::state::{
    CONTEST_STATE_CAPACITY, CONTEST_STATE_KIND, CONTEST_STATE_PAYLOAD_MAX,
};
use crate::contest::state::{LeaseState, StateClaim};
use crate::error::Result;
use crate::fleet::Fleet;
use crate::id::NetId64;

/// Type namespace for a contest subject.
///
/// This is deliberately smaller than [`crate::OrbitTyped`]. A contest subject is
/// not a ring value family; it is just a caller-owned namespace inside
/// the shared contest table.
pub trait ContestType {
    const KIND: u8;
}

/// The typed subject peers coordinate on.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContestSubject {
    kind: u8,
    label: String,
}

impl ContestSubject {
    pub fn new<T: ContestType>(label: impl Into<String>) -> Self {
        Self {
            kind: T::KIND,
            label: label.into(),
        }
    }

    pub const fn kind(&self) -> u8 {
        self.kind
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    fn as_bytes(&self) -> &[u8] {
        self.label.as_bytes()
    }

    fn from_parts(kind: u8, label: &[u8]) -> Self {
        Self {
            kind,
            label: String::from_utf8_lossy(label).into_owned(),
        }
    }
}

/// Fleet-shared contest handle. Cheap to clone.
#[derive(Clone, Debug)]
pub struct Contest {
    fleet: Arc<Fleet>,
}

impl Contest {
    pub fn new(fleet: Arc<Fleet>) -> Self {
        Self { fleet }
    }

    /// Clear the shared contest state table.
    ///
    /// Intended for owner-controlled boot-time cleanup before peer
    /// processes publish claims. It is not a runtime coordination tool.
    pub fn reset(&self) -> Result<()> {
        self.fleet.contest_state().reset()
    }

    /// Compatibility name for the old ring-backed implementation.
    pub fn reset_ring(&self) -> Result<()> {
        self.reset()
    }

    /// Remove the POSIX SHM state object and its companion lock file.
    /// Existing mappings stay valid until their processes detach.
    #[cfg(unix)]
    pub fn unlink(&self) -> Result<()> {
        self.fleet.contest_state().unlink()
    }

    /// Try to become the first active claimant for a typed subject.
    ///
    /// The caller supplies an owner label only for observability. Orbit
    /// does not interpret it.
    pub fn try_claim<T: ContestType>(
        &self,
        subject: impl Into<String>,
        owner: impl Into<ContestOwner>,
        ttl: Duration,
    ) -> Result<Claim> {
        self.try_claim_at::<T>(subject, owner, ttl, now_ms())
    }

    /// Same as [`Self::try_claim`], but with an explicit clock value.
    /// Useful for deterministic tests and embedders with their own
    /// time source.
    pub fn try_claim_at<T: ContestType>(
        &self,
        subject: impl Into<String>,
        owner: impl Into<ContestOwner>,
        ttl: Duration,
        now_ms: u64,
    ) -> Result<Claim> {
        self.try_claim_subject_at(ContestSubject::new::<T>(subject), owner, ttl, now_ms)
    }

    /// Try to claim a pre-built subject.
    pub fn try_claim_subject(
        &self,
        subject: ContestSubject,
        owner: impl Into<ContestOwner>,
        ttl: Duration,
    ) -> Result<Claim> {
        self.try_claim_subject_at(subject, owner, ttl, now_ms())
    }

    /// Same as [`Self::try_claim_subject`], but with an explicit clock.
    pub fn try_claim_subject_at(
        &self,
        subject: ContestSubject,
        owner: impl Into<ContestOwner>,
        ttl: Duration,
        now_ms: u64,
    ) -> Result<Claim> {
        let owner = owner.into();
        let expires_at_ms = expires_at(now_ms, ttl);
        let claim = self.fleet.contest_state().claim(
            self.fleet.node_id(),
            subject.kind,
            subject.as_bytes(),
            owner.as_bytes(),
            now_ms,
            expires_at_ms,
        )?;
        match claim {
            StateClaim::Claimed(holder) => Ok(Claim::Claimed(Guard::new(
                self.clone(),
                holder_from_state(holder),
            ))),
            StateClaim::Occupied(holder) | StateClaim::BornExpired(holder) => {
                Ok(Claim::YieldTo(holder_from_state(holder)))
            }
        }
    }

    /// Turn an observed holder into a drop-released guard.
    ///
    /// This is useful for re-entrant owners: a process may observe that
    /// it already owns the active claim, continue carrying the same
    /// responsibility, and release the original claim when the guard
    /// leaves scope.
    pub fn guard_holder(&self, holder: Holder) -> Guard {
        Guard::new(self.clone(), holder)
    }

    /// Release a holder observed through a yield path.
    ///
    /// This is useful for re-entrant owners: a process may keep probing
    /// under its own still-active claim, then release that original claim
    /// when the guarded work succeeds.
    pub fn release_holder(&self, holder: &Holder) -> Result<NetId64> {
        self.release_id(&holder.subject, holder.claim_id, now_ms())
    }

    fn release_id(
        &self,
        subject: &ContestSubject,
        claim_id: NetId64,
        _now_ms: u64,
    ) -> Result<NetId64> {
        self.fleet
            .contest_state()
            .release(subject.kind, subject.as_bytes(), claim_id)?;
        Ok(claim_id)
    }

    fn renew_id(
        &self,
        subject: &ContestSubject,
        claim_id: NetId64,
        expires_at_ms: u64,
        now_ms: u64,
    ) -> Result<LeaseState> {
        self.fleet.contest_state().renew(
            subject.kind,
            subject.as_bytes(),
            claim_id,
            expires_at_ms,
            now_ms,
        )
    }
}

fn holder_from_state(holder: LeaseState) -> Holder {
    Holder {
        claim_id: holder.claim_id,
        subject: ContestSubject::from_parts(holder.subject_kind, &holder.subject),
        owner: ContestOwner::from_bytes(&holder.owner),
        claimed_at_ms: holder.claimed_at_ms,
        expires_at_ms: holder.expires_at_ms,
    }
}

/// Observer-facing label for the claimant.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ContestOwner(String);

impl ContestOwner {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn as_bytes(&self) -> &[u8] {
        self.0.as_bytes()
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        Self(String::from_utf8_lossy(bytes).into_owned())
    }
}

impl From<&str> for ContestOwner {
    fn from(value: &str) -> Self {
        Self::new(value)
    }
}

impl From<String> for ContestOwner {
    fn from(value: String) -> Self {
        Self(value)
    }
}

impl From<&String> for ContestOwner {
    fn from(value: &String) -> Self {
        Self(value.clone())
    }
}

/// Current active holder observed for a subject.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Holder {
    pub claim_id: NetId64,
    pub subject: ContestSubject,
    pub owner: ContestOwner,
    pub claimed_at_ms: u64,
    pub expires_at_ms: u64,
}

/// RAII guard returned to the process that owns the earliest active claim.
///
/// The guard is the lifetime of the claim in Rust terms: while it lives,
/// the caller is carrying the responsibility for the contest subject.
/// Dropping it clears the matching state-table entry. If the process is killed
/// before `Drop` runs, the claim remains active only until its encoded expiry.
#[derive(Debug)]
pub struct Guard {
    contest: Contest,
    holder: Holder,
    release_on_drop: bool,
}

impl Guard {
    fn new(contest: Contest, holder: Holder) -> Self {
        Self {
            contest,
            holder,
            release_on_drop: true,
        }
    }

    pub fn holder(&self) -> &Holder {
        &self.holder
    }

    pub fn claim_id(&self) -> NetId64 {
        self.holder.claim_id
    }

    /// A monotonic fencing token for this tenure (Kleppmann). Pass it with
    /// every write to the protected resource and gate that resource with a
    /// [`crate::contest::fence::Fence`]: a stalled holder that resumes after
    /// its lease expired carries a lower token and is rejected, keeping the
    /// resource safe even if mutual exclusion momentarily slipped. The token
    /// is the fleet-wide claim counter — strictly higher for each successive
    /// winner — so it is the transient tenure's identity, not a rank.
    pub fn fence_token(&self) -> FenceToken {
        FenceToken::new(self.holder.claim_id.counter())
    }

    pub fn subject(&self) -> &ContestSubject {
        &self.holder.subject
    }

    pub fn owner(&self) -> &ContestOwner {
        &self.holder.owner
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.holder.expires_at_ms
    }

    /// Extend this tenure's lease by `ttl` from now, without minting a new
    /// claim — the holder keeps the same claim id and fencing token. The
    /// current state-table entry is updated in place. Call before the current
    /// lease elapses (e.g. at `ttl/3`) when work outlives one TTL. Renewal
    /// only extends the current tenure; it never accumulates rank.
    pub fn renew(&mut self, ttl: Duration) -> Result<NetId64> {
        self.renew_at(ttl, now_ms())
    }

    /// [`Self::renew`] with an explicit clock, for deterministic tests and
    /// embedders with their own time source.
    pub fn renew_at(&mut self, ttl: Duration, now_ms: u64) -> Result<NetId64> {
        let expires_at_ms = expires_at(now_ms, ttl);
        let renewed = self.contest.renew_id(
            &self.holder.subject,
            self.holder.claim_id,
            expires_at_ms,
            now_ms,
        )?;
        self.holder.expires_at_ms = renewed.expires_at_ms;
        Ok(renewed.claim_id)
    }

    /// Explicitly release the claim before this guard leaves scope.
    pub fn release(mut self) -> Result<NetId64> {
        let released =
            self.contest
                .release_id(&self.holder.subject, self.holder.claim_id, now_ms());
        if released.is_ok() {
            self.release_on_drop = false;
        }
        released
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if !self.release_on_drop {
            return;
        }
        if let Err(err) =
            self.contest
                .release_id(&self.holder.subject, self.holder.claim_id, now_ms())
        {
            tracing::debug!(
                claim_id = %self.holder.claim_id,
                subject_kind = self.holder.subject.kind(),
                subject = self.holder.subject.label(),
                error = %err,
                "contest guard release failed"
            );
        }
    }
}

#[derive(Debug)]
pub enum Claim {
    Claimed(Guard),
    YieldTo(Holder),
}

impl Claim {
    pub fn is_claimed(&self) -> bool {
        matches!(self, Self::Claimed(_))
    }
}

fn expires_at(now_ms: u64, ttl: Duration) -> u64 {
    let ttl_ms = ttl.as_millis().min(u128::from(u64::MAX)) as u64;
    now_ms.saturating_add(ttl_ms)
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use super::{Claim, Contest, ContestType};
    use crate::Fleet;

    struct OriginProbe;

    impl ContestType for OriginProbe {
        const KIND: u8 = 1;
    }

    struct OtherProbe;

    impl ContestType for OtherProbe {
        const KIND: u8 = 2;
    }

    #[test]
    fn first_claim_is_claimed_for_subject() {
        let fleet = Arc::new(Fleet::join("first_claim_is_claimed", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        let first = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:1", Duration::from_secs(30), 1_000)
            .expect("contest");
        let second = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:2", Duration::from_secs(30), 1_001)
            .expect("second claim");

        assert!(matches!(first, Claim::Claimed(_)));
        let Claim::YieldTo(holder) = second else {
            panic!("second claimant should yield");
        };
        assert_eq!(holder.owner.as_str(), "worker:1");
    }

    #[test]
    fn different_subject_labels_do_not_compete() {
        let fleet = Arc::new(Fleet::join("first_claim_subjects", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        let first = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:1", Duration::from_secs(30), 1_000)
            .expect("first subject");
        let second = claims
            .try_claim_at::<OriginProbe>("origin:tcp_2", "worker:2", Duration::from_secs(30), 1_001)
            .expect("second subject");

        assert!(first.is_claimed());
        assert!(second.is_claimed());
    }

    #[test]
    fn different_subject_types_do_not_compete() {
        let fleet = Arc::new(Fleet::join("first_claim_subject_types", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        let first = claims
            .try_claim_at::<OriginProbe>("same-label", "worker:1", Duration::from_secs(30), 1_000)
            .expect("first type");
        let second = claims
            .try_claim_at::<OtherProbe>("same-label", "worker:2", Duration::from_secs(30), 1_001)
            .expect("second type");

        assert!(first.is_claimed());
        assert!(second.is_claimed());
    }

    #[test]
    fn releasing_claim_guard_allows_next_claim() {
        let fleet = Arc::new(Fleet::join("first_claim_release", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        let Claim::Claimed(guard) = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:1", Duration::from_secs(30), 1_000)
            .expect("claim")
        else {
            panic!("expected claim");
        };
        guard.release().expect("release");

        let next = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:2", Duration::from_secs(30), 1_100)
            .expect("next claim");

        let Claim::Claimed(guard) = next else {
            panic!("released claim should not block");
        };
        assert_eq!(guard.owner().as_str(), "worker:2");
    }

    #[test]
    fn expired_claim_does_not_block_next_claim() {
        let fleet = Arc::new(Fleet::join("first_claim_expiry", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        let first = claims
            .try_claim_at::<OriginProbe>(
                "origin:tcp_1",
                "worker:1",
                Duration::from_millis(5),
                1_000,
            )
            .expect("contest");
        let second = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:2", Duration::from_secs(30), 1_006)
            .expect("second claim");

        assert!(first.is_claimed());
        let Claim::Claimed(guard) = second else {
            panic!("expired claim should not block");
        };
        assert_eq!(guard.owner().as_str(), "worker:2");
    }

    #[test]
    fn yielding_claim_releases_itself() {
        let fleet = Arc::new(Fleet::join("yielding_claim_release", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        let Claim::Claimed(first_guard) = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:1", Duration::from_secs(30), 1_000)
            .expect("contest")
        else {
            panic!("expected initial claim");
        };
        let second = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:2", Duration::from_secs(30), 1_001)
            .expect("second claim");
        assert!(matches!(second, Claim::YieldTo(_)));

        first_guard.release().expect("release first");
        let third = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:3", Duration::from_secs(30), 1_100)
            .expect("third claim");

        let Claim::Claimed(guard) = third else {
            panic!("released yielding claim should not block");
        };
        assert_eq!(guard.owner().as_str(), "worker:3");
    }

    #[test]
    fn renewed_claim_survives_past_original_ttl() {
        let fleet = Arc::new(Fleet::join("contest_renew", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        let Claim::Claimed(mut guard) = claims
            .try_claim_at::<OriginProbe>(
                "origin:tcp_1",
                "worker:1",
                Duration::from_millis(10),
                1_000,
            )
            .expect("claim")
        else {
            panic!("expected initial claim");
        };

        // Renew before the original 1_010 expiry, extending to 1_015.
        guard
            .renew_at(Duration::from_millis(10), 1_005)
            .expect("renew");
        assert_eq!(guard.expires_at_ms(), 1_015);

        // A contender at 1_012: the original TTL (1_010) has elapsed, but the
        // renewal keeps worker:1 the holder, on its original earliest claim.
        let second = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:2", Duration::from_secs(30), 1_012)
            .expect("second claim");
        let Claim::YieldTo(holder) = second else {
            panic!("renewed claim must still hold the subject");
        };
        assert_eq!(holder.owner.as_str(), "worker:1");
        assert_eq!(holder.expires_at_ms, 1_015);

        // Past the renewed expiry, the subject is contestable again.
        let third = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:3", Duration::from_secs(30), 1_020)
            .expect("third claim");
        let Claim::Claimed(guard3) = third else {
            panic!("expired renewed claim should not block");
        };
        assert_eq!(guard3.owner().as_str(), "worker:3");
    }

    #[test]
    fn fence_rejects_superseded_holder() {
        use crate::contest::fence::Fence;

        let fleet = Arc::new(Fleet::join("contest_fence", 2).expect("fleet"));
        let claims = Contest::new(fleet);
        let fence = Fence::new();

        let Claim::Claimed(g1) = claims
            .try_claim_at::<OriginProbe>(
                "origin:tcp_1",
                "worker:1",
                Duration::from_millis(5),
                1_000,
            )
            .expect("claim")
        else {
            panic!("expected initial claim");
        };
        let t1 = g1.fence_token();
        assert!(fence.admit(t1)); // worker:1 writes under its tenure

        // worker:1 stalls and loses the lease (released here for the test);
        // worker:2 takes over.
        drop(g1);
        let Claim::Claimed(g2) = claims
            .try_claim_at::<OriginProbe>("origin:tcp_1", "worker:2", Duration::from_secs(30), 1_010)
            .expect("claim2")
        else {
            panic!("expected takeover claim");
        };
        let t2 = g2.fence_token();
        assert!(t2 > t1, "a successive winner must carry a higher token");
        assert!(fence.admit(t2)); // new holder accepted

        // Stale worker:1 wakes up and tries to write with its old token.
        assert!(
            !fence.admit(t1),
            "a superseded holder must be fenced out at the resource"
        );
    }

    #[test]
    fn active_claim_survives_churn_beyond_old_ring_capacity() {
        let fleet = Arc::new(Fleet::join("contest_state_churn", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        let Claim::Claimed(mut long_lived) = claims
            .try_claim_at::<OriginProbe>("long-lived", "holder", Duration::from_secs(30), 1_000)
            .expect("long-lived claim")
        else {
            panic!("expected long-lived claim");
        };

        // The old ring model lost the original CLAIM after enough unrelated
        // claim/release frames wrapped the history window. Current-state
        // storage keeps the live subject in its slot while released subjects
        // reuse tombstones.
        let mut now = 1_000u64;
        for index in 0..2_000 {
            if let Claim::Claimed(g) = claims
                .try_claim_at::<OriginProbe>(
                    format!("churn:{index}"),
                    "w",
                    Duration::from_secs(30),
                    now,
                )
                .expect("churn claim")
            {
                g.release().expect("churn release");
            }
            now += 1;
        }

        long_lived
            .renew_at(Duration::from_secs(30), now)
            .expect("renew after churn");
        let contender = claims
            .try_claim_at::<OriginProbe>(
                "long-lived",
                "contender",
                Duration::from_secs(30),
                now + 1,
            )
            .expect("contender");
        let Claim::YieldTo(holder) = contender else {
            panic!("unrelated churn must not evict an active claim");
        };
        assert_eq!(holder.claim_id, long_lived.claim_id());
        assert_eq!(holder.owner.as_str(), "holder");
    }

    #[test]
    fn stale_guard_cannot_renew_or_release_successor() {
        let fleet = Arc::new(Fleet::join("contest_stale_guard", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        let Claim::Claimed(mut stale) = claims
            .try_claim_at::<OriginProbe>("subject", "first", Duration::from_millis(5), 1_000)
            .expect("first claim")
        else {
            panic!("expected first claim");
        };
        let Claim::Claimed(successor) = claims
            .try_claim_at::<OriginProbe>("subject", "second", Duration::from_secs(30), 1_006)
            .expect("successor claim")
        else {
            panic!("expired claim should be replaceable");
        };

        assert!(
            stale.renew_at(Duration::from_secs(30), 1_007).is_err(),
            "an expired, superseded guard must not resurrect its lease"
        );
        drop(stale);

        let contender = claims
            .try_claim_at::<OriginProbe>("subject", "third", Duration::from_secs(30), 1_008)
            .expect("contender");
        let Claim::YieldTo(holder) = contender else {
            panic!("stale Drop must not release the successor");
        };
        assert_eq!(holder.claim_id, successor.claim_id());
        assert_eq!(holder.owner.as_str(), "second");
    }

    #[test]
    fn full_state_table_does_not_evict_live_subjects() {
        use super::CONTEST_STATE_CAPACITY;
        use crate::Error;

        let fleet = Arc::new(Fleet::join("contest_state_full", 1).expect("fleet"));
        let claims = Contest::new(fleet);
        let mut guards = Vec::with_capacity(CONTEST_STATE_CAPACITY);

        for index in 0..CONTEST_STATE_CAPACITY {
            let Claim::Claimed(guard) = claims
                .try_claim_at::<OriginProbe>(
                    format!("subject:{index}"),
                    "owner",
                    Duration::from_secs(30),
                    1_000,
                )
                .expect("table slot")
            else {
                panic!("each distinct subject should occupy one slot");
            };
            guards.push(guard);
        }

        let error = claims
            .try_claim_at::<OriginProbe>("one-too-many", "owner", Duration::from_secs(30), 1_001)
            .expect_err("a full table must fail instead of evicting a live lease");
        assert!(matches!(
            error,
            Error::ContestStateFull {
                capacity: CONTEST_STATE_CAPACITY
            }
        ));
    }
}
