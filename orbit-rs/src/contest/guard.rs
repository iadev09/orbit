//! Contest coordination over one Orbit ring.
//!
//! `Contest` is not a race primitive. It turns simultaneous interest in
//! the same typed subject into a small Claim/Yield protocol: every peer
//! may publish a claim, the earliest active claim receives a
//! drop-released [`Guard`], and later claims receive `YieldTo(holder)`.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};

use crate::OrbitTyped;
use crate::RingSpec;
use crate::contest::fence::FenceToken;
use crate::error::{Error, Result};
use crate::fleet::Fleet;
use crate::id::NetId64;
use crate::ring::cursor::{RingCursor, RingLoss};

/// Contest frame payload limit for V0. On Unix this matches the SHM ring
/// slot payload size; non-Unix keeps the same bounded contract.
pub const CONTEST_RING_SPEC: RingSpec = RingSpec::new(1024, 256);
pub const CONTEST_PAYLOAD_MAX: usize = CONTEST_RING_SPEC.payload_capacity;

pub const CONTEST_RING_KIND: u8 = 222;
pub const CONTEST_FRAME_KIND_CLAIM: u8 = 1;
pub const CONTEST_FRAME_KIND_RELEASE: u8 = 2;
pub const CONTEST_FRAME_KIND_RENEW: u8 = 3;

const CLAIM_HEADER_LEN: usize = 1 + 2 + 2 + 8 + 8;
const RELEASE_HEADER_LEN: usize = 8 + 1 + 2;
const RENEW_HEADER_LEN: usize = 8 + 8 + 1 + 2;

/// Bounded loss-driven re-polls a claim performs before deciding, when the
/// ring window has torn/uncommitted slots that could hide an earlier claim.
/// Each retry is a fresh poll separated only by a cooperative `yield_now` —
/// never a timed sleep — so `try_claim` blocks no thread and is safe to call
/// directly from an async runtime. A torn seqlock write commits in
/// nanoseconds, so a few yields settle it; a slot that never commits is a
/// crashed mid-write (no real claim) and the caller proceeds.
const CLAIM_SETTLE_ATTEMPTS: u32 = 3;

/// Dedicated ring record marker for contest frames.
#[derive(Clone, Debug)]
pub struct ContestRecord;

impl OrbitTyped for ContestRecord {
    // Hand-picked V0 kind. Build-time KIND allocation will replace
    // these manual values later.
    const KIND: u8 = CONTEST_RING_KIND;
    const RING_SPEC: RingSpec = CONTEST_RING_SPEC;
}

/// Type namespace for a contest subject.
///
/// This is deliberately smaller than [`OrbitTyped`]. A contest subject is
/// not a ring value family; it is just a caller-owned namespace inside
/// the shared contest ring.
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

    /// Clear the shared contest ring.
    ///
    /// Intended for owner-controlled boot-time cleanup before peer
    /// processes publish claims. It is not a runtime coordination tool.
    pub fn reset_ring(&self) -> Result<()> {
        self.fleet.reset_ring::<ContestRecord>().map_err(Error::Io)
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
        let payload = encode_claim(
            subject.kind,
            subject.as_bytes(),
            owner.as_bytes(),
            now_ms,
            expires_at_ms,
        )?;
        let claim_id =
            self.fleet
                .publish::<ContestRecord>(CONTEST_FRAME_KIND_CLAIM, now_ms, payload);

        // Decide against a loss-aware view of the ring window. A bare snapshot
        // can mistake a lower, not-yet-readable claim for "absent" and hand two
        // peers the same subject; see FINDINGS "CONTEST (claude ultra)".
        //
        // - A strictly-earlier *visible* claim -> yield (any loss is moot:
        //   we lose regardless).
        // - We are the earliest visible claim on a clean window -> Claimed.
        // - `unavailable` loss (a torn / uncommitted slot) -> re-poll a bounded
        //   number of times, separated by a cooperative yield (never a sleep —
        //   async-safe); each retry re-scans the window so the torn slot is
        //   re-read once it commits. A slot that never commits is a crashed
        //   mid-write, i.e. not a real claim, so we may proceed.
        //
        // `poll_active` scans only the resident window from its floor, so normal
        // history aging is never mistaken for loss. A live claim pushed out of
        // the capacity-sized window by extreme write volume is a documented
        // limit, backed by fencing tokens ([`crate::contest::fence`]) at the
        // protected resource.
        let mut attempt: u32 = 0;
        loop {
            let (earliest, loss) = self.poll_active(&subject, now_ms);
            match earliest {
                Some(holder) if holder.claim_id.counter() != claim_id.counter() => {
                    let _ = self.release_id(&subject, claim_id, now_ms);
                    return Ok(Claim::YieldTo(holder));
                }
                Some(holder) => {
                    if loss.unavailable > 0 && attempt < CLAIM_SETTLE_ATTEMPTS {
                        attempt += 1;
                        std::thread::yield_now();
                        continue;
                    }
                    return Ok(Claim::Claimed(Guard::new(self.clone(), holder)));
                }
                None => {
                    if loss.unavailable > 0 && attempt < CLAIM_SETTLE_ATTEMPTS {
                        attempt += 1;
                        std::thread::yield_now();
                        continue;
                    }
                    // Subject is free but our own claim is not observable
                    // (e.g. ttl == 0 born-expired). Preserve the prior
                    // YieldTo(self) contract for that degenerate case.
                    return Ok(Claim::YieldTo(Holder {
                        claim_id,
                        subject: subject.clone(),
                        owner: owner.clone(),
                        claimed_at_ms: now_ms,
                        expires_at_ms,
                    }));
                }
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
        now_ms: u64,
    ) -> Result<NetId64> {
        let payload = encode_release(subject.kind, subject.as_bytes(), claim_id)?;
        Ok(self
            .fleet
            .publish::<ContestRecord>(CONTEST_FRAME_KIND_RELEASE, now_ms, payload))
    }

    fn renew_id(
        &self,
        subject: &ContestSubject,
        claim_id: NetId64,
        expires_at_ms: u64,
        now_ms: u64,
    ) -> Result<NetId64> {
        let payload = encode_renew(subject.kind, subject.as_bytes(), claim_id, expires_at_ms)?;
        Ok(self
            .fleet
            .publish::<ContestRecord>(CONTEST_FRAME_KIND_RENEW, now_ms, payload))
    }

    /// Reconstruct the earliest active claim for `subject` from the ring,
    /// together with any window loss observed during the walk.
    ///
    /// The loss is load-bearing: a caller must not treat "no holder found"
    /// as "subject free" when frames that could carry an earlier claim were
    /// overwritten or were momentarily unreadable.
    fn poll_active(&self, subject: &ContestSubject, now_ms: u64) -> (Option<Holder>, RingLoss) {
        // Scan only the resident window, starting at its floor (head -
        // capacity). Starting from counter 0 would make `poll_ring` report the
        // entire scrolled-out history as `overwritten` — that is normal ring
        // aging, NOT a lost-frame signal, and on a mature ring it is always
        // non-zero, so it must never be treated as a loss. From the floor,
        // `overwritten` is structurally 0; only `unavailable` (torn /
        // uncommitted slots) is a real loss signal.
        let head = self.fleet.head::<ContestRecord>();
        let capacity = self.fleet.ring_capacity::<ContestRecord>() as u64;
        let mut cursor = RingCursor::from_counter(head.saturating_sub(capacity));
        let poll = self.fleet.poll_ring::<ContestRecord>(&mut cursor);
        let loss = RingLoss {
            overwritten: 0,
            unavailable: poll.loss.unavailable,
        };
        let mut active = BTreeMap::<u64, Holder>::new();

        for frame in poll.frames {
            match decode_frame(frame.kind, &frame.payload) {
                Some(DecodedContestFrame::Claim(decoded))
                    if decoded.subject_kind == subject.kind
                        && decoded.subject == subject.as_bytes() =>
                {
                    active.insert(
                        frame.id.counter(),
                        Holder {
                            claim_id: frame.id,
                            subject: ContestSubject::from_parts(
                                decoded.subject_kind,
                                decoded.subject,
                            ),
                            owner: ContestOwner::from_bytes(decoded.owner),
                            claimed_at_ms: decoded.claimed_at_ms,
                            expires_at_ms: decoded.expires_at_ms,
                        },
                    );
                }
                Some(DecodedContestFrame::Renew(decoded))
                    if decoded.subject_kind == subject.kind
                        && decoded.subject == subject.as_bytes() =>
                {
                    // Extend the existing tenure in place — same counter, so
                    // the holder keeps its earliest-claim position. A renewal
                    // whose original CLAIM has already scrolled out of the
                    // capacity-sized window is unreconstructable here (the
                    // documented eviction limit); with a large window this does
                    // not arise for a tenure renewing within its TTL.
                    if let Some(holder) = active.get_mut(&decoded.claim_id.counter()) {
                        holder.expires_at_ms = holder.expires_at_ms.max(decoded.expires_at_ms);
                    }
                }
                Some(DecodedContestFrame::Release(decoded))
                    if decoded.subject_kind == subject.kind
                        && decoded.subject == subject.as_bytes() =>
                {
                    active.remove(&decoded.claim_id.counter());
                }
                _ => {}
            }
        }

        // Expiry is evaluated after renewals, so a renewed lease that
        // outlived its original TTL is not dropped.
        active.retain(|_, holder| holder.expires_at_ms > now_ms);

        (active.into_values().next(), loss)
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
/// Dropping it publishes a release frame. If the process is killed before
/// `Drop` runs, the claim remains active only until its encoded expiry.
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
    /// is the per-ring claim counter — strictly higher for each successive
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
    /// claim — the holder keeps its earliest-claim position. Publishes a
    /// renewal frame and advances the in-memory expiry. Call before the
    /// current lease elapses (e.g. at `ttl/3`) when work outlives one TTL,
    /// so a live holder is not revoked mid-flight. Renewal only extends the
    /// current tenure; it never accumulates rank.
    pub fn renew(&mut self, ttl: Duration) -> Result<NetId64> {
        self.renew_at(ttl, now_ms())
    }

    /// [`Self::renew`] with an explicit clock, for deterministic tests and
    /// embedders with their own time source.
    pub fn renew_at(&mut self, ttl: Duration, now_ms: u64) -> Result<NetId64> {
        let expires_at_ms = expires_at(now_ms, ttl);
        let id = self.contest.renew_id(
            &self.holder.subject,
            self.holder.claim_id,
            expires_at_ms,
            now_ms,
        )?;
        self.holder.expires_at_ms = expires_at_ms;
        Ok(id)
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

struct DecodedClaim<'a> {
    subject_kind: u8,
    subject: &'a [u8],
    owner: &'a [u8],
    claimed_at_ms: u64,
    expires_at_ms: u64,
}

struct DecodedRelease<'a> {
    subject_kind: u8,
    subject: &'a [u8],
    claim_id: NetId64,
}

struct DecodedRenew<'a> {
    subject_kind: u8,
    subject: &'a [u8],
    claim_id: NetId64,
    expires_at_ms: u64,
}

enum DecodedContestFrame<'a> {
    Claim(DecodedClaim<'a>),
    Release(DecodedRelease<'a>),
    Renew(DecodedRenew<'a>),
}

fn encode_claim(
    subject_kind: u8,
    subject: &[u8],
    owner: &[u8],
    claimed_at_ms: u64,
    expires_at_ms: u64,
) -> Result<Bytes> {
    let total = CLAIM_HEADER_LEN + subject.len() + owner.len();
    if subject.len() > u16::MAX as usize
        || owner.len() > u16::MAX as usize
        || total > CONTEST_PAYLOAD_MAX
    {
        return Err(Error::ContestFrameTooLarge {
            subject_len: subject.len(),
            owner_len: owner.len(),
            max_payload: CONTEST_PAYLOAD_MAX,
        });
    }

    let mut buf = BytesMut::with_capacity(total);
    buf.put_u8(subject_kind);
    buf.put_u16_le(subject.len() as u16);
    buf.put_u16_le(owner.len() as u16);
    buf.put_u64_le(claimed_at_ms);
    buf.put_u64_le(expires_at_ms);
    buf.put_slice(subject);
    buf.put_slice(owner);
    Ok(buf.freeze())
}

fn encode_release(subject_kind: u8, subject: &[u8], claim_id: NetId64) -> Result<Bytes> {
    let total = RELEASE_HEADER_LEN + subject.len();
    if subject.len() > u16::MAX as usize || total > CONTEST_PAYLOAD_MAX {
        return Err(Error::ContestFrameTooLarge {
            subject_len: subject.len(),
            owner_len: 0,
            max_payload: CONTEST_PAYLOAD_MAX,
        });
    }

    let mut buf = BytesMut::with_capacity(total);
    buf.put_u64_le(claim_id.raw());
    buf.put_u8(subject_kind);
    buf.put_u16_le(subject.len() as u16);
    buf.put_slice(subject);
    Ok(buf.freeze())
}

fn encode_renew(
    subject_kind: u8,
    subject: &[u8],
    claim_id: NetId64,
    expires_at_ms: u64,
) -> Result<Bytes> {
    let total = RENEW_HEADER_LEN + subject.len();
    if subject.len() > u16::MAX as usize || total > CONTEST_PAYLOAD_MAX {
        return Err(Error::ContestFrameTooLarge {
            subject_len: subject.len(),
            owner_len: 0,
            max_payload: CONTEST_PAYLOAD_MAX,
        });
    }

    let mut buf = BytesMut::with_capacity(total);
    buf.put_u64_le(claim_id.raw());
    buf.put_u64_le(expires_at_ms);
    buf.put_u8(subject_kind);
    buf.put_u16_le(subject.len() as u16);
    buf.put_slice(subject);
    Ok(buf.freeze())
}

fn decode_frame(frame_kind: u8, payload: &Bytes) -> Option<DecodedContestFrame<'_>> {
    match frame_kind {
        CONTEST_FRAME_KIND_CLAIM => decode_claim(payload).map(DecodedContestFrame::Claim),
        CONTEST_FRAME_KIND_RELEASE => decode_release(payload).map(DecodedContestFrame::Release),
        CONTEST_FRAME_KIND_RENEW => decode_renew(payload).map(DecodedContestFrame::Renew),
        _ => None,
    }
}

fn decode_claim(payload: &Bytes) -> Option<DecodedClaim<'_>> {
    if payload.len() < CLAIM_HEADER_LEN {
        return None;
    }

    let subject_kind = payload[0];
    let subject_len = u16::from_le_bytes(payload[1..3].try_into().ok()?) as usize;
    let owner_len = u16::from_le_bytes(payload[3..5].try_into().ok()?) as usize;
    let claimed_at_ms = u64::from_le_bytes(payload[5..13].try_into().ok()?);
    let expires_at_ms = u64::from_le_bytes(payload[13..21].try_into().ok()?);
    let subject_start = CLAIM_HEADER_LEN;
    let subject_end = subject_start.checked_add(subject_len)?;
    let owner_end = subject_end.checked_add(owner_len)?;
    if payload.len() < owner_end {
        return None;
    }

    Some(DecodedClaim {
        subject_kind,
        subject: &payload[subject_start..subject_end],
        owner: &payload[subject_end..owner_end],
        claimed_at_ms,
        expires_at_ms,
    })
}

fn decode_release(payload: &Bytes) -> Option<DecodedRelease<'_>> {
    if payload.len() < RELEASE_HEADER_LEN {
        return None;
    }

    let claim_id = NetId64::from_raw(u64::from_le_bytes(payload[0..8].try_into().ok()?));
    let subject_kind = payload[8];
    let subject_len = u16::from_le_bytes(payload[9..11].try_into().ok()?) as usize;
    let subject_start = RELEASE_HEADER_LEN;
    let subject_end = subject_start.checked_add(subject_len)?;
    if payload.len() < subject_end {
        return None;
    }

    Some(DecodedRelease {
        subject_kind,
        subject: &payload[subject_start..subject_end],
        claim_id,
    })
}

fn decode_renew(payload: &Bytes) -> Option<DecodedRenew<'_>> {
    if payload.len() < RENEW_HEADER_LEN {
        return None;
    }

    let claim_id = NetId64::from_raw(u64::from_le_bytes(payload[0..8].try_into().ok()?));
    let expires_at_ms = u64::from_le_bytes(payload[8..16].try_into().ok()?);
    let subject_kind = payload[16];
    let subject_len = u16::from_le_bytes(payload[17..19].try_into().ok()?) as usize;
    let subject_start = RENEW_HEADER_LEN;
    let subject_end = subject_start.checked_add(subject_len)?;
    if payload.len() < subject_end {
        return None;
    }

    Some(DecodedRenew {
        subject_kind,
        subject: &payload[subject_start..subject_end],
        claim_id,
        expires_at_ms,
    })
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
    fn claim_succeeds_after_ring_wraps_past_capacity() {
        let fleet = Arc::new(Fleet::join("contest_wrap", 2).expect("fleet"));
        let claims = Contest::new(fleet);

        // Churn the shared contest ring well past its capacity so the write
        // head sits far above the window floor. Structural history aging must
        // NOT be mistaken for lost frames: a claim must still succeed (the
        // pre-fix code fail-closed here on every call once head > capacity).
        let mut now = 1_000u64;
        for _ in 0..700 {
            if let Claim::Claimed(g) = claims
                .try_claim_at::<OriginProbe>("churn", "w", Duration::from_secs(30), now)
                .expect("churn claim")
            {
                g.release().expect("churn release");
            }
            now += 1;
        }

        let fresh = claims
            .try_claim_at::<OtherProbe>("fresh", "winner", Duration::from_secs(30), now)
            .expect("fresh claim");
        let Claim::Claimed(guard) = fresh else {
            panic!("claim after the ring wrapped past capacity must be Claimed");
        };
        assert_eq!(guard.owner().as_str(), "winner");
    }
}
