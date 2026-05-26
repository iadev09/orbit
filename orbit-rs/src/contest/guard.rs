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

use crate::error::{Error, Result};
use crate::fleet::Fleet;
use crate::id::NetId64;
use crate::typed::OrbitTyped;

/// Contest frame payload limit for V0. On Unix this matches the SHM ring
/// slot payload size; non-Unix keeps the same bounded contract.
#[cfg(unix)]
pub const CONTEST_PAYLOAD_MAX: usize = crate::ring::shm::PAYLOAD_MAX;
#[cfg(not(unix))]
pub const CONTEST_PAYLOAD_MAX: usize = 256;

pub const CONTEST_RING_KIND: u8 = 222;
pub const CONTEST_FRAME_KIND_CLAIM: u8 = 1;
pub const CONTEST_FRAME_KIND_RELEASE: u8 = 2;

const CLAIM_HEADER_LEN: usize = 1 + 2 + 2 + 8 + 8;
const RELEASE_HEADER_LEN: usize = 8 + 1 + 2;

/// Dedicated ring record marker for contest frames.
#[derive(Clone, Debug)]
pub struct ContestRecord;

impl OrbitTyped for ContestRecord {
    // Hand-picked V0 kind. Build-time KIND allocation will replace
    // these manual values later.
    const KIND: u8 = CONTEST_RING_KIND;
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

        let Some(holder) = self.active_holder(&subject, now_ms) else {
            return Ok(Claim::YieldTo(Holder {
                claim_id,
                subject,
                owner,
                claimed_at_ms: now_ms,
                expires_at_ms,
            }));
        };

        if holder.claim_id.counter() == claim_id.counter() {
            Ok(Claim::Claimed(Guard::new(self.clone(), holder)))
        } else {
            let _ = self.release_id(&subject, claim_id, now_ms);
            Ok(Claim::YieldTo(holder))
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

    fn active_holder(&self, subject: &ContestSubject, now_ms: u64) -> Option<Holder> {
        let mut cursor = self.fleet.cursor_from_start::<ContestRecord>();
        let poll = self.fleet.poll_ring::<ContestRecord>(&mut cursor);
        let mut active = BTreeMap::<u64, Holder>::new();

        for frame in poll.frames {
            match decode_frame(frame.kind, &frame.payload) {
                Some(DecodedContestFrame::Claim(decoded))
                    if decoded.subject_kind == subject.kind
                        && decoded.subject == subject.as_bytes()
                        && decoded.expires_at_ms > now_ms =>
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
                Some(DecodedContestFrame::Release(decoded))
                    if decoded.subject_kind == subject.kind
                        && decoded.subject == subject.as_bytes() =>
                {
                    active.remove(&decoded.claim_id.counter());
                }
                _ => {}
            }
        }

        active.into_values().next()
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

    pub fn subject(&self) -> &ContestSubject {
        &self.holder.subject
    }

    pub fn owner(&self) -> &ContestOwner {
        &self.holder.owner
    }

    pub fn expires_at_ms(&self) -> u64 {
        self.holder.expires_at_ms
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

enum DecodedContestFrame<'a> {
    Claim(DecodedClaim<'a>),
    Release(DecodedRelease<'a>),
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

fn decode_frame(frame_kind: u8, payload: &Bytes) -> Option<DecodedContestFrame<'_>> {
    match frame_kind {
        CONTEST_FRAME_KIND_CLAIM => decode_claim(payload).map(DecodedContestFrame::Claim),
        CONTEST_FRAME_KIND_RELEASE => decode_release(payload).map(DecodedContestFrame::Release),
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
}
