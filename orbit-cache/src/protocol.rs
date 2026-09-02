use std::cmp::Ordering;

use bytes::{BufMut, Bytes, BytesMut};
use orbit_rs::{Frame, NetId64};

use crate::layout::CacheLayout;
use crate::{Error, Result};

const PROTOCOL_VERSION: u8 = 1;
pub(crate) const FRAME_KIND_PUT: u8 = 1;
pub(crate) const FRAME_KIND_DELETE: u8 = 2;
pub(crate) const FRAME_KIND_RESET: u8 = 3;
pub(crate) const FRAME_KIND_PAYLOAD: u8 = 1;

// version + key_len + expires_at + first_payload_id + payload_version +
// chunk_count + value_len
pub(crate) const MIN_MUTATION_HEADER_LEN: usize = 1 + 2 + 8 + 8 + 8 + 4 + 8;

/// Deterministic last-write-wins order for cache mutations from independent
/// node lanes.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct CacheRevision {
    pub sequence: u64,
    pub mutation_id: NetId64,
}

impl Ord for CacheRevision {
    fn cmp(&self, other: &Self) -> Ordering {
        self.sequence
            .cmp(&other.sequence)
            .then_with(|| self.mutation_id.raw().cmp(&other.mutation_id.raw()))
    }
}

impl PartialOrd for CacheRevision {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Address of one value retained in the payload ring.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PayloadRef {
    pub first_id: NetId64,
    pub payload_version: u64,
    pub chunk_count: u32,
    pub value_len: u64,
}

/// One cache fact decoded from the mutation ring.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CacheMutation {
    Put {
        key: Bytes,
        revision: CacheRevision,
        expires_at_ms: Option<u64>,
        payload: PayloadRef,
    },
    Delete {
        key: Bytes,
        revision: CacheRevision,
    },
    Reset {
        revision: CacheRevision,
    },
}

impl CacheMutation {
    pub fn revision(&self) -> CacheRevision {
        match self {
            Self::Put { revision, .. }
            | Self::Delete { revision, .. }
            | Self::Reset { revision } => *revision,
        }
    }
}

pub(crate) fn max_key_len<L: CacheLayout>() -> usize {
    L::MUTATION_RING_SPEC
        .payload_capacity
        .saturating_sub(MIN_MUTATION_HEADER_LEN)
        .min(u16::MAX as usize)
}

pub(crate) fn encode_put<L: CacheLayout>(
    key: &[u8],
    expires_at_ms: Option<u64>,
    payload: PayloadRef,
) -> Result<Bytes> {
    validate_key::<L>(key)?;
    let mut out = BytesMut::with_capacity(MIN_MUTATION_HEADER_LEN + key.len());
    out.put_u8(PROTOCOL_VERSION);
    out.put_u16_le(key.len() as u16);
    out.put_u64_le(expires_at_ms.unwrap_or(0));
    out.put_u64_le(payload.first_id.raw());
    out.put_u64_le(payload.payload_version);
    out.put_u32_le(payload.chunk_count);
    out.put_u64_le(payload.value_len);
    out.put_slice(key);
    Ok(out.freeze())
}

pub(crate) fn encode_delete<L: CacheLayout>(key: &[u8]) -> Result<Bytes> {
    validate_key::<L>(key)?;
    let mut out = BytesMut::with_capacity(MIN_MUTATION_HEADER_LEN + key.len());
    out.put_u8(PROTOCOL_VERSION);
    out.put_u16_le(key.len() as u16);
    out.put_u64_le(0);
    out.put_u64_le(0);
    out.put_u64_le(0);
    out.put_u32_le(0);
    out.put_u64_le(0);
    out.put_slice(key);
    Ok(out.freeze())
}

pub(crate) fn encode_reset() -> Bytes {
    let mut out = BytesMut::with_capacity(MIN_MUTATION_HEADER_LEN);
    out.put_u8(PROTOCOL_VERSION);
    out.put_u16_le(0);
    out.put_u64_le(0);
    out.put_u64_le(0);
    out.put_u64_le(0);
    out.put_u32_le(0);
    out.put_u64_le(0);
    out.freeze()
}

pub(crate) fn decode(frame: &Frame) -> Option<CacheMutation> {
    if frame.payload.len() < MIN_MUTATION_HEADER_LEN || frame.payload[0] != PROTOCOL_VERSION {
        return None;
    }

    let key_len = u16::from_le_bytes(frame.payload[1..3].try_into().ok()?) as usize;
    let expires_at_ms = u64::from_le_bytes(frame.payload[3..11].try_into().ok()?);
    let first_payload_id =
        NetId64::from_raw(u64::from_le_bytes(frame.payload[11..19].try_into().ok()?));
    let payload_version = u64::from_le_bytes(frame.payload[19..27].try_into().ok()?);
    let chunk_count = u32::from_le_bytes(frame.payload[27..31].try_into().ok()?);
    let value_len = u64::from_le_bytes(frame.payload[31..39].try_into().ok()?);
    let key_end = MIN_MUTATION_HEADER_LEN.checked_add(key_len)?;
    if key_end != frame.payload.len() {
        return None;
    }

    let revision = CacheRevision {
        sequence: frame.ver,
        mutation_id: frame.id,
    };
    let key = frame.payload.slice(MIN_MUTATION_HEADER_LEN..key_end);
    match frame.kind {
        FRAME_KIND_PUT
            if key_len > 0
                && first_payload_id.kind() != 0
                && chunk_count > 0
                && value_len <= usize::MAX as u64 =>
        {
            Some(CacheMutation::Put {
                key,
                revision,
                expires_at_ms: (expires_at_ms != 0).then_some(expires_at_ms),
                payload: PayloadRef {
                    first_id: first_payload_id,
                    payload_version,
                    chunk_count,
                    value_len,
                },
            })
        }
        FRAME_KIND_DELETE
            if key_len > 0
                && expires_at_ms == 0
                && first_payload_id.raw() == 0
                && payload_version == 0
                && chunk_count == 0
                && value_len == 0 =>
        {
            Some(CacheMutation::Delete { key, revision })
        }
        FRAME_KIND_RESET
            if key_len == 0
                && expires_at_ms == 0
                && first_payload_id.raw() == 0
                && payload_version == 0
                && chunk_count == 0
                && value_len == 0 =>
        {
            Some(CacheMutation::Reset { revision })
        }
        _ => None,
    }
}

fn validate_key<L: CacheLayout>(key: &[u8]) -> Result<()> {
    let max = max_key_len::<L>();
    if key.is_empty() || key.len() > max {
        return Err(Error::KeyTooLarge {
            key_len: key.len(),
            max,
        });
    }
    Ok(())
}
