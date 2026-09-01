//! `OrbitCache` — fleet-shared binary K,V cache over the Orbit ring.
//!
//! This is the raw primitive layer: keys and values are bytes, the
//! shared truth is the fleet ring, and higher layers decide how bytes
//! become application structs or FFI payloads.
//!
//! ## V0 shape
//!
//! Each mutation is one ring frame:
//!
//! ```text
//! [ op:u8 ][ key_len:u16 LE ][ value_len:u16 LE ][ expires_at_ms:u64 LE ]
//! [ key bytes ][ value bytes ]
//! ```
//!
//! Reads walk the ring backwards and stop at the newest frame for the
//! scoped key. A delete frame shadows older puts. A reset frame shadows
//! older puts/deletes for the cache prefix. An expired put is a miss
//! and also shadows older puts, matching normal cache TTL semantics.
//!
//! ## Why no typed values here
//!
//! `orbit-rs` is framework-agnostic. It does not know application
//! object models, external cache APIs, FFI values, or serde choices. It
//! only preserves bytes and coarse cache semantics. Typed decode/L1
//! lives in adapter crates above `orbit-rs`.
//!
//! ## Deferred substrate
//!
//! V0 stores the value inline in the ring frame, so payloads are small
//! and bounded by the SHM ring payload size. The eventual larger-value
//! shape is ring-as-mutation-log plus an indexed SHM arena. The public
//! byte-oriented API is intended to survive that swap.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::{BufMut, Bytes, BytesMut};

use crate::OrbitTyped;
use crate::error::{Error, Result};
use crate::fleet::Fleet;
use crate::id::NetId64;

/// Cache frame payload limit for V0. On Unix this matches the SHM
/// ring's fixed slot payload size; non-Unix keeps the same contract so
/// tests and callers do not accidentally rely on unbounded in-memory
/// frames.
#[cfg(unix)]
pub const CACHE_PAYLOAD_MAX: usize = crate::ring::shm::PAYLOAD_MAX;
#[cfg(not(unix))]
pub const CACHE_PAYLOAD_MAX: usize = 256;

const HEADER_LEN: usize = 1 + 2 + 2 + 8;
const OP_PUT: u8 = 1;
const OP_DELETE: u8 = 2;
const OP_RESET: u8 = 3;

/// Dedicated ring kind for raw cache mutations.
#[derive(Clone, Debug)]
struct OrbitCacheRecord;

impl OrbitTyped for OrbitCacheRecord {
    // Hand-picked V0 kind. Build-time KIND allocation will replace
    // these manual values later.
    const KIND: u8 = 200;
}

/// Fleet-shared binary cache. Cheap to clone.
#[derive(Clone)]
pub struct OrbitCache {
    fleet: Arc<Fleet>,
    prefix: Arc<str>,
}

/// Decoded cache entry returned by [`OrbitCache::get_entry`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OrbitCacheEntry {
    pub value: Bytes,
    pub epoch: u64,
    pub expires_at_ms: Option<u64>,
}

/// Honest read result: either the newest matching frame is a value, or
/// the key is currently absent. `epoch` is the ring counter of the
/// newest matching frame when one exists, useful for local L1
/// invalidation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OrbitCacheRead {
    Hit(OrbitCacheEntry),
    Miss { epoch: Option<u64> },
}

impl OrbitCacheRead {
    pub fn into_value(self) -> Option<Vec<u8>> {
        match self {
            Self::Hit(entry) => Some(entry.value.to_vec()),
            Self::Miss { .. } => None,
        }
    }

    pub fn epoch(&self) -> Option<u64> {
        match self {
            Self::Hit(entry) => Some(entry.epoch),
            Self::Miss { epoch } => *epoch,
        }
    }
}

impl OrbitCache {
    /// Build a cache without a key prefix.
    pub fn new(fleet: Arc<Fleet>) -> Self {
        Self::with_prefix(fleet, "")
    }

    /// Build a cache that prepends `prefix` to every key before it
    /// hits the wire. Prefixes are byte identity, not display labels;
    /// use them for namespaces such as `php:apcu:` or `rust:User:`.
    pub fn with_prefix(fleet: Arc<Fleet>, prefix: impl Into<Arc<str>>) -> Self {
        Self {
            fleet,
            prefix: prefix.into(),
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Store `value` under `key`. `ttl = None` means forever until a
    /// later put/delete shadows it.
    pub fn put(&self, key: &str, value: &[u8], ttl: Option<Duration>) -> Result<NetId64> {
        let expires_at_ms = ttl
            .map(|ttl| now_ms().saturating_add(duration_ms(ttl)))
            .unwrap_or(0);
        let payload = encode_frame(
            OP_PUT,
            self.scoped_key(key).as_bytes(),
            value,
            expires_at_ms,
        )?;
        Ok(self
            .fleet
            .publish::<OrbitCacheRecord>(OP_PUT, expires_at_ms, payload))
    }

    /// Delete `key` by publishing a tombstone frame.
    pub fn delete(&self, key: &str) -> Result<NetId64> {
        let payload = encode_frame(OP_DELETE, self.scoped_key(key).as_bytes(), &[], 0)?;
        Ok(self
            .fleet
            .publish::<OrbitCacheRecord>(OP_DELETE, 0, payload))
    }

    /// Reset the current cache prefix by publishing a namespace
    /// tombstone. Existing frames remain in the ring, but reads treat
    /// older matching entries as absent.
    pub fn reset(&self) -> Result<NetId64> {
        let payload = encode_frame(OP_RESET, self.prefix.as_bytes(), &[], 0)?;
        Ok(self.fleet.publish::<OrbitCacheRecord>(OP_RESET, 0, payload))
    }

    /// Read the current value bytes, if present and unexpired.
    pub fn get(&self, key: &str) -> Option<Vec<u8>> {
        self.get_entry(key).into_value()
    }

    /// Read with epoch metadata for adapter-level L1 caches.
    pub fn get_entry(&self, key: &str) -> OrbitCacheRead {
        let scoped = self.scoped_key(key);
        let expected_key = scoped.as_bytes();
        let head = self.fleet.head::<OrbitCacheRecord>();
        if head == 0 {
            return OrbitCacheRead::Miss { epoch: None };
        }

        let capacity = self.fleet.ring_capacity::<OrbitCacheRecord>() as u64;
        let walk_count = head.min(capacity);
        let now = now_ms();

        for i in 0..walk_count {
            let counter = head - 1 - i;
            let Some(frame) = self.fleet.read_at::<OrbitCacheRecord>(counter) else {
                if counter == 0 {
                    break;
                }
                continue;
            };
            let Some(decoded) = decode_frame(&frame.payload) else {
                if counter == 0 {
                    break;
                }
                continue;
            };
            if decoded.op == OP_RESET && expected_key.starts_with(decoded.key) {
                return OrbitCacheRead::Miss {
                    epoch: Some(frame.id.counter()),
                };
            }
            if decoded.key != expected_key {
                if counter == 0 {
                    break;
                }
                continue;
            }

            let epoch = frame.id.counter();
            return match decoded.op {
                OP_PUT if decoded.expires_at_ms != 0 && decoded.expires_at_ms <= now => {
                    OrbitCacheRead::Miss { epoch: Some(epoch) }
                }
                OP_PUT => OrbitCacheRead::Hit(OrbitCacheEntry {
                    value: frame.payload.slice(decoded.value_start..decoded.value_end),
                    epoch,
                    expires_at_ms: (decoded.expires_at_ms != 0).then_some(decoded.expires_at_ms),
                }),
                OP_DELETE => OrbitCacheRead::Miss { epoch: Some(epoch) },
                _ => OrbitCacheRead::Miss { epoch: Some(epoch) },
            };
        }

        OrbitCacheRead::Miss { epoch: None }
    }

    fn scoped_key(&self, key: &str) -> String {
        if self.prefix.is_empty() {
            key.to_string()
        } else {
            format!("{}{}", self.prefix, key)
        }
    }
}

struct DecodedFrame<'a> {
    op: u8,
    key: &'a [u8],
    value_start: usize,
    value_end: usize,
    expires_at_ms: u64,
}

fn encode_frame(op: u8, key: &[u8], value: &[u8], expires_at_ms: u64) -> Result<Bytes> {
    let total = HEADER_LEN + key.len() + value.len();
    if key.len() > u16::MAX as usize || value.len() > u16::MAX as usize || total > CACHE_PAYLOAD_MAX
    {
        return Err(Error::CacheFrameTooLarge {
            key_len: key.len(),
            value_len: value.len(),
            max_payload: CACHE_PAYLOAD_MAX,
        });
    }

    let mut buf = BytesMut::with_capacity(total);
    buf.put_u8(op);
    buf.put_u16_le(key.len() as u16);
    buf.put_u16_le(value.len() as u16);
    buf.put_u64_le(expires_at_ms);
    buf.put_slice(key);
    buf.put_slice(value);
    Ok(buf.freeze())
}

fn decode_frame(payload: &Bytes) -> Option<DecodedFrame<'_>> {
    if payload.len() < HEADER_LEN {
        return None;
    }

    let op = payload[0];
    let key_len = u16::from_le_bytes(payload[1..3].try_into().ok()?) as usize;
    let value_len = u16::from_le_bytes(payload[3..5].try_into().ok()?) as usize;
    let expires_at_ms = u64::from_le_bytes(payload[5..13].try_into().ok()?);
    let key_start = HEADER_LEN;
    let key_end = key_start.checked_add(key_len)?;
    let value_end = key_end.checked_add(value_len)?;
    if payload.len() < value_end {
        return None;
    }

    Some(DecodedFrame {
        op,
        key: &payload[key_start..key_end],
        value_start: key_end,
        value_end,
        expires_at_ms,
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

fn duration_ms(ttl: Duration) -> u64 {
    ttl.as_millis().max(1).min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::OrbitCache;
    use crate::Fleet;

    #[test]
    fn reset_shadows_previous_values_inside_prefix() {
        let fleet = Fleet::join("cache_reset", 1).expect("fleet").into();
        let cache = OrbitCache::with_prefix(fleet, "php:");

        cache.put("token", b"old", None).expect("put");
        assert_eq!(cache.get("token"), Some(b"old".to_vec()));

        cache.reset().expect("reset");
        assert_eq!(cache.get("token"), None);

        cache.put("token", b"new", None).expect("put after reset");
        assert_eq!(cache.get("token"), Some(b"new".to_vec()));
    }
}
