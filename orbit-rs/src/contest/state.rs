//! Fixed-capacity current-state storage for [`super::guard::Contest`].
//!
//! Contest leases are state, not history. Each subject occupies at most one
//! open-addressed table slot; renew updates that slot and release tombstones
//! it. The SHM backend serializes mutations with a process-recoverable file
//! lock, while a process-local mutex covers sibling threads.

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use crate::error::{Error, Result};
use crate::fleet::NodeId;
use crate::id::NetId64;
#[cfg(unix)]
use crate::shm::{ShmRegion, ring_segment_name};

/// Fleet-scoped SHM kind reserved for the Contest state table.
/// It must not be reused by an [`crate::OrbitTyped`] ring.
pub const CONTEST_STATE_KIND: u8 = 222;

/// Maximum number of simultaneously resident Contest subjects.
pub const CONTEST_STATE_CAPACITY: usize = 1024;

/// Maximum combined bytes of a subject label and owner label.
///
/// This preserves the previous Contest V0 inline limit: the old 256-byte
/// claim frame spent 21 bytes on metadata, leaving 235 bytes for labels.
pub const CONTEST_STATE_PAYLOAD_MAX: usize = 235;

const CONTEST_COUNTER_MAX: u64 = 0xFF_FFFF_FFFF;
const SLOT_EMPTY: u8 = 0;
const SLOT_OCCUPIED: u8 = 1;
const SLOT_TOMBSTONE: u8 = 2;
const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

#[cfg(unix)]
const STATE_MAGIC: u32 = 0x4F_43_53_54; // "OCST"
#[cfg(unix)]
const STATE_VERSION: u16 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LeaseState {
    pub(crate) claim_id: NetId64,
    pub(crate) subject_kind: u8,
    pub(crate) subject: Vec<u8>,
    pub(crate) owner: Vec<u8>,
    pub(crate) claimed_at_ms: u64,
    pub(crate) expires_at_ms: u64,
}

pub(crate) enum StateClaim {
    Claimed(LeaseState),
    Occupied(LeaseState),
    BornExpired(LeaseState),
}

pub(crate) struct ContestStateStore {
    backend: StateBacking,
}

enum StateBacking {
    InMemory(Mutex<MemoryTable>),
    #[cfg(unix)]
    Shm(ShmStateStore),
}

impl ContestStateStore {
    pub(crate) fn in_memory() -> Self {
        Self {
            backend: StateBacking::InMemory(Mutex::new(MemoryTable::new())),
        }
    }

    #[cfg(unix)]
    pub(crate) fn shm(fleet_name: &str) -> Self {
        Self {
            backend: StateBacking::Shm(ShmStateStore::new(fleet_name)),
        }
    }

    pub(crate) fn claim(
        &self,
        node_id: NodeId,
        subject_kind: u8,
        subject: &[u8],
        owner: &[u8],
        claimed_at_ms: u64,
        expires_at_ms: u64,
    ) -> Result<StateClaim> {
        validate_labels(subject, owner)?;
        self.with_table(|header, slots| {
            claim(
                header,
                slots,
                ClaimInput {
                    node_id,
                    subject_kind,
                    subject,
                    owner,
                    claimed_at_ms,
                    expires_at_ms,
                },
            )
        })
    }

    pub(crate) fn renew(
        &self,
        subject_kind: u8,
        subject: &[u8],
        claim_id: NetId64,
        expires_at_ms: u64,
        now_ms: u64,
    ) -> Result<LeaseState> {
        validate_labels(subject, &[])?;
        self.with_table(|_header, slots| {
            renew(
                slots,
                subject_kind,
                subject,
                claim_id,
                expires_at_ms,
                now_ms,
            )
        })
    }

    pub(crate) fn release(
        &self,
        subject_kind: u8,
        subject: &[u8],
        claim_id: NetId64,
    ) -> Result<()> {
        validate_labels(subject, &[])?;
        self.with_table(|_header, slots| {
            release(slots, subject_kind, subject, claim_id);
            Ok(())
        })
    }

    pub(crate) fn reset(&self) -> Result<()> {
        self.with_table(|header, slots| {
            for slot in slots {
                slot.clear();
            }
            header.next_claim.store(0, Ordering::Relaxed);
            Ok(())
        })
    }

    #[cfg(unix)]
    pub(crate) fn unlink(&self) -> Result<()> {
        match &self.backend {
            StateBacking::InMemory(_) => self.reset(),
            StateBacking::Shm(store) => store.open()?.unlink().map_err(Error::Io),
        }
    }

    fn with_table<T>(
        &self,
        operation: impl FnOnce(&ContestStateHeader, &mut [ContestStateSlot]) -> Result<T>,
    ) -> Result<T> {
        match &self.backend {
            StateBacking::InMemory(table) => {
                let mut table = lock_unpoisoned(table);
                let MemoryTable { header, slots } = &mut *table;
                operation(header, slots)
            }
            #[cfg(unix)]
            StateBacking::Shm(store) => store.open()?.with_table(operation).map_err(Error::Io)?,
        }
    }
}

impl std::fmt::Debug for ContestStateStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContestStateStore")
            .field(
                "backing",
                &match self.backend {
                    StateBacking::InMemory(_) => "in-memory",
                    #[cfg(unix)]
                    StateBacking::Shm(_) => "shm",
                },
            )
            .finish()
    }
}

struct MemoryTable {
    header: ContestStateHeader,
    slots: Vec<ContestStateSlot>,
}

impl MemoryTable {
    fn new() -> Self {
        Self {
            header: ContestStateHeader::new(),
            slots: (0..CONTEST_STATE_CAPACITY)
                .map(|_| ContestStateSlot::empty())
                .collect(),
        }
    }
}

#[repr(C, align(64))]
struct ContestStateHeader {
    #[cfg(unix)]
    magic: u32,
    #[cfg(not(unix))]
    _magic: u32,
    #[cfg(unix)]
    version: u16,
    #[cfg(not(unix))]
    _version: u16,
    header_size: u16,
    capacity: u32,
    slot_size: u32,
    next_claim: AtomicU64,
    _reserved: [u8; 40],
}

impl ContestStateHeader {
    fn new() -> Self {
        Self {
            #[cfg(unix)]
            magic: STATE_MAGIC,
            #[cfg(not(unix))]
            _magic: 0,
            #[cfg(unix)]
            version: STATE_VERSION,
            #[cfg(not(unix))]
            _version: 0,
            header_size: std::mem::size_of::<Self>() as u16,
            capacity: CONTEST_STATE_CAPACITY as u32,
            slot_size: std::mem::size_of::<ContestStateSlot>() as u32,
            next_claim: AtomicU64::new(0),
            _reserved: [0; 40],
        }
    }
}

#[repr(C, align(64))]
struct ContestStateSlot {
    state: AtomicU8,
    subject_kind: u8,
    subject_len: u16,
    owner_len: u16,
    _reserved: u16,
    subject_hash: u64,
    claim_id: u64,
    claimed_at_ms: u64,
    expires_at_ms: u64,
    payload: [u8; CONTEST_STATE_PAYLOAD_MAX],
}

impl ContestStateSlot {
    fn empty() -> Self {
        Self {
            state: AtomicU8::new(SLOT_EMPTY),
            subject_kind: 0,
            subject_len: 0,
            owner_len: 0,
            _reserved: 0,
            subject_hash: 0,
            claim_id: 0,
            claimed_at_ms: 0,
            expires_at_ms: 0,
            payload: [0; CONTEST_STATE_PAYLOAD_MAX],
        }
    }

    fn state(&self) -> u8 {
        self.state.load(Ordering::Acquire)
    }

    fn subject(&self) -> &[u8] {
        &self.payload[..usize::from(self.subject_len)]
    }

    fn owner(&self) -> &[u8] {
        let start = usize::from(self.subject_len);
        let end = start + usize::from(self.owner_len);
        &self.payload[start..end]
    }

    fn matches(&self, hash: u64, subject_kind: u8, subject: &[u8]) -> bool {
        self.state() == SLOT_OCCUPIED
            && self.subject_hash == hash
            && self.subject_kind == subject_kind
            && self.subject() == subject
    }

    fn holder(&self) -> LeaseState {
        LeaseState {
            claim_id: NetId64::from_raw(self.claim_id),
            subject_kind: self.subject_kind,
            subject: self.subject().to_vec(),
            owner: self.owner().to_vec(),
            claimed_at_ms: self.claimed_at_ms,
            expires_at_ms: self.expires_at_ms,
        }
    }

    fn write(&mut self, hash: u64, holder: &LeaseState) {
        // A process dying during the rewrite leaves a tombstone, never a
        // partially committed live lease. The process lock itself is then
        // released by the kernel.
        self.state.store(SLOT_TOMBSTONE, Ordering::Release);
        self.subject_kind = holder.subject_kind;
        self.subject_len = holder.subject.len() as u16;
        self.owner_len = holder.owner.len() as u16;
        self.subject_hash = hash;
        self.claim_id = holder.claim_id.raw();
        self.claimed_at_ms = holder.claimed_at_ms;
        self.expires_at_ms = holder.expires_at_ms;
        self.payload.fill(0);
        self.payload[..holder.subject.len()].copy_from_slice(&holder.subject);
        self.payload[holder.subject.len()..holder.subject.len() + holder.owner.len()]
            .copy_from_slice(&holder.owner);
        self.state.store(SLOT_OCCUPIED, Ordering::Release);
    }

    fn tombstone(&self) {
        self.state.store(SLOT_TOMBSTONE, Ordering::Release);
    }

    fn clear(&self) {
        self.state.store(SLOT_EMPTY, Ordering::Release);
    }
}

struct ClaimInput<'a> {
    node_id: NodeId,
    subject_kind: u8,
    subject: &'a [u8],
    owner: &'a [u8],
    claimed_at_ms: u64,
    expires_at_ms: u64,
}

fn claim(
    header: &ContestStateHeader,
    slots: &mut [ContestStateSlot],
    input: ClaimInput<'_>,
) -> Result<StateClaim> {
    let hash = subject_hash(input.subject_kind, input.subject);
    let mut insertion = None;

    for offset in 0..slots.len() {
        let index = probe_index(hash, offset, slots.len());
        let slot = &mut slots[index];
        match slot.state() {
            SLOT_EMPTY => {
                insertion.get_or_insert(index);
                break;
            }
            SLOT_TOMBSTONE => {
                insertion.get_or_insert(index);
            }
            SLOT_OCCUPIED if slot.expires_at_ms <= input.claimed_at_ms => {
                slot.tombstone();
                insertion.get_or_insert(index);
            }
            SLOT_OCCUPIED if slot.matches(hash, input.subject_kind, input.subject) => {
                return Ok(StateClaim::Occupied(slot.holder()));
            }
            _ => {}
        }
    }

    let counter = mint_counter(header)?;
    let claim_id = NetId64::make(CONTEST_STATE_KIND, input.node_id.get(), counter);
    let holder = LeaseState {
        claim_id,
        subject_kind: input.subject_kind,
        subject: input.subject.to_vec(),
        owner: input.owner.to_vec(),
        claimed_at_ms: input.claimed_at_ms,
        expires_at_ms: input.expires_at_ms,
    };

    if input.expires_at_ms <= input.claimed_at_ms {
        return Ok(StateClaim::BornExpired(holder));
    }

    let index = insertion.ok_or(Error::ContestStateFull {
        capacity: slots.len(),
    })?;
    slots[index].write(hash, &holder);
    Ok(StateClaim::Claimed(holder))
}

fn renew(
    slots: &mut [ContestStateSlot],
    subject_kind: u8,
    subject: &[u8],
    claim_id: NetId64,
    requested_expires_at_ms: u64,
    now_ms: u64,
) -> Result<LeaseState> {
    let hash = subject_hash(subject_kind, subject);
    if let Some(index) = find_slot(slots, hash, subject_kind, subject) {
        let slot = &mut slots[index];
        if slot.claim_id == claim_id.raw() && slot.expires_at_ms > now_ms {
            let expires_at_ms = slot.expires_at_ms.max(requested_expires_at_ms);
            let holder = LeaseState {
                claim_id,
                subject_kind,
                subject: subject.to_vec(),
                owner: slot.owner().to_vec(),
                claimed_at_ms: slot.claimed_at_ms,
                expires_at_ms,
            };
            slot.write(hash, &holder);
            return Ok(holder);
        }
    }

    Err(Error::ContestLeaseLost { claim_id })
}

fn release(slots: &mut [ContestStateSlot], subject_kind: u8, subject: &[u8], claim_id: NetId64) {
    let hash = subject_hash(subject_kind, subject);
    if let Some(slot) = find_slot(slots, hash, subject_kind, subject).map(|index| &slots[index])
        && slot.claim_id == claim_id.raw()
    {
        slot.tombstone();
    }
}

fn find_slot(
    slots: &[ContestStateSlot],
    hash: u64,
    subject_kind: u8,
    subject: &[u8],
) -> Option<usize> {
    for offset in 0..slots.len() {
        let index = probe_index(hash, offset, slots.len());
        let slot = &slots[index];
        match slot.state() {
            SLOT_EMPTY => return None,
            SLOT_OCCUPIED if slot.matches(hash, subject_kind, subject) => return Some(index),
            _ => {}
        }
    }
    None
}

fn mint_counter(header: &ContestStateHeader) -> Result<u64> {
    let counter = header.next_claim.load(Ordering::Relaxed);
    if counter > CONTEST_COUNTER_MAX {
        return Err(Error::ContestIdExhausted);
    }
    header
        .next_claim
        .store(counter.saturating_add(1), Ordering::Relaxed);
    Ok(counter)
}

fn validate_labels(subject: &[u8], owner: &[u8]) -> Result<()> {
    if subject.len() > u16::MAX as usize
        || owner.len() > u16::MAX as usize
        || subject.len().saturating_add(owner.len()) > CONTEST_STATE_PAYLOAD_MAX
    {
        return Err(Error::ContestEntryTooLarge {
            subject_len: subject.len(),
            owner_len: owner.len(),
            max_payload: CONTEST_STATE_PAYLOAD_MAX,
        });
    }
    Ok(())
}

fn subject_hash(subject_kind: u8, subject: &[u8]) -> u64 {
    let mut hash = FNV_OFFSET_BASIS;
    hash ^= u64::from(subject_kind);
    hash = hash.wrapping_mul(FNV_PRIME);
    for byte in subject {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn probe_index(hash: u64, offset: usize, capacity: usize) -> usize {
    debug_assert!(capacity.is_power_of_two());
    (hash as usize).wrapping_add(offset) & (capacity - 1)
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|error| error.into_inner())
}

#[cfg(unix)]
struct ShmStateStore {
    name: String,
    opened: Mutex<Option<Arc<ShmContestTable>>>,
}

#[cfg(unix)]
impl ShmStateStore {
    fn new(fleet_name: &str) -> Self {
        Self {
            name: ring_segment_name(fleet_name, CONTEST_STATE_KIND),
            opened: Mutex::new(None),
        }
    }

    fn open(&self) -> Result<Arc<ShmContestTable>> {
        let mut opened = lock_unpoisoned(&self.opened);
        if let Some(table) = opened.as_ref() {
            return Ok(table.clone());
        }
        let table = Arc::new(ShmContestTable::open_or_create(&self.name).map_err(Error::Io)?);
        *opened = Some(table.clone());
        Ok(table)
    }
}

#[cfg(unix)]
struct ShmContestTable {
    region: ShmRegion,
    local_lock: Mutex<()>,
}

#[cfg(unix)]
impl ShmContestTable {
    fn open_or_create(name: &str) -> std::io::Result<Self> {
        use std::io;
        use std::ptr;

        let size = shm_segment_size();
        let (region, _initialization_lock) = ShmRegion::open_or_create_locked(name, size)?;
        if region.created() {
            unsafe {
                ptr::write(
                    region.as_ptr().cast::<ContestStateHeader>(),
                    ContestStateHeader::new(),
                );
                let slots = region
                    .as_ptr()
                    .add(std::mem::size_of::<ContestStateHeader>());
                ptr::write_bytes(
                    slots,
                    0,
                    CONTEST_STATE_CAPACITY * std::mem::size_of::<ContestStateSlot>(),
                );
            }
        } else {
            let header = unsafe { &*region.as_ptr().cast::<ContestStateHeader>() };
            if header.magic != STATE_MAGIC {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {name} has wrong magic 0x{:08X} (expected 0x{STATE_MAGIC:08X})",
                        header.magic
                    ),
                ));
            }
            if header.version != STATE_VERSION
                || usize::from(header.header_size) != std::mem::size_of::<ContestStateHeader>()
                || header.capacity as usize != CONTEST_STATE_CAPACITY
                || header.slot_size as usize != std::mem::size_of::<ContestStateSlot>()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("SHM segment {name} has an incompatible Contest state layout"),
                ));
            }
        }

        Ok(Self {
            region,
            local_lock: Mutex::new(()),
        })
    }

    fn with_table<T>(
        &self,
        operation: impl FnOnce(&ContestStateHeader, &mut [ContestStateSlot]) -> Result<T>,
    ) -> std::io::Result<Result<T>> {
        let _local = lock_unpoisoned(&self.local_lock);
        let _process = self.region.lock_exclusive()?;
        let header = unsafe { &*self.region.as_ptr().cast::<ContestStateHeader>() };
        let slots = unsafe {
            std::slice::from_raw_parts_mut(
                self.region
                    .as_ptr()
                    .add(std::mem::size_of::<ContestStateHeader>())
                    .cast::<ContestStateSlot>(),
                CONTEST_STATE_CAPACITY,
            )
        };
        Ok(operation(header, slots))
    }

    fn unlink(&self) -> std::io::Result<()> {
        self.region.unlink()
    }
}

#[cfg(unix)]
fn shm_segment_size() -> usize {
    std::mem::size_of::<ContestStateHeader>()
        + CONTEST_STATE_CAPACITY * std::mem::size_of::<ContestStateSlot>()
}

const _: () = assert!(CONTEST_STATE_CAPACITY.is_power_of_two());
const _: () = assert!(std::mem::size_of::<ContestStateHeader>() == 64);
const _: () = assert!(std::mem::size_of::<ContestStateSlot>() == 320);
