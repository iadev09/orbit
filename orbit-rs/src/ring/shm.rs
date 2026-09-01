//! `ShmRing` — POSIX SHM-backed ring buffer (V1 substrate).
//!
//! The runtime substrate that finally makes Orbit's "fleet sees each
//! other" promise real. Cross-process visibility is a memory-coherent
//! `mmap` with shared atomics; no kernel round-trips per access.
//!
//! ## Layout (single SHM segment per `OrbitTyped` kind)
//!
//! ```text
//! offset 0                              offset HEADER_SIZE
//! ┌──────────────────────────┬──────────────────────────────────────┐
//! │  ShmRingHeader (64B)     │  Slot[0] | Slot[1] | … | Slot[N-1]   │
//! │ magic/version/kind/spec  │  each lane-specific stride           │
//! │ capacity / write_pos     │                                      │
//! └──────────────────────────┴──────────────────────────────────────┘
//! ```
//!
//! ## Write protocol (LMAX-Disruptor-flavored, lock-free)
//!
//! 1. `counter = header.write_pos.fetch_add(1)` — claim a counter
//! 2. `slot = slots[counter % capacity]`
//! 3. `slot.seq = 2*counter + 1` (odd → writing)
//! 4. fill `id`, `kind`, `ver`, `payload_len`, `payload[..len]`
//! 5. `slot.seq = 2*counter + 2` (even → committed)
//!
//! ## Read protocol
//!
//! 1. read `seq_pre = slot.seq` (Acquire)
//! 2. if odd → writer in progress, return None / retry
//! 3. read all fields into locals
//! 4. read `seq_post = slot.seq` (Acquire)
//! 5. if `seq_pre != seq_post` → torn write, retry
//! 6. validate counter encoded in seq matches the one we want
//!
//! Tearing is detected, never silently merged. A reader that loses
//! the race against the writer simply retries (or returns None for
//! head-read).
//!
//! ## Per-kind payload size
//!
//! Every `OrbitTyped::KIND` declares its own `RingSpec`. The payload
//! capacity determines that SHM segment's slot stride; unrelated lanes
//! no longer pay for one global maximum. Capacity and payload capacity
//! are persisted in the header and verified by every attaching process.

#![cfg(unix)]

use std::ptr;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;

use crate::NodeId;
use crate::id::NetId64;
use crate::ring::{Frame, RingSpec};
use crate::shm::{self, ShmRegion};

// ─────────────────────────────────────────────────────────────────────
// On-disk (well, on-SHM) layout
// ─────────────────────────────────────────────────────────────────────

const MAGIC: u32 = 0x4F524254; // "ORBT" big-endian when displayed
const VERSION: u32 = 2;
const SLOT_ALIGNMENT: usize = 64;

/// Cache-line aligned to keep the header on its own line.
#[repr(C, align(64))]
struct ShmRingHeader {
    write_pos: AtomicU64,
    capacity: u64,
    magic: u32,
    version: u32,
    payload_capacity: u32,
    slot_stride: u32,
    kind: u8,
    /// Explicitly fills one cache line; field order avoids implicit
    /// alignment padding that would otherwise make this header 128B.
    _reserved: [u8; 31],
}

/// Fixed prefix of one dynamically-strided SHM slot.
#[repr(C)]
struct ShmSlotHeader {
    /// LMAX-style sequence: odd while a writer is mid-flight, even
    /// once committed. Reader checks seq before/after content read
    /// to detect torn writes.
    seq: AtomicU64,
    /// `NetId64::raw()` of the frame that occupies this slot.
    id: u64,
    /// `Frame::ver` — caller-supplied version / tick.
    ver: u64,
    /// Length of the meaningful prefix of `payload`.
    payload_len: u32,
    /// `Frame::kind` — the message-class byte (state/event/cmd/…).
    kind: u8,
    _reserved: [u8; 3],
}

const SLOT_HEADER_SIZE: usize = std::mem::size_of::<ShmSlotHeader>();
const HEADER_SIZE: usize = std::mem::size_of::<ShmRingHeader>();

const _: () = assert!(HEADER_SIZE == 64);
const _: () = assert!(SLOT_HEADER_SIZE == 32);

fn invalid_input(message: impl Into<String>) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidInput, message.into())
}

fn checked_layout(spec: RingSpec) -> std::io::Result<(usize, usize)> {
    if spec.capacity == 0 {
        return Err(invalid_input("ShmRing capacity must be > 0"));
    }
    if !spec.capacity.is_power_of_two() {
        return Err(invalid_input("ShmRing capacity must be a power of two"));
    }
    if spec.payload_capacity > u32::MAX as usize {
        return Err(invalid_input("ShmRing payload capacity must fit in u32"));
    }

    let unaligned = SLOT_HEADER_SIZE
        .checked_add(spec.payload_capacity)
        .ok_or_else(|| invalid_input("ShmRing slot size overflow"))?;
    let slot_stride = unaligned
        .checked_add(SLOT_ALIGNMENT - 1)
        .map(|value| value & !(SLOT_ALIGNMENT - 1))
        .ok_or_else(|| invalid_input("ShmRing slot stride overflow"))?;
    if slot_stride > u32::MAX as usize {
        return Err(invalid_input("ShmRing slot stride must fit in u32"));
    }
    let slots_size = spec
        .capacity
        .checked_mul(slot_stride)
        .ok_or_else(|| invalid_input("ShmRing segment size overflow"))?;
    let segment_size = HEADER_SIZE
        .checked_add(slots_size)
        .ok_or_else(|| invalid_input("ShmRing segment size overflow"))?;
    Ok((slot_stride, segment_size))
}

/// Compute the SHM segment size required by a ring spec.
pub fn segment_size_for_spec(spec: RingSpec) -> std::io::Result<usize> {
    checked_layout(spec).map(|(_, segment_size)| segment_size)
}

// ─────────────────────────────────────────────────────────────────────
// ShmRing
// ─────────────────────────────────────────────────────────────────────

/// Cross-process ring buffer backed by a POSIX SHM segment.
///
/// Use [`ShmRing::open_or_create`] to attach to (or create) the
/// segment by name. Multiple processes calling this with the same name and
/// matching [`RingSpec`] share the underlying memory; the first process to
/// call it does the one-time header initialization.
pub struct ShmRing {
    region: ShmRegion,
    kind: u8,
    capacity: usize,
    payload_capacity: usize,
    slot_stride: usize,
}

impl ShmRing {
    /// Open or create a SHM-backed ring under `fleet_name` for type
    /// kind `kind` with `spec`. The first process to call
    /// this initializes the header; later attachers reuse it.
    pub fn open_or_create(fleet_name: &str, kind: u8, spec: RingSpec) -> std::io::Result<Self> {
        let (slot_stride, size) = checked_layout(spec)?;
        let name = shm::ring_segment_name(fleet_name, kind);
        let region = ShmRegion::open_or_create(&name, size)?;

        // Initialize header on first creation; subsequent attachers
        // skip and rely on whatever the creator wrote.
        if region.created() {
            // SAFETY: region is mapped, header lives at offset 0,
            // size is right because we just created with this size.
            unsafe {
                let header_ptr = region.as_ptr() as *mut ShmRingHeader;
                ptr::write(
                    header_ptr,
                    ShmRingHeader {
                        write_pos: AtomicU64::new(0),
                        capacity: spec.capacity as u64,
                        magic: MAGIC,
                        version: VERSION,
                        payload_capacity: spec.payload_capacity as u32,
                        slot_stride: slot_stride as u32,
                        kind,
                        _reserved: [0; 31],
                    },
                );

                // Zero out the slot region so all `seq` values start
                // at 0 (== "never written, even, slot empty"). 0 is
                // valid as both a u64 atomic and as bytes for our
                // POD slot shape.
                let slots_ptr = region.as_ptr().add(HEADER_SIZE);
                ptr::write_bytes(slots_ptr, 0, spec.capacity * slot_stride);
            }
        } else {
            // Attaching to an existing segment — sanity-check the header.
            // SAFETY: region is mapped; header lives at offset 0.
            let header = unsafe { &*(region.as_ptr() as *const ShmRingHeader) };
            if header.magic != MAGIC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} has wrong magic 0x{:08X} (expected 0x{:08X})",
                        name, header.magic, MAGIC
                    ),
                ));
            }
            if header.version != VERSION {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} version {} != local {}",
                        name, header.version, VERSION
                    ),
                ));
            }
            if header.kind != kind {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} kind {} != requested {}",
                        name, header.kind, kind
                    ),
                ));
            }
            if header.capacity as usize != spec.capacity {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} capacity {} != requested {}",
                        name, header.capacity, spec.capacity
                    ),
                ));
            }
            if header.payload_capacity as usize != spec.payload_capacity {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} payload capacity {} != requested {}",
                        name, header.payload_capacity, spec.payload_capacity
                    ),
                ));
            }
            if header.slot_stride as usize != slot_stride {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "SHM segment {} slot stride {} != requested {}",
                        name, header.slot_stride, slot_stride
                    ),
                ));
            }
        }

        Ok(Self {
            region,
            kind,
            capacity: spec.capacity,
            payload_capacity: spec.payload_capacity,
            slot_stride,
        })
    }

    /// True when this handle was the one that *created* the SHM
    /// segment (vs attaching to an already-existing one).
    pub fn created(&self) -> bool {
        self.region.created()
    }

    /// Remove the SHM segment name. Existing mappings stay valid;
    /// new opens will fail until `open_or_create` recreates it.
    /// Call from the owner process at fleet shutdown.
    pub fn unlink(&self) -> std::io::Result<()> {
        self.region.unlink()
    }

    /// The KIND byte this ring carries.
    pub fn kind(&self) -> u8 {
        self.kind
    }

    /// Total slot count.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Maximum inline payload bytes for this ring lane.
    pub fn payload_capacity(&self) -> usize {
        self.payload_capacity
    }

    pub fn spec(&self) -> RingSpec {
        RingSpec::new(self.capacity, self.payload_capacity)
    }

    fn header(&self) -> &ShmRingHeader {
        // SAFETY: header is the first thing in the region, mapped
        // for the lifetime of `self`, and the region is at least
        // HEADER_SIZE bytes (we asked for that).
        unsafe { &*(self.region.as_ptr() as *const ShmRingHeader) }
    }

    fn slot_ptr(&self, idx: usize) -> *mut ShmSlotHeader {
        debug_assert!(idx < self.capacity);
        // SAFETY: slots region begins at HEADER_SIZE; idx is bounded.
        unsafe {
            let base = self.region.as_ptr().add(HEADER_SIZE);
            base.add(idx * self.slot_stride) as *mut ShmSlotHeader
        }
    }

    unsafe fn payload_ptr(slot_ptr: *mut ShmSlotHeader) -> *mut u8 {
        unsafe { slot_ptr.cast::<u8>().add(SLOT_HEADER_SIZE) }
    }

    /// Monotonic head — number of writes ever performed on this ring.
    pub fn head(&self) -> u64 {
        self.header().write_pos.load(Ordering::Acquire)
    }

    /// Append a frame. Atomically reserves the next counter, mints
    /// the [`NetId64`], writes the slot. Returns the minted id.
    pub fn write(
        &self,
        node_id: NodeId,
        frame_kind: u8,
        ver: u64,
        payload: Bytes,
    ) -> std::io::Result<NetId64> {
        if payload.len() > self.payload_capacity {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "payload {} > ring payload capacity {}",
                    payload.len(),
                    self.payload_capacity
                ),
            ));
        }

        let counter = self.header().write_pos.fetch_add(1, Ordering::AcqRel);
        let id = NetId64::make(self.kind, node_id.get(), counter);
        let slot_idx = (counter as usize) & (self.capacity - 1);
        let slot_ptr = self.slot_ptr(slot_idx);

        // Disruptor-style write: seq goes odd → write content → seq goes even.
        // The atomic store ordering pairs with the reader's Acquire.
        unsafe {
            let slot = &*slot_ptr;
            let mid_seq = counter
                .checked_mul(2)
                .and_then(|v| v.checked_add(1))
                .expect("seq overflow");
            let final_seq = mid_seq.wrapping_add(1);

            slot.seq.store(mid_seq, Ordering::Release);

            // Now write content fields. We use raw writes through
            // the mut pointer to bypass &-borrow rules; nothing else
            // touches this slot until we publish via the second seq.
            let slot_mut = slot_ptr;
            ptr::addr_of_mut!((*slot_mut).id).write(id.raw());
            ptr::addr_of_mut!((*slot_mut).ver).write(ver);
            let len = payload.len();
            ptr::addr_of_mut!((*slot_mut).payload_len).write(len as u32);
            ptr::addr_of_mut!((*slot_mut).kind).write(frame_kind);
            let payload_ptr = Self::payload_ptr(slot_mut);
            ptr::copy_nonoverlapping(payload.as_ptr(), payload_ptr, len);

            slot.seq.store(final_seq, Ordering::Release);
        }

        Ok(id)
    }

    /// Read the slot whose counter matches `id.counter()`. Returns
    /// `None` if the slot has been overwritten, was never written,
    /// or a torn read could not be reconciled across two retries.
    pub fn read(&self, id: NetId64) -> Option<Frame> {
        if id.kind() != self.kind {
            return None;
        }
        let counter = id.counter();
        let slot_idx = (counter as usize) & (self.capacity - 1);
        let slot_ptr = self.slot_ptr(slot_idx);

        // Two retries — torn writes happen but resolve quickly.
        for _ in 0..3 {
            let Some(frame) = (unsafe { read_committed_frame(slot_ptr, self.payload_capacity) })
            else {
                continue;
            };
            if frame.id.counter() == counter {
                return Some(frame);
            } else {
                // Slot now holds a different (later) id — wraparound.
                return None;
            }
        }
        None
    }

    /// Read the most recent frame (head - 1). Returns `None` if no
    /// write has happened yet, or a torn read could not resolve.
    pub fn read_head(&self) -> Option<Frame> {
        let head = self.head();
        if head == 0 {
            return None;
        }
        let counter = head - 1;
        let slot_idx = (counter as usize) & (self.capacity - 1);
        let slot_ptr = self.slot_ptr(slot_idx);

        for _ in 0..3 {
            if let Some(frame) = unsafe { read_committed_frame(slot_ptr, self.payload_capacity) } {
                return Some(frame);
            }
        }
        None
    }

    /// Read whatever frame currently occupies the slot at
    /// `counter % capacity`, regardless of which counter is stored
    /// there. Used by walking readers that need slot-by-slot access
    /// without knowing the writer's `NetId64` ahead of time.
    pub fn read_at(&self, counter: u64) -> Option<Frame> {
        let slot_idx = (counter as usize) & (self.capacity - 1);
        let slot_ptr = self.slot_ptr(slot_idx);
        for _ in 0..3 {
            if let Some(frame) = unsafe { read_committed_frame(slot_ptr, self.payload_capacity) } {
                return Some(frame);
            }
        }
        None
    }

    /// Clear all slots and reset the write head to zero.
    ///
    /// Intended for owner-controlled boot-time cleanup. Do not call
    /// while other processes are publishing to this ring: it rewrites
    /// the shared slot memory in place.
    pub fn reset(&self) {
        // SAFETY: the region is mapped and the slot area begins at
        // HEADER_SIZE. The caller must ensure the ring is quiescent.
        unsafe {
            let slots_ptr = self.region.as_ptr().add(HEADER_SIZE);
            ptr::write_bytes(slots_ptr, 0, self.capacity * self.slot_stride);
        }
        self.header().write_pos.store(0, Ordering::Release);
    }
}

// ─────────────────────────────────────────────────────────────────────
// ShmRingRegistry — per-fleet, per-KIND map of ShmRings
// ─────────────────────────────────────────────────────────────────────

/// Type-keyed registry of [`ShmRing`]s held by a fleet. One ring per
/// `OrbitTyped::KIND` byte; created on demand the first time a kind
/// is published or queried.
pub struct ShmRingRegistry {
    fleet_name: String,
    rings: dashmap::DashMap<u8, std::sync::Arc<ShmRing>>,
}

impl ShmRingRegistry {
    pub fn new(fleet_name: impl Into<String>) -> Self {
        Self {
            fleet_name: fleet_name.into(),
            rings: dashmap::DashMap::new(),
        }
    }

    /// Get-or-create the SHM ring for `kind`. Failure here means the
    /// SHM open or attach failed (permissions, name too long, etc.)
    /// and is propagated as `io::Error`.
    pub fn get_or_create_for<T: crate::OrbitTyped>(
        &self,
    ) -> std::io::Result<std::sync::Arc<ShmRing>> {
        if let Some(entry) = self.rings.get(&T::KIND) {
            if entry.spec() != T::RING_SPEC {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "OrbitTyped KIND {} was reused with ring spec {:?}; existing spec is {:?}",
                        T::KIND,
                        T::RING_SPEC,
                        entry.spec()
                    ),
                ));
            }
            return Ok(entry.clone());
        }
        let ring = std::sync::Arc::new(ShmRing::open_or_create(
            &self.fleet_name,
            T::KIND,
            T::RING_SPEC,
        )?);
        let entry = self.rings.entry(T::KIND).or_insert_with(|| ring.clone());
        if entry.spec() != T::RING_SPEC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "OrbitTyped KIND {} raced with ring spec {:?}; installed spec is {:?}",
                    T::KIND,
                    T::RING_SPEC,
                    entry.spec()
                ),
            ));
        }
        Ok(entry.clone())
    }

    /// Look up a ring that has already been created for `kind`.
    /// Returns `None` if no such ring exists yet.
    pub fn lookup(&self, kind: u8) -> Option<std::sync::Arc<ShmRing>> {
        self.rings.get(&kind).map(|e| e.clone())
    }
}

/// Read a slot's content; returns `None` if the seq indicates an
/// in-flight write or if pre/post seqs disagree (torn read).
///
/// # Safety
///
/// `slot_ptr` must point at a valid `ShmSlot` mapped into our
/// address space and aligned per the `repr(C, align(64))` layout.
unsafe fn read_committed_frame(
    slot_ptr: *mut ShmSlotHeader,
    payload_capacity: usize,
) -> Option<Frame> {
    let slot = unsafe { &*slot_ptr };
    let seq_pre = slot.seq.load(Ordering::Acquire);
    if seq_pre == 0 {
        // never written
        return None;
    }
    if seq_pre & 1 == 1 {
        // writer in progress
        return None;
    }

    // Read content fields.
    let id = NetId64::from_raw(unsafe { ptr::addr_of!((*slot_ptr).id).read() });
    let kind = unsafe { ptr::addr_of!((*slot_ptr).kind).read() };
    let ver = unsafe { ptr::addr_of!((*slot_ptr).ver).read() };
    let payload_len = unsafe { ptr::addr_of!((*slot_ptr).payload_len).read() } as usize;
    if payload_len > payload_capacity {
        // corrupt — bail
        return None;
    }
    let payload_src = unsafe { slot_ptr.cast::<u8>().add(SLOT_HEADER_SIZE) as *const u8 };
    let mut payload_buf = vec![0u8; payload_len];
    unsafe { ptr::copy_nonoverlapping(payload_src, payload_buf.as_mut_ptr(), payload_len) };

    let seq_post = slot.seq.load(Ordering::Acquire);
    if seq_pre != seq_post {
        // torn write — caller can retry
        return None;
    }

    Some(Frame {
        id,
        kind,
        ver,
        payload: Bytes::from(payload_buf),
    })
}
