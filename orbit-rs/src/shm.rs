//! POSIX shared-memory helpers — V1 substrate for cross-process rings.
//!
//! Wraps `shm_open` / `ftruncate` / `mmap` / `munmap` / `shm_unlink`
//! into a small, RAII-friendly API. Unix-only; Windows support is
//! a separate concern (Win32 named file mapping) that can land later.
//!
//! ## Naming
//!
//! Segments are named `/orbit-{fleet}-{kind}-{uid}` — fleet name from
//! the embedder, KIND from `OrbitTyped::KIND`, UID from `geteuid()`.
//! UID-scoping avoids the `/dev/shm` sticky-bit cross-user collision
//! problem (a stale segment owned by one user blocks another from
//! `shm_unlink`-ing it on next boot).
//!
//! ## Lifetime
//!
//! [`ShmRegion`] owns the mapped pointer and unmaps on drop. It does
//! NOT `shm_unlink` on drop — the segment lives until an explicit
//! [`ShmRegion::unlink`] call. This matches POSIX convention: a
//! segment with mapped users is not removed; `shm_unlink` only
//! prevents *new* opens, the current mapping stays valid until the
//! last process unmaps.

#![cfg(unix)]

use std::ffi::CString;
use std::io;
use std::ptr::NonNull;

/// A mapped POSIX SHM region. Drop unmaps; `unlink` removes the
/// underlying name (only the *creator* should call it on shutdown).
pub struct ShmRegion {
    name: CString,
    ptr: NonNull<u8>,
    len: usize,
    /// True when this handle was the one that *created* the segment
    /// (so it knows to `shm_unlink` if asked). Other attachers see
    /// `false`.
    created: bool,
}

impl ShmRegion {
    /// Open or create a shared-memory segment of `size` bytes,
    /// memory-mapped read/write. Idempotent: if the segment already
    /// exists with the same name and enough mapped bytes, it is reused
    /// (`created = false`). First creation does `ftruncate(size)`;
    /// later opens verify the existing object before mapping it. Some
    /// platforms report a page-rounded SHM size, so a larger `st_size`
    /// is valid; the owning data structure must verify its own header.
    pub fn open_or_create(name: &str, size: usize) -> io::Result<Self> {
        let cname = CString::new(name)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "shm name has nul byte"))?;

        // Try create-exclusive first; if it already exists, open.
        let (fd, created) = unsafe {
            // SAFETY: passing a valid C string and well-known POSIX flags.
            let fd = libc::shm_open(
                cname.as_ptr(),
                libc::O_RDWR | libc::O_CREAT | libc::O_EXCL,
                0o600,
            );
            if fd >= 0 {
                (fd, true)
            } else {
                // Could be EEXIST (already created by a peer) or another error.
                let err = io::Error::last_os_error();
                if err.raw_os_error() != Some(libc::EEXIST) {
                    return Err(err);
                }
                let fd = libc::shm_open(cname.as_ptr(), libc::O_RDWR, 0o600);
                if fd < 0 {
                    return Err(io::Error::last_os_error());
                }
                (fd, false)
            }
        };

        // Size the segment on first creation.
        if created {
            // SAFETY: fd is a valid POSIX fd we just received.
            let rc = unsafe { libc::ftruncate(fd, size as libc::off_t) };
            if rc != 0 {
                let err = io::Error::last_os_error();
                unsafe { libc::close(fd) };
                let _ = unsafe { libc::shm_unlink(cname.as_ptr()) };
                return Err(err);
            }
        }

        // Never mmap beyond the real SHM object: access past it can raise
        // SIGBUS. A larger reported size is valid on platforms (notably
        // macOS) that page-round POSIX SHM objects; callers verify their
        // own ABI metadata after mapping.
        let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
        let stat_rc = unsafe { libc::fstat(fd, stat.as_mut_ptr()) };
        if stat_rc != 0 {
            let err = io::Error::last_os_error();
            unsafe { libc::close(fd) };
            if created {
                let _ = unsafe { libc::shm_unlink(cname.as_ptr()) };
            }
            return Err(err);
        }
        let actual_size = unsafe { stat.assume_init() }.st_size;
        if actual_size < 0 || (actual_size as usize) < size {
            unsafe { libc::close(fd) };
            if created {
                let _ = unsafe { libc::shm_unlink(cname.as_ptr()) };
            }
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "SHM segment {name} size {actual_size} is smaller than requested mapping {size}"
                ),
            ));
        }

        // Memory-map the segment.
        // SAFETY: fd valid, size positive, flags well-known.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                size,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd,
                0,
            )
        };
        // The fd is no longer needed once mapped.
        unsafe { libc::close(fd) };

        if ptr == libc::MAP_FAILED {
            let err = io::Error::last_os_error();
            if created {
                let _ = unsafe { libc::shm_unlink(cname.as_ptr()) };
            }
            return Err(err);
        }

        // SAFETY: mmap returned a non-null pointer (we just checked).
        let ptr = NonNull::new(ptr.cast::<u8>()).expect("mmap returned non-null on success");

        Ok(Self {
            name: cname,
            ptr,
            len: size,
            created,
        })
    }

    /// Raw mapped pointer to the start of the region.
    pub fn as_ptr(&self) -> *mut u8 {
        self.ptr.as_ptr()
    }

    /// Length of the mapped region (the `size` passed to `open_or_create`).
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// True when this handle was the one that created the segment.
    /// Useful for picking which process performs first-time
    /// initialization of the header.
    pub fn created(&self) -> bool {
        self.created
    }

    /// Remove the underlying segment name. Existing mappings stay
    /// valid until each process drops its `ShmRegion`. Use only on
    /// shutdown / fleet teardown by the process that owns lifecycle.
    pub fn unlink(&self) -> io::Result<()> {
        // SAFETY: name is a valid C string.
        let rc = unsafe { libc::shm_unlink(self.name.as_ptr()) };
        if rc != 0 {
            let err = io::Error::last_os_error();
            // ENOENT is fine — segment was already unlinked.
            if err.raw_os_error() == Some(libc::ENOENT) {
                return Ok(());
            }
            return Err(err);
        }
        Ok(())
    }
}

impl Drop for ShmRegion {
    fn drop(&mut self) {
        // SAFETY: ptr came from mmap of `self.len` bytes; munmap is the inverse.
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.len);
        }
    }
}

// SAFETY: the underlying region is shared memory and synchronization
// happens at the slot level (atomic seq counters); the handle itself
// is just a pointer + length, safe to send/share.
unsafe impl Send for ShmRegion {}
unsafe impl Sync for ShmRegion {}

/// Build the conventional name for an Orbit ring segment.
pub fn ring_segment_name(fleet_name: &str, kind: u8) -> String {
    // SAFETY: `geteuid` always returns a value; no error path.
    let uid = unsafe { libc::geteuid() };
    format!("/orbit-{fleet_name}-{kind}-{uid}")
}
