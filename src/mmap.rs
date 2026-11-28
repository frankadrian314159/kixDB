//! Cross-platform read-only file memory mapping (`Mmap`), with `FILE_SHARE_DELETE` on Windows so mapped files can be
//! replaced atomically while concurrent snapshot readers finish.
use std::fmt;
use std::fs;
use std::io;
use std::ops::Deref;
use std::path::Path;
use std::ptr::NonNull;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Mmap {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: `Mmap` is an immutable read-only OS view of file pages, valid until `Drop` unmaps it.
unsafe impl Send for Mmap {}
unsafe impl Sync for Mmap {}

impl fmt::Debug for Mmap {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Mmap({} bytes)", self.len)
    }
}

impl Deref for Mmap {
    type Target = [u8];
    #[inline]
    fn deref(&self) -> &[u8] {
        if self.len == 0 {
            &[]
        } else {
            // SAFETY: `ptr` points to `len` read-only mapped bytes valid for `&self`'s lifetime.
            unsafe { std::slice::from_raw_parts(self.ptr.as_ptr(), self.len) }
        }
    }
}

#[cfg(windows)]
mod sys {
    use super::*;
    use std::ffi::c_void;
    use std::os::windows::fs::OpenOptionsExt;
    use std::os::windows::io::AsRawHandle;

    type Handle = *mut c_void;
    const PAGE_READONLY: u32 = 0x02;
    const FILE_MAP_READ: u32 = 0x04;
    const SHARE_ALL: u32 = 0x01 | 0x02 | 0x04; // READ | WRITE | DELETE

    extern "system" {
        fn CreateFileMappingW(
            file: Handle,
            attrs: *const c_void,
            protect: u32,
            max_size_high: u32,
            max_size_low: u32,
            name: *const u16,
        ) -> Handle;
        fn MapViewOfFile(mapping: Handle, access: u32, offset_high: u32, offset_low: u32, bytes: usize) -> *mut c_void;
        fn UnmapViewOfFile(addr: *const c_void) -> i32;
        fn CloseHandle(handle: Handle) -> i32;
    }

    pub fn map(path: &Path) -> io::Result<(NonNull<u8>, usize)> {
        let file = fs::OpenOptions::new().read(true).share_mode(SHARE_ALL).open(path)?;
        let len = usize::try_from(file.metadata()?.len()).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "file too large"))?;
        if len == 0 {
            return Ok((NonNull::dangling(), 0));
        }
        // SAFETY: `file` is a valid open handle; `CloseHandle(mapping)` is safe immediately after `MapViewOfFile`.
        unsafe {
            let mapping = CreateFileMappingW(file.as_raw_handle() as Handle, std::ptr::null(), PAGE_READONLY, 0, 0, std::ptr::null());
            if mapping.is_null() {
                return Err(io::Error::last_os_error());
            }
            let view = MapViewOfFile(mapping, FILE_MAP_READ, 0, 0, len);
            CloseHandle(mapping);
            let ptr = NonNull::new(view.cast::<u8>()).ok_or_else(io::Error::last_os_error)?;
            Ok((ptr, len))
        }
    }

    pub unsafe fn unmap(ptr: NonNull<u8>, len: usize) {
        if len > 0 {
            UnmapViewOfFile(ptr.as_ptr().cast());
        }
    }
}

#[cfg(unix)]
mod sys {
    use super::*;
    use std::ffi::c_void;
    use std::fs::File;
    use std::os::fd::AsRawFd;

    #[cfg(target_pointer_width = "64")]
    type OffT = i64;
    #[cfg(not(target_pointer_width = "64"))]
    type OffT = isize;

    const PROT_READ: i32 = 1;
    const MAP_PRIVATE: i32 = 2;

    extern "C" {
        fn mmap(addr: *mut c_void, len: usize, prot: i32, flags: i32, fd: i32, offset: OffT) -> *mut c_void;
        fn munmap(addr: *mut c_void, len: usize) -> i32;
    }

    pub fn map(path: &Path) -> io::Result<(NonNull<u8>, usize)> {
        let file = File::open(path)?;
        let len = usize::try_from(file.metadata()?.len()).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "file too large"))?;
        if len == 0 {
            return Ok((NonNull::dangling(), 0));
        }
        // SAFETY: `file` is open for reading of length `len > 0`; `MAP_PRIVATE` maps read-only pages.
        unsafe {
            let view = mmap(std::ptr::null_mut(), len, PROT_READ, MAP_PRIVATE, file.as_raw_fd(), 0);
            if view == (!0usize as *mut c_void) || view.is_null() {
                return Err(io::Error::last_os_error());
            }
            Ok((NonNull::new_unchecked(view.cast::<u8>()), len))
        }
    }

    pub unsafe fn unmap(ptr: NonNull<u8>, len: usize) {
        if len > 0 {
            munmap(ptr.as_ptr().cast(), len);
        }
    }
}

impl Mmap {
    pub fn open(path: &Path) -> io::Result<Self> {
        let (ptr, len) = sys::map(path)?;
        Ok(Self { ptr, len })
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        // SAFETY: `self.ptr` and `self.len` were returned by `sys::map`.
        unsafe { sys::unmap(self.ptr, self.len) }
    }
}

static REPLACE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Write `data` to `path`, moving aside any actively mapped file on Windows (`ERROR_USER_MAPPED_FILE = 1224`) first.
pub fn write_file_replacing(path: &Path, data: &[u8]) -> io::Result<()> {
    match fs::write(path, data) {
        Ok(()) => Ok(()),
        Err(e) if cfg!(windows) && path.exists() && (e.raw_os_error() == Some(1224) || e.kind() == io::ErrorKind::PermissionDenied) => {
            let seq = REPLACE_SEQ.fetch_add(1, Ordering::Relaxed);
            let aside = path.with_extension(format!("kix_old_{}_{seq}", std::process::id()));
            fs::rename(path, &aside)?;
            let res = fs::write(path, data);
            let _ = fs::remove_file(&aside);
            res
        }
        Err(e) => Err(e),
    }
}
