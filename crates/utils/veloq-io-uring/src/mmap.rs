//! RAII owners for the shared memory used by an io_uring instance.

use core::ptr;

use veloq_std::{
    io::{Error, Result},
    os::unix::fd::RawFd,
};

/// A region mapped from an io_uring file descriptor.
pub(crate) struct Mmap {
    address: *mut u8,
    length: usize,
}

impl Mmap {
    /// Map `length` bytes at an io_uring mapping offset.
    pub(crate) fn new(fd: RawFd, offset: u64, length: usize) -> Result<Self> {
        if length == 0 || offset > libc::off_t::MAX as u64 {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }

        let address = unsafe {
            libc::mmap(
                ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED | libc::MAP_POPULATE,
                fd,
                offset as libc::off_t,
            )
        };

        if address == libc::MAP_FAILED {
            Err(Error::last_os_error())
        } else {
            Ok(Self {
                address: address.cast(),
                length,
            })
        }
    }

    #[inline]
    pub(crate) fn as_mut_ptr(&self) -> *mut u8 {
        self.address
    }

    /// Return a pointer at an offset validated while constructing the map.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the requested object is fully contained in
    /// this mapping and that its alignment is valid for the target type.
    #[inline]
    pub(crate) unsafe fn offset(&self, offset: u32) -> *mut u8 {
        debug_assert!((offset as usize) < self.length);
        unsafe { self.address.add(offset as usize) }
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        unsafe {
            let _ = libc::munmap(self.address.cast(), self.length);
        }
    }
}
