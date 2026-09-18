//! RAII owners for the shared memory used by an io_uring instance.

use core::{convert::TryFrom, ptr};

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
    ///
    /// The mapping is made inaccessible to children created by `fork`. A failure
    /// to install that policy tears down the mapping and is returned to the
    /// caller; silently keeping an io_uring mapping fork-visible would violate
    /// the ring ownership contract.
    pub(crate) fn new(fd: RawFd, offset: u64, length: usize, populate: bool) -> Result<Self> {
        if length == 0 || offset > libc::off_t::MAX as u64 {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }

        let page_size = page_size()?;
        if !offset.is_multiple_of(page_size as u64) {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }

        let mut flags = libc::MAP_SHARED;
        if populate {
            flags |= libc::MAP_POPULATE;
        }

        let address = unsafe {
            libc::mmap(
                ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                flags,
                fd,
                offset as libc::off_t,
            )
        };

        if address == libc::MAP_FAILED {
            Err(Error::last_os_error())
        } else {
            if let Err(error) = apply_dontfork(address, length) {
                unsafe {
                    let _ = libc::munmap(address, length);
                }
                return Err(error);
            }

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

    /// Return a pointer to a checked subrange of the mapping.
    pub(crate) fn range(&self, offset: u32, length: usize) -> Result<*mut u8> {
        if length == 0 {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }
        let offset =
            usize::try_from(offset).map_err(|_| Error::from_raw_os_error(libc::EOVERFLOW))?;
        let end = offset
            .checked_add(length)
            .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))?;
        if end > self.length {
            return Err(Error::from_raw_os_error(libc::EINVAL));
        }

        let address = (self.address as usize)
            .checked_add(offset)
            .ok_or_else(|| Error::from_raw_os_error(libc::EOVERFLOW))?;
        Ok(address as *mut u8)
    }
}

fn apply_dontfork(address: *mut libc::c_void, length: usize) -> Result<()> {
    if unsafe { libc::madvise(address, length, libc::MADV_DONTFORK) } != 0 {
        return Err(Error::last_os_error());
    }
    Ok(())
}

fn page_size() -> Result<usize> {
    let page_size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    usize::try_from(page_size)
        .ok()
        .filter(|value| *value != 0)
        .ok_or_else(|| Error::from_raw_os_error(libc::EINVAL))
}

impl Drop for Mmap {
    fn drop(&mut self) {
        debug_assert!(!self.address.is_null());
        debug_assert!(self.length != 0);
        unsafe {
            let _ = libc::munmap(self.address.cast(), self.length);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn range_rejects_out_of_bounds_objects() {
        let length = page_size().expect("page size must be available");
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
                -1,
                0,
            )
        };
        assert_ne!(raw, libc::MAP_FAILED);
        let mapping = Mmap {
            address: raw.cast(),
            length,
        };

        assert!(mapping.range(0, length).is_ok());
        assert_eq!(
            mapping.range(1, length).unwrap_err().raw_os_error(),
            Some(libc::EINVAL)
        );
    }

    #[test]
    fn dontfork_mapping_is_not_visible_after_fork() {
        let length = page_size().expect("page size must be available");
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                length,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
                -1,
                0,
            )
        };
        assert_ne!(raw, libc::MAP_FAILED);
        apply_dontfork(raw, length).expect("MADV_DONTFORK must be supported");
        let mapping = Mmap {
            address: raw.cast(),
            length,
        };

        let child = unsafe { libc::fork() };
        assert!(child >= 0, "fork failed: {:?}", Error::last_os_error());
        if child == 0 {
            let mut residency = 0_u8;
            let result =
                unsafe { libc::mincore(mapping.address.cast(), length, &mut residency as *mut u8) };
            let absent =
                result == -1 && Error::last_os_error().raw_os_error() == Some(libc::ENOMEM);
            unsafe { libc::_exit(i32::from(!absent)) };
        }

        let mut status = 0;
        assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
        assert!(libc::WIFEXITED(status));
        assert_eq!(libc::WEXITSTATUS(status), 0);
    }
}
