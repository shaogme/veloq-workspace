//! Public ABI types used by submission and registration.

use core::{marker::PhantomData, ptr};

use veloq_std::{os::unix::fd::RawFd, time::Duration};

use crate::sys;

/// A raw file descriptor that has not been registered with the ring.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Fd(pub RawFd);

/// An index into the ring's registered file table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Fixed(pub u32);

/// A descriptor selector accepted by opcodes that support fixed files.
pub trait UseFixed: Copy {
    /// Return the raw descriptor when this is a normal file descriptor.
    fn raw_fd(self) -> Option<RawFd>;

    /// Return the registered-file index when this is a fixed descriptor.
    fn fixed_index(self) -> Option<u32>;
}

impl UseFixed for Fd {
    #[inline]
    fn raw_fd(self) -> Option<RawFd> {
        Some(self.0)
    }

    #[inline]
    fn fixed_index(self) -> Option<u32> {
        None
    }
}

impl UseFixed for Fixed {
    #[inline]
    fn raw_fd(self) -> Option<RawFd> {
        None
    }

    #[inline]
    fn fixed_index(self) -> Option<u32> {
        Some(self.0)
    }
}

/// Flags accepted by the `Fsync` operation.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[repr(transparent)]
pub struct FsyncFlags(u32);

impl FsyncFlags {
    /// Flush data without requiring metadata to be synchronized.
    pub const DATASYNC: Self = Self(sys::IORING_FSYNC_DATASYNC);

    #[inline]
    pub const fn empty() -> Self {
        Self(0)
    }

    #[inline]
    pub const fn bits(self) -> u32 {
        self.0
    }

    #[inline]
    pub const fn from_bits_retain(bits: u32) -> Self {
        Self(bits)
    }

    #[inline]
    pub const fn contains(self, other: Self) -> bool {
        self.0 & other.0 == other.0
    }

    #[inline]
    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// The Linux `__kernel_timespec` used by extended enter waits.
#[derive(Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct Timespec(pub(crate) sys::KernelTimespec);

impl Timespec {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self(sys::KernelTimespec {
            tv_sec: 0,
            tv_nsec: 0,
        })
    }

    #[inline]
    #[must_use]
    pub const fn sec(mut self, sec: u64) -> Self {
        self.0.tv_sec = sec as i64;
        self
    }

    #[inline]
    #[must_use]
    pub const fn nsec(mut self, nsec: u32) -> Self {
        self.0.tv_nsec = nsec as i64;
        self
    }
}

impl From<Duration> for Timespec {
    #[inline]
    fn from(duration: Duration) -> Self {
        Self::new()
            .sec(duration.as_secs())
            .nsec(duration.subsec_nanos())
    }
}

/// Arguments passed to `io_uring_enter` with `EXT_ARG`.
///
/// The lifetime transition in the builder methods prevents a timespec or
/// signal mask from being dropped while the resulting argument is retained.
#[derive(Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct SubmitArgs<'prev: 'now, 'now> {
    pub(crate) args: sys::IoUringGeteventsArg,
    prev: PhantomData<&'prev ()>,
    now: PhantomData<&'now ()>,
}

impl SubmitArgs<'static, 'static> {
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            args: sys::IoUringGeteventsArg {
                sigmask: 0,
                sigmask_size: 0,
                min_wait_usec: 0,
                timespec: 0,
            },
            prev: PhantomData,
            now: PhantomData,
        }
    }
}

impl<'prev, 'now> SubmitArgs<'prev, 'now> {
    /// Set the signal mask used while waiting.
    #[inline]
    #[must_use]
    pub fn sigmask<'next>(self, sigmask: &'next libc::sigset_t) -> SubmitArgs<'now, 'next> {
        let mut args = self.args;
        args.sigmask = ptr::from_ref(sigmask) as u64;
        args.sigmask_size = core::mem::size_of::<libc::sigset_t>() as u32;
        SubmitArgs {
            args,
            prev: PhantomData,
            now: PhantomData,
        }
    }

    /// Set the minimum wait timeout timespec.
    #[inline]
    #[must_use]
    pub fn timespec<'next>(self, timespec: &'next Timespec) -> SubmitArgs<'now, 'next> {
        let mut args = self.args;
        args.timespec = ptr::from_ref(&timespec.0) as u64;
        SubmitArgs {
            args,
            prev: PhantomData,
            now: PhantomData,
        }
    }
}

/// An entry in a provided-buffer ring.
#[derive(Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct BufRingEntry(pub(crate) sys::IoUringBuf);

#[allow(clippy::len_without_is_empty)]
impl BufRingEntry {
    #[inline]
    pub fn set_addr(&mut self, addr: u64) {
        self.0.addr = addr;
    }

    #[inline]
    pub fn addr(&self) -> u64 {
        self.0.addr
    }

    #[inline]
    pub fn set_len(&mut self, len: u32) {
        self.0.len = len;
    }

    #[inline]
    pub fn len(&self) -> u32 {
        self.0.len
    }

    #[inline]
    pub fn set_bid(&mut self, bid: u16) {
        self.0.bid = bid;
    }

    #[inline]
    pub fn bid(&self) -> u16 {
        self.0.bid
    }

    /// Return the shared tail field at the start of a provided-buffer ring.
    ///
    /// # Safety
    ///
    /// `ring_base` must point to the first initialized entry of a valid
    /// provided-buffer ring. The mapping must remain valid for the returned
    /// pointer's use and the caller must perform the required atomic access.
    #[inline]
    pub unsafe fn tail(ring_base: *const Self) -> *const u16 {
        unsafe { ptr::addr_of!((*ring_base).0.resv) }
    }
}

#[cfg(test)]
mod tests {
    use core::mem::{align_of, offset_of, size_of};

    use super::*;

    #[test]
    fn public_types_match_kernel_layouts() {
        assert_eq!(size_of::<Timespec>(), 16);
        assert_eq!(align_of::<Timespec>(), 8);
        assert_eq!(size_of::<SubmitArgs<'static, 'static>>(), 24);
        assert_eq!(size_of::<BufRingEntry>(), 16);
        assert_eq!(align_of::<BufRingEntry>(), 8);
        assert_eq!(offset_of!(sys::IoUringBuf, len), 8);
        assert_eq!(offset_of!(sys::IoUringBuf, bid), 12);
        assert_eq!(offset_of!(sys::IoUringBuf, resv), 14);
    }

    #[test]
    fn timespec_accepts_veloq_duration() {
        let timespec = Timespec::from(Duration::new(3, 17));
        assert_eq!(timespec.0.tv_sec, 3);
        assert_eq!(timespec.0.tv_nsec, 17);
    }
}
