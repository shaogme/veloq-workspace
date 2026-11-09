use core::mem::zeroed;
use libc::EOPNOTSUPP;

use veloq_std::io::{Error, ErrorKind, Result};

use crate::{
    internal::{
        squeue::Entry,
        types::{AsyncCancelFlags, TimeoutFlags, Timespec},
    },
    sys,
};

#[derive(Debug)]
pub struct Timeout {
    timespec: *const Timespec,
    count: u32,
    flags: TimeoutFlags,
}

impl Timeout {
    /// Create a timeout request.
    ///
    /// # Safety
    ///
    /// `timespec` must point to an initialized [`Timespec`] that remains valid and unmoved until
    /// the kernel has finished the request.
    #[inline]
    pub unsafe fn new(timespec: *const Timespec) -> Self {
        Self {
            timespec,
            count: 0,
            flags: TimeoutFlags::empty(),
        }
    }

    #[inline]
    pub const fn count(mut self, count: u32) -> Self {
        self.count = count;
        self
    }

    #[inline]
    pub const fn flags(mut self, flags: TimeoutFlags) -> Self {
        self.flags = flags;
        self
    }

    fn validate_flags(flags: TimeoutFlags) -> Result<()> {
        let bits = flags.bits();
        let known = sys::IORING_TIMEOUT_ABS
            | sys::IORING_TIMEOUT_UPDATE
            | sys::IORING_TIMEOUT_BOOTTIME
            | sys::IORING_TIMEOUT_REALTIME
            | sys::IORING_LINK_TIMEOUT_UPDATE
            | sys::IORING_TIMEOUT_ETIME_SUCCESS
            | sys::IORING_TIMEOUT_MULTISHOT;
        validate_known_flags(bits, known)?;
        if (bits & sys::IORING_TIMEOUT_CLOCK_MASK).count_ones() > 1 {
            return Err(invalid_input());
        }
        if bits & sys::IORING_TIMEOUT_UPDATE_MASK != 0 {
            return Err(unsupported());
        }
        Ok(())
    }

    #[inline]
    pub fn build(self) -> Result<Entry> {
        Self::validate_flags(self.flags)?;
        let mut sqe = zeroed_sqe();
        sqe.opcode = Self::CODE;
        sqe.fd = -1;
        sqe.addr_or_splice_off_in = self.timespec as usize as u64;
        sqe.len = 1;
        sqe.off_or_addr2 = self.count as u64;
        sqe.op_flags = self.flags.bits();
        Ok(Entry::from_raw(sqe))
    }

    pub const CODE: u8 = sys::IORING_OP_TIMEOUT;
}

/// Cancel an existing request identified by its user data.
#[derive(Debug)]
pub struct AsyncCancel {
    user_data: u64,
    flags: AsyncCancelFlags,
}

impl AsyncCancel {
    #[inline]
    pub fn new(user_data: u64) -> Self {
        Self {
            user_data,
            flags: AsyncCancelFlags::empty(),
        }
    }

    #[inline]
    pub const fn flags(mut self, flags: AsyncCancelFlags) -> Self {
        self.flags = flags;
        self
    }

    fn validate_flags(flags: AsyncCancelFlags) -> Result<()> {
        let bits = flags.bits();
        let known = sys::IORING_ASYNC_CANCEL_ALL
            | sys::IORING_ASYNC_CANCEL_FD
            | sys::IORING_ASYNC_CANCEL_ANY
            | sys::IORING_ASYNC_CANCEL_FD_FIXED
            | sys::IORING_ASYNC_CANCEL_USERDATA
            | sys::IORING_ASYNC_CANCEL_OP;
        validate_known_flags(bits, known)?;
        let selectors = sys::IORING_ASYNC_CANCEL_FD
            | sys::IORING_ASYNC_CANCEL_FD_FIXED
            | sys::IORING_ASYNC_CANCEL_USERDATA
            | sys::IORING_ASYNC_CANCEL_OP;
        if bits & sys::IORING_ASYNC_CANCEL_ANY != 0 && bits & selectors != 0 {
            return Err(invalid_input());
        }
        if bits & sys::IORING_ASYNC_CANCEL_FD != 0 && bits & sys::IORING_ASYNC_CANCEL_FD_FIXED != 0
        {
            return Err(invalid_input());
        }
        if bits & (sys::IORING_ASYNC_CANCEL_FD | sys::IORING_ASYNC_CANCEL_FD_FIXED) != 0 {
            return Err(unsupported());
        }
        Ok(())
    }

    #[inline]
    pub fn build(self) -> Result<Entry> {
        Self::validate_flags(self.flags)?;
        let mut sqe = zeroed_sqe();
        sqe.opcode = Self::CODE;
        sqe.fd = -1;
        sqe.addr_or_splice_off_in = self.user_data;
        sqe.op_flags = self.flags.bits();
        Ok(Entry::from_raw(sqe))
    }

    pub const CODE: u8 = sys::IORING_OP_ASYNC_CANCEL;
}

#[inline]
fn invalid_input() -> Error {
    Error::from(ErrorKind::InvalidInput)
}

#[inline]
fn unsupported() -> Error {
    Error::from_raw_os_error(EOPNOTSUPP)
}

#[inline]
fn validate_known_flags(flags: u32, known: u32) -> Result<()> {
    if flags & !known != 0 {
        Err(unsupported())
    } else {
        Ok(())
    }
}

#[inline]
fn zeroed_sqe() -> sys::IoUringSqe {
    unsafe { zeroed() }
}
