use core::convert::TryFrom;

use libc::{EIO, EOVERFLOW, c_long, c_void, syscall};

use veloq_std::{
    io::{Error, Result},
    os::unix::fd::RawFd,
};

use crate::sys::{IoUringParams, SYS_IO_URING_ENTER, SYS_IO_URING_REGISTER, SYS_IO_URING_SETUP};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RawSyscallResult {
    Value(i64),
    Errno(i32),
}

impl RawSyscallResult {
    #[inline]
    pub(crate) fn into_result(self) -> Result<i64> {
        match self {
            Self::Value(value) => Ok(value),
            Self::Errno(errno) => Err(Error::from_raw_os_error(errno)),
        }
    }

    #[inline]
    pub(crate) fn decode_direct_return(raw: i64) -> Self {
        if raw >= 0 {
            return Self::Value(raw);
        }

        let errno = raw
            .checked_neg()
            .and_then(|value| i32::try_from(value).ok())
            .unwrap_or(EOVERFLOW);
        Self::Errno(errno)
    }

    #[inline]
    pub(crate) fn decode_libc_return(raw: c_long) -> Self {
        if raw == -1 {
            return Self::Errno(Error::last_os_error().raw_os_error().unwrap_or(EIO));
        }

        Self::decode_direct_return(raw)
    }
}

pub(crate) struct EnterSyscallArgs {
    pub(crate) fd: RawFd,
    pub(crate) to_submit: u32,
    pub(crate) min_complete: u32,
    pub(crate) flags: u32,
    pub(crate) arg: *const c_void,
    pub(crate) arg_size: usize,
}

pub(crate) struct Syscalls;

impl Syscalls {
    #[inline]
    pub(crate) unsafe fn setup(entries: u32, params: *mut IoUringParams) -> Result<RawFd> {
        let result = unsafe { syscall(SYS_IO_URING_SETUP, entries, params) };
        let result = RawSyscallResult::decode_libc_return(result).into_result()?;
        RawFd::try_from(result).map_err(|_| Error::from_raw_os_error(EOVERFLOW))
    }

    #[inline]
    pub(crate) unsafe fn enter(args: EnterSyscallArgs) -> Result<i64> {
        let result = unsafe {
            syscall(
                SYS_IO_URING_ENTER,
                args.fd,
                args.to_submit,
                args.min_complete,
                args.flags,
                args.arg,
                args.arg_size,
            )
        };
        RawSyscallResult::decode_libc_return(result).into_result()
    }

    #[inline]
    pub(crate) unsafe fn register(
        fd: RawFd,
        opcode: u32,
        arg: *const c_void,
        nr_args: u32,
    ) -> Result<i64> {
        let result = unsafe { syscall(SYS_IO_URING_REGISTER, fd, opcode, arg, nr_args) };
        RawSyscallResult::decode_libc_return(result).into_result()
    }
}

#[cfg(test)]
mod tests {
    use core::ptr::null;

    use libc::{EAGAIN, EINVAL, EOVERFLOW};

    use crate::sys::IORING_REGISTER_PROBE;

    use super::{RawSyscallResult, Syscalls};

    #[test]
    fn syscall_errors_use_veloq_io_error() {
        let error = unsafe { Syscalls::register(-1, IORING_REGISTER_PROBE, null(), 0) }
            .expect_err("an invalid ring fd must fail");

        assert_eq!(error.raw_os_error(), Some(EINVAL));
    }

    #[test]
    fn raw_return_decoder_never_exposes_negative_success() {
        assert_eq!(
            RawSyscallResult::decode_direct_return(7),
            RawSyscallResult::Value(7)
        );
        assert_eq!(
            RawSyscallResult::decode_direct_return(-EAGAIN as i64),
            RawSyscallResult::Errno(EAGAIN)
        );
        assert_eq!(
            RawSyscallResult::decode_direct_return(i64::MIN),
            RawSyscallResult::Errno(EOVERFLOW)
        );
    }
}
