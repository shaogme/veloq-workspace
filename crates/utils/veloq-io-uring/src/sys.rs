//! Private Linux `io_uring` ABI declarations and syscall wrappers.
//!
//! These declarations are copied from the layout of Linux's
//! `linux/io_uring.h`, cross-checked against the local upstream
//! `io-uring 0.7.12` cache, and deliberately use fixed-width Rust types.

#![allow(dead_code)]

use core::convert::TryFrom;

use libc::c_long;

use veloq_std::{
    io::{Error, Result},
    os::unix::fd::RawFd,
};

pub(crate) const SYS_IO_URING_SETUP: c_long = libc::SYS_io_uring_setup;
pub(crate) const SYS_IO_URING_ENTER: c_long = libc::SYS_io_uring_enter;
pub(crate) const SYS_IO_URING_REGISTER: c_long = libc::SYS_io_uring_register;

pub(crate) const IORING_SETUP_IOPOLL: u32 = 1;
pub(crate) const IORING_SETUP_SQPOLL: u32 = 2;
pub(crate) const IORING_SETUP_SQ_AFF: u32 = 4;
pub(crate) const IORING_SETUP_CQSIZE: u32 = 8;
pub(crate) const IORING_SETUP_CLAMP: u32 = 16;
pub(crate) const IORING_SETUP_ATTACH_WQ: u32 = 32;
pub(crate) const IORING_SETUP_R_DISABLED: u32 = 64;
pub(crate) const IORING_SETUP_SUBMIT_ALL: u32 = 128;
pub(crate) const IORING_SETUP_COOP_TASKRUN: u32 = 256;
pub(crate) const IORING_SETUP_TASKRUN_FLAG: u32 = 512;
pub(crate) const IORING_SETUP_SQE128: u32 = 1024;
pub(crate) const IORING_SETUP_CQE32: u32 = 2048;
pub(crate) const IORING_SETUP_SINGLE_ISSUER: u32 = 4096;
pub(crate) const IORING_SETUP_DEFER_TASKRUN: u32 = 8192;
pub(crate) const IORING_SETUP_NO_MMAP: u32 = 16_384;
pub(crate) const IORING_SETUP_REGISTERED_FD_ONLY: u32 = 32_768;
pub(crate) const IORING_SETUP_NO_SQARRAY: u32 = 65_536;
pub(crate) const IORING_SETUP_HYBRID_IOPOLL: u32 = 131_072;

pub(crate) const IORING_FEAT_SINGLE_MMAP: u32 = 1;
pub(crate) const IORING_FEAT_NODROP: u32 = 2;
pub(crate) const IORING_FEAT_SUBMIT_STABLE: u32 = 4;
pub(crate) const IORING_FEAT_RW_CUR_POS: u32 = 8;
pub(crate) const IORING_FEAT_CUR_PERSONALITY: u32 = 16;
pub(crate) const IORING_FEAT_FAST_POLL: u32 = 32;
pub(crate) const IORING_FEAT_POLL_32BITS: u32 = 64;
pub(crate) const IORING_FEAT_SQPOLL_NONFIXED: u32 = 128;
pub(crate) const IORING_FEAT_EXT_ARG: u32 = 256;
pub(crate) const IORING_FEAT_NATIVE_WORKERS: u32 = 512;
pub(crate) const IORING_FEAT_RSRC_TAGS: u32 = 1024;
pub(crate) const IORING_FEAT_CQE_SKIP: u32 = 2048;
pub(crate) const IORING_FEAT_LINKED_FILE: u32 = 4096;
pub(crate) const IORING_FEAT_REG_REG_RING: u32 = 8192;
pub(crate) const IORING_FEAT_RECVSEND_BUNDLE: u32 = 16_384;
pub(crate) const IORING_FEAT_MIN_TIMEOUT: u32 = 32_768;
pub(crate) const IORING_FEAT_RW_ATTR: u32 = 65_536;
pub(crate) const IORING_FEAT_NO_IOWAIT: u32 = 131_072;

pub(crate) const IORING_OFF_SQ_RING: u64 = 0;
pub(crate) const IORING_OFF_CQ_RING: u64 = 1 << 27;
pub(crate) const IORING_OFF_SQES: u64 = 1 << 28;
pub(crate) const IORING_OFF_PBUF_RING: u64 = 1 << 31;
pub(crate) const IORING_OFF_PBUF_SHIFT: u32 = 16;
pub(crate) const IORING_OFF_MMAP_MASK: u64 = 0x3fff_ffff;

pub(crate) const IORING_SQ_NEED_WAKEUP: u32 = 1;
pub(crate) const IORING_SQ_CQ_OVERFLOW: u32 = 2;
pub(crate) const IORING_SQ_TASKRUN: u32 = 4;

pub(crate) const IORING_CQ_EVENTFD_DISABLED: u32 = 1;
pub(crate) const IORING_ENTER_GETEVENTS: u32 = 1;
pub(crate) const IORING_ENTER_SQ_WAKEUP: u32 = 2;
pub(crate) const IORING_ENTER_SQ_WAIT: u32 = 4;
pub(crate) const IORING_ENTER_EXT_ARG: u32 = 8;
pub(crate) const IORING_ENTER_REGISTERED_RING: u32 = 16;
pub(crate) const IORING_ENTER_ABS_TIMER: u32 = 32;
pub(crate) const IORING_ENTER_EXT_ARG_REG: u32 = 64;
pub(crate) const IORING_ENTER_NO_IOWAIT: u32 = 128;

pub(crate) const IORING_CQE_F_BUFFER: u32 = 1;
pub(crate) const IORING_CQE_F_MORE: u32 = 2;
pub(crate) const IORING_CQE_F_SOCK_NONEMPTY: u32 = 4;
pub(crate) const IORING_CQE_F_NOTIF: u32 = 8;
pub(crate) const IORING_CQE_F_BUF_MORE: u32 = 16;
pub(crate) const IORING_CQE_BUFFER_SHIFT: u32 = 16;

pub(crate) const IOSQE_FIXED_FILE: u8 = 1;
pub(crate) const IOSQE_IO_DRAIN: u8 = 1 << 1;
pub(crate) const IOSQE_IO_LINK: u8 = 1 << 2;
pub(crate) const IOSQE_IO_HARDLINK: u8 = 1 << 3;
pub(crate) const IOSQE_ASYNC: u8 = 1 << 4;
pub(crate) const IOSQE_BUFFER_SELECT: u8 = 1 << 5;
pub(crate) const IOSQE_CQE_SKIP_SUCCESS: u8 = 1 << 6;

pub(crate) const IORING_FSYNC_DATASYNC: u32 = 1;
pub(crate) const IORING_TIMEOUT_ABS: u32 = 1;
pub(crate) const IORING_TIMEOUT_UPDATE: u32 = 2;
pub(crate) const IORING_TIMEOUT_BOOTTIME: u32 = 4;
pub(crate) const IORING_TIMEOUT_REALTIME: u32 = 8;
pub(crate) const IORING_LINK_TIMEOUT_UPDATE: u32 = 16;
pub(crate) const IORING_TIMEOUT_ETIME_SUCCESS: u32 = 32;
pub(crate) const IORING_TIMEOUT_MULTISHOT: u32 = 64;
pub(crate) const IORING_TIMEOUT_CLOCK_MASK: u32 = IORING_TIMEOUT_BOOTTIME | IORING_TIMEOUT_REALTIME;
pub(crate) const IORING_TIMEOUT_UPDATE_MASK: u32 =
    IORING_TIMEOUT_UPDATE | IORING_LINK_TIMEOUT_UPDATE;
pub(crate) const IORING_ASYNC_CANCEL_ALL: u32 = 1;
pub(crate) const IORING_ASYNC_CANCEL_FD: u32 = 2;
pub(crate) const IORING_ASYNC_CANCEL_ANY: u32 = 4;
pub(crate) const IORING_ASYNC_CANCEL_FD_FIXED: u32 = 8;
pub(crate) const IORING_ASYNC_CANCEL_USERDATA: u32 = 16;
pub(crate) const IORING_ASYNC_CANCEL_OP: u32 = 32;
pub(crate) const IORING_RECVSEND_POLL_FIRST: u32 = 1;
pub(crate) const IORING_RECV_MULTISHOT: u32 = 2;
pub(crate) const IORING_RECVSEND_FIXED_BUF: u32 = 4;
pub(crate) const IORING_ACCEPT_MULTISHOT: u32 = 1;
pub(crate) const IORING_ACCEPT_DONTWAIT: u32 = 2;
pub(crate) const IORING_ACCEPT_POLL_FIRST: u32 = 4;

pub(crate) const IORING_RSRC_REGISTER_SPARSE: u32 = 1;
pub(crate) const IORING_REGISTER_FILES_SKIP: i32 = -2;
pub(crate) const IO_URING_OP_SUPPORTED: u16 = 1;

pub(crate) const IORING_REGISTER_BUFFERS: u32 = 0;
pub(crate) const IORING_UNREGISTER_BUFFERS: u32 = 1;
pub(crate) const IORING_REGISTER_FILES: u32 = 2;
pub(crate) const IORING_UNREGISTER_FILES: u32 = 3;
pub(crate) const IORING_REGISTER_EVENTFD: u32 = 4;
pub(crate) const IORING_UNREGISTER_EVENTFD: u32 = 5;
pub(crate) const IORING_REGISTER_FILES_UPDATE: u32 = 6;
pub(crate) const IORING_REGISTER_EVENTFD_ASYNC: u32 = 7;
pub(crate) const IORING_REGISTER_PROBE: u32 = 8;
pub(crate) const IORING_REGISTER_PERSONALITY: u32 = 9;
pub(crate) const IORING_UNREGISTER_PERSONALITY: u32 = 10;
pub(crate) const IORING_REGISTER_RESTRICTIONS: u32 = 11;
pub(crate) const IORING_REGISTER_ENABLE_RINGS: u32 = 12;
pub(crate) const IORING_REGISTER_FILES2: u32 = 13;
pub(crate) const IORING_REGISTER_FILES_UPDATE2: u32 = 14;
pub(crate) const IORING_REGISTER_BUFFERS2: u32 = 15;
pub(crate) const IORING_REGISTER_BUFFERS_UPDATE: u32 = 16;
pub(crate) const IORING_REGISTER_IOWQ_AFF: u32 = 17;
pub(crate) const IORING_UNREGISTER_IOWQ_AFF: u32 = 18;
pub(crate) const IORING_REGISTER_IOWQ_MAX_WORKERS: u32 = 19;
pub(crate) const IORING_REGISTER_RING_FDS: u32 = 20;
pub(crate) const IORING_UNREGISTER_RING_FDS: u32 = 21;
pub(crate) const IORING_REGISTER_PBUF_RING: u32 = 22;
pub(crate) const IORING_UNREGISTER_PBUF_RING: u32 = 23;
pub(crate) const IORING_REGISTER_SYNC_CANCEL: u32 = 24;
pub(crate) const IORING_REGISTER_FILE_ALLOC_RANGE: u32 = 25;
pub(crate) const IORING_REGISTER_PBUF_STATUS: u32 = 26;
pub(crate) const IORING_REGISTER_NAPI: u32 = 27;
pub(crate) const IORING_UNREGISTER_NAPI: u32 = 28;
pub(crate) const IORING_REGISTER_CLOCK: u32 = 29;
pub(crate) const IORING_REGISTER_CLONE_BUFFERS: u32 = 30;
pub(crate) const IORING_REGISTER_SEND_MSG_RING: u32 = 31;
pub(crate) const IORING_REGISTER_ZCRX_IFQ: u32 = 32;
pub(crate) const IORING_REGISTER_RESIZE_RINGS: u32 = 33;
pub(crate) const IORING_REGISTER_MEM_REGION: u32 = 34;
pub(crate) const IORING_REGISTER_USE_REGISTERED_RING: u32 = 1 << 31;

pub(crate) const IORING_OP_NOP: u8 = 0;
pub(crate) const IORING_OP_READV: u8 = 1;
pub(crate) const IORING_OP_WRITEV: u8 = 2;
pub(crate) const IORING_OP_FSYNC: u8 = 3;
pub(crate) const IORING_OP_READ_FIXED: u8 = 4;
pub(crate) const IORING_OP_WRITE_FIXED: u8 = 5;
pub(crate) const IORING_OP_SYNC_FILE_RANGE: u8 = 8;
pub(crate) const IORING_OP_SENDMSG: u8 = 9;
pub(crate) const IORING_OP_RECVMSG: u8 = 10;
pub(crate) const IORING_OP_TIMEOUT: u8 = 11;
pub(crate) const IORING_OP_ACCEPT: u8 = 13;
pub(crate) const IORING_OP_ASYNC_CANCEL: u8 = 14;
pub(crate) const IORING_OP_CONNECT: u8 = 16;
pub(crate) const IORING_OP_FALLOCATE: u8 = 17;
pub(crate) const IORING_OP_OPENAT: u8 = 18;
pub(crate) const IORING_OP_CLOSE: u8 = 19;
pub(crate) const IORING_OP_READ: u8 = 22;
pub(crate) const IORING_OP_WRITE: u8 = 23;
pub(crate) const IORING_OP_SEND: u8 = 26;
pub(crate) const IORING_OP_RECV: u8 = 27;
pub(crate) const IORING_OP_OPENAT2: u8 = 28;
pub(crate) const IORING_OP_EPOLL_CTL: u8 = 29;
pub(crate) const IORING_OP_SPLICE: u8 = 30;
pub(crate) const IORING_OP_PROVIDE_BUFFERS: u8 = 31;
pub(crate) const IORING_OP_REMOVE_BUFFERS: u8 = 32;
pub(crate) const IORING_OP_TEE: u8 = 33;
pub(crate) const IORING_OP_SHUTDOWN: u8 = 34;
pub(crate) const IORING_OP_RENAMEAT: u8 = 35;
pub(crate) const IORING_OP_UNLINKAT: u8 = 36;
pub(crate) const IORING_OP_MKDIRAT: u8 = 37;
pub(crate) const IORING_OP_SYMLINKAT: u8 = 38;
pub(crate) const IORING_OP_LINKAT: u8 = 39;
pub(crate) const IORING_OP_MSG_RING: u8 = 40;
pub(crate) const IORING_OP_FSETXATTR: u8 = 41;
pub(crate) const IORING_OP_SETXATTR: u8 = 42;
pub(crate) const IORING_OP_FGETXATTR: u8 = 43;
pub(crate) const IORING_OP_GETXATTR: u8 = 44;
pub(crate) const IORING_OP_SOCKET: u8 = 45;
pub(crate) const IORING_OP_URING_CMD: u8 = 46;
pub(crate) const IORING_OP_SEND_ZC: u8 = 47;
pub(crate) const IORING_OP_SENDMSG_ZC: u8 = 48;
pub(crate) const IORING_OP_READ_MULTISHOT: u8 = 49;
pub(crate) const IORING_OP_WAITID: u8 = 50;
pub(crate) const IORING_OP_FUTEX_WAIT: u8 = 51;
pub(crate) const IORING_OP_FUTEX_WAKE: u8 = 52;
pub(crate) const IORING_OP_FUTEX_WAITV: u8 = 53;
pub(crate) const IORING_OP_FIXED_FD_INSTALL: u8 = 54;
pub(crate) const IORING_OP_FTRUNCATE: u8 = 55;
pub(crate) const IORING_OP_BIND: u8 = 56;
pub(crate) const IORING_OP_LISTEN: u8 = 57;
pub(crate) const IORING_OP_RECV_ZC: u8 = 58;
pub(crate) const IORING_OP_EPOLL_WAIT: u8 = 59;
pub(crate) const IORING_OP_READV_FIXED: u8 = 60;
pub(crate) const IORING_OP_WRITEV_FIXED: u8 = 61;
pub(crate) const IORING_OP_PIPE: u8 = 62;

#[cfg(not(any(
    target_arch = "aarch64",
    target_arch = "loongarch64",
    target_arch = "powerpc64",
    target_arch = "riscv64",
    target_arch = "x86_64",
)))]
compile_error!(
    "veloq-io-uring ABI is supported only on x86_64, aarch64, riscv64, loongarch64, and powerpc64"
);

/// The result of a raw syscall before it is converted to Veloq's I/O error type.
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
}

/// Decode a direct-syscall return value, whose negative values encode `-errno`.
#[inline]
pub(crate) fn decode_direct_return(raw: i64) -> RawSyscallResult {
    if raw >= 0 {
        return RawSyscallResult::Value(raw);
    }

    let errno = raw
        .checked_neg()
        .and_then(|value| i32::try_from(value).ok())
        .unwrap_or(libc::EOVERFLOW);
    RawSyscallResult::Errno(errno)
}

/// Decode the return convention used by `libc::syscall`.
#[inline]
pub(crate) fn decode_libc_return(raw: c_long) -> RawSyscallResult {
    if raw == -1 {
        return RawSyscallResult::Errno(Error::last_os_error().raw_os_error().unwrap_or(libc::EIO));
    }

    decode_direct_return(raw)
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct KernelTimespec {
    pub(crate) tv_sec: i64,
    pub(crate) tv_nsec: i64,
}

/// A SQE with union members represented by fixed-width storage.
#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringSqe {
    pub(crate) opcode: u8,
    pub(crate) flags: u8,
    pub(crate) ioprio: u16,
    pub(crate) fd: i32,
    pub(crate) off_or_addr2: u64,
    pub(crate) addr_or_splice_off_in: u64,
    pub(crate) len: u32,
    pub(crate) op_flags: u32,
    pub(crate) user_data: u64,
    pub(crate) buf_index_group: [u8; 2],
    pub(crate) personality: u16,
    pub(crate) splice_fd_in_or_file_index: u32,
    pub(crate) command: [u64; 2],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringCqe {
    pub(crate) user_data: u64,
    pub(crate) result: i32,
    pub(crate) flags: u32,
    pub(crate) big_cqe: [u64; 0],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct SqRingOffsets {
    pub(crate) head: u32,
    pub(crate) tail: u32,
    pub(crate) ring_mask: u32,
    pub(crate) ring_entries: u32,
    pub(crate) flags: u32,
    pub(crate) dropped: u32,
    pub(crate) array: u32,
    pub(crate) resv1: u32,
    pub(crate) user_addr: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct CqRingOffsets {
    pub(crate) head: u32,
    pub(crate) tail: u32,
    pub(crate) ring_mask: u32,
    pub(crate) ring_entries: u32,
    pub(crate) overflow: u32,
    pub(crate) cqes: u32,
    pub(crate) flags: u32,
    pub(crate) resv1: u32,
    pub(crate) user_addr: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringParams {
    pub(crate) sq_entries: u32,
    pub(crate) cq_entries: u32,
    pub(crate) flags: u32,
    pub(crate) sq_thread_cpu: u32,
    pub(crate) sq_thread_idle: u32,
    pub(crate) features: u32,
    pub(crate) workqueue_fd: u32,
    pub(crate) resv: [u32; 3],
    pub(crate) sq_off: SqRingOffsets,
    pub(crate) cq_off: CqRingOffsets,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringFilesUpdate {
    pub(crate) offset: u32,
    pub(crate) resv: u32,
    pub(crate) fds: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringRsrcRegister {
    pub(crate) nr: u32,
    pub(crate) flags: u32,
    pub(crate) resv2: u64,
    pub(crate) data: u64,
    pub(crate) tags: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringRsrcUpdate {
    pub(crate) offset: u32,
    pub(crate) resv: u32,
    pub(crate) data: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringRsrcUpdate2 {
    pub(crate) offset: u32,
    pub(crate) resv: u32,
    pub(crate) data: u64,
    pub(crate) tags: u64,
    pub(crate) nr: u32,
    pub(crate) resv2: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringBufReg {
    pub(crate) ring_addr: u64,
    pub(crate) ring_entries: u32,
    pub(crate) bgid: u16,
    pub(crate) flags: u16,
    pub(crate) resv: [u64; 3],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringBuf {
    pub(crate) addr: u64,
    pub(crate) len: u32,
    pub(crate) bid: u16,
    pub(crate) resv: u16,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringGeteventsArg {
    pub(crate) sigmask: u64,
    pub(crate) sigmask_size: u32,
    pub(crate) min_wait_usec: u32,
    pub(crate) timespec: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringProbeOp {
    pub(crate) op: u8,
    pub(crate) resv: u8,
    pub(crate) flags: u16,
    pub(crate) resv2: u32,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct IoUringProbe {
    pub(crate) last_op: u8,
    pub(crate) ops_len: u8,
    pub(crate) resv: u16,
    pub(crate) resv2: [u32; 3],
    pub(crate) ops: [IoUringProbeOp; 0],
}

/// Invoke `io_uring_setup` and convert the libc error boundary once.
pub(crate) unsafe fn io_uring_setup(entries: u32, params: *mut IoUringParams) -> Result<RawFd> {
    let result = unsafe { libc::syscall(SYS_IO_URING_SETUP, entries, params) };
    let result = decode_libc_return(result).into_result()?;
    RawFd::try_from(result).map_err(|_| Error::from_raw_os_error(libc::EOVERFLOW))
}

/// Invoke `io_uring_enter` and convert the libc error boundary once.
pub(crate) unsafe fn io_uring_enter(
    fd: RawFd,
    to_submit: u32,
    min_complete: u32,
    flags: u32,
    arg: *const libc::c_void,
    arg_size: usize,
) -> Result<i64> {
    let result = unsafe {
        libc::syscall(
            SYS_IO_URING_ENTER,
            fd,
            to_submit,
            min_complete,
            flags,
            arg,
            arg_size,
        )
    };
    decode_libc_return(result).into_result()
}

/// Invoke `io_uring_register` and convert the libc error boundary once.
pub(crate) unsafe fn io_uring_register(
    fd: RawFd,
    opcode: u32,
    arg: *const libc::c_void,
    nr_args: u32,
) -> Result<i64> {
    let result = unsafe { libc::syscall(SYS_IO_URING_REGISTER, fd, opcode, arg, nr_args) };
    decode_libc_return(result).into_result()
}

#[cfg(test)]
mod tests {
    use core::mem::{align_of, offset_of, size_of};

    use super::*;

    const _: () = {
        assert!(size_of::<KernelTimespec>() == 16);
        assert!(align_of::<KernelTimespec>() == 8);

        assert!(size_of::<IoUringSqe>() == 64);
        assert!(align_of::<IoUringSqe>() == 8);
        assert!(offset_of!(IoUringSqe, opcode) == 0);
        assert!(offset_of!(IoUringSqe, fd) == 4);
        assert!(offset_of!(IoUringSqe, off_or_addr2) == 8);
        assert!(offset_of!(IoUringSqe, addr_or_splice_off_in) == 16);
        assert!(offset_of!(IoUringSqe, len) == 24);
        assert!(offset_of!(IoUringSqe, op_flags) == 28);
        assert!(offset_of!(IoUringSqe, user_data) == 32);
        assert!(offset_of!(IoUringSqe, buf_index_group) == 40);
        assert!(offset_of!(IoUringSqe, personality) == 42);
        assert!(offset_of!(IoUringSqe, splice_fd_in_or_file_index) == 44);
        assert!(offset_of!(IoUringSqe, command) == 48);

        assert!(size_of::<IoUringCqe>() == 16);
        assert!(align_of::<IoUringCqe>() == 8);
        assert!(offset_of!(IoUringCqe, user_data) == 0);
        assert!(offset_of!(IoUringCqe, result) == 8);
        assert!(offset_of!(IoUringCqe, flags) == 12);

        assert!(size_of::<SqRingOffsets>() == 40);
        assert!(size_of::<CqRingOffsets>() == 40);
        assert!(size_of::<IoUringParams>() == 120);
        assert!(align_of::<IoUringParams>() == 8);
        assert!(offset_of!(IoUringParams, sq_off) == 40);
        assert!(offset_of!(IoUringParams, cq_off) == 80);
        assert!(offset_of!(SqRingOffsets, user_addr) == 32);
        assert!(offset_of!(CqRingOffsets, user_addr) == 32);

        assert!(size_of::<IoUringFilesUpdate>() == 16);
        assert!(size_of::<IoUringRsrcRegister>() == 32);
        assert!(size_of::<IoUringRsrcUpdate>() == 16);
        assert!(size_of::<IoUringRsrcUpdate2>() == 32);
        assert!(size_of::<IoUringBufReg>() == 40);
        assert!(size_of::<IoUringBuf>() == 16);
        assert!(size_of::<IoUringGeteventsArg>() == 24);
        assert!(size_of::<IoUringProbeOp>() == 8);
        assert!(size_of::<IoUringProbe>() == 16);
        assert!(align_of::<IoUringProbe>() == 4);
        assert!(offset_of!(IoUringBufReg, ring_entries) == 8);
        assert!(offset_of!(IoUringBufReg, bgid) == 12);
        assert!(offset_of!(IoUringBufReg, flags) == 14);
        assert!(offset_of!(IoUringBuf, len) == 8);
        assert!(offset_of!(IoUringBuf, bid) == 12);
        assert!(offset_of!(IoUringBuf, resv) == 14);
        assert!(offset_of!(IoUringGeteventsArg, timespec) == 16);
        assert!(offset_of!(IoUringProbe, ops) == 16);
    };

    #[test]
    fn syscall_numbers_match_linux_uapi() {
        assert_eq!(SYS_IO_URING_SETUP, 425);
        assert_eq!(SYS_IO_URING_ENTER, 426);
        assert_eq!(SYS_IO_URING_REGISTER, 427);
    }

    #[test]
    fn entry_layout_matches_linux_uapi() {
        assert_eq!(size_of::<IoUringSqe>(), 64);
        assert_eq!(align_of::<IoUringSqe>(), 8);
        assert_eq!(offset_of!(IoUringSqe, opcode), 0);
        assert_eq!(offset_of!(IoUringSqe, flags), 1);
        assert_eq!(offset_of!(IoUringSqe, ioprio), 2);
        assert_eq!(offset_of!(IoUringSqe, fd), 4);
        assert_eq!(offset_of!(IoUringSqe, off_or_addr2), 8);
        assert_eq!(offset_of!(IoUringSqe, addr_or_splice_off_in), 16);
        assert_eq!(offset_of!(IoUringSqe, len), 24);
        assert_eq!(offset_of!(IoUringSqe, op_flags), 28);
        assert_eq!(offset_of!(IoUringSqe, user_data), 32);
        assert_eq!(offset_of!(IoUringSqe, buf_index_group), 40);
        assert_eq!(offset_of!(IoUringSqe, personality), 42);
        assert_eq!(offset_of!(IoUringSqe, splice_fd_in_or_file_index), 44);
        assert_eq!(offset_of!(IoUringSqe, command), 48);

        assert_eq!(size_of::<IoUringCqe>(), 16);
        assert_eq!(align_of::<IoUringCqe>(), 8);
        assert_eq!(offset_of!(IoUringCqe, user_data), 0);
        assert_eq!(offset_of!(IoUringCqe, result), 8);
        assert_eq!(offset_of!(IoUringCqe, flags), 12);
    }

    #[test]
    fn ring_parameter_layout_matches_linux_uapi() {
        assert_eq!(size_of::<SqRingOffsets>(), 40);
        assert_eq!(size_of::<CqRingOffsets>(), 40);
        assert_eq!(size_of::<IoUringParams>(), 120);
        assert_eq!(align_of::<IoUringParams>(), 8);
        assert_eq!(offset_of!(IoUringParams, sq_off), 40);
        assert_eq!(offset_of!(IoUringParams, cq_off), 80);
        assert_eq!(offset_of!(SqRingOffsets, user_addr), 32);
        assert_eq!(offset_of!(CqRingOffsets, user_addr), 32);
    }

    #[test]
    fn register_parameter_layout_matches_linux_uapi() {
        assert_eq!(size_of::<IoUringFilesUpdate>(), 16);
        assert_eq!(size_of::<IoUringRsrcRegister>(), 32);
        assert_eq!(size_of::<IoUringRsrcUpdate>(), 16);
        assert_eq!(size_of::<IoUringRsrcUpdate2>(), 32);
        assert_eq!(size_of::<IoUringBufReg>(), 40);
        assert_eq!(size_of::<IoUringBuf>(), 16);
        assert_eq!(size_of::<IoUringGeteventsArg>(), 24);
        assert_eq!(size_of::<IoUringProbeOp>(), 8);
        assert_eq!(size_of::<IoUringProbe>(), 16);
        assert_eq!(align_of::<IoUringProbe>(), 4);
        assert_eq!(offset_of!(IoUringBufReg, ring_entries), 8);
        assert_eq!(offset_of!(IoUringBufReg, bgid), 12);
        assert_eq!(offset_of!(IoUringBufReg, flags), 14);
        assert_eq!(offset_of!(IoUringBuf, len), 8);
        assert_eq!(offset_of!(IoUringBuf, bid), 12);
        assert_eq!(offset_of!(IoUringBuf, resv), 14);
        assert_eq!(offset_of!(IoUringGeteventsArg, timespec), 16);
        assert_eq!(offset_of!(IoUringProbe, ops), 16);
    }

    #[test]
    fn syscall_errors_use_veloq_io_error() {
        let error = unsafe { io_uring_register(-1, IORING_REGISTER_PROBE, core::ptr::null(), 0) }
            .expect_err("an invalid ring fd must fail");

        assert_eq!(error.raw_os_error(), Some(libc::EINVAL));
    }

    #[test]
    fn raw_return_decoder_never_exposes_negative_success() {
        assert_eq!(decode_direct_return(7), RawSyscallResult::Value(7));
        assert_eq!(
            decode_direct_return(-libc::EAGAIN as i64),
            RawSyscallResult::Errno(libc::EAGAIN)
        );
        assert_eq!(
            decode_direct_return(i64::MIN),
            RawSyscallResult::Errno(libc::EOVERFLOW)
        );
    }
}
