//! Private Linux `io_uring` ABI declarations and syscall wrappers.
//!
//! These declarations are copied from the layout of Linux's
//! `linux/io_uring.h`, cross-checked against the local upstream
//! `io-uring 0.7.12` cache, and deliberately use fixed-width Rust types.

#![allow(dead_code)]

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
pub(crate) const IORING_SETUP_CQSIZE: u32 = 8;
pub(crate) const IORING_SETUP_COOP_TASKRUN: u32 = 256;
pub(crate) const IORING_SETUP_SQE128: u32 = 1024;
pub(crate) const IORING_SETUP_CQE32: u32 = 2048;
pub(crate) const IORING_SETUP_SINGLE_ISSUER: u32 = 4096;
pub(crate) const IORING_SETUP_DEFER_TASKRUN: u32 = 8192;
pub(crate) const IORING_SETUP_NO_MMAP: u32 = 16_384;
pub(crate) const IORING_SETUP_NO_SQARRAY: u32 = 65_536;

pub(crate) const IORING_FEAT_SINGLE_MMAP: u32 = 1;
pub(crate) const IORING_FEAT_NODROP: u32 = 2;
pub(crate) const IORING_FEAT_EXT_ARG: u32 = 256;

pub(crate) const IORING_OFF_SQ_RING: u64 = 0;
pub(crate) const IORING_OFF_CQ_RING: u64 = 1 << 27;
pub(crate) const IORING_OFF_SQES: u64 = 1 << 28;
pub(crate) const IORING_OFF_PBUF_RING: u64 = 1 << 31;
pub(crate) const IORING_OFF_PBUF_SHIFT: u32 = 16;

pub(crate) const IORING_SQ_NEED_WAKEUP: u32 = 1;
pub(crate) const IORING_SQ_CQ_OVERFLOW: u32 = 2;

pub(crate) const IORING_CQ_EVENTFD_DISABLED: u32 = 1;
pub(crate) const IORING_ENTER_GETEVENTS: u32 = 1;
pub(crate) const IORING_ENTER_SQ_WAKEUP: u32 = 2;
pub(crate) const IORING_ENTER_SQ_WAIT: u32 = 4;
pub(crate) const IORING_ENTER_EXT_ARG: u32 = 8;
pub(crate) const IORING_ENTER_REGISTERED_RING: u32 = 16;

pub(crate) const IORING_CQE_F_BUFFER: u32 = 1;
pub(crate) const IORING_CQE_F_MORE: u32 = 2;
pub(crate) const IORING_CQE_BUFFER_SHIFT: u32 = 16;

pub(crate) const IOSQE_FIXED_FILE: u8 = 1;
pub(crate) const IOSQE_BUFFER_SELECT: u8 = 1 << 5;

pub(crate) const IORING_FSYNC_DATASYNC: u32 = 1;
pub(crate) const IORING_RECV_MULTISHOT: u32 = 2;
pub(crate) const IORING_ACCEPT_MULTISHOT: u32 = 1;

pub(crate) const IORING_RSRC_REGISTER_SPARSE: u32 = 1;
pub(crate) const IORING_REGISTER_FILES_SKIP: i32 = -2;
pub(crate) const IO_URING_OP_SUPPORTED: u16 = 1;

pub(crate) const IORING_REGISTER_BUFFERS: u32 = 0;
pub(crate) const IORING_REGISTER_FILES: u32 = 2;
pub(crate) const IORING_REGISTER_FILES_UPDATE: u32 = 6;
pub(crate) const IORING_REGISTER_PROBE: u32 = 8;
pub(crate) const IORING_REGISTER_BUFFERS_UPDATE: u32 = 16;
pub(crate) const IORING_REGISTER_PBUF_RING: u32 = 22;
pub(crate) const IORING_UNREGISTER_PBUF_RING: u32 = 23;

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
    if result == -1 {
        Err(Error::last_os_error())
    } else {
        Ok(result as RawFd)
    }
}

/// Invoke `io_uring_enter` and convert the libc error boundary once.
pub(crate) unsafe fn io_uring_enter(
    fd: RawFd,
    to_submit: u32,
    min_complete: u32,
    flags: u32,
    arg: *const libc::c_void,
    arg_size: usize,
) -> Result<i32> {
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
    if result == -1 {
        Err(Error::last_os_error())
    } else {
        Ok(result as i32)
    }
}

/// Invoke `io_uring_register` and convert the libc error boundary once.
pub(crate) unsafe fn io_uring_register(
    fd: RawFd,
    opcode: u32,
    arg: *const libc::c_void,
    nr_args: u32,
) -> Result<i32> {
    let result = unsafe { libc::syscall(SYS_IO_URING_REGISTER, fd, opcode, arg, nr_args) };
    if result == -1 {
        Err(Error::last_os_error())
    } else {
        Ok(result as i32)
    }
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
}
