//! Linux `io_uring` ABI declarations.
//!
//! These declarations are copied from the layout of Linux's
//! `linux/io_uring.h`, cross-checked against the local upstream
//! `io-uring 0.7.12` cache, and deliberately use fixed-width Rust types.

#[cfg(test)]
mod tests;

use libc::{SYS_io_uring_enter, SYS_io_uring_register, SYS_io_uring_setup, c_long};

pub(crate) const SYS_IO_URING_SETUP: c_long = SYS_io_uring_setup;
pub(crate) const SYS_IO_URING_ENTER: c_long = SYS_io_uring_enter;
pub(crate) const SYS_IO_URING_REGISTER: c_long = SYS_io_uring_register;

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

pub(crate) const IORING_OFF_SQ_RING: u64 = 0;
pub(crate) const IORING_OFF_CQ_RING: u64 = 1 << 27;
pub(crate) const IORING_OFF_SQES: u64 = 1 << 28;

pub(crate) const IORING_SQ_NEED_WAKEUP: u32 = 1;
pub(crate) const IORING_SQ_CQ_OVERFLOW: u32 = 2;

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
pub(crate) const IORING_RECV_MULTISHOT: u32 = 2;
pub(crate) const IORING_ACCEPT_MULTISHOT: u32 = 1;
pub(crate) const IORING_ACCEPT_DONTWAIT: u32 = 2;
pub(crate) const IORING_ACCEPT_POLL_FIRST: u32 = 4;

pub(crate) const IORING_RSRC_REGISTER_SPARSE: u32 = 1;
pub(crate) const IO_URING_OP_SUPPORTED: u16 = 1;

pub(crate) const IORING_REGISTER_BUFFERS: u32 = 0;
pub(crate) const IORING_UNREGISTER_BUFFERS: u32 = 1;
pub(crate) const IORING_REGISTER_FILES: u32 = 2;
pub(crate) const IORING_UNREGISTER_FILES: u32 = 3;
pub(crate) const IORING_REGISTER_FILES_UPDATE: u32 = 6;
pub(crate) const IORING_REGISTER_PROBE: u32 = 8;
pub(crate) const IORING_REGISTER_FILES2: u32 = 13;
pub(crate) const IORING_REGISTER_FILES_UPDATE2: u32 = 14;
pub(crate) const IORING_REGISTER_BUFFERS2: u32 = 15;
pub(crate) const IORING_REGISTER_BUFFERS_UPDATE: u32 = 16;
pub(crate) const IORING_REGISTER_PBUF_RING: u32 = 22;
pub(crate) const IORING_UNREGISTER_PBUF_RING: u32 = 23;

pub(crate) const IORING_OP_NOP: u8 = 0;
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
pub(crate) struct IoUringRsrcRegister {
    pub(crate) nr: u32,
    pub(crate) flags: u32,
    pub(crate) resv2: u64,
    pub(crate) data: u64,
    pub(crate) tags: u64,
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default)]
#[cfg(test)]
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
