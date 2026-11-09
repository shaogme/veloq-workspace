use core::mem::{align_of, offset_of, size_of};

use crate::sys::{
    CqRingOffsets, IoUringBuf, IoUringBufReg, IoUringCqe, IoUringFilesUpdate, IoUringGeteventsArg,
    IoUringParams, IoUringProbe, IoUringProbeOp, IoUringRsrcRegister, IoUringRsrcUpdate,
    IoUringRsrcUpdate2, IoUringSqe, KernelTimespec, SYS_IO_URING_ENTER, SYS_IO_URING_REGISTER,
    SYS_IO_URING_SETUP, SqRingOffsets,
};

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
