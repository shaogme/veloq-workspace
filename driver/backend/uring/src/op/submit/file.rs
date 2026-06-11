use crate::driver::UringDriver;
use crate::error::{UringDriverResult as DriverResult, UringError};
use crate::op::payload::KernelRef;
use crate::op::{
    Close, Fallocate, FallocateRaw, Fsync, FsyncRaw, Open, ReadFixed, ReadRaw, SyncFileRange,
    SyncFileRangeRaw, WriteFixed, WriteRaw,
};
use diagweave::prelude::*;
use io_uring::{opcode, squeue, types};
use veloq_buf::PoolKind;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::driver::SubmitTokenContext;
use veloq_driver_core::op::{checked_read_buf_range, checked_write_buf_range};

use super::{invalid_buf_io_range, resolve_any_fd, resolve_file_fd};

pub(crate) unsafe fn make_sqe_read_fixed(
    _kernel: &mut KernelRef<ReadFixed>,
    rw_op: &mut ReadFixed,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let region_info = rw_op.buf.resolve_region_info();
    let (ptr, len) = checked_read_buf_range(&mut rw_op.buf, rw_op.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_read_fixed", err))?;
    let offset = rw_op.offset;
    let fixed_fd = resolve_file_fd(
        &driver.file_slots,
        rw_op.fd,
        "uring.op.submit.make_sqe_read_fixed",
    )?;

    let is_registered = if region_info.pool_kind == PoolKind::SlotBased {
        driver
            .registered_chunks
            .get(region_info.id.as_usize())
            .unwrap_or(false)
    } else {
        false
    };

    if is_registered {
        let fixed_idx = region_info.id.raw();
        Ok(opcode::ReadFixed::new(fixed_fd, ptr, len, fixed_idx)
            .offset(offset)
            .build())
    } else {
        Ok(opcode::Read::new(fixed_fd, ptr, len).offset(offset).build())
    }
}

pub(crate) unsafe fn make_sqe_read_raw(
    _kernel: &mut KernelRef<ReadRaw>,
    rw_op: &mut ReadRaw,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let region_info = rw_op.buf.resolve_region_info();
    let (ptr, len) = checked_read_buf_range(&mut rw_op.buf, rw_op.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_read_raw", err))?;
    let fd = rw_op.fd.as_fd();

    let is_registered = if region_info.pool_kind == PoolKind::SlotBased {
        driver
            .registered_chunks
            .get(region_info.id.as_usize())
            .unwrap_or(false)
    } else {
        false
    };

    if is_registered {
        let fixed_idx = region_info.id.raw();
        Ok(opcode::ReadFixed::new(types::Fd(fd), ptr, len, fixed_idx)
            .offset(rw_op.offset)
            .build())
    } else {
        Ok(opcode::Read::new(types::Fd(fd), ptr, len)
            .offset(rw_op.offset)
            .build())
    }
}

pub(crate) unsafe fn make_sqe_write_fixed(
    _kernel: &mut KernelRef<WriteFixed>,
    rw_op: &mut WriteFixed,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let region_info = rw_op.buf.resolve_region_info();
    let (ptr, len) = checked_write_buf_range(&rw_op.buf, rw_op.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_write_fixed", err))?;
    let offset = rw_op.offset;
    let fixed_fd = resolve_file_fd(
        &driver.file_slots,
        rw_op.fd,
        "uring.op.submit.make_sqe_write_fixed",
    )?;

    let is_registered = if region_info.pool_kind == PoolKind::SlotBased {
        driver
            .registered_chunks
            .get(region_info.id.as_usize())
            .unwrap_or(false)
    } else {
        false
    };

    if is_registered {
        let fixed_idx = region_info.id.raw();
        Ok(opcode::WriteFixed::new(fixed_fd, ptr, len, fixed_idx)
            .offset(offset)
            .build())
    } else {
        Ok(opcode::Write::new(fixed_fd, ptr, len)
            .offset(offset)
            .build())
    }
}

pub(crate) unsafe fn make_sqe_write_raw(
    _kernel: &mut KernelRef<WriteRaw>,
    rw_op: &mut WriteRaw,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let region_info = rw_op.buf.resolve_region_info();
    let (ptr, len) = checked_write_buf_range(&rw_op.buf, rw_op.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_write_raw", err))?;
    let fd = rw_op.fd.as_fd();

    let is_registered = if region_info.pool_kind == PoolKind::SlotBased {
        driver
            .registered_chunks
            .get(region_info.id.as_usize())
            .unwrap_or(false)
    } else {
        false
    };

    if is_registered {
        let fixed_idx = region_info.id.raw();
        Ok(opcode::WriteFixed::new(types::Fd(fd), ptr, len, fixed_idx)
            .offset(rw_op.offset)
            .build())
    } else {
        Ok(opcode::Write::new(types::Fd(fd), ptr, len)
            .offset(rw_op.offset)
            .build())
    }
}

pub(crate) unsafe fn make_sqe_close(
    _kernel: &mut KernelRef<Close>,
    close_op: &mut Close,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let idx = close_op.fd.fixed_index() as usize;
    if let Some(crate::driver::RegisteredFileEntry::BorrowedFd { .. }) =
        driver.file_slots.get(idx).and_then(|s| s.entry.as_ref())
    {
        return Err(UringError::InvalidInput
            .report(
                "uring.op.submit.make_sqe_close",
                "Close is only valid for owned registered file descriptors",
            )
            .push_ctx("scope", "uring.op.submit.make_sqe_close")
            .with_ctx("fd_fixed_index", close_op.fd.fixed_index())
            .attach_note("borrowed fd Close rejected"));
    }
    let fixed_fd = resolve_any_fd(
        &driver.file_slots,
        close_op.fd,
        "uring.op.submit.make_sqe_close",
    )?;
    Ok(opcode::Close::new(fixed_fd).build())
}

pub(crate) unsafe fn make_sqe_fsync(
    _kernel: &mut KernelRef<Fsync>,
    fsync_op: &mut Fsync,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let flags = if fsync_op.datasync {
        io_uring::types::FsyncFlags::DATASYNC
    } else {
        io_uring::types::FsyncFlags::empty()
    };

    let fixed_fd = resolve_file_fd(
        &driver.file_slots,
        fsync_op.fd,
        "uring.op.submit.make_sqe_fsync",
    )?;
    Ok(opcode::Fsync::new(fixed_fd).flags(flags).build())
}

pub(crate) unsafe fn make_sqe_fsync_raw(
    _kernel: &mut KernelRef<FsyncRaw>,
    fsync_op: &mut FsyncRaw,
    _driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let flags = if fsync_op.datasync {
        io_uring::types::FsyncFlags::DATASYNC
    } else {
        io_uring::types::FsyncFlags::empty()
    };

    let fd = fsync_op.fd.as_fd();
    Ok(opcode::Fsync::new(types::Fd(fd)).flags(flags).build())
}

pub(crate) unsafe fn make_sqe_sync_range(
    _kernel: &mut KernelRef<SyncFileRange>,
    sync_op: &mut SyncFileRange,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let nbytes = if sync_op.nbytes > u32::MAX as u64 {
        if sync_op.nbytes == u64::MAX {
            0
        } else {
            return Err(UringError::InvalidInput
                .to_report()
                .push_ctx("scope", "uring.op.submit.make_sqe_sync_range")
                .with_ctx("nbytes", sync_op.nbytes)
                .with_ctx("max_nbytes", u32::MAX as u64)
                .attach_note("sync_file_range nbytes exceeds 32-bit limit and is not u64::MAX"));
        }
    } else {
        sync_op.nbytes as u32
    };

    let fixed_fd = resolve_file_fd(
        &driver.file_slots,
        sync_op.fd,
        "uring.op.submit.make_sqe_sync_range",
    )?;
    Ok(opcode::SyncFileRange::new(fixed_fd, nbytes)
        .offset(sync_op.offset)
        .flags(sync_op.flags)
        .build())
}

pub(crate) unsafe fn make_sqe_sync_range_raw(
    _kernel: &mut KernelRef<SyncFileRangeRaw>,
    sync_op: &mut SyncFileRangeRaw,
    _driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let nbytes = if sync_op.nbytes > u32::MAX as u64 {
        if sync_op.nbytes == u64::MAX {
            0
        } else {
            return Err(UringError::InvalidInput
                .to_report()
                .push_ctx("scope", "uring.op.submit.make_sqe_sync_range_raw")
                .with_ctx("nbytes", sync_op.nbytes)
                .with_ctx("max_nbytes", u32::MAX as u64)
                .attach_note("sync_file_range nbytes exceeds 32-bit limit and is not u64::MAX"));
        }
    } else {
        sync_op.nbytes as u32
    };

    let fd = sync_op.fd.as_fd();
    Ok(opcode::SyncFileRange::new(types::Fd(fd), nbytes)
        .offset(sync_op.offset)
        .flags(sync_op.flags)
        .build())
}

pub(crate) unsafe fn make_sqe_fallocate(
    _kernel: &mut KernelRef<Fallocate>,
    fallocate_op: &mut Fallocate,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let fixed_fd = resolve_file_fd(
        &driver.file_slots,
        fallocate_op.fd,
        "uring.op.submit.make_sqe_fallocate",
    )?;
    Ok(opcode::Fallocate::new(fixed_fd, fallocate_op.len)
        .offset(fallocate_op.offset)
        .mode(fallocate_op.mode)
        .build())
}

pub(crate) unsafe fn make_sqe_fallocate_raw(
    _kernel: &mut KernelRef<FallocateRaw>,
    fallocate_op: &mut FallocateRaw,
    _driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let fd = fallocate_op.fd.as_fd();
    Ok(opcode::Fallocate::new(types::Fd(fd), fallocate_op.len)
        .offset(fallocate_op.offset)
        .mode(fallocate_op.mode)
        .build())
}

pub(crate) unsafe fn make_sqe_open(
    _kernel: &mut crate::op::payload::OpenPayload,
    user: &mut Open,
    _driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let path_ptr = user.path.as_slice().as_ptr() as *const _;
    Ok(opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_ptr)
        .flags(user.flags)
        .mode(user.mode)
        .build())
}

pub(crate) fn resolve_chunks_read_fixed(
    _kernel: &KernelRef<ReadFixed>,
    rw_op: &ReadFixed,
    chunks: &mut [ChunkId],
) -> usize {
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}

pub(crate) fn resolve_chunks_read_raw(
    _kernel: &KernelRef<ReadRaw>,
    rw_op: &ReadRaw,
    chunks: &mut [ChunkId],
) -> usize {
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}

pub(crate) fn resolve_chunks_write_fixed(
    _kernel: &KernelRef<WriteFixed>,
    rw_op: &WriteFixed,
    chunks: &mut [ChunkId],
) -> usize {
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}

pub(crate) fn resolve_chunks_write_raw(
    _kernel: &KernelRef<WriteRaw>,
    rw_op: &WriteRaw,
    chunks: &mut [ChunkId],
) -> usize {
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}
