use crate::{
    config::IoFd,
    driver::{RegisteredFileEntry, SqeEnv, SqeFd},
    error::{UringError, UringResult},
    op::{
        Close, Fallocate, FallocateRaw, Fsync, FsyncRaw, Open, ReadFixed, ReadRaw, SyncFileRange,
        SyncFileRangeRaw, WriteFixed, WriteRaw,
        payload::{KernelRef, OpenPayload},
    },
};
use diagweave::prelude::*;
use io_uring::{opcode, squeue, types};
use veloq_buf::{PoolKind, heap::ChunkId};
use veloq_driver_core::driver::SubmitTokenContext;
use veloq_std::string::ToString;

use super::{invalid_buf_io_range, resolve_any_fd, resolve_file_fd, sqe_with_fd};

pub(crate) unsafe fn make_sqe_read_fixed(
    _kernel: &mut KernelRef<ReadFixed>,
    rw_op: &mut ReadFixed,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let region_info = rw_op.buf.resolve_region_info();
    let (ptr, len) = rw_op
        .buf
        .checked_read_range(rw_op.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_read_fixed", err))?;
    let offset = rw_op.offset;
    let fd = resolve_file_fd(
        env.file_table,
        rw_op.fd,
        "uring.op.submit.make_sqe_read_fixed",
    )?;

    let is_registered =
        region_info.pool_kind == PoolKind::SlotBased && env.is_chunk_registered(region_info.id);

    if is_registered {
        let fixed_idx = region_info.id.raw();
        Ok(sqe_with_fd!(fd, |f| opcode::ReadFixed::new(
            f, ptr, len, fixed_idx
        )
        .offset(offset)
        .build()))
    } else {
        Ok(sqe_with_fd!(fd, |f| opcode::Read::new(f, ptr, len)
            .offset(offset)
            .build()))
    }
}

pub(crate) unsafe fn make_sqe_read_raw(
    _kernel: &mut KernelRef<ReadRaw>,
    rw_op: &mut ReadRaw,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let region_info = rw_op.buf.resolve_region_info();
    let (ptr, len) = rw_op
        .buf
        .checked_read_range(rw_op.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_read_raw", err))?;
    let fd = rw_op.fd.as_fd();

    let is_registered =
        region_info.pool_kind == PoolKind::SlotBased && env.is_chunk_registered(region_info.id);

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
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let region_info = rw_op.buf.resolve_region_info();
    let (ptr, len) = rw_op
        .buf
        .checked_write_range(rw_op.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_write_fixed", err))?;
    let offset = rw_op.offset;
    let fd = resolve_file_fd(
        env.file_table,
        rw_op.fd,
        "uring.op.submit.make_sqe_write_fixed",
    )?;

    let is_registered =
        region_info.pool_kind == PoolKind::SlotBased && env.is_chunk_registered(region_info.id);

    if is_registered {
        let fixed_idx = region_info.id.raw();
        Ok(sqe_with_fd!(fd, |f| opcode::WriteFixed::new(
            f, ptr, len, fixed_idx
        )
        .offset(offset)
        .build()))
    } else {
        Ok(sqe_with_fd!(fd, |f| opcode::Write::new(f, ptr, len)
            .offset(offset)
            .build()))
    }
}

pub(crate) unsafe fn make_sqe_write_raw(
    _kernel: &mut KernelRef<WriteRaw>,
    rw_op: &mut WriteRaw,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let region_info = rw_op.buf.resolve_region_info();
    let (ptr, len) = rw_op
        .buf
        .checked_write_range(rw_op.buf_offset)
        .map_err(|err| invalid_buf_io_range("uring.op.submit.make_sqe_write_raw", err))?;
    let fd = rw_op.fd.as_fd();

    let is_registered =
        region_info.pool_kind == PoolKind::SlotBased && env.is_chunk_registered(region_info.id);

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
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let scope = "uring.op.submit.make_sqe_close";
    // `Close` hands the descriptor to the kernel, so it is only valid where the driver owns
    // the handle. Which side of the file table the descriptor lives on decides where to look:
    // a registered one has a slot entry, a direct one is tracked by handle.
    let owned = match close_op.fd {
        IoFd::Registered { index, .. } => !matches!(
            env.file_table.entry(index),
            Some(RegisteredFileEntry::BorrowedFd { .. })
        ),
        IoFd::Direct(raw) => env.file_table.owns_direct(raw),
    };
    if !owned {
        return Err(UringError::InvalidInput
            .report(
                scope,
                "Close is only valid for owned registered file descriptors",
            )
            .push_ctx("scope", scope)
            .with_ctx("fd", close_op.fd.to_string())
            .attach_note("borrowed fd Close rejected"));
    }
    let fd = resolve_any_fd(env.file_table, close_op.fd, scope)?;
    Ok(sqe_with_fd!(fd, |f| opcode::Close::new(f).build()))
}

pub(crate) unsafe fn make_sqe_fsync(
    _kernel: &mut KernelRef<Fsync>,
    fsync_op: &mut Fsync,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let flags = if fsync_op.datasync {
        io_uring::types::FsyncFlags::DATASYNC
    } else {
        io_uring::types::FsyncFlags::empty()
    };

    let fd = resolve_file_fd(
        env.file_table,
        fsync_op.fd,
        "uring.op.submit.make_sqe_fsync",
    )?;
    Ok(sqe_with_fd!(fd, |f| opcode::Fsync::new(f)
        .flags(flags)
        .build()))
}

pub(crate) unsafe fn make_sqe_fsync_raw(
    _kernel: &mut KernelRef<FsyncRaw>,
    fsync_op: &mut FsyncRaw,
    _env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
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
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let nbytes = if sync_op.nbytes > u32::MAX as u64 {
        if sync_op.nbytes == u64::MAX {
            0
        } else {
            return UringError::InvalidInput
                .push_ctx("scope", "uring.op.submit.make_sqe_sync_range")
                .with_ctx("nbytes", sync_op.nbytes)
                .with_ctx("max_nbytes", u32::MAX as u64)
                .attach_note("sync_file_range nbytes exceeds 32-bit limit and is not u64::MAX");
        }
    } else {
        sync_op.nbytes as u32
    };

    let fd = resolve_file_fd(
        env.file_table,
        sync_op.fd,
        "uring.op.submit.make_sqe_sync_range",
    )?;
    Ok(sqe_with_fd!(fd, |f| opcode::SyncFileRange::new(f, nbytes)
        .offset(sync_op.offset)
        .flags(sync_op.flags)
        .build()))
}

pub(crate) unsafe fn make_sqe_sync_range_raw(
    _kernel: &mut KernelRef<SyncFileRangeRaw>,
    sync_op: &mut SyncFileRangeRaw,
    _env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let nbytes = if sync_op.nbytes > u32::MAX as u64 {
        if sync_op.nbytes == u64::MAX {
            0
        } else {
            return UringError::InvalidInput
                .push_ctx("scope", "uring.op.submit.make_sqe_sync_range_raw")
                .with_ctx("nbytes", sync_op.nbytes)
                .with_ctx("max_nbytes", u32::MAX as u64)
                .attach_note("sync_file_range nbytes exceeds 32-bit limit and is not u64::MAX");
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
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let fd = resolve_file_fd(
        env.file_table,
        fallocate_op.fd,
        "uring.op.submit.make_sqe_fallocate",
    )?;
    Ok(sqe_with_fd!(fd, |f| opcode::Fallocate::new(
        f,
        fallocate_op.len
    )
    .offset(fallocate_op.offset)
    .mode(fallocate_op.mode)
    .build()))
}

pub(crate) unsafe fn make_sqe_fallocate_raw(
    _kernel: &mut KernelRef<FallocateRaw>,
    fallocate_op: &mut FallocateRaw,
    _env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let fd = fallocate_op.fd.as_fd();
    Ok(opcode::Fallocate::new(types::Fd(fd), fallocate_op.len)
        .offset(fallocate_op.offset)
        .mode(fallocate_op.mode)
        .build())
}

pub(crate) unsafe fn make_sqe_open(
    _kernel: &mut OpenPayload,
    user: &mut Open,
    _env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
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
