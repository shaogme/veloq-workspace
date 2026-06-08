use crate::driver::UringDriver;
use crate::error::{UringDriverResult as DriverResult, UringError};
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

macro_rules! make_rw_fixed {
    ($fn_name:ident, $variant:ident, $type_raw:path, $type_fixed:path) => {
        pub(crate) unsafe fn $fn_name(
            _kernel: &mut crate::op::payload::KernelRef<$variant>,
            rw_op: &mut $variant,
            driver: &mut UringDriver,
            _token: SubmitTokenContext,
        ) -> DriverResult<squeue::Entry> {
            let region_info = rw_op.buf.resolve_region_info();
            let (ptr, len) =
                checked_read_buf_range(&mut rw_op.buf, rw_op.buf_offset).map_err(|err| {
                    invalid_buf_io_range(concat!("uring.op.submit.", stringify!($fn_name)), err)
                })?;
            let offset = rw_op.offset;
            let fixed_fd = resolve_file_fd(
                &driver.registered_files,
                &driver.file_generations,
                rw_op.fd,
                concat!("uring.op.submit.", stringify!($fn_name)),
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
                Ok($type_fixed(fixed_fd, ptr, len, fixed_idx)
                    .offset(offset)
                    .build())
            } else {
                Ok($type_raw(fixed_fd, ptr, len).offset(offset).build())
            }
        }
    };
    ($fn_name:ident, $variant:ident, $type_raw:path, $type_fixed:path, write) => {
        pub(crate) unsafe fn $fn_name(
            _kernel: &mut crate::op::payload::KernelRef<$variant>,
            rw_op: &mut $variant,
            driver: &mut UringDriver,
            _token: SubmitTokenContext,
        ) -> DriverResult<squeue::Entry> {
            let region_info = rw_op.buf.resolve_region_info();
            let (ptr, len) =
                checked_write_buf_range(&rw_op.buf, rw_op.buf_offset).map_err(|err| {
                    invalid_buf_io_range(concat!("uring.op.submit.", stringify!($fn_name)), err)
                })?;
            let offset = rw_op.offset;
            let fixed_fd = resolve_file_fd(
                &driver.registered_files,
                &driver.file_generations,
                rw_op.fd,
                concat!("uring.op.submit.", stringify!($fn_name)),
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
                Ok($type_fixed(fixed_fd, ptr, len, fixed_idx)
                    .offset(offset)
                    .build())
            } else {
                Ok($type_raw(fixed_fd, ptr, len).offset(offset).build())
            }
        }
    };
}

macro_rules! make_rw_raw {
    ($fn_name:ident, $OpType:ident, $type_raw:path, write) => {
        pub(crate) unsafe fn $fn_name(
            _kernel: &mut crate::op::payload::KernelRef<$OpType>,
            rw_op: &mut $OpType,
            driver: &mut UringDriver,
            _token: SubmitTokenContext,
        ) -> DriverResult<squeue::Entry> {
            let region_info = rw_op.buf.resolve_region_info();
            let (ptr, len) =
                checked_write_buf_range(&rw_op.buf, rw_op.buf_offset).map_err(|err| {
                    invalid_buf_io_range(concat!("uring.op.submit.", stringify!($fn_name)), err)
                })?;
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
                Ok($type_raw(types::Fd(fd), ptr, len)
                    .offset(rw_op.offset)
                    .build())
            }
        }
    };
    ($fn_name:ident, $OpType:ident, $type_raw:path) => {
        pub(crate) unsafe fn $fn_name(
            _kernel: &mut crate::op::payload::KernelRef<$OpType>,
            rw_op: &mut $OpType,
            driver: &mut UringDriver,
            _token: SubmitTokenContext,
        ) -> DriverResult<squeue::Entry> {
            let region_info = rw_op.buf.resolve_region_info();
            let (ptr, len) =
                checked_read_buf_range(&mut rw_op.buf, rw_op.buf_offset).map_err(|err| {
                    invalid_buf_io_range(concat!("uring.op.submit.", stringify!($fn_name)), err)
                })?;
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
                Ok($type_raw(types::Fd(fd), ptr, len)
                    .offset(rw_op.offset)
                    .build())
            }
        }
    };
}

make_rw_fixed!(
    make_sqe_read_fixed,
    ReadFixed,
    opcode::Read::new,
    opcode::ReadFixed::new
);
make_rw_raw!(make_sqe_read_raw, ReadRaw, opcode::Read::new);
make_rw_fixed!(
    make_sqe_write_fixed,
    WriteFixed,
    opcode::Write::new,
    opcode::WriteFixed::new,
    write
);
make_rw_raw!(make_sqe_write_raw, WriteRaw, opcode::Write::new, write);

pub(crate) unsafe fn make_sqe_close(
    _kernel: &mut crate::op::payload::KernelRef<Close>,
    close_op: &mut Close,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let fixed_fd = resolve_any_fd(
        &driver.registered_files,
        &driver.file_generations,
        close_op.fd,
        "uring.op.submit.make_sqe_close",
    )?;
    Ok(opcode::Close::new(fixed_fd).build())
}

pub(crate) unsafe fn make_sqe_fsync(
    _kernel: &mut crate::op::payload::KernelRef<Fsync>,
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
        &driver.registered_files,
        &driver.file_generations,
        fsync_op.fd,
        "uring.op.submit.make_sqe_fsync",
    )?;
    Ok(opcode::Fsync::new(fixed_fd).flags(flags).build())
}

pub(crate) unsafe fn make_sqe_fsync_raw(
    _kernel: &mut crate::op::payload::KernelRef<FsyncRaw>,
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
    _kernel: &mut crate::op::payload::KernelRef<SyncFileRange>,
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
        &driver.registered_files,
        &driver.file_generations,
        sync_op.fd,
        "uring.op.submit.make_sqe_sync_range",
    )?;
    Ok(opcode::SyncFileRange::new(fixed_fd, nbytes)
        .offset(sync_op.offset)
        .flags(sync_op.flags)
        .build())
}

pub(crate) unsafe fn make_sqe_sync_range_raw(
    _kernel: &mut crate::op::payload::KernelRef<SyncFileRangeRaw>,
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
    _kernel: &mut crate::op::payload::KernelRef<Fallocate>,
    fallocate_op: &mut Fallocate,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let fixed_fd = resolve_file_fd(
        &driver.registered_files,
        &driver.file_generations,
        fallocate_op.fd,
        "uring.op.submit.make_sqe_fallocate",
    )?;
    Ok(opcode::Fallocate::new(fixed_fd, fallocate_op.len)
        .offset(fallocate_op.offset)
        .mode(fallocate_op.mode)
        .build())
}

pub(crate) unsafe fn make_sqe_fallocate_raw(
    _kernel: &mut crate::op::payload::KernelRef<FallocateRaw>,
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
    _kernel: &crate::op::payload::KernelRef<ReadFixed>,
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
    _kernel: &crate::op::payload::KernelRef<ReadRaw>,
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
    _kernel: &crate::op::payload::KernelRef<WriteFixed>,
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
    _kernel: &crate::op::payload::KernelRef<WriteRaw>,
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
