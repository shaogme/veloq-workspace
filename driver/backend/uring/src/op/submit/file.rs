use crate::driver::UringDriver;
use crate::error::{UringDriverResult as DriverResult, UringError};
use crate::op::{UringOp, UringUserPayload};
use diagweave::prelude::*;
use io_uring::{opcode, squeue, types};
use veloq_buf::PoolKind;
use veloq_buf::heap::ChunkId;
use veloq_driver_core::op::{checked_read_buf_range, checked_write_buf_range};

use super::{invalid_buf_io_range, payload_variant_mismatch, resolve_any_fd, resolve_file_fd};

macro_rules! make_rw_fixed {
    ($fn_name:ident, $variant:ident, $type_raw:path, $type_fixed:path) => {
        pub(crate) unsafe fn $fn_name(
            _op: &mut UringOp,
            driver: &mut UringDriver,
            user_data: usize,
        ) -> DriverResult<squeue::Entry> {
            let storage = driver.ops.slot_storage_mut(user_data).ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let payload = storage.payload.as_mut().ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let rw_op = match payload {
                crate::op::UringUserPayload::$variant(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };

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
            _op: &mut UringOp,
            driver: &mut UringDriver,
            user_data: usize,
        ) -> DriverResult<squeue::Entry> {
            let storage = driver.ops.slot_storage_mut(user_data).ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let payload = storage.payload.as_mut().ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let rw_op = match payload {
                crate::op::UringUserPayload::$variant(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };
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
            _op: &mut UringOp,
            driver: &mut UringDriver,
            user_data: usize,
        ) -> DriverResult<squeue::Entry> {
            let storage = driver.ops.slot_storage_mut(user_data).ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let payload = storage.payload.as_mut().ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let rw_op = match payload {
                crate::op::UringUserPayload::$OpType(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };
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
            _op: &mut UringOp,
            driver: &mut UringDriver,
            user_data: usize,
        ) -> DriverResult<squeue::Entry> {
            let storage = driver.ops.slot_storage_mut(user_data).ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let payload = storage.payload.as_mut().ok_or_else(|| {
                payload_variant_mismatch(concat!("uring.op.submit.", stringify!($fn_name)))
            })?;
            let rw_op = match payload {
                crate::op::UringUserPayload::$OpType(p) => p,
                _ => {
                    return Err(payload_variant_mismatch(concat!(
                        "uring.op.submit.",
                        stringify!($fn_name)
                    )));
                }
            };
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

impl_default_completion!(on_complete_read_fixed);
impl_lifecycle!(drop_read_fixed, Read, direct_fd);

impl_default_completion!(on_complete_write_fixed);
impl_lifecycle!(drop_write_fixed, Write, direct_fd);

pub(crate) unsafe fn make_sqe_close(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_close"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_close"))?;
    let close_op = match payload {
        crate::op::UringUserPayload::Close(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_close")),
    };
    let fixed_fd = resolve_any_fd(
        &driver.registered_files,
        &driver.file_generations,
        close_op.fd,
        "uring.op.submit.make_sqe_close",
    )?;
    Ok(opcode::Close::new(fixed_fd).build())
}

impl_default_completion!(on_complete_close);
impl_lifecycle!(drop_close, Close, direct_fd);

pub(crate) unsafe fn make_sqe_fsync(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fsync"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fsync"))?;
    let fsync_op = match payload {
        crate::op::UringUserPayload::Fsync(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_fsync")),
    };
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

impl_default_completion!(on_complete_fsync);
impl_lifecycle!(drop_fsync, Fsync, direct_fd);

pub(crate) unsafe fn make_sqe_fsync_raw(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fsync_raw"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fsync_raw"))?;
    let fsync_op = match payload {
        crate::op::UringUserPayload::FsyncRaw(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_fsync_raw",
            ));
        }
    };
    let flags = if fsync_op.datasync {
        io_uring::types::FsyncFlags::DATASYNC
    } else {
        io_uring::types::FsyncFlags::empty()
    };

    let fd = fsync_op.fd.as_fd();
    Ok(opcode::Fsync::new(types::Fd(fd)).flags(flags).build())
}

pub(crate) unsafe fn make_sqe_sync_range(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_sync_range"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_sync_range"))?;
    let sync_op = match payload {
        crate::op::UringUserPayload::SyncFileRange(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_sync_range",
            ));
        }
    };
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

impl_default_completion!(on_complete_sync_range);
impl_lifecycle!(drop_sync_range, SyncRange, direct_fd);

pub(crate) unsafe fn make_sqe_sync_range_raw(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_sync_range_raw"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_sync_range_raw"))?;
    let sync_op = match payload {
        crate::op::UringUserPayload::SyncFileRangeRaw(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_sync_range_raw",
            ));
        }
    };
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
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fallocate"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fallocate"))?;
    let fallocate_op = match payload {
        crate::op::UringUserPayload::Fallocate(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_fallocate",
            ));
        }
    };
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

impl_default_completion!(on_complete_fallocate);
impl_lifecycle!(drop_fallocate, Fallocate, direct_fd);

pub(crate) unsafe fn make_sqe_fallocate_raw(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fallocate_raw"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_fallocate_raw"))?;
    let fallocate_op = match payload {
        crate::op::UringUserPayload::FallocateRaw(p) => p,
        _ => {
            return Err(payload_variant_mismatch(
                "uring.op.submit.make_sqe_fallocate_raw",
            ));
        }
    };
    let fd = fallocate_op.fd.as_fd();
    Ok(opcode::Fallocate::new(types::Fd(fd), fallocate_op.len)
        .offset(fallocate_op.offset)
        .mode(fallocate_op.mode)
        .build())
}

pub(crate) unsafe fn make_sqe_open(
    _op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_open"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_open"))?;
    let user = match payload {
        crate::op::UringUserPayload::Open(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_open")),
    };
    let path_ptr = user.path.as_slice().as_ptr() as *const _;
    Ok(opcode::OpenAt::new(types::Fd(libc::AT_FDCWD), path_ptr)
        .flags(user.flags)
        .mode(user.mode)
        .build())
}

impl_lifecycle!(drop_open, Open, no_fd);

pub(crate) unsafe fn resolve_chunks_read_fixed(
    _op: &UringOp,
    payload: &UringUserPayload,
    chunks: &mut [ChunkId],
) -> usize {
    let rw_op = match payload {
        crate::op::UringUserPayload::ReadFixed(p) => p,
        _ => return 0,
    };
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}

pub(crate) unsafe fn resolve_chunks_read_raw(
    op: &UringOp,
    payload: &UringUserPayload,
    chunks: &mut [ChunkId],
) -> usize {
    unsafe { resolve_chunks_read_fixed(op, payload, chunks) }
}

pub(crate) unsafe fn resolve_chunks_write_fixed(
    _op: &UringOp,
    payload: &UringUserPayload,
    chunks: &mut [ChunkId],
) -> usize {
    let rw_op = match payload {
        crate::op::UringUserPayload::WriteFixed(p) => p,
        _ => return 0,
    };
    let info = rw_op.buf.resolve_region_info();
    if info.pool_kind == PoolKind::SlotBased {
        chunks[0] = info.id;
        1
    } else {
        0
    }
}

pub(crate) unsafe fn resolve_chunks_write_raw(
    op: &UringOp,
    payload: &UringUserPayload,
    chunks: &mut [ChunkId],
) -> usize {
    unsafe { resolve_chunks_write_fixed(op, payload, chunks) }
}
