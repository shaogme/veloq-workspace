#[macro_use]
mod macros;

#[macro_use]
mod file;
#[macro_use]
mod net;

pub(crate) use file::*;
pub(crate) use net::*;

use crate::config::{IoFd, RawHandleKind};
use crate::driver::{RegisteredFileEntry, UringDriver, resolve_registered_fixed_fd};
use crate::error::{UringDriverResult as DriverResult, UringError};
use crate::op::{UringOp, UringOpPayload, UringUserPayload};
use diagweave::prelude::*;
use io_uring::{opcode, squeue, types};
use tracing::warn;
use veloq_driver_core::DriverCoreError;
use veloq_driver_core::driver::{CompletionCleanup, CompletionCleanupGuard};
use veloq_driver_core::op::BufIoRangeError;

#[inline]
fn payload_variant_mismatch(scope: &'static str) -> Report<UringError> {
    UringError::InvalidState.report(scope, "UringOpPayload variant mismatch")
}

#[inline]
fn invalid_buf_io_range(scope: &'static str, err: BufIoRangeError) -> Report<UringError> {
    UringError::InvalidInput
        .report(scope, err.note())
        .with_ctx("buffer_offset", err.buffer_offset())
        .with_ctx("buffer_length", err.buffer_length())
        .with_ctx("buffer_capacity", err.buffer_capacity())
        .with_ctx("buffer_bound", err.buffer_bound())
        .with_ctx("buffer_bound_kind", err.buffer_bound_kind().name())
        .with_ctx("submission_length", err.submission_length())
}

#[inline]
fn resolve_file_fd(
    registered_files: &[Option<RegisteredFileEntry>],
    file_generations: &[u64],
    fd: IoFd,
    scope: &'static str,
) -> DriverResult<types::Fixed> {
    resolve_registered_fixed_fd(
        registered_files,
        file_generations,
        fd,
        Some(RawHandleKind::File),
        scope,
    )
    .map(types::Fixed)
}

#[inline]
fn resolve_socket_fd(
    registered_files: &[Option<RegisteredFileEntry>],
    file_generations: &[u64],
    fd: IoFd,
    scope: &'static str,
) -> DriverResult<types::Fixed> {
    resolve_registered_fixed_fd(
        registered_files,
        file_generations,
        fd,
        Some(RawHandleKind::Socket),
        scope,
    )
    .map(types::Fixed)
}

#[inline]
fn resolve_any_fd(
    registered_files: &[Option<RegisteredFileEntry>],
    file_generations: &[u64],
    fd: IoFd,
    scope: &'static str,
) -> DriverResult<types::Fixed> {
    resolve_registered_fixed_fd(registered_files, file_generations, fd, None, scope)
        .map(types::Fixed)
}

pub(crate) unsafe fn on_complete_default(
    _op: &mut UringOp,
    _payload: &mut UringUserPayload,
    result: i32,
) -> DriverResult<usize> {
    if result >= 0 {
        Ok(result as usize)
    } else {
        Err(UringError::CompletionWait
            .report(
                "uring.op.submit.on_complete_default",
                "kernel completion returned error",
            )
            .set_error_code(-result))
    }
}

pub(crate) unsafe fn completion_cleanup_noop(
    _op: &mut UringOp,
    _result: i32,
) -> CompletionCleanupGuard {
    CompletionCleanupGuard::default()
}

pub(crate) unsafe fn completion_cleanup_close_raw_fd(
    _op: &mut UringOp,
    result: i32,
) -> CompletionCleanupGuard {
    if result < 0 {
        return CompletionCleanupGuard::default();
    }
    CompletionCleanupGuard::new(CompletionCleanup::new(move || {
        // SAFETY: successful open/accept CQEs transfer a fresh raw fd that no user future owns yet.
        let close_res = unsafe { libc::close(result) };
        if close_res != 0 {
            let error = std::io::Error::last_os_error();
            warn!(
                fd = result,
                errno = error.raw_os_error(),
                "failed to close unconsumed uring completion fd"
            );
            return Err(DriverCoreError::System
                .to_report()
                .push_ctx("scope", "uring.op.submit.completion_cleanup_close_raw_fd")
                .set_error_code(error.raw_os_error().unwrap_or(libc::EIO))
                .attach_note(error.to_string()));
        }
        Ok(())
    }))
}

pub(crate) unsafe fn make_sqe_timeout(
    op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_timeout"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_timeout"))?;
    let user = match payload {
        crate::op::UringUserPayload::Timeout(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_timeout")),
    };

    let kernel = match &mut op.payload {
        UringOpPayload::Timeout(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_timeout")),
    };

    kernel.ts[0] = user.duration.as_secs() as i64;
    kernel.ts[1] = user.duration.subsec_nanos() as i64;
    let ts_ptr = kernel.ts.as_ptr() as *const types::Timespec;

    Ok(opcode::Timeout::new(ts_ptr).build())
}

impl_default_completion!(on_complete_timeout);
impl_lifecycle!(drop_timeout, Timeout, no_fd);

pub(crate) unsafe fn make_sqe_wakeup(
    op: &mut UringOp,
    driver: &mut UringDriver,
    user_data: usize,
) -> DriverResult<squeue::Entry> {
    let storage = driver
        .ops
        .slot_storage_mut(user_data)
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_wakeup"))?;
    let payload = storage
        .payload
        .as_mut()
        .ok_or_else(|| payload_variant_mismatch("uring.op.submit.make_sqe_wakeup"))?;
    let user = match payload {
        crate::op::UringUserPayload::Wakeup(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_wakeup")),
    };

    let kernel = match &mut op.payload {
        UringOpPayload::Wakeup(p) => p,
        _ => return Err(payload_variant_mismatch("uring.op.submit.make_sqe_wakeup")),
    };

    let fixed_fd = resolve_file_fd(
        &driver.registered_files,
        &driver.file_generations,
        user.fd,
        "uring.op.submit.make_sqe_wakeup",
    )?;
    Ok(opcode::Read::new(fixed_fd, kernel.buf.as_mut_ptr(), 8).build())
}

impl_default_completion!(on_complete_wakeup);
impl_lifecycle!(drop_wakeup, Wakeup, no_fd);

pub(crate) unsafe fn get_timeout_timeout(
    _op: &UringOp,
    payload: &UringUserPayload,
) -> Option<std::time::Duration> {
    match payload {
        crate::op::UringUserPayload::Timeout(p) => Some(p.duration),
        _ => None,
    }
}

pub(crate) unsafe fn get_timeout_none(
    _op: &UringOp,
    _payload: &UringUserPayload,
) -> Option<std::time::Duration> {
    None
}

pub(crate) unsafe fn resolve_chunks_none(
    _op: &UringOp,
    _payload: &UringUserPayload,
    _chunks: &mut [veloq_buf::heap::ChunkId],
) -> usize {
    0
}
