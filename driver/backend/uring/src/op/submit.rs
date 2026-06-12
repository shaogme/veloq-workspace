mod file;
mod net;

pub(super) use file::*;
pub(super) use net::*;

use crate::config::{IoFd, RawHandleKind};
use crate::driver::{FileSlot, UringDriver, resolve_registered_fixed_fd};
use crate::error::{UringDriverResult as DriverResult, UringError};
use crate::op::{Timeout, Wakeup};
use diagweave::prelude::*;
use io_uring::{opcode, squeue, types};
use tracing::warn;
use veloq_buf::BufIoRangeError;
use veloq_driver_core::DriverCoreError;
use veloq_driver_core::driver::{CompletionCleanup, CompletionCleanupGuard, SubmitTokenContext};

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
    file_slots: &[FileSlot],
    fd: IoFd,
    scope: &'static str,
) -> DriverResult<types::Fixed> {
    resolve_registered_fixed_fd(file_slots, fd, Some(RawHandleKind::File), scope).map(types::Fixed)
}

#[inline]
fn resolve_socket_fd(
    file_slots: &[FileSlot],
    fd: IoFd,
    scope: &'static str,
) -> DriverResult<types::Fixed> {
    resolve_registered_fixed_fd(file_slots, fd, Some(RawHandleKind::Socket), scope)
        .map(types::Fixed)
}

#[inline]
fn resolve_any_fd(
    file_slots: &[FileSlot],
    fd: IoFd,
    scope: &'static str,
) -> DriverResult<types::Fixed> {
    resolve_registered_fixed_fd(file_slots, fd, None, scope).map(types::Fixed)
}

pub(crate) fn completion_cleanup_close_raw_fd(result: i32) -> CompletionCleanupGuard {
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
    kernel: &mut crate::op::payload::TimeoutPayload,
    user: &mut Timeout,
    _driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    kernel.ts[0] = user.duration.as_secs() as i64;
    kernel.ts[1] = user.duration.subsec_nanos() as i64;
    let ts_ptr = kernel.ts.as_ptr() as *const types::Timespec;

    Ok(opcode::Timeout::new(ts_ptr).build())
}

pub(crate) unsafe fn make_sqe_wakeup(
    kernel: &mut crate::op::payload::WakeupPayload,
    user: &mut Wakeup,
    driver: &mut UringDriver,
    _token: SubmitTokenContext,
) -> DriverResult<squeue::Entry> {
    let fixed_fd = resolve_file_fd(
        &driver.file_slots,
        user.fd,
        "uring.op.submit.make_sqe_wakeup",
    )?;
    Ok(opcode::Read::new(fixed_fd, kernel.buf.as_mut_ptr(), 8).build())
}
