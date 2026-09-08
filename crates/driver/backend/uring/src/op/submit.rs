mod file;
mod net;

pub(super) use file::*;
pub(super) use net::*;

use crate::{
    config::{IoFd, RawHandleKind},
    driver::{FileTable, SqeEnv, SqeFd},
    error::{UringError, UringResult},
    op::{
        Timeout, Wakeup,
        payload::{TimeoutPayload, WakeupPayload},
    },
};
use diagweave::prelude::*;
use io_uring::{opcode, squeue, types};
use tracing::warn;
use veloq_buf::BufIoRangeError;
use veloq_driver_core::{
    DriverCoreError,
    driver::{CompletionCleanup, CompletionCleanupGuard, SubmitTokenContext},
};
use veloq_std::{io, string::ToString};

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
fn resolve_file_fd(table: &FileTable, fd: IoFd, scope: &'static str) -> UringResult<SqeFd> {
    table.resolve(fd, Some(RawHandleKind::File), scope)
}

#[inline]
fn resolve_socket_fd(table: &FileTable, fd: IoFd, scope: &'static str) -> UringResult<SqeFd> {
    table.resolve(fd, Some(RawHandleKind::Socket), scope)
}

#[inline]
pub(super) fn resolve_socket_fd_direct(
    table: &FileTable,
    fd: IoFd,
    scope: &'static str,
) -> UringResult<SqeFd> {
    table.resolve_direct(fd, Some(RawHandleKind::Socket), scope)
}

#[inline]
fn resolve_any_fd(table: &FileTable, fd: IoFd, scope: &'static str) -> UringResult<SqeFd> {
    table.resolve(fd, None, scope)
}

/// Builds an SQE for either shape a resolved descriptor can take.
///
/// Every `io_uring` opcode accepts `impl sealed::UseFixed`, which both `types::Fixed` and
/// `types::Fd` implement — but the trait is crate-private, so the builder cannot be written
/// generically over it. Expanding the body once per variant is the way to keep the two paths
/// from drifting apart. Callers must have `SqeFd` and `io_uring::types` in scope.
macro_rules! sqe_with_fd {
    ($fd:expr, |$name:ident| $build:expr) => {
        match $fd {
            SqeFd::Fixed(index) => {
                let $name = types::Fixed(index);
                $build
            }
            SqeFd::Direct(raw) => {
                let $name = types::Fd(raw);
                $build
            }
        }
    };
}

pub(crate) use sqe_with_fd;

pub(crate) fn completion_cleanup_close_raw_fd(result: i32) -> CompletionCleanupGuard {
    if result < 0 {
        return CompletionCleanupGuard::default();
    }
    CompletionCleanupGuard::new(CompletionCleanup::new(move || {
        // SAFETY: successful open/accept CQEs transfer a fresh raw fd that no user future owns yet.
        let close_res = unsafe { libc::close(result) };
        if close_res != 0 {
            let error = io::Error::last_os_error();
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
    kernel: &mut TimeoutPayload,
    user: &mut Timeout,
    _env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    kernel.ts = types::Timespec::new()
        .sec(user.duration.as_secs())
        .nsec(user.duration.subsec_nanos());
    let ts_ptr = &kernel.ts as *const types::Timespec;

    Ok(opcode::Timeout::new(ts_ptr).build())
}

pub(crate) unsafe fn make_sqe_wakeup(
    kernel: &mut WakeupPayload,
    user: &mut Wakeup,
    env: &SqeEnv<'_>,
    _token: SubmitTokenContext,
) -> UringResult<squeue::Entry> {
    let fd = resolve_file_fd(env.file_table, user.fd, "uring.op.submit.make_sqe_wakeup")?;
    Ok(sqe_with_fd!(fd, |f| opcode::Read::new(
        f,
        kernel.buf.as_mut_ptr(),
        8
    )
    .build()))
}
