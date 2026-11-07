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
use libc::{EIO, close};
use tracing::warn;
use veloq_buf::BufIoRangeError;
use veloq_driver_core::{
    CompletionCleanup, CompletionCleanupGuard, DriverCoreError, SubmitTokenContext,
};
use veloq_io_uring::{opcode, squeue, types};
use veloq_std::{io, pin::Pin, string::ToString};

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

pub(crate) struct SubmissionSupport;

impl SubmissionSupport {
    pub(crate) fn opcode_build<T>(scope: &'static str, result: io::Result<T>) -> UringResult<T> {
        result.map_err(|error| {
            let kind = if error.kind() == io::ErrorKind::Unsupported {
                UringError::Unsupported
            } else {
                UringError::InvalidInput
            };
            kind.io_report(scope, error)
        })
    }

    pub(super) fn invalid_buf_io_range(
        scope: &'static str,
        err: BufIoRangeError,
    ) -> Report<UringError> {
        UringError::InvalidInput
            .report(scope, err.note())
            .with_ctx("buffer_offset", err.buffer_offset())
            .with_ctx("buffer_length", err.buffer_length())
            .with_ctx("buffer_capacity", err.buffer_capacity())
            .with_ctx("buffer_bound", err.buffer_bound())
            .with_ctx("buffer_bound_kind", err.buffer_bound_kind().name())
            .with_ctx("submission_length", err.submission_length())
    }

    pub(super) fn resolve_file_fd(
        table: &FileTable,
        fd: IoFd,
        scope: &'static str,
    ) -> UringResult<SqeFd> {
        table.resolve(fd, Some(RawHandleKind::File), scope)
    }

    pub(super) fn resolve_socket_fd(
        table: &FileTable,
        fd: IoFd,
        scope: &'static str,
    ) -> UringResult<SqeFd> {
        table.resolve(fd, Some(RawHandleKind::Socket), scope)
    }

    pub(super) fn resolve_socket_fd_direct(
        table: &FileTable,
        fd: IoFd,
        scope: &'static str,
    ) -> UringResult<SqeFd> {
        table.resolve_direct(fd, Some(RawHandleKind::Socket), scope)
    }

    pub(super) fn resolve_any_fd(
        table: &FileTable,
        fd: IoFd,
        scope: &'static str,
    ) -> UringResult<SqeFd> {
        table.resolve(fd, None, scope)
    }

    pub(crate) fn completion_cleanup_close_raw_fd(result: i32) -> CompletionCleanupGuard {
        if result < 0 {
            return CompletionCleanupGuard::default();
        }
        CompletionCleanupGuard::new(CompletionCleanup::new(move || {
            // SAFETY: successful open/accept CQEs transfer a fresh raw fd that no user future owns yet.
            let close_res = unsafe { close(result) };
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
                    .set_error_code(error.raw_os_error().unwrap_or(EIO))
                    .attach_note(error.to_string()));
            }
            Ok(())
        }))
    }

    pub(crate) unsafe fn make_sqe_timeout(
        mut kernel: Pin<&mut TimeoutPayload>,
        user: &mut Timeout,
        _env: &SqeEnv<'_>,
        _token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        let kernel = kernel.as_mut().get_mut();
        *kernel.timespec_mut() = types::Timespec::try_from(user.duration).map_err(|error| {
            UringError::InvalidInput.io_report("uring.op.submit.timeout_timespec", error)
        })?;
        let ts_ptr = kernel.timespec_mut() as *mut types::Timespec as *const types::Timespec;

        Self::opcode_build(
            "uring.op.submit.timeout_opcode",
            unsafe { opcode::Timeout::new(ts_ptr) }.build(),
        )
    }

    pub(crate) unsafe fn make_sqe_wakeup(
        mut kernel: Pin<&mut WakeupPayload>,
        user: &mut Wakeup,
        env: &SqeEnv<'_>,
        _token: SubmitTokenContext,
    ) -> UringResult<squeue::Entry> {
        let kernel = kernel.as_mut().get_mut();
        let fd =
            Self::resolve_file_fd(env.file_table(), user.fd, "uring.op.submit.make_sqe_wakeup")?;
        Self::opcode_build(
            "uring.op.submit.wakeup_opcode",
            sqe_with_fd!(fd, |f| unsafe {
                opcode::Read::new(f, kernel.buffer_mut().as_mut_ptr(), 8)
            }
            .build()),
        )
    }
}
