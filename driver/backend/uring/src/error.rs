use core::convert::TryFrom;

use diagweave::prelude::*;
use veloq_driver_core::{DriverCoreError, DriverError, DriverResult};

set! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub UringError = {
        #[display("io_uring driver initialization failed")]
        DriverInit,
        #[display("io_uring completion wait failed")]
        CompletionWait,
        #[display("io_uring operation submission failed")]
        Submission,
        #[display("io_uring registration failed")]
        Registration,
        #[display("failed to resolve io_uring file descriptor")]
        ResolveFd,
        #[display("socket operation failed")]
        Socket,
        #[display("invalid input")]
        InvalidInput,
        #[display("invalid internal state")]
        InvalidState,
        #[display("unsupported operation")]
        Unsupported,
        #[display("internal error")]
        Internal,
    }
}

pub type UringResult<T> = Result<T, Report<UringError>>;
pub type UringDriverResult<T> = DriverResult<T, UringError>;

impl UringError {
    #[inline]
    pub(crate) fn report(self, scope: &'static str, detail: impl ToString) -> Report<Self> {
        self.to_report()
            .set_error_code(uring_fallback_errno(self))
            .with_ctx("scope", scope)
            .attach_note(detail.to_string())
    }

    #[inline]
    pub(crate) fn io_report<E>(self, scope: &'static str, error: E) -> Report<Self>
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let error_ref = &error as &dyn std::any::Any;
        let os_code = error_ref
            .downcast_ref::<std::io::Error>()
            .and_then(std::io::Error::raw_os_error);
        let detail = error.to_string();
        let report = self
            .to_report()
            .with_ctx("scope", scope)
            .attach_note(detail)
            .with_diag_src_err(error);
        if let Some(code) = os_code {
            report.set_error_code(code)
        } else {
            report
        }
    }
}

impl DriverError for UringError {
    #[inline]
    fn from_core_report(report: Report<DriverCoreError>) -> Report<Self> {
        let kind = *report.inner();
        report
            .with_ctx("driver_core_kind", kind.to_string())
            .map_err(|_| Self::Internal)
    }
}

#[inline]
fn neg_code(code: i32) -> Option<i32> {
    (code != 0).then_some(-code.abs())
}

#[inline]
pub(crate) fn uring_fallback_errno(kind: UringError) -> i32 {
    match kind {
        UringError::DriverInit => 5,     // EIO
        UringError::CompletionWait => 5, // EIO
        UringError::Submission => 11,    // EAGAIN
        UringError::Registration => 12,  // ENOMEM
        UringError::ResolveFd => 9,      // EBADF
        UringError::Socket => 5,         // EIO
        UringError::InvalidInput => 22,  // EINVAL
        UringError::InvalidState => 5,   // EIO
        UringError::Unsupported => 95,   // EOPNOTSUPP
        UringError::Internal => 5,       // EIO
    }
}

#[inline]
pub(crate) fn uring_report_to_event_res(report: &Report<UringError>) -> i32 {
    if let Some(code) = report
        .error_code()
        .and_then(|code| i32::try_from(code).ok())
        && let Some(res) = neg_code(code)
    {
        return res;
    }
    -uring_fallback_errno(*report.inner())
}

pub(crate) trait UringResultExt<T> {
    fn to_driver_result(
        self,
        kind: UringError,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T, UringError>;
}

impl<T> UringResultExt<T> for UringResult<T> {
    fn to_driver_result(
        self,
        kind: UringError,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T, UringError> {
        let detail = detail.to_string();
        self.map_report(|report| {
            tracing::error!(kind = %kind, scope = %scope, detail = %detail, "driver error report");
            report
                .with_ctx("scope", scope)
                .with_ctx("driver_error_kind", kind.to_string())
                .attach_note(detail)
                .attach_note("driver error report captured")
        })
    }
}
