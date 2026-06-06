use core::convert::TryFrom;

use diagweave::prelude::*;
use veloq_driver_core::{DriverCoreError, DriverError, DriverResult};

use crate::rio::error::RioError;

set! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub IocpError = {
        #[display("IOCP driver initialization failed")]
        DriverInit,
        #[display("IOCP completion wait failed")]
        CompletionWait,
        #[display("IOCP operation submission failed")]
        Submission,
        #[display("IOCP registration failed")]
        Registration,
        #[display(transparent)]
        Rio(#[from] RioError),
        #[display("failed to resolve IO handle")]
        ResolveFd,
        #[display("socket operation failed")]
        Socket,
        #[display("Win32 API call failed")]
        Win32,
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

pub type IocpResult<T> = Result<T, Report<IocpError>>;
pub type IocpDriverResult<T> = DriverResult<T, IocpError>;

impl IocpError {
    #[inline]
    pub(crate) fn report(self, scope: &'static str, detail: impl ToString) -> Report<Self> {
        self.to_report()
            .set_error_code(iocp_fallback_errno(self))
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

impl DriverError for IocpError {
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
pub(crate) fn iocp_fallback_errno(kind: IocpError) -> i32 {
    match kind {
        IocpError::DriverInit => 5,       // EIO
        IocpError::CompletionWait => 110, // ETIMEDOUT
        IocpError::Submission => 11,      // EAGAIN
        IocpError::Registration => 12,    // ENOMEM
        IocpError::Rio(_) => 5,           // EIO
        IocpError::ResolveFd => 9,        // EBADF
        IocpError::Socket => 5,           // EIO
        IocpError::Win32 => 5,            // EIO
        IocpError::InvalidInput => 22,    // EINVAL
        IocpError::InvalidState => 5,     // EIO
        IocpError::Unsupported => 95,     // EOPNOTSUPP
        IocpError::Internal => 5,         // EIO
    }
}

#[inline]
pub(crate) fn iocp_fallback_event_res(kind: IocpError) -> i32 {
    -iocp_fallback_errno(kind)
}

#[inline]
pub(crate) fn iocp_report_to_event_res(report: &Report<IocpError>) -> i32 {
    if let Some(code) = report
        .error_code()
        .and_then(|code| i32::try_from(code).ok())
        && let Some(res) = neg_code(code)
    {
        return res;
    }
    iocp_fallback_event_res(*report.inner())
}

pub(crate) trait IocpResultExt<T> {
    fn to_driver_result(
        self,
        kind: IocpError,
        scope: &'static str,
        detail: impl ToString,
    ) -> IocpDriverResult<T>;
}

impl<T> IocpResultExt<T> for IocpResult<T> {
    fn to_driver_result(
        self,
        kind: IocpError,
        scope: &'static str,
        detail: impl ToString,
    ) -> IocpDriverResult<T> {
        let detail = detail.to_string();
        self.map_report(|report| {
            tracing::error!(kind = %kind, scope = %scope, detail = %detail, "driver error report");
            report
                .set_accumulate_src_chain(true)
                .with_ctx("scope", scope)
                .with_ctx("driver_error_kind", kind.to_string())
                .attach_note(detail)
                .attach_note("driver error report captured")
        })
    }
}
