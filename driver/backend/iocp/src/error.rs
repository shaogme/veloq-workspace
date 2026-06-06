use diagweave::prelude::*;
use veloq_driver_core::{DriverCoreError, DriverResult};

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

impl From<DriverCoreError> for IocpError {
    fn from(kind: DriverCoreError) -> Self {
        match kind {
            DriverCoreError::InvalidInput => Self::InvalidInput,
            DriverCoreError::InvalidState => Self::InvalidState,
            DriverCoreError::Submission => Self::Submission,
            DriverCoreError::Completion | DriverCoreError::Timeout => Self::CompletionWait,
            DriverCoreError::Registration => Self::Registration,
            DriverCoreError::Socket => Self::Socket,
            DriverCoreError::Unsupported => Self::Unsupported,
            DriverCoreError::Internal | DriverCoreError::System => Self::Internal,
        }
    }
}

pub(crate) trait IocpResultExt<T> {
    fn to_driver_result(
        self,
        kind: DriverCoreError,
        scope: &'static str,
        detail: impl ToString,
    ) -> IocpDriverResult<T>;
}

impl<T> IocpResultExt<T> for IocpResult<T> {
    fn to_driver_result(
        self,
        kind: DriverCoreError,
        scope: &'static str,
        detail: impl ToString,
    ) -> IocpDriverResult<T> {
        let detail = detail.to_string();
        self.map_report(|report| {
            tracing::error!(kind = %kind, scope = %scope, detail = %detail, "driver error report");
            report
                .set_accumulate_src_chain(true)
                .with_ctx("scope", scope)
                .with_ctx("driver_core_kind", kind.to_string())
                .attach_note(detail)
                .attach_note("driver error report captured")
        })
    }
}
