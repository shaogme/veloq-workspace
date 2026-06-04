use diagweave::prelude::*;
use veloq_driver_core::{DriverErrorKind, DriverResult, ResultAsDriverExt};

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
        #[display("internal error")]
        Internal,
    }
}

pub type IocpResult<T> = Result<T, Report<IocpError>>;

pub(crate) trait IocpResultExt<T> {
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T>;
}

impl<T> IocpResultExt<T> for IocpResult<T> {
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T> {
        ResultAsDriverExt::to_driver_result(self, kind, scope, detail)
    }
}

#[inline]
pub(crate) fn from_io_error<E>(
    context: IocpError,
    scope: &'static str,
    error: E,
) -> Report<IocpError>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let error_ref = &error as &dyn std::any::Any;
    let os_code = error_ref
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::raw_os_error);
    let detail = error.to_string();
    let report = context
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
