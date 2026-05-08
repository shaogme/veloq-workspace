use diagweave::{report::Report, set};
use veloq_driver_core::error::{DriverErrorKind, DriverResult, ResultAsDriverExt};

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

pub(crate) trait UringResultExt<T> {
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T>;
}

impl<T> UringResultExt<T> for UringResult<T> {
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
    context: UringError,
    scope: &'static str,
    error: E,
) -> Report<UringError>
where
    E: std::error::Error + Send + Sync + 'static,
{
    let error_ref = &error as &dyn std::any::Any;
    let os_code = error_ref
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::raw_os_error);
    let detail = error.to_string();
    let report = Report::new(context)
        .with_ctx("scope", scope)
        .attach_note(detail)
        .with_diag_src_err(error);
    if let Some(code) = os_code {
        report.set_error_code(code)
    } else {
        report
    }
}
