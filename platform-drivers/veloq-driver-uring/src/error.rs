use diagweave::report::Report;
use std::fmt;
use veloq_driver_core::error::{DriverErrorKind, DriverResult, ResultAsDriverExt};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum UringError {
    DriverInit,
    CompletionWait,
    Submission,
    Registration,
    ResolveFd,
    Socket,
    InvalidInput,
    InvalidState,
    Unsupported,
    Internal,
}

impl fmt::Display for UringError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DriverInit => write!(f, "io_uring driver initialization failed"),
            Self::CompletionWait => write!(f, "io_uring completion wait failed"),
            Self::Submission => write!(f, "io_uring operation submission failed"),
            Self::Registration => write!(f, "io_uring registration failed"),
            Self::ResolveFd => write!(f, "failed to resolve io_uring file descriptor"),
            Self::Socket => write!(f, "socket operation failed"),
            Self::InvalidInput => write!(f, "invalid input"),
            Self::InvalidState => write!(f, "invalid internal state"),
            Self::Unsupported => write!(f, "unsupported operation"),
            Self::Internal => write!(f, "internal error"),
        }
    }
}

impl std::error::Error for UringError {}

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
    E: fmt::Display + Send + Sync + 'static,
{
    let error_ref = &error as &dyn std::any::Any;
    let os_code = error_ref
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::raw_os_error);
    let report = Report::new(context)
        .with_ctx("scope", scope)
        .attach_note(error.to_string());
    if let Some(code) = os_code {
        report.set_error_code(code)
    } else {
        report
    }
}
