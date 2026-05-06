use error_stack::Report;
use std::fmt;
use veloq_driver_core::error::{DriverDiag, DriverErrorKind, DriverResult, ResultAsDriverExt};

pub type IocpDiag = DriverDiag<i32>;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum IocpError {
    DriverInit,
    CompletionWait,
    Submission,
    Rio,
    ResolveFd,
    Socket,
    Win32,
    InvalidInput,
    InvalidState,
    Internal,
}

impl fmt::Display for IocpError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DriverInit => write!(f, "IOCP driver initialization failed"),
            Self::CompletionWait => write!(f, "IOCP completion wait failed"),
            Self::Submission => write!(f, "IOCP operation submission failed"),
            Self::Rio => write!(f, "RIO operation failed"),
            Self::ResolveFd => write!(f, "failed to resolve IO handle"),
            Self::Socket => write!(f, "socket operation failed"),
            Self::Win32 => write!(f, "Win32 API call failed"),
            Self::InvalidInput => write!(f, "invalid input"),
            Self::InvalidState => write!(f, "invalid internal state"),
            Self::Internal => write!(f, "internal error"),
        }
    }
}

impl std::error::Error for IocpError {}

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
    E: fmt::Display + Send + Sync + 'static,
{
    let error_ref = &error as &dyn std::any::Any;
    let os_code = error_ref
        .downcast_ref::<std::io::Error>()
        .and_then(std::io::Error::raw_os_error);
    let message = if error_ref.is::<Report<IocpError>>() {
        "IOCP report".to_string()
    } else {
        error.to_string()
    };
    let diag = IocpDiag::new(scope).with_error_detail(os_code, message);
    Report::new(context).attach(diag)
}
