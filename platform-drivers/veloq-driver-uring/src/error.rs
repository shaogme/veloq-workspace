use error_stack::Report;
use std::fmt;

#[derive(Debug, Clone)]
pub struct UringDiag {
    pub scope: &'static str,
    pub error_code: Option<i32>,
    pub original_message: Option<String>,
    pub fields: Vec<(&'static str, String)>,
}

impl UringDiag {
    pub fn new(scope: &'static str) -> Self {
        Self {
            scope,
            error_code: None,
            original_message: None,
            fields: Vec::new(),
        }
    }

    pub fn with_error(mut self, err: &std::io::Error) -> Self {
        self.error_code = err.raw_os_error();
        self.original_message = Some(err.to_string());
        self
    }

    pub fn field(mut self, key: &'static str, value: impl ToString) -> Self {
        self.fields.push((key, value.to_string()));
        self
    }
}

impl fmt::Display for UringDiag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "uring_diag(scope={}", self.scope)?;
        if let Some(code) = self.error_code {
            write!(f, ", error_code={code}")?;
        }
        if let Some(ref msg) = self.original_message {
            write!(f, ", original_error={msg}")?;
        }
        for (k, v) in &self.fields {
            write!(f, ", {k}={v}")?;
        }
        write!(f, ")")
    }
}

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

#[derive(Debug)]
pub struct UringIoError {
    pub report: Report<UringError>,
    pub detail: String,
}

impl fmt::Display for UringIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "io_uring error ({}): {:#}", self.detail, self.report)
    }
}

impl std::error::Error for UringIoError {}

pub trait UringReportExt {
    fn to_io_error(self, detail: impl Into<String>) -> std::io::Error;
}

impl UringReportExt for Report<UringError> {
    fn to_io_error(self, detail: impl Into<String>) -> std::io::Error {
        let detail = detail.into();
        tracing::error!(detail = %detail, report = ?&self, "io_uring error report");
        std::io::Error::other(UringIoError {
            report: self,
            detail,
        })
    }
}

pub trait UringResultExt<T> {
    fn to_io_result(self, detail: impl Into<String>) -> std::io::Result<T>;
}

impl<T> UringResultExt<T> for UringResult<T> {
    fn to_io_result(self, detail: impl Into<String>) -> std::io::Result<T> {
        self.map_err(|e| e.to_io_error(detail))
    }
}

#[inline]
pub fn from_io_error(
    context: UringError,
    scope: &'static str,
    error: std::io::Error,
) -> Report<UringError> {
    let diag = UringDiag::new(scope).with_error(&error);
    Report::new(context).attach(diag)
}
