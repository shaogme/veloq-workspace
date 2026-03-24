use error_stack::Report;
use std::fmt;

#[derive(Debug, Clone)]
pub struct IocpDiag {
    pub scope: &'static str,
    pub error_code: Option<i32>,
    pub original_message: Option<String>,
    pub fields: Vec<(&'static str, String)>,
}

impl IocpDiag {
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

impl fmt::Display for IocpDiag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "iocp_diag(scope={}", self.scope)?;
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

#[derive(Debug)]
pub struct IocpIoError {
    pub report: Report<IocpError>,
    pub detail: String,
}

impl fmt::Display for IocpIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "IOCP error ({}): {:#}", self.detail, self.report)
    }
}

impl std::error::Error for IocpIoError {}

pub trait IocpReportExt {
    fn to_io_error(self, detail: impl Into<String>) -> std::io::Error;
}

impl IocpReportExt for Report<IocpError> {
    fn to_io_error(self, detail: impl Into<String>) -> std::io::Error {
        let detail = detail.into();
        tracing::error!(detail = %detail, report = ?&self, "IOCP error report");
        std::io::Error::other(IocpIoError {
            report: self,
            detail,
        })
    }
}

pub trait IocpResultExt<T> {
    fn to_io_result(self, detail: impl Into<String>) -> std::io::Result<T>;
}

impl<T> IocpResultExt<T> for IocpResult<T> {
    fn to_io_result(self, detail: impl Into<String>) -> std::io::Result<T> {
        self.map_err(|e| e.to_io_error(detail))
    }
}

#[inline]
pub fn from_io_error(
    context: IocpError,
    scope: &'static str,
    error: std::io::Error,
) -> Report<IocpError> {
    let diag = IocpDiag::new(scope).with_error(&error);
    Report::new(context).attach(diag)
}
