use error_stack::Report;
use std::fmt;

#[derive(Debug, Clone)]
pub struct DriverDiag<C = i32> {
    pub scope: &'static str,
    pub error_code: Option<C>,
    pub original_message: Option<String>,
    pub fields: Vec<(&'static str, String)>,
}

impl<C> DriverDiag<C> {
    #[inline]
    pub fn new(scope: &'static str) -> Self {
        Self {
            scope,
            error_code: None,
            original_message: None,
            fields: Vec::new(),
        }
    }

    #[inline]
    pub fn with_error(mut self, code: C, msg: impl ToString) -> Self {
        self.error_code = Some(code);
        self.original_message = Some(msg.to_string());
        self
    }

    #[inline]
    pub fn with_error_detail(mut self, error_code: Option<C>, message: impl ToString) -> Self {
        self.error_code = error_code;
        self.original_message = Some(message.to_string());
        self
    }

    #[inline]
    pub fn field(mut self, key: &'static str, value: impl ToString) -> Self {
        self.fields.push((key, value.to_string()));
        self
    }
}

impl<C> fmt::Display for DriverDiag<C>
where
    C: fmt::Display,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "driver_diag(scope={}", self.scope)?;
        if let Some(ref code) = self.error_code {
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverErrorKind {
    InvalidInput,
    InvalidState,
    Submission,
    Completion,
    Registration,
    Socket,
    Timeout,
    Unsupported,
    Internal,
    System,
}

impl fmt::Display for DriverErrorKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidInput => f.write_str("invalid input"),
            Self::InvalidState => f.write_str("invalid state"),
            Self::Submission => f.write_str("submission failed"),
            Self::Completion => f.write_str("completion failed"),
            Self::Registration => f.write_str("registration failed"),
            Self::Socket => f.write_str("socket operation failed"),
            Self::Timeout => f.write_str("timeout"),
            Self::Unsupported => f.write_str("unsupported"),
            Self::Internal => f.write_str("internal error"),
            Self::System => f.write_str("system error"),
        }
    }
}

impl std::error::Error for DriverErrorKind {}

pub type DriverResult<T> = Result<T, Report<DriverErrorKind>>;
pub type DriverErrorReport = Report<DriverErrorKind>;

#[inline]
fn neg_code(code: i32) -> Option<i32> {
    (code != 0).then_some(-code.abs())
}

#[inline]
fn diag_code_i32(report: &DriverErrorReport) -> Option<i32> {
    if let Some(diag) = report.downcast_ref::<DriverDiag<i32>>()
        && let Some(code) = diag.error_code
        && let Some(res) = neg_code(code)
    {
        return Some(res);
    }
    if let Some(diag) = report.downcast_ref::<DriverDiag<u32>>()
        && let Some(code) = diag.error_code
        && let Ok(code_i32) = i32::try_from(code)
        && let Some(res) = neg_code(code_i32)
    {
        return Some(res);
    }
    None
}

#[inline]
pub fn driver_error_kind_fallback_errno(kind: DriverErrorKind) -> i32 {
    match kind {
        DriverErrorKind::InvalidInput => 22, // EINVAL
        DriverErrorKind::InvalidState => 5,  // EIO
        DriverErrorKind::Submission => 11,   // EAGAIN
        DriverErrorKind::Completion => 5,    // EIO
        DriverErrorKind::Registration => 12, // ENOMEM
        DriverErrorKind::Socket => 5,        // EIO
        DriverErrorKind::Timeout => 110,     // ETIMEDOUT
        DriverErrorKind::Unsupported => 95,  // EOPNOTSUPP
        DriverErrorKind::Internal => 5,      // EIO
        DriverErrorKind::System => 5,        // EIO
    }
}

#[inline]
pub fn driver_error_report_to_event_res(report: &DriverErrorReport) -> i32 {
    if let Some(res) = diag_code_i32(report) {
        return res;
    }
    if let Some(kind) = report.downcast_ref::<DriverErrorKind>() {
        return -driver_error_kind_fallback_errno(*kind);
    }
    -5 // EIO
}

#[inline]
pub fn driver_error(
    kind: DriverErrorKind,
    scope: &'static str,
    detail: impl ToString,
) -> DriverErrorReport {
    let detail = detail.to_string();
    Report::new(kind).attach(DriverDiag::<i32>::new(scope).with_error_detail(None, detail))
}

#[inline]
pub fn driver_os_error(
    kind: DriverErrorKind,
    scope: &'static str,
    code: i32,
    detail: impl ToString,
) -> DriverErrorReport {
    let detail = detail.to_string();
    Report::new(kind).attach(DriverDiag::new(scope).with_error(code, detail))
}

pub trait ResultAsDriverExt<T, E> {
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T>;
}

impl<T, E> ResultAsDriverExt<T, E> for Result<T, Report<E>>
where
    E: fmt::Debug + fmt::Display + Send + Sync + 'static,
{
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T> {
        let detail = detail.to_string();
        self.map_err(|_report| {
            tracing::error!(kind = %kind, scope = %scope, detail = %detail, "driver error report");
            Report::new(kind)
                .attach(DriverDiag::<i32>::new(scope).with_error_detail(None, detail))
                .attach("driver error report captured")
        })
    }
}
