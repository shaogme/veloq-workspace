use core::convert::TryFrom;
use core::fmt;

use diagweave::{report::Report, set};

set! {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub DriverErrorKind = {
        #[display("invalid input")]
        InvalidInput,
        #[display("invalid state")]
        InvalidState,
        #[display("submission failed")]
        Submission,
        #[display("completion failed")]
        Completion,
        #[display("registration failed")]
        Registration,
        #[display("socket operation failed")]
        Socket,
        #[display("timeout")]
        Timeout,
        #[display("unsupported")]
        Unsupported,
        #[display("internal error")]
        Internal,
        #[display("system error")]
        System,
    }
}

pub type DriverResult<T> = Result<T, Report<DriverErrorKind>>;
pub type DriverErrorReport = Report<DriverErrorKind>;

#[inline]
fn neg_code(code: i32) -> Option<i32> {
    (code != 0).then_some(-code.abs())
}

#[inline]
fn diag_code_i32(report: &DriverErrorReport) -> Option<i32> {
    report
        .error_code()
        .and_then(|code| i32::try_from(code).ok())
        .and_then(neg_code)
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
    -driver_error_kind_fallback_errno(*report.inner())
}

#[inline]
pub fn driver_error(
    kind: DriverErrorKind,
    scope: &'static str,
    detail: impl ToString,
) -> DriverErrorReport {
    let detail = detail.to_string();
    Report::new(kind)
        .with_ctx("scope", scope)
        .attach_note(detail)
}

#[inline]
pub fn driver_os_error(
    kind: DriverErrorKind,
    scope: &'static str,
    code: i32,
    detail: impl ToString,
) -> DriverErrorReport {
    let detail = detail.to_string();
    Report::new(kind)
        .with_ctx("scope", scope)
        .set_error_code(code)
        .attach_note(detail)
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
    E: fmt::Debug + fmt::Display + std::error::Error + Send + Sync + 'static,
{
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T> {
        let detail = detail.to_string();
        self.map_err(|report| {
            tracing::error!(kind = %kind, scope = %scope, detail = %detail, "driver error report");
            report
                .set_accumulate_src_chain(true)
                .map_err(|_| kind)
                .with_ctx("scope", scope)
                .attach_note(detail)
                .attach_note("driver error report captured")
        })
    }
}
