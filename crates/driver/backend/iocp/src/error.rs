use core::convert::TryFrom;
use veloq_std::{fmt::Display, io::Error as IoError, string::ToString};

use diagweave::prelude::*;
use veloq_driver_core::{DriverCoreError, DriverError};

use crate::rio::RioError;

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

impl IocpError {
    #[inline]
    pub(crate) fn report(
        self,
        scope: &'static str,
        detail: impl Display + Send + Sync + 'static,
    ) -> Report<Self> {
        self.to_report()
            .set_error_code(Self::fallback_errno(self))
            .push_ctx("scope", scope)
            .attach_note(detail)
    }

    #[inline]
    pub(crate) fn io_report(self, scope: &'static str, error: IoError) -> Report<Self> {
        let os_code = error.raw_os_error();
        let report = self
            .to_report()
            .push_ctx("scope", scope)
            .with_diag_src_err(error);
        if let Some(code) = os_code {
            report.set_error_code(code)
        } else {
            report
        }
    }

    pub(crate) fn fallback_errno(kind: Self) -> i32 {
        match kind {
            Self::DriverInit => 5,
            Self::CompletionWait => 110,
            Self::Submission => 11,
            Self::Registration => 12,
            Self::Rio(RioError::InvalidInput | RioError::BufferRegistrationRequired) => 22,
            Self::Rio(_) => 5,
            Self::ResolveFd => 9,
            Self::Socket | Self::Win32 | Self::InvalidState | Self::Internal => 5,
            Self::InvalidInput => 22,
            Self::Unsupported => 95,
        }
    }

    pub(crate) fn fallback_event_res(kind: Self) -> i32 {
        -Self::fallback_errno(kind)
    }

    pub(crate) fn report_to_event_res(report: &Report<Self>) -> i32 {
        if let Some(code) = report
            .error_code()
            .and_then(|code| i32::try_from(code).ok())
            && let Some(res) = neg_code(code)
        {
            return res;
        }
        Self::fallback_event_res(*report.inner())
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

#[cfg(test)]
mod tests {
    use crate::rio::RioError;

    use super::IocpError;

    #[test]
    fn rio_invalid_input_falls_back_to_einval() {
        assert_eq!(
            IocpError::fallback_errno(IocpError::Rio(RioError::InvalidInput)),
            22
        );
    }

    #[test]
    fn rio_buffer_registration_required_falls_back_to_einval() {
        assert_eq!(
            IocpError::fallback_errno(IocpError::Rio(RioError::BufferRegistrationRequired)),
            22
        );
    }
}
