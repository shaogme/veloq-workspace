use veloq_driver_core::{CompletionEnvelope, CompletionIdentity};
use veloq_std::time::Duration;

use crate::{
    driver::{RIO_EVENT_KEY, completion::COMP_BACKEND_IOCP},
    win32::Overlapped,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum WaitBudgetSource {
    External,
    Timer,
    Probe,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct WaitBudget {
    pub(super) duration: Duration,
    pub(super) source: WaitBudgetSource,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum IocpCompletionStatusKind {
    RioWake,
    ControlKey,
    OverlappedUser { queue_key: usize },
    PostedToken,
    NullFailure,
    Unknown,
}

pub(super) struct CompletionStatusClassifier;

impl CompletionStatusClassifier {
    pub(super) fn wait_budget(
        external_timeout: Option<Duration>,
        internal_timeout: Option<Duration>,
        probe: Duration,
    ) -> WaitBudget {
        let mut budget = WaitBudget {
            duration: probe,
            source: WaitBudgetSource::Probe,
        };

        if let Some(internal) = internal_timeout
            && internal <= budget.duration
        {
            budget = WaitBudget {
                duration: internal,
                source: WaitBudgetSource::Timer,
            };
        }
        if let Some(external) = external_timeout
            && external <= budget.duration
        {
            budget = WaitBudget {
                duration: external,
                source: WaitBudgetSource::External,
            };
        }
        budget
    }

    pub(super) fn classify(
        key: usize,
        overlapped: *mut Overlapped,
        success: bool,
    ) -> IocpCompletionStatusKind {
        if key == RIO_EVENT_KEY {
            IocpCompletionStatusKind::RioWake
        } else if !overlapped.is_null() {
            IocpCompletionStatusKind::OverlappedUser { queue_key: key }
        } else if !success && key == 0 {
            IocpCompletionStatusKind::NullFailure
        } else if matches!(
            CompletionEnvelope::from_raw_parts(COMP_BACKEND_IOCP, key as u64, 0, 0).identity(),
            CompletionIdentity::Waker(_)
                | CompletionIdentity::Cancel(_)
                | CompletionIdentity::UnknownControl { .. }
        ) {
            IocpCompletionStatusKind::ControlKey
        } else if key != 0 || success {
            IocpCompletionStatusKind::PostedToken
        } else {
            IocpCompletionStatusKind::Unknown
        }
    }

    pub(super) fn duration_to_wait_ms(duration: Duration) -> u32 {
        if duration.is_zero() {
            0
        } else {
            let millis = duration.as_millis();
            let rounded = millis.saturating_add(u128::from(
                !duration.subsec_nanos().is_multiple_of(1_000_000),
            ));
            rounded.clamp(1, u32::MAX as u128) as u32
        }
    }

    pub(super) fn status_res(success: bool, error_code: Option<u32>, bytes: u32) -> i32 {
        if success {
            bytes.min(i32::MAX as u32) as i32
        } else {
            -(error_code.unwrap_or(0).min(i32::MAX as u32) as i32)
        }
    }

    pub(super) fn status_flags(success: bool, error_code: Option<u32>) -> u32 {
        u32::from(success) | (error_code.unwrap_or(0).min(u32::MAX >> 1) << 1)
    }
}
