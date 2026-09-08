use veloq_std::{
    error::Error,
    fmt,
    string::String,
    sync::{
        Arc,
        atomic::{AtomicU8, Ordering},
    },
};

use diagweave::prelude::*;

use crate::{
    error::{IocpError, IocpResult},
    win32::IoCompletionPort,
};
use veloq_driver_core::driver::{CompletionToken, RemoteWaker};

// ============================================================================
// Error Context & Logic
// ============================================================================

#[derive(Debug, Clone, Copy)]
pub(crate) enum IocpErrorContext {
    CompletionWait,
}

impl fmt::Display for IocpErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CompletionWait => f.write_str("IOCP completion wait failed"),
        }
    }
}

impl Error for IocpErrorContext {}

impl From<IocpErrorContext> for IocpError {
    fn from(value: IocpErrorContext) -> Self {
        match value {
            IocpErrorContext::CompletionWait => IocpError::CompletionWait,
        }
    }
}

fn sanitize_field(s: &str) -> String {
    s.replace('\n', "\\n").replace('\r', "\\r")
}

pub(crate) fn iocp_msg(ctx: IocpErrorContext, detail: impl Into<String>) -> Report<IocpError> {
    let detail = detail.into();
    IocpError::from(ctx)
        .to_report()
        .push_ctx("scope", "iocp/common")
        .with_ctx("detail", sanitize_field(&detail))
        .attach_note(detail)
}

// ============================================================================
// Waker
// ============================================================================

pub(crate) const WAKER_IDLE: u8 = 0;
pub(crate) const WAKER_NOTIFIED: u8 = 1;
pub(crate) const WAKER_PROCESSING: u8 = 2;
pub(crate) const WAKER_REARM: u8 = 3;

/// A waker that posts a completion status to the port to wake up the event loop.
pub(crate) struct IocpWaker {
    pub(crate) port: Arc<IoCompletionPort>,
    pub(crate) notification_state: Arc<AtomicU8>,
}

impl RemoteWaker<IocpError> for IocpWaker {
    fn wake(&self) -> IocpResult<()> {
        loop {
            match self.notification_state.load(Ordering::Acquire) {
                WAKER_IDLE => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_IDLE,
                            WAKER_NOTIFIED,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        if let Err(error) = self.port.notify(CompletionToken::waker(0)) {
                            self.notification_state
                                .compare_exchange(
                                    WAKER_NOTIFIED,
                                    WAKER_IDLE,
                                    Ordering::AcqRel,
                                    Ordering::Acquire,
                                )
                                .ok();
                            return Err(error
                                .push_ctx("scope", "iocp/common")
                                .attach_note("failed to notify remote waker"));
                        }
                        return Ok(());
                    }
                }
                WAKER_NOTIFIED | WAKER_REARM => return Ok(()),
                WAKER_PROCESSING => {
                    if self
                        .notification_state
                        .compare_exchange(
                            WAKER_PROCESSING,
                            WAKER_REARM,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return Ok(());
                    }
                }
                _ => unreachable!("invalid IOCP waker state"),
            }
        }
    }
}
