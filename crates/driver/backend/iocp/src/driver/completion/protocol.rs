use veloq_std::num::NonZeroU8;

use diagweave::prelude::*;
use veloq_driver_core::driver::{CancelMode, CompletionBackend};

use crate::{
    error::{IocpError, IocpResult},
    rio::{RioState, SocketInflightToken},
};

pub(crate) const COMP_BACKEND_IOCP: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(1) {
        Some(val) => val,
        None => unreachable!(),
    });

pub(crate) const COMP_BACKEND_RIO: CompletionBackend =
    CompletionBackend::Backend(match NonZeroU8::new(3) {
        Some(val) => val,
        None => unreachable!(),
    });

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IocpSubmissionFailurePhase {
    Initial,
    Replacement,
}

impl IocpSubmissionFailurePhase {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Replacement => "replacement",
        }
    }
}

pub(crate) enum IocpSyntheticCompletion {
    None,
    Cancel { mode: CancelMode },
}

impl IocpSyntheticCompletion {
    #[inline]
    pub(crate) fn cancel_mode(&self) -> CancelMode {
        match self {
            Self::Cancel { mode } => *mode,
            Self::None => CancelMode::UserVisible,
        }
    }

    #[inline]
    pub(crate) fn take_submission_failure(&mut self) -> Option<Report<IocpError>> {
        None
    }
}

#[derive(Default)]
pub(crate) struct IocpPostCompletionEffects {
    drain_socket_cleanup: bool,
}

impl IocpPostCompletionEffects {
    pub(crate) fn request_socket_cleanup(&mut self) {
        self.drain_socket_cleanup = true;
    }

    pub(crate) const fn requires_socket_cleanup(&self) -> bool {
        self.drain_socket_cleanup
    }
}

pub(crate) enum IocpBackendEffect {
    None,
    SocketInflight(SocketInflightToken),
}

impl Default for IocpBackendEffect {
    #[inline]
    fn default() -> Self {
        Self::None
    }
}

impl IocpBackendEffect {
    pub(crate) fn finish(
        self,
        rio: &mut RioState,
        post: &mut IocpPostCompletionEffects,
    ) -> IocpResult<()> {
        match self {
            Self::None => Ok(()),
            Self::SocketInflight(token) => {
                rio.release_socket_inflight_token(token).trans()?;
                post.request_socket_cleanup();
                Ok(())
            }
        }
    }
}
