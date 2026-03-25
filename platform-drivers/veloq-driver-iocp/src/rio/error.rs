use std::fmt;
use veloq_driver_core::error::{
    DriverDiag, DriverErrorKind, DriverResult, ResultAsDriverExt,
};

pub type RioDiag = DriverDiag<u32>;

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum RioError {
    LibraryLoad,
    BufferRegistration,
    CqCreation,
    RqCreation,
    Datapath,
    ResourceExhaustion,
    NotSupported,
    Internal,
}

impl fmt::Display for RioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LibraryLoad => write!(f, "RIO library or function pointers load failure"),
            Self::BufferRegistration => write!(f, "failed to register memory buffer for RIO"),
            Self::CqCreation => write!(f, "failed to create RIO completion queue"),
            Self::RqCreation => write!(f, "failed to create RIO request queue"),
            Self::Datapath => write!(f, "RIO datapath operation error"),
            Self::ResourceExhaustion => write!(f, "RIO resource limit reached"),
            Self::NotSupported => write!(f, "RIO not supported or initialized"),
            Self::Internal => write!(f, "RIO internal inconsistency"),
        }
    }
}

impl std::error::Error for RioError {}

pub type RioResult<T> = Result<T, error_stack::Report<RioError>>;

pub(crate) trait RioResultExt<T> {
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T>;
}

impl<T> RioResultExt<T> for RioResult<T> {
    fn to_driver_result(
        self,
        kind: DriverErrorKind,
        scope: &'static str,
        detail: impl ToString,
    ) -> DriverResult<T> {
        ResultAsDriverExt::to_driver_result(self, kind, scope, detail)
    }
}
