use diagweave::prelude::*;

use crate::error::{IocpDriverResult, IocpError};

set! {
    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    pub RioError = {
        #[display("RIO library or function pointers load failure")]
        LibraryLoad,
        #[display("failed to register memory buffer for RIO")]
        BufferRegistration,
        #[display("failed to create RIO completion queue")]
        CqCreation,
        #[display("failed to create RIO request queue")]
        RqCreation,
        #[display("RIO datapath operation error")]
        Datapath,
        #[display("RIO resource limit reached")]
        ResourceExhaustion,
        #[display("RIO not supported or initialized")]
        NotSupported,
        #[display("RIO internal inconsistency")]
        Internal,
    }
}

pub type RioResult<T> = Result<T, Report<RioError>>;

pub(crate) trait RioResultExt<T> {
    fn to_driver_result(
        self,
        kind: IocpError,
        scope: &'static str,
        detail: impl ToString,
    ) -> IocpDriverResult<T>;
}

impl<T> RioResultExt<T> for RioResult<T> {
    fn to_driver_result(
        self,
        kind: IocpError,
        scope: &'static str,
        detail: impl ToString,
    ) -> IocpDriverResult<T> {
        let detail = detail.to_string();
        self.map_report(|report| {
            tracing::error!(kind = %kind, scope = %scope, detail = %detail, "driver error report");
            report
                .set_accumulate_src_chain(true)
                .map_err(IocpError::Rio)
                .with_ctx("scope", scope)
                .with_ctx("driver_error_kind", kind.to_string())
                .attach_note(detail)
                .attach_note("driver error report captured")
        })
    }
}
