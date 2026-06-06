use diagweave::prelude::*;
use veloq_driver_core::{DriverErrorKind, DriverResult};

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
        let detail = detail.to_string();
        self.map_report(|report| {
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
