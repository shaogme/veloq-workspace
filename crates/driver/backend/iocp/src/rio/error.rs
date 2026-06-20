use diagweave::prelude::*;

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
        #[display("RIO invalid input")]
        InvalidInput,
        #[display("RIO resource limit reached")]
        ResourceExhaustion,
        #[display("RIO not supported or initialized")]
        NotSupported,
        #[display("RIO internal inconsistency")]
        Internal,
    }
}

pub type RioResult<T> = Result<T, Report<RioError>>;
