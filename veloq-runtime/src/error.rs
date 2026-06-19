use diagweave::{Report, set};
use std::result::Result as StdResult;

set! {
    pub RuntimeError = {
        #[display("worker id {worker_id} is out of bounds (worker count: {worker_count})")]
        WorkerIdOutOfBounds {
            worker_id: usize,
            worker_count: usize,
        },

        #[display("failed to dispatch job to worker {target_worker} (current: {current_worker})")]
        DispatchFailed {
            target_worker: usize,
            current_worker: usize,
        },

        #[display("worker_factory has already been taken")]
        WorkerFactoryAlreadyTaken,

        #[display("receivers has already been taken")]
        ReceiversAlreadyTaken,

        #[display("receivers deques exhausted when spawning worker {worker_id}")]
        DequesExhausted { worker_id: usize },

        #[display("receivers deques exhausted for main worker")]
        MainWorkerDequeExhausted,

        #[display("failed to set thread-local storage for worker {worker_id}: {source}")]
        TlsSetOwnedFailed {
            worker_id: usize,
            source: veloq_tls::TlsErrorKind,
        },

        #[display("runtime invariant violation at {site}: {detail}")]
        InvariantViolation {
            site: &'static str,
            detail: &'static str,
        },

        #[display("mutex poisoned: {component}")]
        PoisonedLock {
            component: &'static str,
        },

        #[display("runtime binding is missing")]
        MissingRuntimeBinding,

        #[display("arena layout overflow during {op}")]
        ArenaLayoutOverflow {
            op: &'static str,
        },

        #[display("arena allocation returned null during {op}")]
        ArenaAllocationNull {
            op: &'static str,
        },

        #[display("task result unavailable at {stage}")]
        TaskResultUnavailable {
            stage: &'static str,
        },
    }
}

pub type Result<T> = StdResult<T, Report<RuntimeError>>;
