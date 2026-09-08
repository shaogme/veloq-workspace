use diagweave::{Report, set};
use std::{borrow::Cow, fmt, result::Result as StdResult};

/// 后端 remote waker 失败时跨 runtime 边界传递的稳定诊断。
///
/// 该类型不携带 backend 泛型，因此 runtime 不需要依赖任何平台 driver。`detail` 保留
/// 后端报告的文本快照，避免把只能在 driver 线程使用的报告对象跨线程或跨 crate 保存。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeWakeError {
    pub backend: &'static str,
    pub worker_id: usize,
    pub operation: &'static str,
    pub detail: String,
}

impl fmt::Display for RuntimeWakeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} worker {} failed during {}: {}",
            self.backend, self.worker_id, self.operation, self.detail
        )
    }
}

impl std::error::Error for RuntimeWakeError {}

/// Driver 在 runtime 边界失败时使用的稳定诊断。
///
/// 该类型不携带具体 backend 的错误泛型；`detail` 保存底层 report 的文本快照，
/// 使 worker loop、`block_on` 和 scope join 可以沿用同一错误类型观察失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeDriverError {
    pub backend: &'static str,
    pub worker_id: usize,
    pub phase: &'static str,
    pub operation: &'static str,
    pub detail: String,
}

impl fmt::Display for RuntimeDriverError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} worker {} failed during {} {}: {}",
            self.backend, self.worker_id, self.phase, self.operation, self.detail
        )
    }
}

impl std::error::Error for RuntimeDriverError {}

set! {
    pub RuntimeError = {
        #[display("worker id {worker_id} is out of bounds (worker count: {worker_count})")]
        WorkerIdOutOfBounds {
            worker_id: usize,
            worker_count: usize,
        },

        #[display("worker count {worker_count} exceeds the supported maximum {max_worker_count}")]
        WorkerCountTooLarge {
            worker_count: usize,
            max_worker_count: usize,
        },

        #[display("failed to dispatch job to worker {target_worker} (current: {current_worker})")]
        DispatchFailed {
            target_worker: usize,
            current_worker: usize,
        },

        #[display("failed to spawn worker thread: {source}")]
        ThreadSpawnFailed {
            #[source]
            source: veloq_std::thread::ThreadError,
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
            detail: Cow<'static, str>,
        },

        #[display("runtime remote wake failed: {source}")]
        WakeFailed {
            #[source]
            source: RuntimeWakeError,
        },

        #[display("runtime driver failed: {source}")]
        DriverFailed {
            #[source]
            source: RuntimeDriverError,
        },

        #[display("mutex poisoned: {component}")]
        PoisonedLock {
            component: &'static str,
        },

        #[display("runtime binding is missing")]
        MissingRuntimeBinding,

        #[display("the runtime shut down before the entry future completed")]
        ShutdownBeforeCompletion,

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
