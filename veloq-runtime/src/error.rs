use diagweave::set;

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
    }
}

pub type Result<T> = std::result::Result<T, diagweave::report::Report<RuntimeError>>;
