use diagweave::set;

set! {
    pub RuntimeError = {
        #[display("failed to dispatch job to worker {target_worker} (current: {current_worker})")]
        DispatchFailed {
            target_worker: usize,
            current_worker: usize,
        },
    }
}

pub type Result<T> = std::result::Result<T, diagweave::report::Report<RuntimeError>>;
