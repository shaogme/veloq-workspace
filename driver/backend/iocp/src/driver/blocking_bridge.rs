use veloq_blocking::{BlockingTask, get_blocking_pool};

pub(crate) struct BlockingBridge;

impl BlockingBridge {
    pub(crate) fn submit(task: BlockingTask) -> bool {
        get_blocking_pool().execute(task).is_ok()
    }
}
