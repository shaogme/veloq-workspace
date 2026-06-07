use std::collections::VecDeque;

use crate::config::{RegisteredHandle, SocketKey};

pub(crate) struct DeferredSocketCleanup {
    pub(crate) handle: SocketKey,
    pub(crate) entry: RegisteredHandle,
}

pub(crate) struct HandleRegistry {
    pub(crate) registered_files: Vec<Option<RegisteredHandle>>,
    pub(crate) file_generations: Vec<u64>,
    pub(crate) free_slots: Vec<usize>,
    pub(crate) deferred_socket_cleanup: VecDeque<DeferredSocketCleanup>,
    pub(crate) socket_generation_counter: u64,
}

impl HandleRegistry {
    pub(crate) fn new() -> Self {
        Self {
            registered_files: Vec::new(),
            file_generations: Vec::new(),
            free_slots: Vec::new(),
            deferred_socket_cleanup: VecDeque::new(),
            socket_generation_counter: 1,
        }
    }
}
