use std::collections::VecDeque;

use crate::config::{IoFd, RegisteredHandle, SocketKey};

pub(crate) struct DeferredSocketCleanup {
    handle: SocketKey,
    entry: RegisteredHandle,
}

pub(crate) struct HandleRegistry {
    registered_files: Vec<Option<RegisteredHandle>>,
    file_generations: Vec<u64>,
    free_slots: Vec<usize>,
    deferred_socket_cleanup: VecDeque<DeferredSocketCleanup>,
    socket_generation_counter: u64,
}

impl DeferredSocketCleanup {
    fn new(handle: SocketKey, entry: RegisteredHandle) -> Self {
        Self { handle, entry }
    }

    pub(crate) fn handle(&self) -> SocketKey {
        self.handle
    }

    pub(crate) fn into_entry(self) -> RegisteredHandle {
        self.entry
    }
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

    pub(crate) fn registered_files(&self) -> &[Option<RegisteredHandle>] {
        &self.registered_files
    }

    pub(crate) fn file_generations(&self) -> &[u64] {
        &self.file_generations
    }

    #[cfg(test)]
    pub(crate) fn registered_file(&self, idx: usize) -> Option<&RegisteredHandle> {
        self.registered_files.get(idx).and_then(Option::as_ref)
    }

    pub(crate) fn next_socket_generation(&mut self) -> u64 {
        let generation = self.socket_generation_counter;
        self.socket_generation_counter = self.socket_generation_counter.wrapping_add(1);
        if self.socket_generation_counter == 0 {
            self.socket_generation_counter = 1;
        }
        generation
    }

    pub(crate) fn insert_registered(&mut self, entry: RegisteredHandle) -> IoFd {
        let idx = if let Some(idx) = self.free_slots.pop() {
            self.registered_files[idx] = Some(entry);
            idx
        } else {
            self.registered_files.push(Some(entry));
            self.file_generations.push(0);
            self.registered_files.len() - 1
        };
        IoFd::fixed_with_generation(idx as u32, self.file_generations[idx])
    }

    pub(crate) fn take_for_unregister(&mut self, fd: IoFd) -> Option<(usize, RegisteredHandle)> {
        let idx = fd.fixed_index() as usize;
        if idx >= self.registered_files.len() {
            return None;
        }
        if self.file_generations.get(idx).copied() != Some(fd.generation()) {
            return None;
        }
        self.registered_files[idx].take().map(|entry| (idx, entry))
    }

    pub(crate) fn release_slot(&mut self, idx: usize) {
        self.free_slots.push(idx);
        self.file_generations[idx] = self.file_generations[idx].wrapping_add(1);
    }

    pub(crate) fn deferred_cleanup_len(&self) -> usize {
        self.deferred_socket_cleanup.len()
    }

    pub(crate) fn pop_deferred_cleanup(&mut self) -> Option<DeferredSocketCleanup> {
        self.deferred_socket_cleanup.pop_front()
    }

    pub(crate) fn push_deferred_cleanup(&mut self, pending: DeferredSocketCleanup) {
        self.deferred_socket_cleanup.push_back(pending);
    }

    pub(crate) fn defer_socket_cleanup(&mut self, handle: SocketKey, entry: RegisteredHandle) {
        self.deferred_socket_cleanup
            .push_back(DeferredSocketCleanup::new(handle, entry));
    }
}
