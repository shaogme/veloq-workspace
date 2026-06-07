use std::collections::VecDeque;

use diagweave::prelude::{ResultReportExt, Transform};
use veloq_driver_core::driver::RegisterFd;
use windows_sys::Win32::Networking::WinSock::{
    SO_TYPE, SOCKET, SOL_SOCKET, WSAENOTSOCK, WSAGetLastError, getsockopt,
};

use crate::config::{IoFd, RawHandle, RawHandleKind, RegisteredHandle, SocketKey};
use crate::driver::{IocpDriver, IocpDriverResult};
use crate::error::{IocpError, IocpResult};

pub(super) struct DeferredSocketCleanup {
    handle: SocketKey,
    entry: RegisteredHandle,
}

pub(super) struct HandleRegistry {
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

    fn handle(&self) -> SocketKey {
        self.handle
    }

    fn into_entry(self) -> RegisteredHandle {
        self.entry
    }
}

impl HandleRegistry {
    pub(super) fn new() -> Self {
        Self {
            registered_files: Vec::new(),
            file_generations: Vec::new(),
            free_slots: Vec::new(),
            deferred_socket_cleanup: VecDeque::new(),
            socket_generation_counter: 1,
        }
    }

    pub(super) fn registered_files(&self) -> &[Option<RegisteredHandle>] {
        &self.registered_files
    }

    pub(super) fn file_generations(&self) -> &[u64] {
        &self.file_generations
    }

    #[cfg(test)]
    pub(crate) fn registered_file(&self, idx: usize) -> Option<&RegisteredHandle> {
        self.registered_files.get(idx).and_then(Option::as_ref)
    }

    fn next_socket_generation(&mut self) -> u64 {
        let generation = self.socket_generation_counter;
        self.socket_generation_counter = self.socket_generation_counter.wrapping_add(1);
        if self.socket_generation_counter == 0 {
            self.socket_generation_counter = 1;
        }
        generation
    }

    fn insert_registered(&mut self, entry: RegisteredHandle) -> IoFd {
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

    fn take_for_unregister(&mut self, fd: IoFd) -> Option<(usize, RegisteredHandle)> {
        let idx = fd.fixed_index() as usize;
        if idx >= self.registered_files.len() {
            return None;
        }
        if self.file_generations.get(idx).copied() != Some(fd.generation()) {
            return None;
        }
        self.registered_files[idx].take().map(|entry| (idx, entry))
    }

    fn release_slot(&mut self, idx: usize) {
        self.free_slots.push(idx);
        self.file_generations[idx] = self.file_generations[idx].wrapping_add(1);
    }

    fn deferred_cleanup_len(&self) -> usize {
        self.deferred_socket_cleanup.len()
    }

    fn pop_deferred_cleanup(&mut self) -> Option<DeferredSocketCleanup> {
        self.deferred_socket_cleanup.pop_front()
    }

    fn push_deferred_cleanup(&mut self, pending: DeferredSocketCleanup) {
        self.deferred_socket_cleanup.push_back(pending);
    }

    fn defer_socket_cleanup(&mut self, handle: SocketKey, entry: RegisteredHandle) {
        self.deferred_socket_cleanup
            .push_back(DeferredSocketCleanup::new(handle, entry));
    }
}

impl<'a> IocpDriver<'a> {
    /// Fallback probe for potentially untrusted raw handles.
    ///
    /// We trust `RawHandle` enum semantics by default. Probe is only used when a
    /// `File`-tagged handle may actually be a socket.
    pub(crate) fn detect_socket_from_file_handle(handle: RawHandle) -> IocpResult<bool> {
        let socket = handle.raw().as_socket();
        let mut ty = 0i32;
        let mut len = std::mem::size_of::<i32>() as i32;
        // SAFETY: buffer pointers are valid for getsockopt call.
        let ret = unsafe {
            getsockopt(
                socket as SOCKET,
                SOL_SOCKET,
                SO_TYPE,
                &mut ty as *mut i32 as *mut u8,
                &mut len,
            )
        };
        if ret == 0 {
            return Ok(true);
        }
        // SAFETY: reads last winsock error after getsockopt failure.
        let err = unsafe { WSAGetLastError() };
        if err == WSAENOTSOCK {
            Ok(false)
        } else {
            Err(IocpError::ResolveFd.io_report(
                "iocp/driver.detect_socket_from_file_handle",
                std::io::Error::from_raw_os_error(err),
            ))
        }
    }

    pub(super) fn track_socket_submit_pending(&mut self, key: SocketKey) {
        let _ = self.rio.state_mut().try_acquire_socket_inflight(key);
    }

    pub(super) fn release_socket_inflight_for_op(&mut self, user_data: usize) {
        let socket_key = self
            .ops
            .get_slot_entry_op_storage_and_entry_mut(user_data)
            .and_then(|(_, _, op_opt, _)| {
                let op = op_opt.as_mut()?;
                if !op.header.in_flight {
                    return None;
                }
                op.header.in_flight = false;
                op.header
                    .resolved_handle
                    .filter(|h| h.is_socket())
                    .map(|h| h.actor_key())
            });

        if let Some(key) = socket_key {
            self.rio.state_mut().release_socket_inflight(key);
            self.drain_deferred_socket_cleanup();
        }
    }

    pub(super) fn drain_deferred_socket_cleanup(&mut self) {
        let mut rounds = self.handles.deferred_cleanup_len();
        while rounds > 0 {
            rounds -= 1;
            let Some(pending) = self.handles.pop_deferred_cleanup() else {
                break;
            };

            let key = pending.handle();
            let ready = self.rio.state().socket_ready_for_cleanup(key);

            if ready {
                self.rio.state_mut().shutdown_actor(key);
                self.rio.state_mut().forget_socket_runtime(key);
                drop(pending.into_entry());
            } else {
                self.handles.push_deferred_cleanup(pending);
            }
        }
    }

    /// Registers a chunk of memory for RIO operations.
    pub(crate) fn register_chunk(
        &mut self,
        id: u16,
        ptr: *const u8,
        len: usize,
    ) -> IocpDriverResult<()> {
        self.rio
            .state_mut()
            .register_chunk(id, ptr, len)
            .push_ctx("scope", "iocp/driver")
            .attach_note("failed to register RIO chunk")
            .trans()?;
        Ok(())
    }

    /// Registers a set of file/socket handles for use with the driver.
    pub(crate) fn register_files<'h>(
        &mut self,
        files: Vec<RegisterFd<'h, crate::config::IocpHandle>>,
    ) -> IocpDriverResult<Vec<IoFd>> {
        let mut registered = Vec::with_capacity(files.len());
        for file in files {
            let (handle, is_owned_input) = match file {
                RegisterFd::Borrowed(h) => (RawHandle::new(h.raw()), false),
                RegisterFd::Owned(h) => (h.into_raw(), true),
            };
            // Trust enum semantics first; only probe file-tagged handles as fallback.
            let mut canonical = match handle.kind() {
                RawHandleKind::Socket => handle,
                RawHandleKind::File => {
                    if Self::detect_socket_from_file_handle(handle)
                        .push_ctx("scope", "iocp/driver")
                        .attach_note("detect socket from file handle failed")?
                    {
                        RawHandle::new(crate::config::IocpHandle::for_socket(
                            handle.raw().as_handle(),
                        ))
                    } else {
                        handle
                    }
                }
            };
            let kind = canonical.kind();
            if kind == RawHandleKind::Socket {
                let mut raw = canonical.raw();
                if let crate::config::IocpHandle::Socket { generation: g, .. } = &mut raw
                    && *g == 0
                {
                    *g = self.handles.next_socket_generation();
                }
                canonical = RawHandle::new(raw);

                self.rio
                    .state_mut()
                    .mark_socket_registered(canonical.raw().actor_key());
            }
            let entry = if is_owned_input {
                // SAFETY: ownership comes from RegisterFd::Owned and is transferred
                // into the registered slot for deterministic lifecycle management.
                RegisteredHandle::Owned(unsafe { crate::OwnedRawHandle::from_raw_owned(canonical) })
            } else {
                // Borrowed handles must remain non-owning to avoid accidental close/double-close.
                RegisteredHandle::Weak(canonical)
            };
            registered.push(self.handles.insert_registered(entry));
        }
        Ok(registered)
    }

    /// Unregisters a set of previously registered files.
    pub(crate) fn unregister_files(&mut self, files: Vec<IoFd>) -> IocpDriverResult<()> {
        for fd in files {
            if let Some((idx, entry)) = self.handles.take_for_unregister(fd) {
                if entry.as_raw().kind() == RawHandleKind::Socket {
                    let key = entry.as_raw().raw().actor_key();
                    if self.rio.state_mut().begin_socket_cleanup(key) {
                        self.rio.state_mut().shutdown_actor(key);
                        self.rio.state_mut().forget_socket_runtime(key);
                    } else {
                        self.handles.defer_socket_cleanup(key, entry);
                    }
                }
                self.handles.release_slot(idx);
            }
        }
        Ok(())
    }
}
