use std::collections::VecDeque;
use std::io;

use diagweave::prelude::*;
use veloq_driver_core::driver::RegisterFd;
use windows_sys::Win32::Foundation::CloseHandle;
use windows_sys::Win32::Networking::WinSock::{
    INVALID_SOCKET, SO_TYPE, SOCKET, SOCKET_ERROR, SOL_SOCKET, WSAENOTSOCK, WSAGetLastError,
    closesocket, getsockopt,
};

use crate::config::{
    IoFd, IocpHandle, RawHandle, RawHandleKind, RegisteredHandle, RegisteredSlot, SocketKey,
};
use crate::driver::{IocpDriver, IocpDriverResult};
use crate::error::{IocpError, IocpResult};
use crate::rio::RioState;

pub(super) struct DeferredSocketCleanup {
    handle: SocketKey,
    entry: RegisteredHandle,
}

const REGISTERED_SLOT_SHRINK_MIN_CAPACITY: usize = 1024;

pub(super) struct HandleRegistry {
    slots: Vec<RegisteredSlot>,
    free_slots: Vec<u32>,
    deferred_socket_cleanup: VecDeque<DeferredSocketCleanup>,
    file_generation_counter: u64,
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
            slots: Vec::new(),
            free_slots: Vec::new(),
            deferred_socket_cleanup: VecDeque::new(),
            file_generation_counter: 1,
            socket_generation_counter: 1,
        }
    }

    pub(super) fn registered_slots(&self) -> &[RegisteredSlot] {
        &self.slots
    }

    pub(super) fn submission_slots(&mut self) -> &mut [RegisteredSlot] {
        &mut self.slots
    }

    #[cfg(test)]
    pub(crate) fn registered_file(&self, idx: usize) -> Option<&RegisteredHandle> {
        self.slots.get(idx).and_then(|slot| slot.handle.as_ref())
    }

    fn next_file_generation(&mut self) -> u64 {
        let generation = self.file_generation_counter;
        self.file_generation_counter = self.file_generation_counter.wrapping_add(1);
        if self.file_generation_counter == 0 {
            self.file_generation_counter = 1;
        }
        generation
    }

    fn next_socket_generation(&mut self) -> u64 {
        let generation = self.socket_generation_counter;
        self.socket_generation_counter = self.socket_generation_counter.wrapping_add(1);
        if self.socket_generation_counter == 0 {
            self.socket_generation_counter = 1;
        }
        generation
    }

    fn insert_registered(&mut self, entry: RegisteredHandle) -> IocpResult<IoFd> {
        let (idx, fixed_index) = if let Some(fixed_index) = self.free_slots.pop() {
            let idx = fixed_index as usize;
            let slot_len = self.slots.len();
            let Some(slot) = self.slots.get_mut(idx) else {
                return IocpError::Internal
                    .with_ctx("free_slot_index", idx)
                    .with_ctx("registered_slot_len", slot_len)
                    .attach_note("free registered file slot index out of bounds");
            };
            debug_assert!(slot.handle.is_none(), "free registered slot is occupied");
            slot.handle = Some(entry);
            slot.association = None;
            (idx, fixed_index)
        } else {
            let fixed_index = u32::try_from(self.slots.len()).map_err(|_| {
                IocpError::Registration
                    .to_report()
                    .with_ctx("registered_slot_len", self.slots.len())
                    .attach_note("registered file table index exceeds IoFd range")
            })?;
            let generation = self.next_file_generation();
            self.slots.push(RegisteredSlot::occupied(entry, generation));
            (fixed_index as usize, fixed_index)
        };
        Ok(IoFd::fixed_with_generation(
            fixed_index,
            self.slots[idx].generation,
        ))
    }

    fn take_for_unregister(&mut self, fd: IoFd) -> Option<(u32, RegisteredHandle)> {
        let idx = fd.fixed_index();
        let slot = self.slots.get_mut(idx as usize)?;
        if slot.generation != fd.generation() {
            return None;
        }
        slot.handle.take().map(|entry| (idx, entry))
    }

    fn take_owned_for_close(&mut self, fd: IoFd) -> IocpResult<(u32, RegisteredHandle)> {
        let idx = fd.fixed_index();
        let Some(slot) = self.slots.get_mut(idx as usize) else {
            return IocpError::ResolveFd
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .attach_note("registered file descriptor index out of bounds");
        };

        if slot.generation != fd.generation() {
            return IocpError::ResolveFd
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .with_ctx("current_generation", slot.generation)
                .attach_note("stale registered file descriptor generation");
        }

        match slot.handle.as_ref() {
            Some(RegisteredHandle::Owned(_)) => Ok((
                idx,
                slot.handle
                    .take()
                    .expect("owned registered handle disappeared during close"),
            )),
            Some(RegisteredHandle::Weak(_)) => IocpError::InvalidInput
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .attach_note("Close is only valid for owned registered file descriptors"),
            None => IocpError::ResolveFd
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .attach_note("invalid registered file descriptor"),
        }
    }

    fn release_slot(&mut self, idx: u32) {
        let generation = self.next_file_generation();
        let slot = &mut self.slots[idx as usize];
        debug_assert!(
            slot.handle.is_none(),
            "released registered slot is occupied"
        );
        slot.generation = generation;
        slot.association = None;
        self.free_slots.push(idx);
        self.compact_tail_slots();
    }

    fn compact_tail_slots(&mut self) {
        let old_len = self.slots.len();
        while self.slots.last().is_some_and(|slot| slot.handle.is_none()) {
            self.slots.pop();
        }

        let new_len = self.slots.len();
        if new_len == old_len {
            return;
        }

        self.free_slots.retain(|&idx| (idx as usize) < new_len);
        self.shrink_slot_capacity_if_needed();
    }

    fn shrink_slot_capacity_if_needed(&mut self) {
        let slot_target = self.slots.len().max(REGISTERED_SLOT_SHRINK_MIN_CAPACITY);
        if self.slots.capacity() > slot_target.saturating_mul(2) {
            self.slots.shrink_to(slot_target);
        }

        let free_target = self
            .free_slots
            .len()
            .max(REGISTERED_SLOT_SHRINK_MIN_CAPACITY);
        if self.free_slots.capacity() > free_target.saturating_mul(2) {
            self.free_slots.shrink_to(free_target);
        }
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

fn close_iocp_handle(handle: IocpHandle) -> io::Result<usize> {
    match handle {
        IocpHandle::File { handle } => {
            let ret = unsafe { CloseHandle(handle) };
            if ret == 0 {
                Err(io::Error::last_os_error())
            } else {
                Ok(0)
            }
        }
        IocpHandle::Socket { handle, .. } => {
            let socket = handle as SOCKET;
            let ret = unsafe { closesocket(socket) };
            if ret == SOCKET_ERROR || socket == INVALID_SOCKET {
                Err(io::Error::from_raw_os_error(unsafe { WSAGetLastError() }))
            } else {
                Ok(0)
            }
        }
    }
}

fn close_owned_entry_now(entry: RegisteredHandle) -> io::Result<usize> {
    match entry {
        RegisteredHandle::Owned(handle) => close_iocp_handle(handle.into_raw().raw()),
        RegisteredHandle::Weak(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Close is only valid for owned registered file descriptors",
        )),
    }
}

pub(super) fn close_registered_owned_fd(
    handles: &mut HandleRegistry,
    rio: &mut RioState,
    fd: IoFd,
) -> IocpResult<(IocpHandle, io::Result<usize>)> {
    let (idx, entry) = handles.take_owned_for_close(fd)?;
    let raw = entry.as_raw().raw();
    handles.release_slot(idx);

    let result = if raw.kind() == RawHandleKind::Socket {
        let key = raw.actor_key();
        if rio.begin_socket_cleanup(key) {
            rio.shutdown_actor(key);
            rio.forget_socket_runtime(key);
            close_owned_entry_now(entry)
        } else {
            handles.defer_socket_cleanup(key, entry);
            Ok(0)
        }
    } else {
        close_owned_entry_now(entry)
    };

    Ok((raw, result))
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

    pub(super) fn release_socket_inflight_for_op(&mut self, user_data: usize) -> IocpResult<()> {
        let Some(token) = self
            .ops
            .active_tokens()
            .find(|token| token.index() == user_data)
        else {
            return Ok(());
        };
        let socket_inflight =
            self.ops
                .active_slot_bundle_mut(token)
                .and_then(|(_, _, op_opt, _)| {
                    let op = op_opt.as_mut()?;
                    let was_in_flight = op.header.in_flight;
                    if was_in_flight {
                        op.header.in_flight = false;
                    }
                    let socket_inflight = op.header.socket_inflight.take();
                    debug_assert!(
                        socket_inflight.is_some()
                            || !was_in_flight
                            || op.header.resolved_handle.is_none_or(|h| !h.is_socket())
                            || Self::is_rio_op(op),
                        "kernel-pending socket op completed without socket inflight token"
                    );
                    socket_inflight
                });

        if let Some(token) = socket_inflight {
            self.rio
                .state_mut()
                .release_socket_inflight_token(token)
                .trans()?;
            self.drain_deferred_socket_cleanup();
        }
        Ok(())
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
        id: veloq_buf::heap::ChunkId,
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
        enum InputHandle {
            Borrowed(RawHandle),
            Owned(crate::OwnedRawHandle),
        }

        impl InputHandle {
            fn raw(&self) -> RawHandle {
                match self {
                    Self::Borrowed(handle) => *handle,
                    Self::Owned(handle) => RawHandle::new(handle.raw()),
                }
            }

            fn into_entry(self, canonical: RawHandle) -> RegisteredHandle {
                match self {
                    Self::Borrowed(_) => {
                        // Borrowed handles must remain non-owning to avoid accidental close/double-close.
                        RegisteredHandle::Weak(canonical)
                    }
                    Self::Owned(handle) => {
                        let _ = handle.into_raw();
                        // SAFETY: ownership comes from RegisterFd::Owned and is transferred
                        // into the registered slot for deterministic lifecycle management.
                        RegisteredHandle::Owned(unsafe {
                            crate::OwnedRawHandle::from_raw_owned(canonical)
                        })
                    }
                }
            }
        }

        let mut prepared = Vec::with_capacity(files.len());
        for file in files {
            let input = match file {
                RegisterFd::Borrowed(h) => InputHandle::Borrowed(RawHandle::new(h.raw())),
                RegisterFd::Owned(h) => InputHandle::Owned(h),
            };
            let handle = input.raw();
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
            }

            let socket_key = canonical
                .raw()
                .is_socket()
                .then_some(canonical.raw().actor_key());
            prepared.push((input.into_entry(canonical), socket_key));
        }

        let mut registered = Vec::with_capacity(prepared.len());
        let mut socket_keys = Vec::new();
        for (entry, socket_key) in prepared {
            match self.handles.insert_registered(entry) {
                Ok(fd) => {
                    if let Some(key) = socket_key {
                        socket_keys.push(key);
                    }
                    registered.push(fd);
                }
                Err(report) => {
                    for fd in registered.drain(..) {
                        if let Some((idx, _entry)) = self.handles.take_for_unregister(fd) {
                            self.handles.release_slot(idx);
                        }
                    }
                    return Err(report);
                }
            }
        }

        for key in socket_keys {
            self.rio.state_mut().mark_socket_registered(key);
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
