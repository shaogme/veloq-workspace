use windows_sys::Win32::Networking::WinSock::{
    SO_TYPE, SOCKET, SOL_SOCKET, WSAENOTSOCK, WSAGetLastError, getsockopt,
};

use diagweave::prelude::{ResultReportExt, Transform};
use veloq_driver_core::driver::RegisterFd;

use crate::config::{IoFd, IocpHandle, RawHandle, RawHandleKind, RegisteredHandle, SocketKey};
use crate::driver::{IocpDriver, IocpDriverResult};
use crate::error::{IocpError, IocpResult};

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

    pub(crate) fn track_socket_submit_pending(&mut self, key: SocketKey) {
        let _ = self.rio.state_mut().try_acquire_socket_inflight(key);
    }

    pub(crate) fn release_socket_inflight_for_op(&mut self, user_data: usize) {
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

    pub(crate) fn drain_deferred_socket_cleanup(&mut self) {
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
        files: Vec<RegisterFd<'h, IocpHandle>>,
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
                        RawHandle::new(IocpHandle::for_socket(handle.raw().as_handle()))
                    } else {
                        handle
                    }
                }
            };
            let kind = canonical.kind();
            if kind == RawHandleKind::Socket {
                let mut raw = canonical.raw();
                if let IocpHandle::Socket { generation: g, .. } = &mut raw
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
