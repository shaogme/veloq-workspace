//! Kernel-facing RIO dispatch table and submission primitives.
//!
//! This module encapsulates:
//! - dynamic function-pointer extraction from extension tables,
//! - CQ creation/notification lifecycle,
//! - minimal wrappers for `RIOReceive`, `RIOSend`, and `RIOSendEx`,
//! - `RioState` constructors and basic registration entry points.
//!
//! It forms the low-level boundary between high-level runtime orchestration and
//! Windows RIO APIs, keeping unsafe calls and pointer setup in one place.

use crate::BufferRegistrationMode;
use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::ext::Extensions;
use crate::ops::submit::SubmissionResult;
use crate::rio::core::registry::RioRegistry;
use crate::rio::{RioEnv, RioState, RioTarget};
use crate::win32::Overlapped;
use std::io;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    RIO_BUF, RIO_BUFFERID, RIO_CQ, RIO_IOCP_COMPLETION, RIO_NOTIFICATION_COMPLETION, RIO_RQ,
    RIORESULT, SOCKET_ERROR,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct RioBufferId(pub(crate) RIO_BUFFERID);

impl RioBufferId {
    pub(crate) const INVALID: Self = Self(0 as RIO_BUFFERID);

    #[inline]
    pub(crate) fn is_invalid(&self) -> bool {
        self.0 == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct RioCq(pub(crate) RIO_CQ);

impl RioCq {
    pub(crate) const INVALID: Self = Self(0);

    #[inline]
    pub(crate) fn is_invalid(&self) -> bool {
        self.0 == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct RioRq(pub(crate) RIO_RQ);

impl RioRq {
    #[allow(dead_code)]
    pub(crate) const INVALID: Self = Self(0);

    #[allow(dead_code)]
    #[inline]
    pub(crate) fn is_invalid(&self) -> bool {
        self.0 == 0
    }
}

pub(crate) trait RioProvider: Send + Sync {
    fn create_cq(
        &self,
        entries: u32,
        notification: &RIO_NOTIFICATION_COMPLETION,
    ) -> io::Result<RioCq>;
    #[allow(clippy::too_many_arguments)]
    fn create_rq(
        &self,
        socket: usize,
        max_outstanding_recvs: u32,
        max_receive_data_buffers: u32,
        max_outstanding_sends: u32,
        max_send_data_buffers: u32,
        recv_cq: RioCq,
        send_cq: RioCq,
        context: *const std::ffi::c_void,
    ) -> io::Result<RioRq>;
    fn register_buffer(&self, ptr: *const u8, len: u32) -> io::Result<RioBufferId>;
    fn deregister_buffer(&self, id: RioBufferId);
    fn dequeue(&self, cq: RioCq, results: &mut [RIORESULT]) -> u32;
    fn notify(&self, cq: RioCq) -> io::Result<()>;
    fn close_cq(&self, cq: RioCq);
    fn receive(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        num_bufs: u32,
        flags: u32,
        context: *const std::ffi::c_void,
    ) -> io::Result<()>;
    fn send(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        num_bufs: u32,
        flags: u32,
        context: *const std::ffi::c_void,
    ) -> io::Result<()>;
    #[allow(clippy::too_many_arguments)]
    fn send_ex(
        &self,
        rq: RioRq,
        data_buf: &RIO_BUF,
        data_buf_count: u32,
        local_addr: *const RIO_BUF,
        remote_addr: *const RIO_BUF,
        control_buf: *const RIO_BUF,
        flags_buf: *const RIO_BUF,
        flags: u32,
        context: *const std::ffi::c_void,
    ) -> io::Result<()>;
    #[allow(clippy::too_many_arguments)]
    fn receive_ex(
        &self,
        rq: RioRq,
        data_buf: &RIO_BUF,
        data_buf_count: u32,
        local_addr: *const RIO_BUF,
        remote_addr: *const RIO_BUF,
        control_buf: *const RIO_BUF,
        flags_buf: *const RIO_BUF,
        flags: u32,
        context: *const std::ffi::c_void,
    ) -> io::Result<()>;
}

#[derive(Clone, Copy)]
pub(crate) struct RioDispatch {
    pub(crate) create_cq:
        unsafe extern "system" fn(u32, *const RIO_NOTIFICATION_COMPLETION) -> RIO_CQ,
    #[allow(clippy::too_many_arguments)]
    pub(crate) create_rq: unsafe extern "system" fn(
        usize,
        u32,
        u32,
        u32,
        u32,
        RIO_CQ,
        RIO_CQ,
        *const std::ffi::c_void,
    ) -> RIO_RQ,
    pub(crate) register_buffer: unsafe extern "system" fn(*const u8, u32) -> RIO_BUFFERID,
    pub(crate) deregister_buffer: unsafe extern "system" fn(RIO_BUFFERID),
    pub(crate) dequeue: unsafe extern "system" fn(RIO_CQ, *mut RIORESULT, u32) -> u32,
    pub(crate) notify: unsafe extern "system" fn(RIO_CQ) -> i32,
    pub(crate) close_cq: unsafe extern "system" fn(RIO_CQ),
    pub(crate) receive:
        unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const std::ffi::c_void) -> i32,
    pub(crate) send:
        unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const std::ffi::c_void) -> i32,
    #[allow(clippy::too_many_arguments)]
    pub(crate) send_ex: unsafe extern "system" fn(
        RIO_RQ,
        *const RIO_BUF,
        u32,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        u32,
        *const std::ffi::c_void,
    ) -> i32,
    #[allow(clippy::too_many_arguments)]
    pub(crate) receive_ex: unsafe extern "system" fn(
        RIO_RQ,
        *const RIO_BUF,
        u32,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        u32,
        *const std::ffi::c_void,
    ) -> i32,
}

impl RioProvider for RioDispatch {
    #[inline]
    fn create_cq(
        &self,
        entries: u32,
        notification: &RIO_NOTIFICATION_COMPLETION,
    ) -> io::Result<RioCq> {
        // SAFETY: Function pointer is verified at startup. Parameters are validated by the caller.
        let cq = unsafe { (self.create_cq)(entries, notification as *const _) };
        if cq == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIOCreateCompletionQueue failed",
            ));
        }
        Ok(RioCq(cq))
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn create_rq(
        &self,
        socket: usize,
        max_outstanding_recvs: u32,
        max_receive_data_buffers: u32,
        max_outstanding_sends: u32,
        max_send_data_buffers: u32,
        recv_cq: RioCq,
        send_cq: RioCq,
        context: *const std::ffi::c_void,
    ) -> io::Result<RioRq> {
        // SAFETY: Function pointer is verified at startup. Parameters are validated by the caller.
        let rq = unsafe {
            (self.create_rq)(
                socket,
                max_outstanding_recvs,
                max_receive_data_buffers,
                max_outstanding_sends,
                max_send_data_buffers,
                recv_cq.0,
                send_cq.0,
                context,
            )
        };
        if rq == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIOCreateRequestQueue failed",
            ));
        }
        Ok(RioRq(rq))
    }

    #[inline]
    fn register_buffer(&self, ptr: *const u8, len: u32) -> io::Result<RioBufferId> {
        // SAFETY: Function pointer is verified at startup. Buffer must be valid for the given length.
        let id = unsafe { (self.register_buffer)(ptr, len) };
        if id == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIORegisterBuffer failed",
            ));
        }
        Ok(RioBufferId(id))
    }

    #[inline]
    fn deregister_buffer(&self, id: RioBufferId) {
        if !id.is_invalid() {
            // SAFETY: Function pointer is verified at startup.
            unsafe { (self.deregister_buffer)(id.0) };
        }
    }

    #[inline]
    fn dequeue(&self, cq: RioCq, results: &mut [RIORESULT]) -> u32 {
        // SAFETY: Function pointer and results buffer validity are verified by caller.
        unsafe { (self.dequeue)(cq.0, results.as_mut_ptr(), results.len() as u32) }
    }

    #[inline]
    fn notify(&self, cq: RioCq) -> io::Result<()> {
        // SAFETY: Function pointer is verified at startup.
        let ret = unsafe { (self.notify)(cq.0) };
        if ret == SOCKET_ERROR {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIONotify failed",
            ));
        }
        Ok(())
    }

    #[inline]
    fn close_cq(&self, cq: RioCq) {
        if !cq.is_invalid() {
            // SAFETY: Function pointer is verified at startup.
            unsafe { (self.close_cq)(cq.0) };
        }
    }

    #[inline]
    fn receive(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        num_bufs: u32,
        flags: u32,
        context: *const std::ffi::c_void,
    ) -> io::Result<()> {
        // SAFETY: Function pointer is verified at startup. Parameters are validated by the caller.
        let ret = unsafe { (self.receive)(rq.0, buf as *const _, num_bufs, flags, context) };
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIOReceive failed",
            ));
        }
        Ok(())
    }

    #[inline]
    fn send(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        num_bufs: u32,
        flags: u32,
        context: *const std::ffi::c_void,
    ) -> io::Result<()> {
        // SAFETY: Function pointer is verified at startup. Parameters are validated by the caller.
        let ret = unsafe { (self.send)(rq.0, buf as *const _, num_bufs, flags, context) };
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIOSend failed",
            ));
        }
        Ok(())
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn send_ex(
        &self,
        rq: RioRq,
        data_buf: &RIO_BUF,
        data_buf_count: u32,
        local_addr: *const RIO_BUF,
        remote_addr: *const RIO_BUF,
        control_buf: *const RIO_BUF,
        flags_buf: *const RIO_BUF,
        flags: u32,
        context: *const std::ffi::c_void,
    ) -> io::Result<()> {
        // SAFETY: Function pointer is verified at startup. Parameters are validated by the caller.
        let ret = unsafe {
            (self.send_ex)(
                rq.0,
                data_buf,
                data_buf_count,
                local_addr,
                remote_addr,
                control_buf,
                flags_buf,
                flags,
                context,
            )
        };
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIOSendEx failed",
            ));
        }
        Ok(())
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn receive_ex(
        &self,
        rq: RioRq,
        data_buf: &RIO_BUF,
        data_buf_count: u32,
        local_addr: *const RIO_BUF,
        remote_addr: *const RIO_BUF,
        control_buf: *const RIO_BUF,
        flags_buf: *const RIO_BUF,
        flags: u32,
        context: *const std::ffi::c_void,
    ) -> io::Result<()> {
        // SAFETY: Function pointer is verified at startup. Parameters are validated by the caller.
        let ret = unsafe {
            (self.receive_ex)(
                rq.0,
                data_buf,
                data_buf_count,
                local_addr,
                remote_addr,
                control_buf,
                flags_buf,
                flags,
                context,
            )
        };
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIOReceiveEx failed",
            ));
        }
        Ok(())
    }
}

pub(crate) struct RioKernel {
    pub(crate) cq: RioCq,
    _notify_overlapped: Box<Overlapped>,
    pub(crate) dispatch: Option<RioDispatch>,
}

impl RioKernel {
    pub(super) fn from_extensions(
        port: HANDLE,
        entries: u32,
        ext: &Extensions,
    ) -> io::Result<Self> {
        let table = &ext.rio_table;
        let dispatch = RioDispatch {
            create_cq: table.RIOCreateCompletionQueue.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIOCreateCompletionQueue function pointer missing",
                )
            })?,
            create_rq: table.RIOCreateRequestQueue.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIOCreateRequestQueue function pointer missing",
                )
            })?,
            register_buffer: table.RIORegisterBuffer.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIORegisterBuffer function pointer missing",
                )
            })?,
            deregister_buffer: table.RIODeregisterBuffer.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIODeregisterBuffer function pointer missing",
                )
            })?,
            dequeue: table.RIODequeueCompletion.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIODequeueCompletion function pointer missing",
                )
            })?,
            notify: table.RIONotify.ok_or_else(|| {
                io_msg(IocpErrorContext::Rio, "RIONotify function pointer missing")
            })?,
            close_cq: table.RIOCloseCompletionQueue.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIOCloseCompletionQueue function pointer missing",
                )
            })?,
            receive: table.RIOReceive.ok_or_else(|| {
                io_msg(IocpErrorContext::Rio, "RIOReceive function pointer missing")
            })?,
            send: table
                .RIOSend
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOSend function pointer missing"))?,
            send_ex: table.RIOSendEx.ok_or_else(|| {
                io_msg(IocpErrorContext::Rio, "RIOSendEx function pointer missing")
            })?,
            receive_ex: table.RIOReceiveEx.ok_or_else(|| {
                io_msg(
                    IocpErrorContext::Rio,
                    "RIOReceiveEx function pointer missing",
                )
            })?,
        };
        Self::new(port, entries, dispatch)
    }

    fn new(port: HANDLE, entries: u32, dispatch: RioDispatch) -> io::Result<Self> {
        const RIO_EVENT_KEY: usize = usize::MAX - 1;
        let mut notify_overlapped = Box::new(Overlapped::zeroed());
        let notification = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_IOCP_COMPLETION,
            Anonymous: windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0 {
                Iocp: windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0_1 {
                    IocpHandle: port,
                    CompletionKey: RIO_EVENT_KEY as *mut std::ffi::c_void,
                    Overlapped: notify_overlapped.as_mut_ptr().cast(),
                },
            },
        };

        let queue_size = entries.max(128);
        let cq = dispatch.create_cq(queue_size, &notification)?;

        dispatch.notify(cq)?;

        Ok(Self {
            cq,
            _notify_overlapped: notify_overlapped,
            dispatch: Some(dispatch),
        })
    }

    pub(crate) fn noop() -> Self {
        Self {
            cq: RioCq::INVALID,
            _notify_overlapped: Box::new(Overlapped::zeroed()),
            dispatch: None,
        }
    }

    #[inline]
    pub(crate) fn env<'a>(
        &'a self,
        registrar: &'a dyn veloq_buf::BufferRegistrar,
        registration_mode: BufferRegistrationMode,
    ) -> Option<RioEnv<'a>> {
        let dispatch = self.dispatch.as_ref()?;
        Some(RioEnv {
            registrar,
            dispatch,
            cq: self.cq,
            registration_mode,
        })
    }

    #[inline]
    pub(crate) fn dequeue(&self, results: &mut [RIORESULT]) -> u32 {
        let Some(dispatch) = self.dispatch else {
            return 0;
        };
        dispatch.dequeue(self.cq, results)
    }

    #[inline]
    pub(crate) fn rearm_notify(&self) -> io::Result<()> {
        let Some(dispatch) = self.dispatch else {
            return Ok(());
        };
        dispatch.notify(self.cq)
    }

    #[inline]
    pub(crate) fn submit_receive(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        request_context: *const std::ffi::c_void,
    ) -> io::Result<()> {
        let Some(dispatch) = self.dispatch else {
            return Ok(());
        };
        dispatch.receive(rq, buf, 1, 0, request_context)
    }

    #[inline]
    pub(crate) fn submit_send(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        request_context: *const std::ffi::c_void,
    ) -> io::Result<()> {
        let Some(dispatch) = self.dispatch else {
            return Ok(());
        };
        dispatch.send(rq, buf, 1, 0, request_context)
    }

    #[inline]
    pub(crate) fn submit_send_ex(
        &self,
        rq: RioRq,
        data_buf: &RIO_BUF,
        addr_buf: &RIO_BUF,
        request_context: *const std::ffi::c_void,
    ) -> io::Result<()> {
        let Some(dispatch) = self.dispatch else {
            return Ok(());
        };
        dispatch.send_ex(
            rq,
            data_buf,
            1,
            std::ptr::null(),
            addr_buf,
            std::ptr::null(),
            std::ptr::null(),
            0,
            request_context,
        )
    }

    #[inline]
    pub(crate) fn close(&mut self) {
        if !self.cq.is_invalid() {
            if let Some(dispatch) = self.dispatch {
                dispatch.close_cq(self.cq);
            }
            self.cq = RioCq::INVALID;
        }
    }
}

impl RioState {
    pub(crate) fn new(
        port: HANDLE,
        entries: u32,
        ext: &Extensions,
        registration_mode: BufferRegistrationMode,
    ) -> io::Result<Self> {
        let kernel = RioKernel::from_extensions(port, entries, ext)?;

        let rq_depth = entries.clamp(32, 256);

        Ok(Self {
            kernel,
            registry: RioRegistry::new(rq_depth),
            registration_mode,
            actors: rustc_hash::FxHashMap::default(),
            actor_routes: rustc_hash::FxHashMap::default(),
            next_actor_id: 1,
            outstanding_count: 0,
        })
    }

    pub(crate) fn resize_rqs(&mut self, size: usize) {
        self.registry.resize_rqs(size);
    }

    pub(crate) fn clear_registered_rq(&mut self, idx: usize) {
        self.registry.clear_registered_rq(idx);
    }

    pub(crate) fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        let Some(env) = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode)
        else {
            return Ok(());
        };
        self.registry.register_chunk(id, (ptr, len), env)
    }

    pub(crate) fn try_submit_recv(
        &mut self,
        target: RioTarget,
        buf: &mut veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        let RioTarget {
            fd,
            handle,
            user_data,
            generation,
        } = target;
        let Some(dispatch) = self.kernel.dispatch else {
            return Ok(SubmissionResult::Pending);
        };
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let rio_buf = self
            .registry
            .prepare_submission(buf, buf.capacity() as u32, env)?;
        let request_context = Self::encode_req_ctx(user_data, generation);
        if let Err(e) = self.kernel.submit_receive(rq, &rio_buf, request_context) {
            Self::free_op_req_ctx(request_context as u64);
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOReceive submission failed: fd={fd:?}, handle={handle:?}, original_error={e}"
                ),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub(crate) fn try_submit_send(
        &mut self,
        target: RioTarget,
        buf: &veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<SubmissionResult> {
        let RioTarget {
            fd,
            handle,
            user_data,
            generation,
        } = target;
        let Some(dispatch) = self.kernel.dispatch else {
            return Ok(SubmissionResult::Pending);
        };
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let rio_buf = self
            .registry
            .prepare_submission(buf, buf.len() as u32, env)?;
        let request_context = Self::encode_req_ctx(user_data, generation);
        if let Err(e) = self.kernel.submit_send(rq, &rio_buf, request_context) {
            Self::free_op_req_ctx(request_context as u64);
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOSend submission failed: fd={fd:?}, handle={handle:?}, original_error={e}"
                ),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }
}
