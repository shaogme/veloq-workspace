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

use crate::config::BufferRegistrationMode;
use crate::driver::iocp::error::{IocpErrorContext, io_error, io_msg};
use crate::driver::iocp::ext::Extensions;
use crate::driver::iocp::rio::{RioEnv, RioState};
use crate::op::IoFd;
use std::io;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    RIO_BUF, RIO_BUFFERID, RIO_CQ, RIO_IOCP_COMPLETION, RIO_NOTIFICATION_COMPLETION, RIO_RQ,
    RIORESULT, SOCKET_ERROR,
};
use windows_sys::Win32::System::IO::OVERLAPPED;

#[derive(Clone, Copy)]
pub(crate) struct RioDispatch {
    pub(crate) create_cq:
        unsafe extern "system" fn(u32, *const RIO_NOTIFICATION_COMPLETION) -> RIO_CQ,
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

pub(crate) struct RioKernel {
    pub(crate) cq: RIO_CQ,
    _notify_overlapped: Box<OVERLAPPED>,
    pub(crate) dispatch: RioDispatch,
}

unsafe extern "system" fn noop_create_cq(_: u32, _: *const RIO_NOTIFICATION_COMPLETION) -> RIO_CQ {
    0
}

unsafe extern "system" fn noop_create_rq(
    _: usize,
    _: u32,
    _: u32,
    _: u32,
    _: u32,
    _: RIO_CQ,
    _: RIO_CQ,
    _: *const std::ffi::c_void,
) -> RIO_RQ {
    0
}

unsafe extern "system" fn noop_register_buffer(_: *const u8, _: u32) -> RIO_BUFFERID {
    0 as RIO_BUFFERID
}

unsafe extern "system" fn noop_deregister_buffer(_: RIO_BUFFERID) {}

unsafe extern "system" fn noop_dequeue(_: RIO_CQ, _: *mut RIORESULT, _: u32) -> u32 {
    0
}

unsafe extern "system" fn noop_notify(_: RIO_CQ) -> i32 {
    0
}

unsafe extern "system" fn noop_close_cq(_: RIO_CQ) {}

unsafe extern "system" fn noop_receive(
    _: RIO_RQ,
    _: *const RIO_BUF,
    _: u32,
    _: u32,
    _: *const std::ffi::c_void,
) -> i32 {
    0
}

unsafe extern "system" fn noop_send(
    _: RIO_RQ,
    _: *const RIO_BUF,
    _: u32,
    _: u32,
    _: *const std::ffi::c_void,
) -> i32 {
    0
}

unsafe extern "system" fn noop_send_ex(
    _: RIO_RQ,
    _: *const RIO_BUF,
    _: u32,
    _: *const RIO_BUF,
    _: *const RIO_BUF,
    _: *const RIO_BUF,
    _: *const RIO_BUF,
    _: u32,
    _: *const std::ffi::c_void,
) -> i32 {
    0
}

unsafe extern "system" fn noop_receive_ex(
    _: RIO_RQ,
    _: *const RIO_BUF,
    _: u32,
    _: *const RIO_BUF,
    _: *const RIO_BUF,
    _: *const RIO_BUF,
    _: *const RIO_BUF,
    _: u32,
    _: *const std::ffi::c_void,
) -> i32 {
    0
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
        let mut notify_overlapped = Box::new(unsafe { std::mem::zeroed::<OVERLAPPED>() });
        let notification = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_IOCP_COMPLETION,
            Anonymous: windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0 {
                Iocp: windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0_1 {
                    IocpHandle: port,
                    CompletionKey: RIO_EVENT_KEY as *mut std::ffi::c_void,
                    Overlapped: (&mut *notify_overlapped as *mut OVERLAPPED).cast(),
                },
            },
        };

        let queue_size = entries.max(128);
        let cq = unsafe { (dispatch.create_cq)(queue_size, &notification as *const _) };
        if cq == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                format!(
                    "RIOCreateCompletionQueue failed: entries={entries}, queue_size={queue_size}"
                ),
            ));
        }

        let notify_ret = unsafe { (dispatch.notify)(cq) };
        if notify_ret == SOCKET_ERROR {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIONotify failed after CQ creation",
            ));
        }

        Ok(Self {
            cq,
            _notify_overlapped: notify_overlapped,
            dispatch,
        })
    }

    pub(crate) fn noop() -> Self {
        let dispatch = RioDispatch {
            create_cq: noop_create_cq,
            create_rq: noop_create_rq,
            register_buffer: noop_register_buffer,
            deregister_buffer: noop_deregister_buffer,
            dequeue: noop_dequeue,
            notify: noop_notify,
            close_cq: noop_close_cq,
            receive: noop_receive,
            send: noop_send,
            send_ex: noop_send_ex,
            receive_ex: noop_receive_ex,
        };
        Self {
            cq: 0,
            _notify_overlapped: Box::new(unsafe { std::mem::zeroed::<OVERLAPPED>() }),
            dispatch,
        }
    }

    #[inline]
    pub(crate) fn env<'a>(
        &'a self,
        registrar: &'a dyn veloq_buf::BufferRegistrar,
        registration_mode: BufferRegistrationMode,
    ) -> RioEnv<'a> {
        RioEnv {
            registrar,
            dispatch: &self.dispatch,
            cq: self.cq,
            registration_mode,
        }
    }

    #[inline]
    pub(crate) fn dequeue(&self, results: *mut RIORESULT, len: u32) -> u32 {
        unsafe { (self.dispatch.dequeue)(self.cq, results, len) }
    }

    #[inline]
    pub(crate) fn rearm_notify(&self) -> io::Result<()> {
        let ret = unsafe { (self.dispatch.notify)(self.cq) };
        if ret == SOCKET_ERROR {
            return Err(io_error(
                IocpErrorContext::Rio,
                RioState::last_wsa_error(),
                "RIONotify failed when rearming CQ",
            ));
        }
        Ok(())
    }

    #[inline]
    pub(crate) fn submit_receive(
        &self,
        rq: RIO_RQ,
        buf: &RIO_BUF,
        request_context: *const std::ffi::c_void,
    ) -> i32 {
        unsafe { (self.dispatch.receive)(rq, buf, 1, 0, request_context) }
    }

    #[inline]
    pub(crate) fn submit_send(
        &self,
        rq: RIO_RQ,
        buf: &RIO_BUF,
        request_context: *const std::ffi::c_void,
    ) -> i32 {
        unsafe { (self.dispatch.send)(rq, buf, 1, 0, request_context) }
    }

    #[inline]
    pub(crate) fn submit_send_ex(
        &self,
        rq: RIO_RQ,
        data_buf: &RIO_BUF,
        addr_buf: &RIO_BUF,
        request_context: *const std::ffi::c_void,
    ) -> i32 {
        unsafe {
            (self.dispatch.send_ex)(
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
    }

    #[inline]
    pub(crate) fn close(&mut self) {
        if self.cq != 0 {
            unsafe { (self.dispatch.close_cq)(self.cq) };
            self.cq = 0;
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
            registry: crate::driver::iocp::rio::core::registry::RioRegistry::new(rq_depth),
            registration_mode,
            actors: rustc_hash::FxHashMap::default(),
            actor_routes: rustc_hash::FxHashMap::default(),
            next_actor_id: 1,
            outstanding_count: 0,
        })
    }

    pub(crate) fn resize_registered_rqs(&mut self, size: usize) {
        self.registry.resize_registered_rqs(size);
    }

    pub(crate) fn clear_registered_rq(&mut self, idx: usize) {
        self.registry.clear_registered_rq(idx);
    }

    pub(crate) fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        let env = self
            .kernel
            .env(&veloq_buf::NoopRegistrar, self.registration_mode);
        self.registry.register_chunk(id, (ptr, len), env)
    }

    pub(crate) fn try_submit_recv(
        &mut self,
        target: (IoFd, HANDLE, usize, u32),
        buf: &mut veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let (fd, handle, user_data, generation) = target;
        let dispatch = self.kernel.dispatch;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let rio_buf = self
            .registry
            .prepare_data_submission(buf, buf.capacity() as u32, env)?;
        let request_context = Self::encode_request_context(user_data, generation);
        let ret = self.kernel.submit_receive(rq, &rio_buf, request_context);
        if ret == 0 {
            Self::free_op_request_context(request_context as u64);
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOReceive submission failed: fd={fd:?}, handle={handle:?}"),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub(crate) fn try_submit_send(
        &mut self,
        target: (IoFd, HANDLE, usize, u32),
        buf: &veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let (fd, handle, user_data, generation) = target;
        let dispatch = self.kernel.dispatch;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
            registration_mode: self.registration_mode,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let rio_buf = self
            .registry
            .prepare_data_submission(buf, buf.len() as u32, env)?;
        let request_context = Self::encode_request_context(user_data, generation);
        let ret = self.kernel.submit_send(rq, &rio_buf, request_context);
        if ret == 0 {
            Self::free_op_request_context(request_context as u64);
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOSend submission failed: fd={fd:?}, handle={handle:?}"),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }
}
