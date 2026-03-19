use crate::BufferRegistrationMode;
use crate::common::{IocpErrorContext, io_error, io_msg};
use crate::ext::Extensions;
use crate::rio::{RioEnv, RioState};
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

pub(crate) struct RioRqConfig {
    pub(crate) socket: usize,
    pub(crate) max_outstanding_recvs: u32,
    pub(crate) max_receive_data_buffers: u32,
    pub(crate) max_outstanding_sends: u32,
    pub(crate) max_send_data_buffers: u32,
    pub(crate) recv_cq: RioCq,
    pub(crate) send_cq: RioCq,
    pub(crate) context: *const std::ffi::c_void,
}

pub(crate) struct RioExConfig<'a> {
    pub(crate) rq: RioRq,
    pub(crate) data_buf: &'a RIO_BUF,
    pub(crate) data_buf_count: u32,
    pub(crate) local_addr: *const RIO_BUF,
    pub(crate) remote_addr: *const RIO_BUF,
    pub(crate) control_buf: *const RIO_BUF,
    pub(crate) flags_buf: *const RIO_BUF,
    pub(crate) flags: u32,
    pub(crate) context: *const std::ffi::c_void,
}

pub(crate) trait RioProvider: Send + Sync {
    fn create_cq(
        &self,
        entries: u32,
        notification: &RIO_NOTIFICATION_COMPLETION,
    ) -> io::Result<RioCq>;

    fn create_rq(&self, config: RioRqConfig) -> io::Result<RioRq>;

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

    fn send_ex(&self, config: RioExConfig<'_>) -> io::Result<()>;

    fn receive_ex(&self, config: RioExConfig<'_>) -> io::Result<()>;
}

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

impl RioProvider for RioDispatch {
    #[inline]
    fn create_cq(
        &self,
        entries: u32,
        notification: &RIO_NOTIFICATION_COMPLETION,
    ) -> io::Result<RioCq> {
        // SAFETY: Function pointer is verified at startup.
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
    fn create_rq(&self, cfg: RioRqConfig) -> io::Result<RioRq> {
        // SAFETY: Function pointer is verified at startup.
        let rq = unsafe {
            (self.create_rq)(
                cfg.socket,
                cfg.max_outstanding_recvs,
                cfg.max_receive_data_buffers,
                cfg.max_outstanding_sends,
                cfg.max_send_data_buffers,
                cfg.recv_cq.0,
                cfg.send_cq.0,
                cfg.context,
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
        // SAFETY: Function pointer is verified at startup.
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
        // SAFETY: Function pointer is verified at startup.
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
        // SAFETY: Function pointer is verified at startup.
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
        // SAFETY: Function pointer is verified at startup.
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
    fn send_ex(&self, cfg: RioExConfig<'_>) -> io::Result<()> {
        // SAFETY: Function pointer is verified at startup.
        let ret = unsafe {
            (self.send_ex)(
                cfg.rq.0,
                cfg.data_buf,
                cfg.data_buf_count,
                cfg.local_addr,
                cfg.remote_addr,
                cfg.control_buf,
                cfg.flags_buf,
                cfg.flags,
                cfg.context,
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
    fn receive_ex(&self, cfg: RioExConfig<'_>) -> io::Result<()> {
        // SAFETY: Function pointer is verified at startup.
        let ret = unsafe {
            (self.receive_ex)(
                cfg.rq.0,
                cfg.data_buf,
                cfg.data_buf_count,
                cfg.local_addr,
                cfg.remote_addr,
                cfg.control_buf,
                cfg.flags_buf,
                cfg.flags,
                cfg.context,
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
            create_cq: table
                .RIOCreateCompletionQueue
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOCreateCompletionQueue missing"))?,
            create_rq: table
                .RIOCreateRequestQueue
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOCreateRequestQueue missing"))?,
            register_buffer: table
                .RIORegisterBuffer
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIORegisterBuffer missing"))?,
            deregister_buffer: table
                .RIODeregisterBuffer
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIODeregisterBuffer missing"))?,
            dequeue: table
                .RIODequeueCompletion
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIODequeueCompletion missing"))?,
            notify: table
                .RIONotify
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIONotify missing"))?,
            close_cq: table
                .RIOCloseCompletionQueue
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOCloseCompletionQueue missing"))?,
            receive: table
                .RIOReceive
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOReceive missing"))?,
            send: table
                .RIOSend
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOSend missing"))?,
            send_ex: table
                .RIOSendEx
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOSendEx missing"))?,
            receive_ex: table
                .RIOReceiveEx
                .ok_or_else(|| io_msg(IocpErrorContext::Rio, "RIOReceiveEx missing"))?,
        };
        Self::new(port, entries, dispatch)
    }

    fn new(port: HANDLE, entries: u32, dispatch: RioDispatch) -> io::Result<Self> {
        const RIO_EVENT_KEY: usize = usize::MAX - 1;
        let mut notify_ov = Box::new(Overlapped::zeroed());
        let notification = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_IOCP_COMPLETION,
            Anonymous: windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0 {
                Iocp: windows_sys::Win32::Networking::WinSock::RIO_NOTIFICATION_COMPLETION_0_1 {
                    IocpHandle: port,
                    CompletionKey: RIO_EVENT_KEY as *mut std::ffi::c_void,
                    Overlapped: notify_ov.as_mut_ptr().cast(),
                },
            },
        };

        let cq = dispatch.create_cq(entries.max(128), &notification)?;
        dispatch.notify(cq)?;

        Ok(Self {
            cq,
            _notify_overlapped: notify_ov,
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
        self.dispatch.map_or(0, |d| d.dequeue(self.cq, results))
    }

    #[inline]
    pub(crate) fn rearm_notify(&self) -> io::Result<()> {
        self.dispatch.map_or(Ok(()), |d| d.notify(self.cq))
    }

    pub(crate) fn submit_receive(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        ctx: *const std::ffi::c_void,
    ) -> io::Result<()> {
        self.dispatch
            .as_ref()
            .ok_or_else(|| io::Error::other("RIO dispatch context lost"))?
            .receive(rq, buf, 1, 0, ctx)
    }

    pub(crate) fn submit_send(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        ctx: *const std::ffi::c_void,
    ) -> io::Result<()> {
        self.dispatch
            .as_ref()
            .ok_or_else(|| io::Error::other("RIO dispatch context lost"))?
            .send(rq, buf, 1, 0, ctx)
    }

    pub(crate) fn submit_send_ex(
        &self,
        rq: RioRq,
        data_buf: &RIO_BUF,
        addr_buf: &RIO_BUF,
        ctx: *const std::ffi::c_void,
    ) -> io::Result<()> {
        let dispatch = self
            .dispatch
            .as_ref()
            .ok_or_else(|| io::Error::other("RIO dispatch context lost"))?;
        dispatch.send_ex(RioExConfig {
            rq,
            data_buf,
            data_buf_count: 1,
            local_addr: std::ptr::null(),
            remote_addr: addr_buf,
            control_buf: std::ptr::null(),
            flags_buf: std::ptr::null(),
            flags: 0,
            context: ctx,
        })
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
