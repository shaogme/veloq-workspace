use crate::{
    BufferRegistrationMode,
    config::BorrowedRawHandle,
    driver::RIO_EVENT_KEY,
    error::IocpError,
    ext::Extensions,
    rio::{
        core::RioEnv,
        error::{RioError, RioResult},
    },
    win32::Overlapped,
};
use diagweave::prelude::*;
use veloq_buf::BufferRegistrar;
use veloq_std::{boxed::Box, ffi::c_void, ptr};
use windows_sys::Win32::Networking::WinSock::{
    RIO_BUF, RIO_BUFFERID, RIO_CQ, RIO_IOCP_COMPLETION, RIO_NOTIFICATION_COMPLETION,
    RIO_NOTIFICATION_COMPLETION_0, RIO_NOTIFICATION_COMPLETION_0_1, RIO_RQ, RIORESULT,
    SOCKET_ERROR, WSAGetLastError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct RioBufferId(RIO_BUFFERID);

impl RioBufferId {
    pub(crate) const INVALID: Self = Self(0 as RIO_BUFFERID);

    #[cfg(test)]
    pub(crate) const fn from_raw(raw: RIO_BUFFERID) -> Self {
        Self(raw)
    }

    pub(crate) const fn raw(self) -> RIO_BUFFERID {
        self.0
    }

    #[inline]
    pub(crate) fn is_invalid(&self) -> bool {
        self.0 == 0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(transparent)]
pub(crate) struct RioCq(RIO_CQ);

impl RioCq {
    pub(crate) const INVALID: Self = Self(0);

    pub(crate) const fn raw(self) -> RIO_CQ {
        self.0
    }

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
    pub(crate) context: *const c_void,
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
    pub(crate) context: *const c_void,
}

pub(crate) trait RioProvider: Send + Sync {
    fn create_cq(
        &self,
        entries: u32,
        notification: &RIO_NOTIFICATION_COMPLETION,
    ) -> RioResult<RioCq>;

    fn create_rq(&self, config: RioRqConfig) -> RioResult<RioRq>;

    fn register_buffer(&self, ptr: *const u8, len: u32) -> RioResult<RioBufferId>;

    fn deregister_buffer(&self, id: RioBufferId);

    fn dequeue(&self, cq: RioCq, results: &mut [RIORESULT]) -> u32;

    fn notify(&self, cq: RioCq) -> RioResult<()>;

    fn close_cq(&self, cq: RioCq);

    fn receive(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        num_bufs: u32,
        flags: u32,
        context: *const c_void,
    ) -> RioResult<()>;

    fn send(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        num_bufs: u32,
        flags: u32,
        context: *const c_void,
    ) -> RioResult<()>;

    fn send_ex(&self, config: RioExConfig<'_>) -> RioResult<()>;

    fn receive_ex(&self, config: RioExConfig<'_>) -> RioResult<()>;
}

#[cfg(test)]
#[derive(Clone, Copy)]
pub(crate) struct RioDispatchFunctions {
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
        *const c_void,
    ) -> RIO_RQ,
    pub(crate) register_buffer: unsafe extern "system" fn(*const u8, u32) -> RIO_BUFFERID,
    pub(crate) deregister_buffer: unsafe extern "system" fn(RIO_BUFFERID),
    pub(crate) dequeue: unsafe extern "system" fn(RIO_CQ, *mut RIORESULT, u32) -> u32,
    pub(crate) notify: unsafe extern "system" fn(RIO_CQ) -> i32,
    pub(crate) close_cq: unsafe extern "system" fn(RIO_CQ),
    pub(crate) receive:
        unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const c_void) -> i32,
    pub(crate) send:
        unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const c_void) -> i32,
    pub(crate) send_ex: unsafe extern "system" fn(
        RIO_RQ,
        *const RIO_BUF,
        u32,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        u32,
        *const c_void,
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
        *const c_void,
    ) -> i32,
}

#[derive(Clone, Copy)]
pub(crate) struct RioDispatch {
    create_cq: unsafe extern "system" fn(u32, *const RIO_NOTIFICATION_COMPLETION) -> RIO_CQ,
    create_rq: unsafe extern "system" fn(
        usize,
        u32,
        u32,
        u32,
        u32,
        RIO_CQ,
        RIO_CQ,
        *const c_void,
    ) -> RIO_RQ,
    register_buffer: unsafe extern "system" fn(*const u8, u32) -> RIO_BUFFERID,
    deregister_buffer: unsafe extern "system" fn(RIO_BUFFERID),
    dequeue: unsafe extern "system" fn(RIO_CQ, *mut RIORESULT, u32) -> u32,
    notify: unsafe extern "system" fn(RIO_CQ) -> i32,
    close_cq: unsafe extern "system" fn(RIO_CQ),
    receive: unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const c_void) -> i32,
    send: unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const c_void) -> i32,
    send_ex: unsafe extern "system" fn(
        RIO_RQ,
        *const RIO_BUF,
        u32,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        u32,
        *const c_void,
    ) -> i32,
    receive_ex: unsafe extern "system" fn(
        RIO_RQ,
        *const RIO_BUF,
        u32,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        *const RIO_BUF,
        u32,
        *const c_void,
    ) -> i32,
}

impl RioDispatch {
    #[cfg(test)]
    pub(crate) fn from_functions(functions: RioDispatchFunctions) -> Self {
        Self {
            create_cq: functions.create_cq,
            create_rq: functions.create_rq,
            register_buffer: functions.register_buffer,
            deregister_buffer: functions.deregister_buffer,
            dequeue: functions.dequeue,
            notify: functions.notify,
            close_cq: functions.close_cq,
            receive: functions.receive,
            send: functions.send,
            send_ex: functions.send_ex,
            receive_ex: functions.receive_ex,
        }
    }
}

impl RioProvider for RioDispatch {
    #[inline]
    fn create_cq(
        &self,
        entries: u32,
        notification: &RIO_NOTIFICATION_COMPLETION,
    ) -> RioResult<RioCq> {
        // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
        let cq = unsafe { (self.create_cq)(entries, notification as *const _) };
        if cq == 0 {
            return Err(RioKernel::last_wsa_report(
                RioError::CqCreation,
                "RIOCreateCompletionQueue",
            ));
        }
        Ok(RioCq(cq))
    }

    #[inline]
    fn create_rq(&self, cfg: RioRqConfig) -> RioResult<RioRq> {
        // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
        let rq = unsafe {
            (self.create_rq)(
                cfg.socket,
                cfg.max_outstanding_recvs,
                cfg.max_receive_data_buffers,
                cfg.max_outstanding_sends,
                cfg.max_send_data_buffers,
                cfg.recv_cq.raw(),
                cfg.send_cq.raw(),
                cfg.context,
            )
        };
        if rq == 0 {
            return Err(RioKernel::last_wsa_report(
                RioError::RqCreation,
                "RIOCreateRequestQueue",
            ));
        }
        Ok(RioRq(rq))
    }

    #[inline]
    fn register_buffer(&self, ptr: *const u8, len: u32) -> RioResult<RioBufferId> {
        // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
        let id = unsafe { (self.register_buffer)(ptr, len) };
        if id == 0 {
            return Err(RioKernel::last_wsa_report(
                RioError::BufferRegistration,
                "RIORegisterBuffer",
            ));
        }
        Ok(RioBufferId(id))
    }

    #[inline]
    fn deregister_buffer(&self, id: RioBufferId) {
        if !id.is_invalid() {
            // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
            unsafe { (self.deregister_buffer)(id.0) };
        }
    }

    #[inline]
    fn dequeue(&self, cq: RioCq, results: &mut [RIORESULT]) -> u32 {
        // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
        unsafe { (self.dequeue)(cq.raw(), results.as_mut_ptr(), results.len() as u32) }
    }

    #[inline]
    fn notify(&self, cq: RioCq) -> RioResult<()> {
        // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
        let ret = unsafe { (self.notify)(cq.raw()) };
        if ret == SOCKET_ERROR {
            return Err(RioKernel::last_wsa_report(RioError::Internal, "RIONotify"));
        }
        Ok(())
    }

    #[inline]
    fn close_cq(&self, cq: RioCq) {
        if !cq.is_invalid() {
            // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
            unsafe { (self.close_cq)(cq.raw()) };
        }
    }

    #[inline]
    fn receive(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        num_bufs: u32,
        flags: u32,
        context: *const c_void,
    ) -> RioResult<()> {
        // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
        let ret = unsafe { (self.receive)(rq.0, buf as *const _, num_bufs, flags, context) };
        if ret == 0 {
            return Err(RioKernel::last_wsa_report(RioError::Datapath, "RIOReceive"));
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
        context: *const c_void,
    ) -> RioResult<()> {
        // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
        let ret = unsafe { (self.send)(rq.0, buf as *const _, num_bufs, flags, context) };
        if ret == 0 {
            return Err(RioKernel::last_wsa_report(RioError::Datapath, "RIOSend"));
        }
        Ok(())
    }

    #[inline]
    fn send_ex(&self, cfg: RioExConfig<'_>) -> RioResult<()> {
        // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
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
            return Err(RioKernel::last_wsa_report(RioError::Datapath, "RIOSendEx"));
        }
        Ok(())
    }

    #[inline]
    fn receive_ex(&self, cfg: RioExConfig<'_>) -> RioResult<()> {
        // SAFETY: Startup verifies this function pointer. / 安全性：启动阶段已验证此函数指针。
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
            return Err(RioKernel::last_wsa_report(
                RioError::Datapath,
                "RIOReceiveEx",
            ));
        }
        Ok(())
    }
}

pub(crate) struct RioKernel {
    cq: RioCq,
    _notify_overlapped: Box<Overlapped>,
    dispatch: Option<RioDispatch>,
}

impl RioKernel {
    pub(crate) const fn cq(&self) -> RioCq {
        self.cq
    }

    pub(crate) const fn dispatch_table(&self) -> Option<RioDispatch> {
        self.dispatch
    }

    #[cfg(test)]
    pub(crate) fn set_dispatch_table(&mut self, dispatch: Option<RioDispatch>) {
        self.dispatch = dispatch;
    }

    pub(crate) fn last_wsa_error_code() -> i32 {
        // SAFETY: Winsock exposes this thread-local error accessor without preconditions. / 安全性：Winsock 的线程本地错误读取函数没有额外前置条件。
        unsafe { WSAGetLastError() }
    }

    pub(crate) fn last_wsa_report(context: RioError, scope: &'static str) -> Report<RioError> {
        let code = Self::last_wsa_error_code() as u32;
        context
            .to_report()
            .push_ctx("scope", scope)
            .set_error_code(code)
            .attach_note(
                IocpError::Internal
                    .to_report()
                    .push_ctx("scope", scope)
                    .set_error_code(code as i32)
                    .attach_note("winsock error"),
            )
    }

    pub(crate) fn from_extensions(
        port: BorrowedRawHandle<'_>,
        entries: u32,
        ext: &Extensions,
    ) -> RioResult<Self> {
        let table = &ext.rio_table;
        let dispatch = RioDispatch {
            create_cq: table
                .RIOCreateCompletionQueue
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIOCreateCompletionQueue missing")?,
            create_rq: table
                .RIOCreateRequestQueue
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIOCreateRequestQueue missing")?,
            register_buffer: table
                .RIORegisterBuffer
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIORegisterBuffer missing")?,
            deregister_buffer: table
                .RIODeregisterBuffer
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIODeregisterBuffer missing")?,
            dequeue: table
                .RIODequeueCompletion
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIODequeueCompletion missing")?,
            notify: table
                .RIONotify
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIONotify missing")?,
            close_cq: table
                .RIOCloseCompletionQueue
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIOCloseCompletionQueue missing")?,
            receive: table
                .RIOReceive
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIOReceive missing")?,
            send: table
                .RIOSend
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIOSend missing")?,
            send_ex: table
                .RIOSendEx
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIOSendEx missing")?,
            receive_ex: table
                .RIOReceiveEx
                .ok_or(RioError::LibraryLoad)
                .attach_note("RIOReceiveEx missing")?,
        };
        Self::new(port, entries, dispatch)
    }

    fn new(port: BorrowedRawHandle<'_>, entries: u32, dispatch: RioDispatch) -> RioResult<Self> {
        let mut notify_ov = Box::new(Overlapped::zeroed());
        let notification = RIO_NOTIFICATION_COMPLETION {
            Type: RIO_IOCP_COMPLETION,
            Anonymous: RIO_NOTIFICATION_COMPLETION_0 {
                Iocp: RIO_NOTIFICATION_COMPLETION_0_1 {
                    IocpHandle: port.raw().as_handle(),
                    CompletionKey: RIO_EVENT_KEY as *mut c_void,
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
        registrar: &'a dyn BufferRegistrar,
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
    pub(crate) fn rearm_notify(&self) -> RioResult<()> {
        self.dispatch.map_or(Ok(()), |d| d.notify(self.cq))
    }

    pub(crate) fn submit_receive(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        ctx: *const c_void,
    ) -> RioResult<()> {
        self.dispatch
            .as_ref()
            .ok_or(RioError::Internal)
            .attach_note("RIO dispatch context lost")?
            .receive(rq, buf, 1, 0, ctx)
    }

    pub(crate) fn submit_send(
        &self,
        rq: RioRq,
        buf: &RIO_BUF,
        ctx: *const c_void,
    ) -> RioResult<()> {
        self.dispatch
            .as_ref()
            .ok_or(RioError::Internal)
            .attach_note("RIO dispatch context lost")?
            .send(rq, buf, 1, 0, ctx)
    }

    pub(crate) fn submit_send_ex(
        &self,
        rq: RioRq,
        data_buf: &RIO_BUF,
        addr_buf: &RIO_BUF,
        ctx: *const c_void,
    ) -> RioResult<()> {
        let dispatch = self
            .dispatch
            .as_ref()
            .ok_or(RioError::Internal)
            .attach_note("RIO dispatch context lost")?;
        dispatch.send_ex(RioExConfig {
            rq,
            data_buf,
            data_buf_count: 1,
            local_addr: ptr::null(),
            remote_addr: addr_buf,
            control_buf: ptr::null(),
            flags_buf: ptr::null(),
            flags: 0,
            context: ctx,
        })
    }

    pub(crate) fn submit_receive_multi(
        &self,
        rq: RioRq,
        data_buf: &RIO_BUF,
        addr_buf: &RIO_BUF,
        ctx: *const c_void,
    ) -> RioResult<()> {
        let dispatch = self
            .dispatch
            .as_ref()
            .ok_or(RioError::Internal)
            .attach_note("RIO dispatch context lost")?;
        // Keep flags at zero until deferred submission is verified; zero commits the batch's last request. / 延迟提交能力经验证前保持 flags 为 0；0 表示提交批次中的最后一个请求。
        dispatch.receive_ex(RioExConfig {
            rq,
            data_buf,
            data_buf_count: 1,
            local_addr: ptr::null(),
            remote_addr: addr_buf,
            control_buf: ptr::null(),
            flags_buf: ptr::null(),
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
