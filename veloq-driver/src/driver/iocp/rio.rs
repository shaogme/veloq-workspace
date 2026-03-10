pub mod pool;
pub mod registry;

use crate::driver::iocp::IocpOp;
use crate::driver::iocp::error::{IocpErrorContext, io_error, io_msg};
use crate::driver::iocp::ext::Extensions;
use crate::driver::iocp::{IocpOpState, OpLifecycle};
use crate::driver::op_registry::OpRegistry;
use crate::driver::slot::{OverlappedEntry, STATE_COMPLETED, STATE_CONSUMED};
use crate::op::IoFd;
use rustc_hash::FxHashMap;
use std::io;
use std::sync::OnceLock;
use std::sync::atomic::Ordering;
use veloq_buf::FixedBuf;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    RIO_BUF, RIO_BUFFERID, RIO_CORRUPT_CQ, RIO_CQ, RIO_IOCP_COMPLETION,
    RIO_NOTIFICATION_COMPLETION, RIO_RQ, RIORESULT, SOCKET_ERROR, WSAGetLastError,
};
use windows_sys::Win32::System::IO::OVERLAPPED;

use self::pool::{POOL_CTX_TAG, UdpPoolManager};
use self::registry::RioRegistry;

const RIO_REAPER_DRAIN_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

#[derive(Clone, Copy)]
pub struct RioEnv<'a> {
    pub registrar: &'a dyn veloq_buf::BufferRegistrar,
    pub dispatch: &'a RioDispatch,
    pub cq: RIO_CQ,
}

pub struct RioContext<'a> {
    pub registry: &'a mut RioRegistry,
    pub env: RioEnv<'a>,
}

#[derive(Clone, Copy)]
enum RioCompletionKind {
    Pool { actor_id: u32, generation: u32 },
    Op { user_data: usize, generation: u32 },
}

struct RioSocketActor {
    actor_id: u32,
    rq: RIO_RQ,
    pool_manager: UdpPoolManager,
}

impl RioSocketActor {
    fn new(actor_id: u32, rq: RIO_RQ) -> Self {
        Self {
            actor_id,
            rq,
            pool_manager: UdpPoolManager::new(),
        }
    }
}

pub struct RioSendToArgs<'a> {
    pub fd: IoFd,
    pub handle: HANDLE,
    pub buf: &'a veloq_buf::FixedBuf,
    pub addr_ptr: *const std::ffi::c_void,
    pub addr_len: i32,
    pub overlapped: *mut OVERLAPPED,
    pub page_idx: usize,
}

pub struct RioRecvFromArgs<'a> {
    pub fd: IoFd,
    pub handle: HANDLE,
    pub buf: &'a mut veloq_buf::FixedBuf,
    pub addr_ptr: *const std::ffi::c_void,
    pub len_ptr: *const i32,
    pub overlapped: *mut OVERLAPPED,
    pub page_idx: usize,
}

pub struct RioUdpStreamArgs<'a> {
    pub fd: IoFd,
    pub handle: HANDLE,
    pub stream_op: &'a mut crate::op::UdpRecvStream,
    pub user_data: usize,
    pub generation: u32,
}

#[derive(Clone, Copy)]
pub(crate) struct RioDispatch {
    pub create_cq: unsafe extern "system" fn(u32, *const RIO_NOTIFICATION_COMPLETION) -> RIO_CQ,
    pub create_rq: unsafe extern "system" fn(
        usize,
        u32,
        u32,
        u32,
        u32,
        RIO_CQ,
        RIO_CQ,
        *const std::ffi::c_void,
    ) -> RIO_RQ,
    pub register_buffer: unsafe extern "system" fn(*const u8, u32) -> RIO_BUFFERID,
    pub deregister_buffer: unsafe extern "system" fn(RIO_BUFFERID),
    pub dequeue: unsafe extern "system" fn(RIO_CQ, *mut RIORESULT, u32) -> u32,
    pub notify: unsafe extern "system" fn(RIO_CQ) -> i32,
    pub close_cq: unsafe extern "system" fn(RIO_CQ),
    pub receive:
        unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const std::ffi::c_void) -> i32,
    pub send:
        unsafe extern "system" fn(RIO_RQ, *const RIO_BUF, u32, u32, *const std::ffi::c_void) -> i32,
    pub send_ex: unsafe extern "system" fn(
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
    pub receive_ex: unsafe extern "system" fn(
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

pub struct RioState {
    pub(crate) kernel: RioKernel,
    pub(crate) registry: RioRegistry,
    actors: FxHashMap<HANDLE, RioSocketActor>,
    actor_routes: FxHashMap<u32, HANDLE>,
    next_actor_id: u32,
    pub(crate) outstanding_count: usize,
}

pub(crate) struct RioKernel {
    cq: RIO_CQ,
    _notify_overlapped: Box<OVERLAPPED>,
    dispatch: RioDispatch,
}
struct DeferredRioCleanup {
    kernel: RioKernel,
    registry: RioRegistry,
    actors: FxHashMap<HANDLE, RioSocketActor>,
    actor_routes: FxHashMap<u32, HANDLE>,
    outstanding_count: usize,
}
// Safety: deferred cleanup task is transferred by ownership to a single reaper thread.
unsafe impl Send for DeferredRioCleanup {}

impl DeferredRioCleanup {
    fn run(self) {
        let mut state = RioState {
            kernel: self.kernel,
            registry: self.registry,
            actors: self.actors,
            actor_routes: self.actor_routes,
            next_actor_id: 1,
            outstanding_count: self.outstanding_count,
        };
        state.begin_shutdown();
        if let Err(e) = state.drain_outstanding_for(RIO_REAPER_DRAIN_TIMEOUT) {
            tracing::warn!(error = ?e, "RioReaper: background drain timed out");
        }
        state.finalize_shutdown_cleanup();
    }
}

fn reaper_sender() -> &'static std::sync::mpsc::Sender<DeferredRioCleanup> {
    static SENDER: OnceLock<std::sync::mpsc::Sender<DeferredRioCleanup>> = OnceLock::new();
    SENDER.get_or_init(|| {
        let (tx, rx) = std::sync::mpsc::channel::<DeferredRioCleanup>();
        std::thread::Builder::new()
            .name("veloq-rio-reaper".to_string())
            .spawn(move || {
                while let Ok(task) = rx.recv() {
                    task.run();
                }
            })
            .expect("failed to spawn veloq-rio-reaper");
        tx
    })
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
    fn from_extensions(port: HANDLE, entries: u32, ext: &Extensions) -> io::Result<Self> {
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

    fn noop() -> Self {
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
    fn env<'a>(&'a self, registrar: &'a dyn veloq_buf::BufferRegistrar) -> RioEnv<'a> {
        RioEnv {
            registrar,
            dispatch: &self.dispatch,
            cq: self.cq,
        }
    }

    #[inline]
    fn dequeue(&self, results: *mut RIORESULT, len: u32) -> u32 {
        unsafe { (self.dispatch.dequeue)(self.cq, results, len) }
    }

    #[inline]
    fn rearm_notify(&self) -> io::Result<()> {
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
    fn submit_receive(
        &self,
        rq: RIO_RQ,
        buf: &RIO_BUF,
        request_context: *const std::ffi::c_void,
    ) -> i32 {
        unsafe { (self.dispatch.receive)(rq, buf, 1, 0, request_context) }
    }

    #[inline]
    fn submit_send(
        &self,
        rq: RIO_RQ,
        buf: &RIO_BUF,
        request_context: *const std::ffi::c_void,
    ) -> i32 {
        unsafe { (self.dispatch.send)(rq, buf, 1, 0, request_context) }
    }

    #[inline]
    fn submit_send_ex(
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
    fn submit_receive_ex(
        &self,
        rq: RIO_RQ,
        data_buf: &RIO_BUF,
        addr_buf: &RIO_BUF,
        request_context: *const std::ffi::c_void,
    ) -> i32 {
        unsafe {
            (self.dispatch.receive_ex)(
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
    fn close(&mut self) {
        if self.cq != 0 {
            unsafe { (self.dispatch.close_cq)(self.cq) };
            self.cq = 0;
        }
    }
}

struct RioCompletionRouter<'a> {
    ops: &'a mut OpRegistry<IocpOp, IocpOpState>,
    registry: &'a mut RioRegistry,
    actors: &'a mut FxHashMap<HANDLE, RioSocketActor>,
    actor_routes: &'a mut FxHashMap<u32, HANDLE>,
    env: RioEnv<'a>,
    outstanding_count: &'a mut usize,
    completed_count: usize,
}

impl<'a> RioCompletionRouter<'a> {
    fn new(
        ops: &'a mut OpRegistry<IocpOp, IocpOpState>,
        registry: &'a mut RioRegistry,
        actors: &'a mut FxHashMap<HANDLE, RioSocketActor>,
        actor_routes: &'a mut FxHashMap<u32, HANDLE>,
        env: RioEnv<'a>,
        outstanding_count: &'a mut usize,
    ) -> Self {
        Self {
            ops,
            registry,
            actors,
            actor_routes,
            env,
            outstanding_count,
            completed_count: 0,
        }
    }

    fn handle_one(&mut self, res: &RIORESULT) {
        let Some(kind) = RioState::decode_request_context(res.RequestContext) else {
            return;
        };

        let mut consume_outstanding = || {
            *self.outstanding_count = self.outstanding_count.saturating_sub(1);
            self.completed_count += 1;
        };

        match kind {
            RioCompletionKind::Pool {
                actor_id,
                generation,
            } => {
                let Some(&handle) = self.actor_routes.get(&actor_id) else {
                    consume_outstanding();
                    return;
                };
                let (pool_submissions, remove_actor) = {
                    let Some(actor) = self.actors.get_mut(&handle) else {
                        consume_outstanding();
                        return;
                    };
                    let Some(slot_idx) = actor.pool_manager.ack_udp_pool_completion(generation)
                    else {
                        consume_outstanding();
                        return;
                    };
                    let mut ctx = RioContext {
                        registry: self.registry,
                        env: self.env,
                    };
                    let submissions = actor.pool_manager.handle_completion(
                        self.ops,
                        actor.rq,
                        actor.actor_id,
                        slot_idx,
                        res,
                        &mut ctx,
                    );
                    let remove = actor
                        .pool_manager
                        .cleanup_shutdown_udp_pool_if_drained(&mut ctx);
                    (submissions, remove)
                };
                if remove_actor {
                    self.actors.remove(&handle);
                    self.actor_routes.remove(&actor_id);
                }
                consume_outstanding();
                *self.outstanding_count += pool_submissions;
            }
            RioCompletionKind::Op {
                user_data,
                generation,
            } => {
                if user_data >= self.ops.local.len() {
                    consume_outstanding();
                    return;
                }

                let op = &mut self.ops.local[user_data];
                let slot = &self.ops.shared.slots[user_data];
                if op.platform_data.generation != generation {
                    consume_outstanding();
                    return;
                }

                if matches!(op.platform_data.lifecycle, OpLifecycle::InFlight) {
                    let result = if res.Status == 0 {
                        Ok(res.BytesTransferred as usize)
                    } else {
                        Err(io::Error::from_raw_os_error(res.Status))
                    };
                    op.platform_data.lifecycle = OpLifecycle::Completed(result);

                    let result_for_slot = if res.Status == 0 {
                        Ok(res.BytesTransferred as usize)
                    } else {
                        Err(io::Error::from_raw_os_error(res.Status))
                    };
                    unsafe { *slot.result.get() = Some(result_for_slot) };
                    slot.state.store(STATE_COMPLETED, Ordering::Release);
                    slot.waker.wake();
                } else if matches!(op.platform_data.lifecycle, OpLifecycle::Cancelled) {
                    if op.platform_data.rio_needs_drain {
                        op.platform_data.rio_drained = true;
                        if slot.state.load(Ordering::Acquire) == STATE_CONSUMED {
                            let _ = std::mem::take(&mut op.platform_data);
                            self.ops.free_indices.push(user_data);
                        }
                    } else {
                        let _ = std::mem::take(&mut op.platform_data);
                        self.ops.free_indices.push(user_data);
                    }
                }

                consume_outstanding();
            }
        }
    }
}

impl RioState {
    #[inline]
    fn encode_request_context(overlapped: *mut OVERLAPPED) -> *const std::ffi::c_void {
        overlapped as *const std::ffi::c_void
    }

    #[inline]
    fn decode_request_context(ctx: u64) -> Option<RioCompletionKind> {
        if ctx == 0 {
            return None;
        }
        let raw = ctx as usize;
        if (raw & POOL_CTX_TAG) == POOL_CTX_TAG {
            let token = ((raw >> 1) & 0xffff_ffff) as u32;
            let actor_id = ((raw >> 33) & 0xffff_ffff) as u32;
            if token == 0 || actor_id == 0 {
                return None;
            }
            return Some(RioCompletionKind::Pool {
                actor_id,
                generation: token,
            });
        }
        let entry = ctx as usize as *const OverlappedEntry;
        let user_data = unsafe { (*entry).user_data };
        let generation = unsafe { (*entry).generation };
        Some(RioCompletionKind::Op {
            user_data,
            generation,
        })
    }

    #[inline]
    fn decode_pool_context(ctx: u64) -> Option<(u32, u32)> {
        if ctx == 0 {
            return None;
        }
        let raw = ctx as usize;
        if (raw & POOL_CTX_TAG) != POOL_CTX_TAG {
            return None;
        }
        let token = ((raw >> 1) & 0xffff_ffff) as u32;
        let actor_id = ((raw >> 33) & 0xffff_ffff) as u32;
        if token == 0 || actor_id == 0 {
            return None;
        }
        Some((actor_id, token))
    }

    fn last_wsa_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe { WSAGetLastError() })
    }

    #[inline]
    fn build_ctx<'a>(registry: &'a mut RioRegistry, env: RioEnv<'a>) -> RioContext<'a> {
        RioContext { registry, env }
    }

    fn alloc_actor_id(&mut self) -> u32 {
        loop {
            let id = self.next_actor_id;
            self.next_actor_id = self.next_actor_id.wrapping_add(1);
            if id == 0 {
                continue;
            }
            if !self.actor_routes.contains_key(&id) {
                return id;
            }
        }
    }

    fn ensure_actor(
        &mut self,
        target: (IoFd, HANDLE),
        env: RioEnv<'_>,
    ) -> io::Result<&mut RioSocketActor> {
        let (fd, handle) = target;
        if !self.actors.contains_key(&handle) {
            let rq = self.registry.create_rq((handle, fd), env)?;
            let actor_id = self.alloc_actor_id();
            self.actor_routes.insert(actor_id, handle);
            self.actors
                .insert(handle, RioSocketActor::new(actor_id, rq));
        }
        Ok(self.actors.get_mut(&handle).expect("actor inserted"))
    }

    pub fn new(port: HANDLE, entries: u32, ext: &Extensions) -> io::Result<Self> {
        let kernel = RioKernel::from_extensions(port, entries, ext)?;

        let rq_depth = entries.clamp(32, 256);

        Ok(Self {
            kernel,
            registry: RioRegistry::new(rq_depth),
            actors: FxHashMap::default(),
            actor_routes: FxHashMap::default(),
            next_actor_id: 1,
            outstanding_count: 0,
        })
    }

    pub fn resize_registered_rqs(&mut self, size: usize) {
        self.registry.resize_registered_rqs(size);
    }

    pub fn clear_registered_rq(&mut self, idx: usize) {
        self.registry.clear_registered_rq(idx);
    }

    pub fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()> {
        let env = self.kernel.env(&veloq_buf::NoopRegistrar);
        self.registry.register_chunk(id, (ptr, len), env)
    }

    pub fn begin_udp_pool_shutdown_for_handle(&mut self, handle: HANDLE) {
        let env = self.kernel.env(&veloq_buf::NoopRegistrar);
        let mut ctx = Self::build_ctx(&mut self.registry, env);
        let mut remove_actor = None;
        if let Some(actor) = self.actors.get_mut(&handle) {
            actor.pool_manager.begin_udp_pool_shutdown();
            if actor
                .pool_manager
                .cleanup_shutdown_udp_pool_if_drained(&mut ctx)
            {
                remove_actor = Some(actor.actor_id);
            }
        }
        if let Some(actor_id) = remove_actor {
            self.actors.remove(&handle);
            self.actor_routes.remove(&actor_id);
        }
    }

    pub fn begin_shutdown(&mut self) {
        for actor in self.actors.values_mut() {
            actor.pool_manager.begin_udp_pool_shutdown();
        }
    }

    pub fn drain_outstanding_for(&mut self, timeout: std::time::Duration) -> io::Result<()> {
        let start = std::time::Instant::now();
        while self.outstanding_count > 0 {
            if start.elapsed() >= timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    format!(
                        "strict close timed out while draining RIO outstanding requests: {}",
                        self.outstanding_count
                    ),
                ));
            }

            const MAX_RESULTS: usize = 128;
            let mut results: [RIORESULT; MAX_RESULTS] = unsafe { std::mem::zeroed() };
            let count = self
                .kernel
                .dequeue(results.as_mut_ptr(), MAX_RESULTS as u32);

            if count == RIO_CORRUPT_CQ {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    "RIO completion queue is corrupt (RIO_CORRUPT_CQ)",
                ));
            }

            if count == 0 {
                std::thread::yield_now();
                continue;
            }

            for res in results.iter().take(count as usize) {
                if let Some((actor_id, completion_generation)) =
                    Self::decode_pool_context(res.RequestContext)
                    && let Some(handle) = self.actor_routes.get(&actor_id).copied()
                    && let Some(actor) = self.actors.get_mut(&handle)
                {
                    let _ = actor
                        .pool_manager
                        .ack_udp_pool_completion(completion_generation);
                    actor.pool_manager.handle_completion_drain_only();
                    let env = self.kernel.env(&veloq_buf::NoopRegistrar);
                    let mut ctx = Self::build_ctx(&mut self.registry, env);
                    if actor
                        .pool_manager
                        .cleanup_shutdown_udp_pool_if_drained(&mut ctx)
                    {
                        self.actor_routes.remove(&actor_id);
                        self.actors.remove(&handle);
                    }
                }
                self.outstanding_count = self.outstanding_count.saturating_sub(1);
            }
        }

        Ok(())
    }

    fn finalize_shutdown_cleanup(&mut self) {
        for actor in self.actors.values_mut() {
            actor.pool_manager.udp_ctx_map.clear();
        }
        let env = self.kernel.env(&veloq_buf::NoopRegistrar);
        let mut ctx = Self::build_ctx(&mut self.registry, env);
        for actor in self.actors.values_mut() {
            actor
                .pool_manager
                .forget_in_flight_and_deregister_rest(&mut ctx);
        }
        self.actors.clear();
        self.actor_routes.clear();
        let _ = ctx;
        let env = self.kernel.env(&veloq_buf::NoopRegistrar);
        self.registry.cleanup_deregister(env);
        self.kernel.close();
    }

    fn take_deferred_cleanup(&mut self) -> Option<DeferredRioCleanup> {
        if self.kernel.cq == 0 {
            return None;
        }
        let kernel = std::mem::replace(&mut self.kernel, RioKernel::noop());
        let registry = std::mem::replace(&mut self.registry, RioRegistry::new(32));
        Some(DeferredRioCleanup {
            kernel,
            registry,
            actors: std::mem::take(&mut self.actors),
            actor_routes: std::mem::take(&mut self.actor_routes),
            outstanding_count: std::mem::take(&mut self.outstanding_count),
        })
    }

    pub fn cancel_udp_recv_waiter(
        &mut self,
        handle: HANDLE,
        uid: (usize, u32),
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) {
        let env = self.kernel.env(registrar);
        let mut ctx = Self::build_ctx(&mut self.registry, env);
        if let Some(actor) = self.actors.get_mut(&handle) {
            actor
                .pool_manager
                .cancel_udp_recv_waiter(uid, actor.rq, actor.actor_id, &mut ctx);
        }
    }

    pub fn process_completions(
        &mut self,
        ops: &mut OpRegistry<IocpOp, IocpOpState>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<usize> {
        const MAX_RIO_RESULTS: usize = 128;
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { std::mem::zeroed() };
        let env = self.kernel.env(registrar);
        let mut router = RioCompletionRouter::new(
            ops,
            &mut self.registry,
            &mut self.actors,
            &mut self.actor_routes,
            env,
            &mut self.outstanding_count,
        );

        loop {
            let count = self
                .kernel
                .dequeue(results.as_mut_ptr(), MAX_RIO_RESULTS as u32);

            if count == RIO_CORRUPT_CQ {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    "RIO completion queue is corrupt (RIO_CORRUPT_CQ)",
                ));
            }
            if count == 0 {
                break;
            }

            for res in results.iter().take(count as usize) {
                router.handle_one(res);
            }

            if count < MAX_RIO_RESULTS as u32 {
                break;
            }
        }

        self.kernel.rearm_notify()?;
        Ok(router.completed_count)
    }

    pub fn try_submit_recv(
        &mut self,
        target: (IoFd, HANDLE, *mut OVERLAPPED),
        buf: &mut veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let (fd, handle, overlapped) = target;
        let dispatch = self.kernel.dispatch;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let rio_buf =
            self.registry
                .prepare_data_submission((fd, handle), buf, buf.capacity() as u32, env)?;
        let request_context = Self::encode_request_context(overlapped);
        let ret = self.kernel.submit_receive(rq, &rio_buf, request_context);
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOReceive submission failed: fd={fd:?}, handle={handle:?}"),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub fn try_submit_send(
        &mut self,
        target: (IoFd, HANDLE, *mut OVERLAPPED),
        buf: &veloq_buf::FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let (fd, handle, overlapped) = target;
        let dispatch = self.kernel.dispatch;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let rio_buf =
            self.registry
                .prepare_data_submission((fd, handle), buf, buf.len() as u32, env)?;
        let request_context = Self::encode_request_context(overlapped);
        let ret = self.kernel.submit_send(rq, &rio_buf, request_context);
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!("RIOSend submission failed: fd={fd:?}, handle={handle:?}"),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub fn try_submit_send_to(
        &mut self,
        args: RioSendToArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        use windows_sys::Win32::Networking::WinSock::{
            AF_INET, AF_INET6, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
        };
        let RioSendToArgs {
            fd,
            handle,
            buf,
            addr_ptr,
            addr_len,
            overlapped,
            page_idx,
        } = args;

        let dispatch = self.kernel.dispatch;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let data_buf =
            self.registry
                .prepare_data_submission((fd, handle), buf, buf.len() as u32, env)?;
        self.registry
            .ensure_slab_page_registration(page_idx, slab_resolver, env)?;
        let (addr_buf_id, base_addr, slab_len) = self.registry.slab_rio_pages[page_idx].unwrap();

        if addr_ptr.is_null() {
            return Err(io_msg(
                IocpErrorContext::Rio,
                "RIO send_to received null remote address pointer",
            ));
        }
        let family = unsafe { (*(addr_ptr as *const SOCKADDR)).sa_family };
        let min_addr_len = match family {
            AF_INET => std::mem::size_of::<SOCKADDR_IN>(),
            AF_INET6 => std::mem::size_of::<SOCKADDR_IN6>(),
            _ => {
                return Err(io_msg(
                    IocpErrorContext::Rio,
                    format!("RIO send_to unsupported address family: family={family}"),
                ));
            }
        };
        if (addr_len as usize) < min_addr_len {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO send_to invalid address length: addr_len={}, min_required={}, family={}",
                    addr_len, min_addr_len, family
                ),
            ));
        }

        let rio_addr_len = std::mem::size_of::<SOCKADDR_INET>();
        let addr_addr = addr_ptr as usize;
        let slab_end = base_addr.saturating_add(slab_len);
        if addr_addr < base_addr || addr_addr >= slab_end {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO send_to address pointer is outside registered slab: page_idx={}, addr_ptr=0x{:x}, slab_base=0x{:x}, slab_len={}, slab_end=0x{:x}",
                    page_idx, addr_addr, base_addr, slab_len, slab_end
                ),
            ));
        }
        let addr_end = addr_addr.saturating_add(rio_addr_len);
        if addr_end > slab_end {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO send_to address range exceeds registered slab: page_idx={}, addr_ptr=0x{:x}, addr_len={}, addr_end=0x{:x}, slab_end=0x{:x}",
                    page_idx, addr_addr, rio_addr_len, addr_end, slab_end
                ),
            ));
        }

        let addr_offset = (addr_addr - base_addr) as u32;
        let addr_buf = RIO_BUF {
            BufferId: addr_buf_id,
            Offset: addr_offset,
            Length: rio_addr_len as u32,
        };
        let request_context = Self::encode_request_context(overlapped);

        let ret = self
            .kernel
            .submit_send_ex(rq, &data_buf, &addr_buf, request_context);
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOSendEx submission failed: fd={fd:?}, handle={handle:?}, page_idx={}, rq=0x{:x}, data_buf_id=0x{:x}, data_off={}, data_len={}, addr_buf_id=0x{:x}, addr_off={}, addr_len={}, addr_ptr=0x{:x}, slab_base=0x{:x}, slab_len={}",
                    page_idx,
                    rq as usize,
                    data_buf.BufferId as usize,
                    data_buf.Offset,
                    data_buf.Length,
                    addr_buf.BufferId as usize,
                    addr_buf.Offset,
                    addr_buf.Length,
                    addr_addr,
                    base_addr,
                    slab_len
                ),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub fn try_submit_recv_from(
        &mut self,
        args: RioRecvFromArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
        slab_resolver: &dyn Fn(usize) -> Option<(*const u8, usize)>,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let RioRecvFromArgs {
            fd,
            handle,
            buf,
            addr_ptr,
            len_ptr: _len_ptr,
            overlapped,
            page_idx,
        } = args;
        let dispatch = self.kernel.dispatch;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
        };
        let rq = self.ensure_actor((fd, handle), env)?.rq;
        let data_buf =
            self.registry
                .prepare_data_submission((fd, handle), buf, buf.capacity() as u32, env)?;
        self.registry
            .ensure_slab_page_registration(page_idx, slab_resolver, env)?;
        let (addr_buf_id, base_addr, slab_len) = self.registry.slab_rio_pages[page_idx].unwrap();

        let addr_addr = addr_ptr as usize;
        let slab_end = base_addr.saturating_add(slab_len);
        let addr_len = std::mem::size_of::<crate::SockAddrStorage>();
        if addr_addr < base_addr || addr_addr >= slab_end {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO recv_from address pointer is outside registered slab: page_idx={}, addr_ptr=0x{:x}, slab_base=0x{:x}, slab_len={}, slab_end=0x{:x}",
                    page_idx, addr_addr, base_addr, slab_len, slab_end
                ),
            ));
        }
        let addr_end = addr_addr.saturating_add(addr_len);
        if addr_end > slab_end {
            return Err(io_msg(
                IocpErrorContext::Rio,
                format!(
                    "RIO recv_from address range exceeds registered slab: page_idx={}, addr_ptr=0x{:x}, addr_len={}, addr_end=0x{:x}, slab_end=0x{:x}",
                    page_idx, addr_addr, addr_len, addr_end, slab_end
                ),
            ));
        }

        let addr_offset = (addr_addr - base_addr) as u32;
        let addr_buf = RIO_BUF {
            BufferId: addr_buf_id,
            Offset: addr_offset,
            Length: addr_len as u32,
        };
        let request_context = Self::encode_request_context(overlapped);

        let ret = self
            .kernel
            .submit_receive_ex(rq, &data_buf, &addr_buf, request_context);
        if ret == 0 {
            return Err(io_error(
                IocpErrorContext::Rio,
                Self::last_wsa_error(),
                format!(
                    "RIOReceiveEx submission failed: fd={fd:?}, handle={handle:?}, page_idx={}, rq=0x{:x}, data_buf_id=0x{:x}, data_off={}, data_len={}, addr_buf_id=0x{:x}, addr_off={}, addr_len={}, addr_ptr=0x{:x}, slab_base=0x{:x}, slab_len={}",
                    page_idx,
                    rq as usize,
                    data_buf.BufferId as usize,
                    data_buf.Offset,
                    data_buf.Length,
                    addr_buf.BufferId as usize,
                    addr_buf.Offset,
                    addr_buf.Length,
                    addr_addr,
                    base_addr,
                    slab_len
                ),
            ));
        }
        self.outstanding_count += 1;
        Ok(SubmissionResult::Pending)
    }

    pub fn try_submit_udp_recv_stream_pooled(
        &mut self,
        args: RioUdpStreamArgs<'_>,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<crate::driver::iocp::submit::SubmissionResult> {
        use crate::driver::iocp::submit::SubmissionResult;
        let RioUdpStreamArgs {
            fd,
            handle,
            stream_op,
            user_data,
            generation,
        } = args;
        let dispatch = self.kernel.dispatch;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
        };
        let _ = self.ensure_actor((fd, handle), env)?;
        let (registry, actors) = (&mut self.registry, &mut self.actors);
        let actor = actors.get_mut(&handle).expect("actor exists");
        let mut ctx = Self::build_ctx(registry, env);
        let (res, pool_submissions) = actor.pool_manager.try_submit_udp_recv_stream_pooled(
            actor.rq,
            actor.actor_id,
            stream_op,
            (user_data, generation),
            &mut ctx,
        )?;
        self.outstanding_count += pool_submissions;
        if matches!(res, SubmissionResult::Pending) {
            self.outstanding_count += 1;
        }
        Ok(res)
    }

    pub fn try_refill_udp_pool(
        &mut self,
        target: (IoFd, HANDLE),
        buf: FixedBuf,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<()> {
        let dispatch = self.kernel.dispatch;
        let env = RioEnv {
            registrar,
            dispatch: &dispatch,
            cq: self.kernel.cq,
        };
        let (fd, handle) = target;
        let _ = self.ensure_actor((fd, handle), env)?;
        let (registry, actors) = (&mut self.registry, &mut self.actors);
        let actor = actors.get_mut(&handle).expect("actor exists");
        let mut ctx = Self::build_ctx(registry, env);
        let pool_submissions =
            actor
                .pool_manager
                .try_refill_udp_pool(actor.rq, actor.actor_id, buf, &mut ctx)?;
        self.outstanding_count += pool_submissions;
        Ok(())
    }

    #[cfg(test)]
    pub(crate) fn udp_pool_debug_stats(
        &self,
        handle: HANDLE,
    ) -> Option<pool::UdpRecvPoolDebugStats> {
        self.actors
            .get(&handle)
            .and_then(|actor| actor.pool_manager.udp_pool_debug_stats())
    }

    #[cfg(test)]
    pub(crate) fn debug_tick_udp_pool_idle(
        &mut self,
        handle: HANDLE,
        ticks: usize,
        registrar: &dyn veloq_buf::BufferRegistrar,
    ) -> io::Result<()> {
        let env = self.kernel.env(registrar);
        let mut ctx = Self::build_ctx(&mut self.registry, env);
        if let Some(actor) = self.actors.get_mut(&handle) {
            for _ in 0..ticks {
                actor
                    .pool_manager
                    .rebalance_udp_pool(actor.rq, actor.actor_id, &mut ctx)?;
            }
        }
        Ok(())
    }
}

impl Drop for RioState {
    fn drop(&mut self) {
        self.begin_shutdown();
        if self.outstanding_count == 0 {
            self.finalize_shutdown_cleanup();
            return;
        }

        if let Some(task) = self.take_deferred_cleanup() {
            let tx = reaper_sender();
            if let Err(err) = tx.send(task) {
                tracing::warn!("RioReaper unavailable, falling back to inline cleanup");
                err.0.run();
            }
            return;
        }

        self.finalize_shutdown_cleanup();
    }
}
