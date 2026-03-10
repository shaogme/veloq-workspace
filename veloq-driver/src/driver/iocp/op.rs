//! IOCP Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `IocpKernelOp`: The Type-Erased kernel operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations split into `(KernelOp, UserPayload)`

use crate::SockAddrStorage;
use crate::driver::PlatformOp;
use crate::driver::iocp::IocpDriver;
use crate::driver::iocp::ext::Extensions;
use crate::driver::iocp::rio::RioState;
use crate::driver::iocp::submit::{self, SubmissionResult};
use crate::op::{
    Accept, Close, Connect, Fallocate, Fsync, IntoPlatformOp, IoFd, Open, ReadFixed, Recv,
    Send as OpSend, SendTo, SyncFileRange, Timeout, UdpRecvStream, UdpRefill, Wakeup, WriteFixed,
};
use std::io;
use std::mem::ManuallyDrop;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{SOCKADDR_IN, SOCKADDR_IN6, WSABUF};
use windows_sys::Win32::System::IO::OVERLAPPED;

// ============================================================================
// OverlappedEntry Definition
// ============================================================================

#[repr(C)]
pub struct OverlappedEntry {
    pub inner: OVERLAPPED,
    pub user_data: usize,
    pub generation: u32,
    pub blocking_result: Option<io::Result<usize>>,
}

impl OverlappedEntry {
    pub fn new(user_data: usize) -> Self {
        Self {
            inner: unsafe { std::mem::zeroed() },
            user_data,
            generation: 0,
            blocking_result: None,
        }
    }
}

// ============================================================================
// SubmitContext Definition
// ============================================================================

pub struct SubmitContext<'a> {
    pub port: HANDLE,
    pub overlapped: *mut OVERLAPPED,
    pub ext: &'a Extensions,
    pub registered_files: &'a [Option<HANDLE>],
    pub registrar: &'a dyn veloq_buf::BufferRegistrar,

    // RIO Support
    pub rio: &'a mut RioState,
    pub slots_per_page: usize,
    pub slab_resolver: &'a dyn Fn(usize) -> Option<(*const u8, usize)>,
}

// ============================================================================
// VTable Definition
// ============================================================================

pub type SubmitFn =
    unsafe fn(op: &mut IocpKernelOp, ctx: &mut SubmitContext) -> io::Result<SubmissionResult>;

pub type OnCompleteFn =
    unsafe fn(op: &mut IocpKernelOp, result: usize, ext: &Extensions) -> io::Result<usize>;

pub type DropFn = unsafe fn(op: &mut IocpKernelOp);

pub type GetFdFn = unsafe fn(op: &IocpKernelOp) -> Option<IoFd>;

pub struct OpVTable {
    pub submit: SubmitFn,
    pub on_complete: Option<OnCompleteFn>,
    pub drop: DropFn,
    pub get_fd: GetFdFn,
}

// ============================================================================
// IocpKernelOp Struct & Union (Type-Erased)
// ============================================================================

use std::ptr::NonNull;

#[repr(C)]
pub struct IocpKernelOp {
    /// Virtual Table for dynamic dispatch
    pub vtable: NonNull<OpVTable>,

    /// Public header accessible directly by Driver
    pub header: OverlappedEntry,

    /// Type-erased payload
    pub payload: IocpOpPayload,
}

impl PlatformOp for IocpKernelOp {}

impl IocpKernelOp {
    /// Helper to access the OverlappedEntry (header).
    /// Kept for compatibility with existing Driver code.
    pub fn entry_mut(&mut self) -> Option<&mut OverlappedEntry> {
        Some(&mut self.header)
    }

    pub fn get_fd(&self) -> Option<IoFd> {
        unsafe { (self.vtable.as_ref().get_fd)(self) }
    }
}

impl Drop for IocpKernelOp {
    fn drop(&mut self) {
        unsafe { (self.vtable.as_ref().drop)(self) };
    }
}

// ============================================================================
// Macro Definition
// ============================================================================

macro_rules! define_iocp_ops {
    (
        $(
            $OpType:ident {
                field: $field:ident,
                $(payload: $Payload:ty,)?
                submit: $submit:path,
                $(on_complete: $complete:path,)?
                drop: $drop:path,
                get_fd: $get_fd:path,
                $(construct: $construct:expr,)?
                $(destruct: $destruct:expr,)?
            }
        ),+ $(,)?
    ) => {
        // Ensure proper alignment
        #[repr(C)]
        pub union IocpOpPayload {
            $(
                pub $field: ManuallyDrop< define_iocp_ops!(@payload_type $OpType $(, $Payload)?) >,
            )+
        }

        $(
            impl IntoPlatformOp<IocpDriver> for $OpType {
                type UserPayload = Box<$OpType>;

                fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::UserPayload) {
                    static TABLE: OpVTable = OpVTable {
                        submit: $submit,
                        on_complete: define_iocp_ops!(@optional_complete $($complete)?),
                        drop: $drop,
                        get_fd: $get_fd,
                    };

                    let mut user = Box::new(self);
                    let user_ptr = std::ptr::NonNull::from(user.as_mut());
                    let payload = define_iocp_ops!(@construct user_ptr, $($construct)?, $OpType $(, $Payload)?);

                    let op = IocpKernelOp {
                        vtable: unsafe { NonNull::new_unchecked(&TABLE as *const _ as *mut _) },
                        header: OverlappedEntry::new(0),
                        payload: IocpOpPayload {
                            $field: ManuallyDrop::new(payload),
                        },
                    };
                    (op, user)
                }

                fn from_user_payload(payload: Self::UserPayload) -> Self {
                    define_iocp_ops!(@destruct payload, $($destruct)?)
                }
            }
        )+
    };

    (@payload_type $OpType:ty) => { KernelRef<$OpType> };
    (@payload_type $OpType:ty, $Payload:ty) => { $Payload };

    (@optional_complete) => { None };
    (@optional_complete $fn:path) => { Some($fn) };

    // Default construct: keep only a pointer to user payload
    (@construct $user_ptr:expr, , $OpType:ty) => { KernelRef { user: $user_ptr } };
    // Custom construct
    (@construct $user_ptr:expr, $construct:expr, $OpType:ty, $Payload:ty) => { ($construct)($user_ptr) };

    // Default destruct: return user payload
    (@destruct $user_payload:expr, ) => { *$user_payload };
    // Custom destruct
    (@destruct $user_payload:expr, $destruct:expr) => { ($destruct)($user_payload) };
}

// ============================================================================
// Payload Structures for Complex Ops
// ============================================================================

pub struct KernelRef<T> {
    pub user: NonNull<T>,
}

pub struct AcceptPayload {
    pub user: NonNull<Accept>,
    pub accept_buffer: [u8; 288],
}

pub struct SendToPayload {
    pub user: NonNull<SendTo>,
    pub wsabuf: WSABUF,
    pub addr: SockAddrStorage,
    pub addr_len: i32,
}

pub struct OpenPayload {
    pub user: NonNull<Open>,
}

pub struct WakeupPayload {
    pub user: NonNull<Wakeup>,
}

pub type IocpOp = IocpKernelOp;

// ============================================================================
// Op Definitions
// ============================================================================

define_iocp_ops! {
    ReadFixed {
        field: read,
        submit: submit::submit_read_fixed,
        drop: submit::drop_read_fixed,
        get_fd: submit::get_fd_read_fixed,
    },
    WriteFixed {
        field: write,
        submit: submit::submit_write_fixed,
        drop: submit::drop_write_fixed,
        get_fd: submit::get_fd_write_fixed,
    },
    Recv {
        field: recv,
        submit: submit::submit_recv,
        drop: submit::drop_recv,
        get_fd: submit::get_fd_recv,
    },
    OpSend {
        field: send,
        submit: submit::submit_send,
        drop: submit::drop_send,
        get_fd: submit::get_fd_send,
    },
    Close {
        field: close,
        submit: submit::submit_close,
        drop: submit::drop_close,
        get_fd: submit::get_fd_close,
    },
    Fsync {
        field: fsync,
        submit: submit::submit_fsync,
        drop: submit::drop_fsync,
        get_fd: submit::get_fd_fsync,
    },
    SyncFileRange {
        field: sync_range,
        submit: submit::submit_sync_range,
        drop: submit::drop_sync_range,
        get_fd: submit::get_fd_sync_range,
    },
    Fallocate {
        field: fallocate,
        submit: submit::submit_fallocate,
        drop: submit::drop_fallocate,
        get_fd: submit::get_fd_fallocate,
    },
    Timeout {
        field: timeout,
        submit: submit::submit_timeout,
        drop: submit::drop_timeout,
        get_fd: submit::get_fd_timeout,
    },
    Connect {
        field: connect,
        submit: submit::submit_connect,
        on_complete: submit::on_complete_connect,
        drop: submit::drop_connect,
        get_fd: submit::get_fd_connect,
    },
    Accept {
        field: accept,
        payload: AcceptPayload,
        submit: submit::submit_accept,
        on_complete: submit::on_complete_accept,
        drop: submit::drop_accept,
        get_fd: submit::get_fd_accept,
        construct: |user| AcceptPayload {
            user,
            accept_buffer: [0; 288],
        },
        destruct: |user: Box<Accept>| *user,
    },
    SendTo {
        field: send_to,
        payload: SendToPayload,
        submit: submit::submit_send_to,
        drop: submit::drop_send_to,
        get_fd: submit::get_fd_send_to,
        construct: |user: std::ptr::NonNull<SendTo>| {
            let op = unsafe { user.as_ref() };
            let (addr, raw_addr_len) = crate::socket_addr_to_storage(op.addr);
            let addr_len = if op.addr.is_ipv4() {
                std::mem::size_of::<SOCKADDR_IN>() as i32
            } else {
                std::mem::size_of::<SOCKADDR_IN6>() as i32
            };
            debug_assert_eq!(raw_addr_len, addr_len);
            let wsabuf = WSABUF {
                len: op.buf.len() as u32,
                buf: op.buf.as_slice().as_ptr() as *mut u8,
            };
            SendToPayload {
                user,
                wsabuf,
                addr,
                addr_len,
            }
        },
        destruct: |user: Box<SendTo>| *user,
    },
    UdpRecvStream {
        field: udp_recv_stream,
        submit: submit::submit_udp_recv_stream,
        on_complete: submit::on_complete_udp_recv_stream,
        drop: submit::drop_udp_recv_stream,
        get_fd: submit::get_fd_udp_recv_stream,
    },
    UdpRefill {
        field: udp_refill,
        submit: submit::submit_udp_refill,
        drop: submit::drop_udp_refill,
        get_fd: submit::get_fd_udp_refill,
    },
    Open {
        field: open,
        payload: OpenPayload,
        submit: submit::submit_open,
        drop: submit::drop_open,
        get_fd: submit::get_fd_open,
        construct: |user| OpenPayload { user },
        destruct: |user: Box<Open>| *user,
    },
    Wakeup {
        field: wakeup,
        payload: WakeupPayload,
        submit: submit::submit_wakeup,
        drop: submit::drop_wakeup,
        get_fd: submit::get_fd_wakeup,
        construct: |user| WakeupPayload { user },
        destruct: |user: Box<Wakeup>| *user,
    },
}
