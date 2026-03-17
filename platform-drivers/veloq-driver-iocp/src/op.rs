//! IOCP Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `IocpKernelOp`: The Type-Erased kernel operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations split into `(KernelOp, UserPayload)`

use std::io;
use std::mem::ManuallyDrop;
use std::ptr::NonNull;

use crate::SockAddrStorage;
use crate::ext::Extensions;
use crate::rio::RioState;
use crate::submit::{self, SubmissionResult};
use crate::{IoFd, RawHandle};

use veloq_driver_core::driver::PlatformOp;
use veloq_driver_core::op::{
    Accept as AcceptBase, Close as CloseBase, Connect as ConnectBase, Fallocate as FallocateBase,
    Fsync as FsyncBase, IntoPlatformOp, OpKind, Open, ReadFixed as ReadFixedBase, Recv as RecvBase,
    Send as OpSendBase, SendTo as SendToBase, SyncFileRange as SyncFileRangeBase, Timeout,
    UdpRecvStream as UdpRecvStreamBase, UdpRefill as UdpRefillBase, Wakeup as WakeupBase,
    WriteFixed as WriteFixedBase,
};

use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::{
    INVALID_SOCKET, IPPROTO_TCP, SOCK_STREAM, SOCKADDR_IN, SOCKADDR_IN6, WSA_FLAG_OVERLAPPED,
    WSA_FLAG_REGISTERED_IO, WSASocketW,
};
use windows_sys::Win32::System::IO::OVERLAPPED;

// ============================================================================
// OverlappedEntry Definition
// ============================================================================

/// A wrapper for the Windows OVERLAPPED structure with additional metadata.
#[repr(C)]
pub struct OverlappedEntry {
    /// The underlying Windows OVERLAPPED structure.
    pub(crate) inner: OVERLAPPED,
    /// User-defined data associated with the operation.
    pub(crate) user_data: usize,
    /// Generation count for slot validation.
    pub(crate) generation: u32,
    /// Result of an offloaded blocking operation.
    pub(crate) blocking_result: Option<io::Result<usize>>,
}

impl OverlappedEntry {
    /// Creates a new `OverlappedEntry` with the given user data.
    pub(crate) fn new(user_data: usize) -> Self {
        Self {
            // SAFETY: OVERLAPPED can be safely zero-initialized.
            inner: unsafe { std::mem::zeroed() },
            user_data,
            generation: 0,
            blocking_result: None,
        }
    }
}

impl Default for OverlappedEntry {
    fn default() -> Self {
        Self::new(0)
    }
}

// SAFETY: OverlappedEntry is safe to send between threads.
unsafe impl Send for OverlappedEntry {}

type ReadFixed = ReadFixedBase<RawHandle>;
type WriteFixed = WriteFixedBase<RawHandle>;
type Recv = RecvBase<RawHandle>;
type OpSend = OpSendBase<RawHandle>;
type Close = CloseBase<RawHandle>;
type Fsync = FsyncBase<RawHandle>;
type Connect = ConnectBase<RawHandle, SockAddrStorage>;
type Accept = AcceptBase<RawHandle, SockAddrStorage>;
type SendTo = SendToBase<RawHandle>;
type SyncFileRange = SyncFileRangeBase<RawHandle>;
type Fallocate = FallocateBase<RawHandle>;
type UdpRecvStream = UdpRecvStreamBase<RawHandle>;
type UdpRefill = UdpRefillBase<RawHandle>;
type Wakeup = WakeupBase<RawHandle>;

// ============================================================================
// SubmitContext Definition
// ============================================================================

/// Context for submitting IOCP operations.
pub(crate) struct SubmitContext<'a> {
    pub(crate) port: HANDLE,
    pub(crate) overlapped: *mut OVERLAPPED,
    pub(crate) ext: &'a Extensions,
    pub(crate) registered_files: &'a [Option<HANDLE>],
    pub(crate) registrar: &'a dyn veloq_buf::BufferRegistrar,

    // RIO Support
    pub(crate) rio: &'a mut RioState,
    pub(crate) slots_per_page: usize,
    pub(crate) slab_resolver: &'a dyn Fn(usize) -> Option<(*const u8, usize)>,
}

// ============================================================================
// VTable Definition
// ============================================================================

pub(crate) type SubmitFn =
    unsafe fn(op: &mut IocpKernelOp, ctx: &mut SubmitContext) -> io::Result<SubmissionResult>;

pub(crate) type OnCompleteFn =
    unsafe fn(op: &mut IocpKernelOp, result: usize, ext: &Extensions) -> io::Result<usize>;

pub(crate) type DropFn = unsafe fn(op: &mut IocpKernelOp);

pub(crate) type GetFdFn = unsafe fn(op: &IocpKernelOp) -> Option<IoFd>;

/// Virtual table for dynamic dispatch of IOCP operations.
pub(crate) struct OpVTable {
    pub(crate) submit: SubmitFn,
    pub(crate) on_complete: Option<OnCompleteFn>,
    pub(crate) drop: DropFn,
    pub(crate) get_fd: GetFdFn,
}

// ============================================================================
// IocpKernelOp Struct & Union (Type-Erased)
// ============================================================================

/// A type-erased IOCP kernel operation.
#[repr(C)]
pub struct IocpKernelOp {
    /// Virtual Table for dynamic dispatch
    pub(crate) vtable: NonNull<OpVTable>,

    /// Public header accessible directly by Driver
    pub(crate) header: OverlappedEntry,

    /// Type-erased payload
    pub(crate) payload: IocpOpPayload,
}

impl PlatformOp for IocpKernelOp {}

impl IocpKernelOp {
    /// Returns the file descriptor associated with the operation, if any.
    pub(crate) fn get_fd(&self) -> Option<IoFd> {
        // SAFETY: vtable pointer is guaranteed to be valid and point to a valid OpVTable.
        unsafe { (self.vtable.as_ref().get_fd)(self) }
    }
}

impl Drop for IocpKernelOp {
    fn drop(&mut self) {
        // SAFETY: vtable pointer is guaranteed to be valid and point to a valid OpVTable.
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
                kind: $kind:expr,
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
        pub(crate) union IocpOpPayload {
            $(
                pub(crate) $field: ManuallyDrop< define_iocp_ops!(@payload_type $OpType $(, $Payload)?) >,
            )+
        }

        $(
            impl IntoPlatformOp<IocpOp> for $OpType {
                type UserPayload = Box<$OpType>;
                const PAYLOAD_KIND: OpKind = $kind;

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
                        // SAFETY: TABLE is a static and its address is guaranteed to be non-null.
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

                fn payload_into_erased(payload: Self::UserPayload) -> veloq_driver_core::slot::ErasedPayload {
                    veloq_driver_core::slot::ErasedPayload {
                        ptr: Box::into_raw(payload) as *mut (),
                        kind: Self::PAYLOAD_KIND as u16,
                        drop_fn: define_iocp_ops!(@drop_raw_fn $OpType),
                    }
                }

                unsafe fn payload_from_raw(ptr: *mut ()) -> Self::UserPayload {
                    // SAFETY: ptr is guaranteed to be a valid pointer to $OpType.
                    unsafe { Box::from_raw(ptr as *mut $OpType) }
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

    (@drop_raw_fn $OpType:ty) => {{
        unsafe fn drop_raw(ptr: *mut ()) {
            // SAFETY: ptr is guaranteed to be a valid pointer to $OpType.
            unsafe { drop(Box::from_raw(ptr as *mut $OpType)) };
        }
        drop_raw
    }};
}

// ============================================================================
// Payload Structures for Complex Ops
// ============================================================================

/// Reference to a kernel operation.
pub(crate) struct KernelRef<T> {
    pub(crate) user: NonNull<T>,
}

/// Payload for the socket accept operation.
pub(crate) struct AcceptPayload {
    pub(crate) user: NonNull<Accept>,
    pub(crate) accept_buffer: [u8; 288],
    pub(crate) accept_socket: RawHandle,
}

/// Payload for the socket send-to operation.
pub(crate) struct SendToPayload {
    pub(crate) user: NonNull<SendTo>,
    pub(crate) addr: SockAddrStorage,
    pub(crate) addr_len: i32,
}

/// Payload for the file open operation.
pub(crate) struct OpenPayload {
    pub(crate) user: NonNull<Open>,
}

/// Alias for the platform-specific IOCP kernel operation.
pub type IocpOp = IocpKernelOp;

// ============================================================================
// Op Definitions
// ============================================================================

define_iocp_ops! {
    ReadFixed {
        field: read,
        kind: OpKind::ReadFixed,
        submit: submit::submit_read_fixed,
        drop: submit::drop_read_fixed,
        get_fd: submit::get_fd_read_fixed,
    },
    WriteFixed {
        field: write,
        kind: OpKind::WriteFixed,
        submit: submit::submit_write_fixed,
        drop: submit::drop_write_fixed,
        get_fd: submit::get_fd_write_fixed,
    },
    Recv {
        field: recv,
        kind: OpKind::Recv,
        submit: submit::submit_recv,
        drop: submit::drop_recv,
        get_fd: submit::get_fd_recv,
    },
    OpSend {
        field: send,
        kind: OpKind::Send,
        submit: submit::submit_send,
        drop: submit::drop_send,
        get_fd: submit::get_fd_send,
    },
    Close {
        field: close,
        kind: OpKind::Close,
        submit: submit::submit_close,
        drop: submit::drop_close,
        get_fd: submit::get_fd_close,
    },
    Fsync {
        field: fsync,
        kind: OpKind::Fsync,
        submit: submit::submit_fsync,
        drop: submit::drop_fsync,
        get_fd: submit::get_fd_fsync,
    },
    SyncFileRange {
        field: sync_range,
        kind: OpKind::SyncFileRange,
        submit: submit::submit_sync_range,
        drop: submit::drop_sync_range,
        get_fd: submit::get_fd_sync_range,
    },
    Fallocate {
        field: fallocate,
        kind: OpKind::Fallocate,
        submit: submit::submit_fallocate,
        drop: submit::drop_fallocate,
        get_fd: submit::get_fd_fallocate,
    },
    Timeout {
        field: timeout,
        kind: OpKind::Timeout,
        submit: submit::submit_timeout,
        drop: submit::drop_timeout,
        get_fd: submit::get_fd_timeout,
    },
    Connect {
        field: connect,
        kind: OpKind::Connect,
        submit: submit::submit_connect,
        on_complete: submit::on_complete_connect,
        drop: submit::drop_connect,
        get_fd: submit::get_fd_connect,
    },
    Accept {
        field: accept,
        payload: AcceptPayload,
        kind: OpKind::Accept,
        submit: submit::submit_accept,
        on_complete: submit::on_complete_accept,
        drop: submit::drop_accept,
        get_fd: submit::get_fd_accept,
        construct: |user: std::ptr::NonNull<Accept>| {
            // SAFETY: user pointer is valid and points to a valid Accept.
            let op = unsafe { user.as_ref() };
            let family = op.addr.0.ss_family;
            // SAFETY: WSASocketW is called with valid parameters for IOCP.
            let socket = unsafe {
                WSASocketW(
                    family as i32,
                    SOCK_STREAM,
                    IPPROTO_TCP,
                    std::ptr::null(),
                    0,
                    WSA_FLAG_OVERLAPPED | WSA_FLAG_REGISTERED_IO,
                )
            };
            let accept_socket = if socket == INVALID_SOCKET {
                RawHandle {
                    handle: std::ptr::null_mut(),
                }
            } else {
                RawHandle {
                    handle: socket as HANDLE,
                }
            };
            AcceptPayload {
                user,
                accept_buffer: [0; 288],
                accept_socket,
            }
        },
        destruct: |user: Box<Accept>| *user,
    },
    SendTo {
        field: send_to,
        payload: SendToPayload,
        kind: OpKind::SendTo,
        submit: submit::submit_send_to,
        drop: submit::drop_send_to,
        get_fd: submit::get_fd_send_to,
        construct: |user: std::ptr::NonNull<SendTo>| {
            // SAFETY: user pointer is valid and points to a valid SendTo.
            let op = unsafe { user.as_ref() };
            let (addr, raw_addr_len) = crate::socket_addr_to_storage(op.addr);
            let addr_len = if op.addr.is_ipv4() {
                std::mem::size_of::<SOCKADDR_IN>() as i32
            } else {
                std::mem::size_of::<SOCKADDR_IN6>() as i32
            };
            debug_assert_eq!(raw_addr_len, addr_len);
            SendToPayload {
                user,
                addr,
                addr_len,
            }
        },
        destruct: |user: Box<SendTo>| *user,
    },
    UdpRecvStream {
        field: udp_recv_stream,
        kind: OpKind::UdpRecvStream,
        submit: submit::submit_udp_recv_stream,
        on_complete: submit::on_complete_udp_recv_stream,
        drop: submit::drop_udp_recv_stream,
        get_fd: submit::get_fd_udp_recv_stream,
    },
    UdpRefill {
        field: udp_refill,
        kind: OpKind::UdpRefill,
        submit: submit::submit_udp_refill,
        drop: submit::drop_udp_refill,
        get_fd: submit::get_fd_udp_refill,
    },
    Open {
        field: open,
        payload: OpenPayload,
        kind: OpKind::Open,
        submit: submit::submit_open,
        drop: submit::drop_open,
        get_fd: submit::get_fd_open,
        construct: |user| OpenPayload { user },
        destruct: |user: Box<Open>| *user,
    },
    Wakeup {
        field: wakeup,
        kind: OpKind::Wakeup,
        submit: submit::submit_wakeup,
        drop: submit::drop_wakeup,
        get_fd: submit::get_fd_wakeup,
        destruct: |user: Box<Wakeup>| *user,
    },
}
