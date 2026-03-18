//! IOCP Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `IocpKernelOp`: The Type-Erased kernel operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations split into `(KernelOp, UserPayload)`

pub(crate) mod overlapped;
pub(crate) mod slot;
pub(crate) mod submit;

pub use overlapped::OverlappedEntry;
pub(crate) use submit::SubmissionResult;

use std::io;
use std::ptr::NonNull;

use crate::config::{IoFd, RawHandle};
use crate::ext::Extensions;
use crate::net::addr::SockAddrStorage;
use crate::rio::RioState;

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

// ============================================================================
// Type Aliases for Core Ops
// ============================================================================

pub(crate) type ReadFixed = ReadFixedBase<RawHandle>;
pub(crate) type WriteFixed = WriteFixedBase<RawHandle>;
pub(crate) type Recv = RecvBase<RawHandle>;
pub(crate) type OpSend = OpSendBase<RawHandle>;
pub(crate) type Close = CloseBase<RawHandle>;
pub(crate) type Fsync = FsyncBase<RawHandle>;
pub(crate) type Connect = ConnectBase<RawHandle, SockAddrStorage>;
pub(crate) type Accept = AcceptBase<RawHandle, SockAddrStorage>;
pub(crate) type SendTo = SendToBase<RawHandle>;
pub(crate) type SyncFileRange = SyncFileRangeBase<RawHandle>;
pub(crate) type Fallocate = FallocateBase<RawHandle>;
pub(crate) type UdpRecvStream = UdpRecvStreamBase<RawHandle>;
pub(crate) type UdpRefill = UdpRefillBase<RawHandle>;
pub(crate) type Wakeup = WakeupBase<RawHandle>;

// ============================================================================
// SubmitContext Definition
// ============================================================================

/// Context for submitting IOCP operations.
pub(crate) struct SubmitContext<'a> {
    pub(crate) port: &'a crate::win32::IoCompletionPort,
    pub(crate) overlapped: *mut windows_sys::Win32::System::IO::OVERLAPPED,
    pub(crate) ext: &'a Extensions,
    pub(crate) registered_files: &'a [Option<HANDLE>],
    pub(crate) registrar: &'a dyn veloq_buf::BufferRegistrar,

    // RIO Support
    pub(crate) rio: &'a mut RioState,
    pub(crate) slots_per_page: usize,
    pub(crate) slab_resolver: &'a dyn Fn(usize) -> Option<(*const u8, usize)>,
}

// ============================================================================
// Macro Definition
// ============================================================================

macro_rules! define_iocp_ops {
    (
        $(
            $OpType:ident {
                variant: $Variant:ident,
                $(payload: $Payload:ty,)?
                kind: $kind:expr,
                submit: $submit:path,
                $(on_complete: $complete:path,)?
                get_fd: $get_fd:path,
                $(construct: $construct:expr,)?
                $(destruct: $destruct:expr,)?
            }
        ),+ $(,)?
    ) => {
        /// Type-safe payload enum for IOCP operations.
        pub(crate) enum IocpOpPayload {
            $(
                $Variant( define_iocp_ops!(@payload_type $OpType $(, $Payload)?) ),
            )+
        }

        /// Virtual table for dynamic dispatch of IOCP operations.
        pub(crate) struct OpVTable {
            pub(crate) submit: unsafe fn(op: &mut IocpKernelOp, ctx: &mut SubmitContext) -> io::Result<SubmissionResult>,
            pub(crate) on_complete: Option<unsafe fn(op: &mut IocpKernelOp, result: usize, ext: &Extensions) -> io::Result<usize>>,
            pub(crate) get_fd: unsafe fn(op: &IocpKernelOp) -> Option<IoFd>,
        }

        /// A type-erased IOCP kernel operation.
        pub struct IocpKernelOp {
            /// Virtual Table for dynamic dispatch
            pub(crate) vtable: NonNull<OpVTable>,
            /// Public header accessible directly by Driver
            pub(crate) header: OverlappedEntry,
            /// Type-safe payload enum
            pub(crate) payload: IocpOpPayload,
        }

        impl PlatformOp for IocpKernelOp {}

        impl IocpKernelOp {
            pub(crate) fn get_fd(&self) -> Option<IoFd> {
                unsafe { (self.vtable.as_ref().get_fd)(self) }
            }
            pub(crate) fn submit(&mut self, ctx: &mut SubmitContext) -> io::Result<SubmissionResult> {
                unsafe { (self.vtable.as_ref().submit)(self, ctx) }
            }
            pub(crate) fn on_complete(&mut self, result: usize, ext: &Extensions) -> io::Result<usize> {
                if let Some(on_complete) = unsafe { self.vtable.as_ref().on_complete } {
                    unsafe { (on_complete)(self, result, ext) }
                } else {
                    Ok(result)
                }
            }
        }

        $(
            impl IntoPlatformOp<IocpOp> for $OpType {
                type UserPayload = Box<$OpType>;
                const PAYLOAD_KIND: OpKind = $kind;

                fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::UserPayload) {
                    static TABLE: OpVTable = OpVTable {
                        submit: |op, ctx| unsafe {
                            if let IocpOpPayload::$Variant(ref mut p) = op.payload {
                                $submit(&mut op.header, p, ctx)
                            } else {
                                unreachable!("Variant mismatch in IocpKernelOp dispatch for {}", stringify!($OpType));
                            }
                        },
                        on_complete: define_iocp_ops!(@optional_complete_shim $OpType, $Variant, $($complete)?),
                        get_fd: |op| unsafe {
                            if let IocpOpPayload::$Variant(ref p) = op.payload {
                                $get_fd(p)
                            } else {
                                unreachable!("Variant mismatch in IocpKernelOp get_fd for {}", stringify!($OpType));
                            }
                        },
                    };

                    let mut user = Box::new(self);
                    let user_ptr = std::ptr::NonNull::from(user.as_mut());
                    let payload = define_iocp_ops!(@construct user_ptr, $($construct)?, $OpType $(, $Payload)?);

                    let op = IocpKernelOp {
                        // SAFETY: TABLE is a static and its address is guaranteed to be non-null.
                        vtable: unsafe { NonNull::new_unchecked(&TABLE as *const _ as *mut _) },
                        header: OverlappedEntry::new(0),
                        payload: IocpOpPayload::$Variant(payload),
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

    (@optional_complete_shim $OpType:ident, $Variant:ident,) => { None };
    (@optional_complete_shim $OpType:ident, $Variant:ident, $fn:path) => {
        Some(|op, result, ext| unsafe {
            if let IocpOpPayload::$Variant(ref mut p) = op.payload {
                $fn(&mut op.header, p, result, ext)
            } else {
                unreachable!("Variant mismatch in IocpKernelOp on_complete for {}", stringify!($OpType));
            }
        })
    };

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
        variant: Read,
        kind: OpKind::ReadFixed,
        submit: submit::submit_read_fixed,
        get_fd: submit::get_fd_read_fixed,
    },
    WriteFixed {
        variant: Write,
        kind: OpKind::WriteFixed,
        submit: submit::submit_write_fixed,
        get_fd: submit::get_fd_write_fixed,
    },
    Recv {
        variant: Recv,
        kind: OpKind::Recv,
        submit: submit::submit_recv,
        get_fd: submit::get_fd_recv,
    },
    OpSend {
        variant: Send,
        kind: OpKind::Send,
        submit: submit::submit_send,
        get_fd: submit::get_fd_send,
    },
    Close {
        variant: Close,
        kind: OpKind::Close,
        submit: submit::submit_close,
        get_fd: submit::get_fd_close,
    },
    Fsync {
        variant: Fsync,
        kind: OpKind::Fsync,
        submit: submit::submit_fsync,
        get_fd: submit::get_fd_fsync,
    },
    SyncFileRange {
        variant: SyncRange,
        kind: OpKind::SyncFileRange,
        submit: submit::submit_sync_range,
        get_fd: submit::get_fd_sync_range,
    },
    Fallocate {
        variant: Fallocate,
        kind: OpKind::Fallocate,
        submit: submit::submit_fallocate,
        get_fd: submit::get_fd_fallocate,
    },
    Timeout {
        variant: Timeout,
        kind: OpKind::Timeout,
        submit: submit::submit_timeout,
        get_fd: submit::get_fd_timeout,
    },
    Connect {
        variant: Connect,
        kind: OpKind::Connect,
        submit: submit::submit_connect,
        on_complete: submit::on_complete_connect,
        get_fd: submit::get_fd_connect,
    },
    Accept {
        variant: Accept,
        payload: AcceptPayload,
        kind: OpKind::Accept,
        submit: submit::submit_accept,
        on_complete: submit::on_complete_accept,
        get_fd: submit::get_fd_accept,
        construct: |user: std::ptr::NonNull<Accept>| {
            // SAFETY: user pointer is valid and points to a valid Accept.
            let op = unsafe { user.as_ref() };
            let family = op.addr.family();
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
        variant: SendTo,
        payload: SendToPayload,
        kind: OpKind::SendTo,
        submit: submit::submit_send_to,
        get_fd: submit::get_fd_send_to,
        construct: |user: std::ptr::NonNull<SendTo>| {
            // SAFETY: user pointer is valid and points to a valid SendTo.
            let op = unsafe { user.as_ref() };
            let (addr, _raw_addr_len) = crate::net::addr::socket_addr_to_storage(op.addr);
            let addr_len = match op.addr {
                std::net::SocketAddr::V4(_) => std::mem::size_of::<SOCKADDR_IN>() as i32,
                std::net::SocketAddr::V6(_) => std::mem::size_of::<SOCKADDR_IN6>() as i32,
            };
            SendToPayload {
                user,
                addr,
                addr_len,
            }
        },
        destruct: |user: Box<SendTo>| *user,
    },
    UdpRecvStream {
        variant: UdpRecvStream,
        kind: OpKind::UdpRecvStream,
        submit: submit::submit_udp_recv_stream,
        on_complete: submit::on_udp_stream_complete,
        get_fd: submit::get_fd_udp_recv_stream,
    },
    UdpRefill {
        variant: UdpRefill,
        kind: OpKind::UdpRefill,
        submit: submit::submit_udp_refill,
        get_fd: submit::get_fd_udp_refill,
    },
    Open {
        variant: Open,
        payload: OpenPayload,
        kind: OpKind::Open,
        submit: submit::submit_open,
        get_fd: submit::get_fd_open,
        construct: |user| OpenPayload { user },
        destruct: |user: Box<Open>| *user,
    },
    Wakeup {
        variant: Wakeup,
        kind: OpKind::Wakeup,
        submit: submit::submit_wakeup,
        get_fd: submit::get_fd_wakeup,
        destruct: |user: Box<Wakeup>| *user,
    },
}
