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

use std::ptr::NonNull;

use crate::config::{IoFd, IocpHandle, OwnedRawHandle, RawHandle, RegisteredHandle};
use crate::error::{IocpError, IocpResult};
use crate::ext::Extensions;
use crate::net::addr::SockAddrStorage;
use crate::rio::RioState;

use veloq_driver_core::driver::PlatformOp;
use veloq_driver_core::error::DriverResult;
use veloq_driver_core::op::{
    Accept as AcceptBase, Close as CloseBase, Connect as ConnectBase, Fallocate as FallocateBase,
    FallocateRaw as FallocateRawBase, Fsync as FsyncBase, FsyncRaw as FsyncRawBase, IntoPlatformOp,
    OpKind, Open, ReadFixed as ReadFixedBase, ReadRaw as ReadRawBase, Recv as RecvBase,
    Send as OpSendBase, SendTo as SendToBase, SyncFileRange as SyncFileRangeBase,
    SyncFileRangeRaw as SyncFileRangeRawBase, Timeout, UdpConnect as UdpConnectBase,
    UdpRecv as UdpRecvBase, UdpRecvStream as UdpRecvStreamBase, UdpSend as UdpSendBase,
    Wakeup as WakeupBase, WriteFixed as WriteFixedBase, WriteRaw as WriteRawBase,
};

use windows_sys::Win32::Networking::WinSock::{SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_STORAGE};

// ============================================================================
// Type Aliases for Core Ops
// ============================================================================

pub(crate) type ReadFixed = ReadFixedBase;
pub(crate) type ReadRaw = ReadRawBase<IocpHandle>;
pub(crate) type WriteFixed = WriteFixedBase;
pub(crate) type WriteRaw = WriteRawBase<IocpHandle>;
pub(crate) type Recv = RecvBase;
pub(crate) type OpSend = OpSendBase;
pub(crate) type UdpRecv = UdpRecvBase;
pub(crate) type UdpSend = UdpSendBase;
pub(crate) type Close = CloseBase;
pub(crate) type Fsync = FsyncBase;
pub(crate) type FsyncRaw = FsyncRawBase<IocpHandle>;
pub(crate) type Connect = ConnectBase<SockAddrStorage>;
pub(crate) type UdpConnect = UdpConnectBase<SockAddrStorage>;
pub(crate) type Accept = AcceptBase<SockAddrStorage>;
pub(crate) type SendTo = SendToBase;
pub(crate) type SyncFileRange = SyncFileRangeBase;
pub(crate) type SyncFileRangeRaw = SyncFileRangeRawBase<IocpHandle>;
pub(crate) type Fallocate = FallocateBase;
pub(crate) type FallocateRaw = FallocateRawBase<IocpHandle>;
pub(crate) type UdpRecvStream = UdpRecvStreamBase;
pub(crate) type Wakeup = WakeupBase;

// ============================================================================
// SubmitContext Definition
// ============================================================================

/// Context for submitting IOCP operations.
pub(crate) struct SubmitContext<'a> {
    pub(crate) port: &'a crate::win32::IoCompletionPort,
    pub(crate) overlapped: *mut crate::win32::Overlapped,
    pub(crate) ext: &'a Extensions,
    pub(crate) registered_files: &'a [Option<RegisteredHandle>],
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
                $(completion: $completion:ty,)?
                $(map_completion: $map_completion:expr,)?
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
            pub(crate) submit: fn(op: &mut IocpKernelOp, ctx: &mut SubmitContext) -> IocpResult<SubmissionResult>,
            pub(crate) on_complete: Option<unsafe fn(op: &mut IocpKernelOp, result: usize, ext: &Extensions) -> IocpResult<usize>>,
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
            pub(crate) fn submit(&mut self, ctx: &mut SubmitContext) -> IocpResult<SubmissionResult> {
                // SAFETY: vtable is initialized from a static TABLE and always non-null.
                let table = unsafe { self.vtable.as_ref() };
                (table.submit)(self, ctx)
            }
            pub(crate) fn on_complete(&mut self, result: usize, ext: &Extensions) -> IocpResult<usize> {
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
                type Completion = define_iocp_ops!(@completion_type $($completion)?);
                type DriverCompletion = usize;
                const PAYLOAD_KIND: OpKind = $kind;

                fn into_kernel_and_payload(self) -> (IocpKernelOp, Self::UserPayload) {
                    static TABLE: OpVTable = OpVTable {
                        submit: |op, ctx| {
                            if let IocpOpPayload::$Variant(ref mut p) = op.payload {
                                $submit(&mut op.header, p, ctx)
                            } else {
                                Err(diagweave::report::Report::new(IocpError::InvalidState).attach_note(format!(
                                    "variant mismatch in IocpKernelOp dispatch for {}",
                                    stringify!($OpType)
                                )))
                            }
                        },
                        on_complete: define_iocp_ops!(@optional_complete_shim $OpType, $Variant, $($complete)?),
                        get_fd: |op| unsafe {
                            if let IocpOpPayload::$Variant(ref p) = op.payload {
                                $get_fd(p)
                            } else {
                                None
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

                fn map_completion_result(
                    &self,
                    res: DriverResult<usize>,
                ) -> DriverResult<Self::Completion> {
                    define_iocp_ops!(@map_completion self, res, $($map_completion)?)
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
                Err(diagweave::report::Report::new(IocpError::InvalidState).attach_note(format!(
                    "variant mismatch in IocpKernelOp on_complete for {}",
                    stringify!($OpType)
                )))
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

    (@completion_type ) => { usize };
    (@completion_type $ty:ty) => { $ty };

    (@map_completion $this:ident, $res:ident, ) => { $res };
    (@map_completion $this:ident, $res:ident, $expr:expr) => { ($expr)($this, $res) };
}

// ============================================================================
// Payload Structures for Complex Ops
// ============================================================================

/// Reference to a kernel operation.
pub(crate) struct KernelRef<T> {
    pub(crate) user: NonNull<T>,
}

/// Payload for the socket accept operation.
pub(crate) const ACCEPT_EX_ADDR_SECTION_LEN: usize = std::mem::size_of::<SOCKADDR_STORAGE>() + 16;
pub(crate) const ACCEPT_EX_OUTPUT_BUFFER_LEN: usize = ACCEPT_EX_ADDR_SECTION_LEN * 2;

pub(crate) struct AcceptPayload {
    pub(crate) user: NonNull<Accept>,
    pub(crate) accept_buffer: [u8; ACCEPT_EX_OUTPUT_BUFFER_LEN],
    pub(crate) accept_socket: Option<OwnedRawHandle>,
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
    ReadRaw {
        variant: ReadRaw,
        kind: OpKind::ReadFixed,
        submit: submit::submit_read_raw,
        get_fd: submit::get_fd_read_raw,
    },
    WriteFixed {
        variant: Write,
        kind: OpKind::WriteFixed,
        submit: submit::submit_write_fixed,
        get_fd: submit::get_fd_write_fixed,
    },
    WriteRaw {
        variant: WriteRaw,
        kind: OpKind::WriteFixed,
        submit: submit::submit_write_raw,
        get_fd: submit::get_fd_write_raw,
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
    UdpRecv {
        variant: UdpRecv,
        kind: OpKind::UdpRecv,
        submit: submit::submit_udp_recv,
        get_fd: submit::get_fd_udp_recv,
    },
    UdpSend {
        variant: UdpSend,
        kind: OpKind::UdpSend,
        submit: submit::submit_udp_send,
        get_fd: submit::get_fd_udp_send,
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
    FsyncRaw {
        variant: FsyncRaw,
        kind: OpKind::Fsync,
        submit: submit::submit_fsync_raw,
        get_fd: submit::get_fd_fsync_raw,
    },
    SyncFileRange {
        variant: SyncRange,
        kind: OpKind::SyncFileRange,
        submit: submit::submit_sync_range,
        get_fd: submit::get_fd_sync_range,
    },
    SyncFileRangeRaw {
        variant: SyncRangeRaw,
        kind: OpKind::SyncFileRange,
        submit: submit::submit_sync_range_raw,
        get_fd: submit::get_fd_sync_range_raw,
    },
    Fallocate {
        variant: Fallocate,
        kind: OpKind::Fallocate,
        submit: submit::submit_fallocate,
        get_fd: submit::get_fd_fallocate,
    },
    FallocateRaw {
        variant: FallocateRaw,
        kind: OpKind::Fallocate,
        submit: submit::submit_fallocate_raw,
        get_fd: submit::get_fd_fallocate_raw,
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
    UdpConnect {
        variant: UdpConnect,
        kind: OpKind::UdpConnect,
        submit: submit::submit_udp_connect,
        on_complete: submit::on_complete_udp_connect,
        get_fd: submit::get_fd_udp_connect,
    },
    Accept {
        variant: Accept,
        payload: AcceptPayload,
        kind: OpKind::Accept,
        submit: submit::submit_accept,
        on_complete: submit::on_complete_accept,
        completion: OwnedRawHandle,
        map_completion: |_op: &Accept, res: DriverResult<usize>| {
            res.map(|raw| unsafe {
                OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_socket(raw as _)))
            })
        },
        get_fd: submit::get_fd_accept,
        construct: |user: std::ptr::NonNull<Accept>| {
            AcceptPayload {
                user,
                accept_buffer: [0; ACCEPT_EX_OUTPUT_BUFFER_LEN],
                accept_socket: None,
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
    Open {
        variant: Open,
        payload: OpenPayload,
        kind: OpKind::Open,
        submit: submit::submit_open,
        completion: OwnedRawHandle,
        map_completion: |_op: &Open, res: DriverResult<usize>| {
            res.map(|raw| unsafe {
                OwnedRawHandle::from_raw_owned(RawHandle::new(IocpHandle::for_file(raw as _)))
            })
        },
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

