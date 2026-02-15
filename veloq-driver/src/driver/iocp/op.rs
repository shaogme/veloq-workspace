//! IOCP Platform-Specific Operation Definitions
//!
//! This module defines:
//! - `IocpOp`: The Type-Erased operation struct using Unions and VTables
//! - `OpVTable`: The virtual table for dynamic dispatch without enums
//! - `IntoPlatformOp` implementations using blind casting

use crate::SockAddrStorage;
use crate::driver::PlatformOp;
use crate::driver::iocp::IocpDriver;
use crate::driver::iocp::ext::Extensions;
use crate::driver::iocp::rio::RioState;
use crate::driver::iocp::submit::{self, SubmissionResult};
use crate::op::{
    Accept, Close, Connect, Fallocate, Fsync, IntoPlatformOp, IoFd, Open, ReadFixed, Recv,
    RecvFrom, Send as OpSend, SendTo, SyncFileRange, Timeout, Wakeup, WriteFixed,
};
use std::io;
use std::mem::ManuallyDrop;
use windows_sys::Win32::Foundation::HANDLE;
use windows_sys::Win32::Networking::WinSock::WSABUF;
use windows_sys::Win32::System::IO::OVERLAPPED;

// ============================================================================
// OverlappedEntry Definition
// ============================================================================

#[repr(C)]
pub struct OverlappedEntry {
    pub inner: OVERLAPPED,
    pub user_data: usize,
    pub blocking_result: Option<io::Result<usize>>,
}

impl OverlappedEntry {
    pub fn new(user_data: usize) -> Self {
        Self {
            inner: unsafe { std::mem::zeroed() },
            user_data,
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

    // RIO Support
    pub rio: Option<&'a mut RioState>,
    pub slots_per_page: usize,
}

// ============================================================================
// VTable Definition
// ============================================================================

pub type SubmitFn =
    unsafe fn(op: &mut IocpOp, ctx: &mut SubmitContext) -> io::Result<SubmissionResult>;

pub type OnCompleteFn =
    unsafe fn(op: &mut IocpOp, result: usize, ext: &Extensions) -> io::Result<usize>;

pub type DropFn = unsafe fn(op: &mut IocpOp);

pub type GetFdFn = unsafe fn(op: &IocpOp) -> Option<IoFd>;

pub struct OpVTable {
    pub submit: SubmitFn,
    pub on_complete: Option<OnCompleteFn>,
    pub drop: DropFn,
    pub get_fd: GetFdFn,
}

// ============================================================================
// IocpOp Struct & Union (Type-Erased)
// ============================================================================

use std::ptr::NonNull;

#[repr(C)]
pub struct IocpOp {
    /// Virtual Table for dynamic dispatch
    pub vtable: NonNull<OpVTable>,

    /// Public header accessible directly by Driver
    pub header: OverlappedEntry,

    /// Type-erased payload
    pub payload: IocpOpPayload,
}

impl PlatformOp for IocpOp {}

impl IocpOp {
    /// Helper to access the OverlappedEntry (header).
    /// Kept for compatibility with existing Driver code.
    pub fn entry_mut(&mut self) -> Option<&mut OverlappedEntry> {
        Some(&mut self.header)
    }

    pub fn get_fd(&self) -> Option<IoFd> {
        unsafe { (self.vtable.as_ref().get_fd)(self) }
    }
}

impl Drop for IocpOp {
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
                fn into_platform_op(self) -> IocpOp {
                    static TABLE: OpVTable = OpVTable {
                        submit: $submit,
                        on_complete: define_iocp_ops!(@optional_complete $($complete)?),
                        drop: $drop,
                        get_fd: $get_fd,
                    };

                    let payload = define_iocp_ops!(@construct self, $($construct)?, $OpType $(, $Payload)?);

                    IocpOp {
                        vtable: unsafe { NonNull::new_unchecked(&TABLE as *const _ as *mut _) },
                        header: OverlappedEntry::new(0),
                        payload: IocpOpPayload {
                            $field: ManuallyDrop::new(payload),
                        },
                    }
                }

                fn from_platform_op(op: IocpOp) -> Self {
                    let op = ManuallyDrop::new(op);
                    let payload = unsafe {
                        ManuallyDrop::into_inner(std::ptr::read(&op.payload.$field))
                    };
                    define_iocp_ops!(@destruct payload, $($destruct)?)
                }
            }
        )+
    };

    (@payload_type $OpType:ty) => { $OpType };
    (@payload_type $OpType:ty, $Payload:ty) => { $Payload };

    (@optional_complete) => { None };
    (@optional_complete $fn:path) => { Some($fn) };

    // Default construct: return self
    (@construct $self:expr, , $OpType:ty) => { $self };
    // Custom construct
    (@construct $self:expr, $construct:expr, $OpType:ty, $Payload:ty) => { ($construct)($self) };

    // Default destruct: return payload (assumes payload is OpType)
    (@destruct $payload:expr, ) => { $payload };
    // Custom destruct
    (@destruct $payload:expr, $destruct:expr) => { ($destruct)($payload) };
}

// ============================================================================
// Payload Structures for Complex Ops
// ============================================================================

pub struct AcceptPayload {
    pub op: Accept,
    pub accept_buffer: [u8; 288],
}

pub struct SendToPayload {
    pub op: SendTo,
    pub wsabuf: WSABUF,
    pub addr: SockAddrStorage,
    pub addr_len: i32,
}

pub struct RecvFromPayload {
    pub op: RecvFrom,
    pub wsabuf: WSABUF,
    pub flags: u32,
    pub addr: SockAddrStorage,
    pub addr_len: i32,
}

pub struct OpenPayload {
    pub op: Open,
}

pub struct WakeupPayload {
    pub op: Wakeup,
}

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
        construct: |op| AcceptPayload {
            op,
            accept_buffer: [0; 288],
        },
        destruct: |p: AcceptPayload| p.op,
    },
    SendTo {
        field: send_to,
        payload: SendToPayload,
        submit: submit::submit_send_to,
        drop: submit::drop_send_to,
        get_fd: submit::get_fd_send_to,
        construct: |op: SendTo| {
            let (addr, addr_len) = crate::socket_addr_to_storage(op.addr);
            let wsabuf = WSABUF {
                len: op.buf.len() as u32,
                buf: op.buf.as_slice().as_ptr() as *mut u8,
            };
            SendToPayload {
                op,
                wsabuf,
                addr,
                addr_len,
            }
        },
        destruct: |p: SendToPayload| p.op,
    },
    RecvFrom {
        field: recv_from,
        payload: RecvFromPayload,
        submit: submit::submit_recv_from,
        drop: submit::drop_recv_from,
        get_fd: submit::get_fd_recv_from,
        construct: |mut op: RecvFrom| {
            let wsabuf = WSABUF {
                len: op.buf.capacity() as u32,
                buf: op.buf.as_mut_ptr(),
            };
            RecvFromPayload {
                op,
                wsabuf,
                flags: 0,
                addr: SockAddrStorage::default(),
                addr_len: std::mem::size_of::<SockAddrStorage>() as i32,
            }
        },
        destruct: |p: RecvFromPayload| {
            let mut val = p.op;
            let len = p.addr_len as usize;
            let addr = unsafe {
                let s = std::slice::from_raw_parts(&p.addr as *const _ as *const u8, len);
                crate::to_socket_addr(s).ok()
            };
            val.addr = addr;
            val
        },
    },
    Open {
        field: open,
        payload: OpenPayload,
        submit: submit::submit_open,
        drop: submit::drop_open,
        get_fd: submit::get_fd_open,
        construct: |op| OpenPayload { op },
        destruct: |p: OpenPayload| p.op,
    },
    Wakeup {
        field: wakeup,
        payload: WakeupPayload,
        submit: submit::submit_wakeup,
        drop: submit::drop_wakeup,
        get_fd: submit::get_fd_wakeup,
        construct: |op| WakeupPayload { op },
        destruct: |p: WakeupPayload| p.op,
    },
}
