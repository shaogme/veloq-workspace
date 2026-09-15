pub(crate) use veloq_driver_core::op::types::{
    Accept as CoreAccept, AcceptMulti as CoreAcceptMulti, AcceptedSocket, Close as CoreClose,
    Connect as CoreConnect, Fallocate as CoreFallocate, FallocateRaw as CoreFallocateRaw,
    Fsync as CoreFsync, FsyncRaw as CoreFsyncRaw, Open, ProvidedBuf, ReadFixed as CoreReadFixed,
    ReadRaw as CoreReadRaw, Recv as CoreRecv, RecvMulti as CoreRecvMulti,
    RecvProvided as CoreRecvProvided, Send as CoreSend, SendTo as CoreSendTo,
    SyncFileRange as CoreSyncFileRange, SyncFileRangeRaw as CoreSyncFileRangeRaw, Timeout,
    UdpConnect as CoreUdpConnect, UdpRecv as CoreUdpRecv, UdpRecvFrom as CoreUdpRecvFrom,
    UdpSend as CoreUdpSend, Wakeup as CoreWakeup, WriteFixed as CoreWriteFixed,
    WriteRaw as CoreWriteRaw,
};

use crate::{
    config::{SockAddrStorage, UringRawHandle},
    error::UringResult,
    net::{bounded_sockaddr_bytes, socket_addr_to_storage, to_socket_addr},
};
use io_uring::types::Timespec;
use veloq_buf::BufIoRangeError;
use veloq_std::{
    marker::{PhantomData, PhantomPinned},
    mem,
    net::SocketAddr,
    pin::Pin,
    ptr,
};

pub(crate) type ReadFixed = CoreReadFixed<UringRawHandle>;
pub(crate) type ReadRaw = CoreReadRaw<UringRawHandle>;
pub(crate) type WriteFixed = CoreWriteFixed<UringRawHandle>;
pub(crate) type WriteRaw = CoreWriteRaw<UringRawHandle>;
pub(crate) type Recv = CoreRecv<UringRawHandle>;
pub(crate) type RecvProvided = CoreRecvProvided<UringRawHandle>;
pub(crate) type RecvMulti = CoreRecvMulti<UringRawHandle>;
pub(crate) type OpSend = CoreSend<UringRawHandle>;
pub(crate) type UdpRecv = CoreUdpRecv<UringRawHandle>;
pub(crate) type UdpSend = CoreUdpSend<UringRawHandle>;
pub(crate) type Connect = CoreConnect<UringRawHandle, SockAddrStorage>;
pub(crate) type UdpConnect = CoreUdpConnect<UringRawHandle, SockAddrStorage>;
pub(crate) type Close = CoreClose<UringRawHandle>;
pub(crate) type Fsync = CoreFsync<UringRawHandle>;
pub(crate) type FsyncRaw = CoreFsyncRaw<UringRawHandle>;
pub(crate) type SyncFileRange = CoreSyncFileRange<UringRawHandle>;
pub(crate) type SyncFileRangeRaw = CoreSyncFileRangeRaw<UringRawHandle>;
pub(crate) type Fallocate = CoreFallocate<UringRawHandle>;
pub(crate) type FallocateRaw = CoreFallocateRaw<UringRawHandle>;
pub(crate) type Accept = CoreAccept<UringRawHandle, SockAddrStorage>;
pub(crate) type AcceptMulti = CoreAcceptMulti<UringRawHandle>;
pub(crate) type SendTo = CoreSendTo<UringRawHandle>;
pub(crate) type UdpRecvFrom = CoreUdpRecvFrom<UringRawHandle>;
pub(crate) type Wakeup = CoreWakeup<UringRawHandle>;

/// Opaque user payload storage generated from the operation declaration rows.
///
/// The concrete storage remains inline in the slot, but callers cannot use the storage as an
/// operation registry by matching on public variants.
#[repr(transparent)]
pub struct UringUserPayload {
    pub(super) storage: super::spec::UringUserPayloadStorage,
}

pub(crate) struct KernelRef<T> {
    pub(crate) marker: PhantomData<T>,
}

pub(crate) fn kernel_ref<T>(_user: &T) -> KernelRef<T> {
    KernelRef {
        marker: PhantomData,
    }
}

pub(crate) struct AcceptPayload {}

/// Kernel payload for [`SendTo`].
///
/// # Safety / Self-Referential Layout
///
/// `msghdr.msg_name` and `msghdr.msg_iov` contain self-referential pointers to `msg_name`
/// and `iovec` within this structure, which are populated during `make_sqe_send_to`.
/// Once populated, this payload MUST NOT be moved in memory until the operation completes
/// or fails in the kernel. `PhantomPinned` enforces !Unpin in the type system.
pub(crate) struct SendToPayload {
    msg_name: libc::sockaddr_storage,
    msg_namelen: libc::socklen_t,
    iovec: [libc::iovec; 1],
    msghdr: libc::msghdr,
    _pin: PhantomPinned,
}

/// Kernel payload for [`UdpRecvFrom`].
///
/// # Safety / Self-Referential Layout
///
/// `msghdr.msg_name` and `msghdr.msg_iov` contain self-referential pointers to `msg_name`
/// and `iovec` within this structure, which are populated during `make_sqe_udp_recv_from`.
/// Once populated, this payload MUST NOT be moved in memory until the operation completes
/// or fails in the kernel. `PhantomPinned` enforces !Unpin in the type system.
pub(crate) struct UdpRecvFromPayload {
    msg_name: libc::sockaddr_storage,
    iovec: [libc::iovec; 1],
    msghdr: libc::msghdr,
    _pin: PhantomPinned,
}

/// A read-only SQE pointer backed by a [`SendToPayload`].
pub(crate) struct SendMsgView<'a> {
    msghdr: &'a libc::msghdr,
}

impl SendMsgView<'_> {
    #[inline]
    pub(crate) fn as_ptr(&self) -> *const libc::msghdr {
        self.msghdr
    }
}

/// A writable SQE pointer backed by a [`UdpRecvFromPayload`].
pub(crate) struct RecvMsgView<'a> {
    msghdr: &'a mut libc::msghdr,
}

impl RecvMsgView<'_> {
    #[inline]
    pub(crate) fn into_ptr(self) -> *mut libc::msghdr {
        self.msghdr
    }
}

pub(crate) struct OpenPayload {}

pub(crate) struct WakeupPayload {
    pub(crate) buf: [u8; 8],
}

pub(crate) struct TimeoutPayload {
    pub(crate) ts: Timespec,
}

fn zeroed_sockaddr_storage() -> libc::sockaddr_storage {
    // C socket storage is intentionally zero-initialized before make_sqe fills it.
    unsafe { mem::zeroed() }
}

fn zeroed_msghdr() -> libc::msghdr {
    // msghdr pointer fields are populated immediately before submission.
    unsafe { mem::zeroed() }
}

impl AcceptPayload {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {}
    }
}

impl SendToPayload {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            msg_name: zeroed_sockaddr_storage(),
            msg_namelen: 0,
            iovec: [libc::iovec {
                iov_base: ptr::null_mut(),
                iov_len: 0,
            }],
            msghdr: zeroed_msghdr(),
            _pin: PhantomPinned,
        }
    }

    /// Initializes the address buffer, iovec, and `sendmsg` header for submission.
    ///
    /// # Safety
    ///
    /// The caller must keep `self` at the same address until the kernel has stopped using the
    /// returned message view. The view stores a pointer into `self`, and the SQE can outlive this
    /// Rust borrow until its completion or cancellation is observed.
    pub(crate) unsafe fn init_send_to<'a>(
        self: Pin<&'a mut Self>,
        user: &mut SendTo,
    ) -> Result<SendMsgView<'a>, BufIoRangeError> {
        // SAFETY: this method requires a pinned receiver and never moves the payload.
        let this = unsafe { self.get_unchecked_mut() };
        let (ptr, len) = user.buf.checked_write_range(user.buf_offset)?;
        this.iovec[0].iov_base = ptr as *mut _;
        this.iovec[0].iov_len = len as usize;

        let (msg_name, msg_namelen) = socket_addr_to_storage(user.addr);
        this.msg_name = msg_name.0;
        this.msg_namelen = msg_namelen;
        this.msghdr.msg_name = ptr::addr_of_mut!(this.msg_name).cast();
        this.msghdr.msg_namelen = this.msg_namelen;
        this.msghdr.msg_iov = this.iovec.as_mut_ptr();
        this.msghdr.msg_iovlen = 1;

        Ok(SendMsgView {
            msghdr: &this.msghdr,
        })
    }

    #[cfg(test)]
    pub(crate) fn test_pointers(
        &mut self,
    ) -> (*mut libc::c_void, *mut libc::iovec, *const libc::msghdr) {
        (
            ptr::addr_of_mut!(self.msg_name).cast(),
            self.iovec.as_mut_ptr(),
            ptr::addr_of!(self.msghdr),
        )
    }
}

impl UdpRecvFromPayload {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            msg_name: zeroed_sockaddr_storage(),
            iovec: [libc::iovec {
                iov_base: ptr::null_mut(),
                iov_len: 0,
            }],
            msghdr: zeroed_msghdr(),
            _pin: PhantomPinned,
        }
    }

    /// Initializes the address buffer, iovec, and `recvmsg` header for submission.
    ///
    /// # Safety
    ///
    /// The caller must keep `self` at the same address until the kernel has stopped using the
    /// returned message view. The view stores pointers into `self`, and the SQE can outlive this
    /// Rust borrow until its completion or cancellation is observed.
    pub(crate) unsafe fn init_recv_from<'a>(
        self: Pin<&'a mut Self>,
        user: &mut UdpRecvFrom,
    ) -> Result<RecvMsgView<'a>, BufIoRangeError> {
        // SAFETY: this method requires a pinned receiver and never moves the payload.
        let this = unsafe { self.get_unchecked_mut() };
        let (ptr, len) = user.buf.checked_read_range(user.buf_offset)?;
        this.iovec[0].iov_base = ptr as *mut _;
        this.iovec[0].iov_len = len as usize;

        this.msghdr.msg_name = ptr::addr_of_mut!(this.msg_name).cast();
        this.msghdr.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as _;
        this.msghdr.msg_iov = this.iovec.as_mut_ptr();
        this.msghdr.msg_iovlen = 1;

        Ok(RecvMsgView {
            msghdr: &mut this.msghdr,
        })
    }

    pub(crate) fn finish_recv_from(&self) -> UringResult<SocketAddr> {
        let addr_bytes = bounded_sockaddr_bytes(
            &self.msg_name,
            self.msghdr.msg_namelen as usize,
            "uring.op.payload.finish_recv_from",
        )?;
        to_socket_addr(addr_bytes)
    }

    #[cfg(test)]
    pub(crate) fn test_pointers(
        &mut self,
    ) -> (*mut libc::c_void, *mut libc::iovec, *const libc::msghdr) {
        (
            ptr::addr_of_mut!(self.msg_name).cast(),
            self.iovec.as_mut_ptr(),
            ptr::addr_of!(self.msghdr),
        )
    }

    #[cfg(test)]
    pub(crate) fn test_set_received_address(
        &mut self,
        storage: libc::sockaddr_storage,
        len: usize,
    ) {
        self.msg_name = storage;
        self.msghdr.msg_namelen = len as _;
    }
}

impl OpenPayload {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self {}
    }
}

impl WakeupPayload {
    #[inline]
    pub(crate) const fn new() -> Self {
        Self { buf: [0; 8] }
    }
}

impl TimeoutPayload {
    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            ts: Timespec::new(),
        }
    }
}
