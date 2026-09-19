pub(crate) use veloq_driver_core::op::types::{
    Accept as CoreAccept, AcceptMulti as CoreAcceptMulti, AcceptedSocket, Close as CoreClose,
    Connect as CoreConnect, Fallocate as CoreFallocate, FallocateRaw as CoreFallocateRaw,
    Fsync as CoreFsync, FsyncRaw as CoreFsyncRaw, Open, ProvidedBuf, ReadFixed as CoreReadFixed,
    ReadRaw as CoreReadRaw, Recv as CoreRecv, RecvMulti as CoreRecvMulti,
    RecvProvided as CoreRecvProvided, Send as CoreSend, SendTo as CoreSendTo,
    SyncFileRange as CoreSyncFileRange, SyncFileRangeRaw as CoreSyncFileRangeRaw, Timeout,
    UdpConnect as CoreUdpConnect, UdpRecvMulti as CoreUdpRecvMulti, UdpRecvPacket,
    UdpSend as CoreUdpSend, Wakeup as CoreWakeup, WriteFixed as CoreWriteFixed,
    WriteRaw as CoreWriteRaw,
};

use crate::{
    config::{SockAddrStorage, UringRawHandle},
    driver::registration::provided_buf::ProvidedBufLease,
    error::{UringError, UringResult},
    net::{socket_addr_to_storage, to_socket_addr},
};
use diagweave::prelude::Report;
use veloq_buf::BufIoRangeError;
use veloq_driver_core::platform::receive_pump::ReceivePendingKey;
use veloq_io_uring::types::Timespec;
use veloq_std::{
    collections::VecDeque,
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
pub(crate) type UdpRecvMulti = CoreUdpRecvMulti<UringRawHandle>;
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

/// Kernel payload for UDP `recvmsg` multishot.
///
/// The kernel writes the address, control area, and payload into the selected provided buffer.
/// The message header is only a stable configuration template; `msg_name` stays null and
/// `msg_iov` points at an inert zero-length iovec, matching the provided-buffer layout.
pub(crate) struct UdpRecvMultiPayload {
    iovec: [libc::iovec; 1],
    msghdr: libc::msghdr,
    control: [u8; UDP_RECV_MULTI_CONTROL_LEN],
    pending: VecDeque<UdpPendingLease>,
    _pin: PhantomPinned,
}

pub(crate) struct UdpPendingLease {
    pub(crate) key: ReceivePendingKey,
    pub(crate) addr: SocketAddr,
    pub(crate) len: usize,
    pub(crate) lease: ProvidedBufLease,
}

const UDP_RECV_MULTI_CONTROL_LEN: usize = 0;

#[repr(C)]
#[derive(Clone, Copy)]
struct IoUringRecvMsgOut {
    namelen: u32,
    controllen: u32,
    payloadlen: u32,
    flags: u32,
}

pub(crate) struct ParsedUdpRecvMsg<'a> {
    pub(crate) addr: SocketAddr,
    pub(crate) payload: &'a [u8],
    pub(crate) flags: u32,
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

/// A writable SQE pointer backed by a UDP `recvmsg` payload.
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

impl UdpRecvMultiPayload {
    pub(crate) fn required_buffer_size(datagram_capacity: usize) -> UringResult<usize> {
        mem::size_of::<IoUringRecvMsgOut>()
            .checked_add(mem::size_of::<libc::sockaddr_storage>())
            .and_then(|size| size.checked_add(UDP_RECV_MULTI_CONTROL_LEN))
            .and_then(|size| size.checked_add(datagram_capacity))
            .ok_or_else(|| {
                UringError::InvalidInput
                    .report(
                        "uring.op.payload.recvmsg_multishot",
                        "recvmsg selected buffer size overflowed",
                    )
                    .with_ctx("datagram_capacity", datagram_capacity)
            })
    }

    #[inline]
    pub(crate) fn new() -> Self {
        Self {
            iovec: [libc::iovec {
                iov_base: ptr::null_mut(),
                iov_len: 0,
            }],
            msghdr: libc::msghdr {
                msg_name: ptr::null_mut(),
                msg_namelen: mem::size_of::<libc::sockaddr_storage>() as _,
                msg_iov: ptr::null_mut(),
                msg_iovlen: 0,
                msg_control: ptr::null_mut(),
                msg_controllen: UDP_RECV_MULTI_CONTROL_LEN,
                msg_flags: 0,
            },
            control: [0; UDP_RECV_MULTI_CONTROL_LEN],
            pending: VecDeque::new(),
            _pin: PhantomPinned,
        }
    }

    /// Return the pinned message configuration used by `RECVMSG` multishot.
    ///
    /// # Safety
    ///
    /// The caller must keep `self` pinned until the kernel has stopped using the message header.
    pub(crate) unsafe fn init_recv_multi<'a>(self: Pin<&'a mut Self>) -> RecvMsgView<'a> {
        // SAFETY: the pinned receiver guarantees that the header address remains stable.
        let this = unsafe { self.get_unchecked_mut() };
        this.msghdr.msg_name = ptr::null_mut();
        this.msghdr.msg_namelen = mem::size_of::<libc::sockaddr_storage>() as _;
        this.msghdr.msg_iov = this.iovec.as_mut_ptr();
        this.msghdr.msg_iovlen = 0;
        this.msghdr.msg_control = if this.control.is_empty() {
            ptr::null_mut()
        } else {
            this.control.as_mut_ptr().cast()
        };
        this.msghdr.msg_controllen = this.control.len();
        this.msghdr.msg_flags = 0;
        RecvMsgView {
            msghdr: &mut this.msghdr,
        }
    }

    pub(crate) fn message(&self) -> &libc::msghdr {
        &self.msghdr
    }

    pub(crate) fn push_pending(&mut self, pending: UdpPendingLease) {
        self.pending.push_back(pending);
    }

    pub(crate) fn take_pending_by_key(
        &mut self,
        key: ReceivePendingKey,
    ) -> Option<UdpPendingLease> {
        let index = self.pending.iter().position(|pending| pending.key == key)?;
        self.pending.remove(index)
    }
}

fn malformed_recvmsg(detail: &'static str) -> Report<UringError> {
    UringError::InvalidInput.report("uring.op.payload.recvmsg_multishot", detail)
}

/// Parse one kernel-selected `recvmsg` buffer.
pub(crate) fn parse_udp_recvmsg<'a>(
    bytes: &'a [u8],
    message: &libc::msghdr,
    datagram_capacity: usize,
) -> UringResult<ParsedUdpRecvMsg<'a>> {
    let header_len = mem::size_of::<IoUringRecvMsgOut>();
    if bytes.len() < header_len {
        return Err(malformed_recvmsg(
            "selected buffer is shorter than recvmsg metadata",
        ));
    }

    let output = unsafe { ptr::read_unaligned(bytes.as_ptr().cast::<IoUringRecvMsgOut>()) };
    let name_capacity = usize::try_from(message.msg_namelen)
        .map_err(|_| malformed_recvmsg("recvmsg name capacity does not fit usize"))?;
    let control_capacity = message.msg_controllen;
    if name_capacity > mem::size_of::<libc::sockaddr_storage>() {
        return Err(malformed_recvmsg(
            "recvmsg name capacity exceeds sockaddr_storage",
        ));
    }
    let name_start = header_len;
    let control_start = name_start
        .checked_add(name_capacity)
        .ok_or_else(|| malformed_recvmsg("recvmsg name offset overflowed"))?;
    let payload_start = control_start
        .checked_add(control_capacity)
        .ok_or_else(|| malformed_recvmsg("recvmsg control offset overflowed"))?;
    if payload_start > bytes.len() {
        return Err(malformed_recvmsg(
            "recvmsg metadata exceeds selected buffer",
        ));
    }

    let name_len = usize::try_from(output.namelen)
        .map_err(|_| malformed_recvmsg("recvmsg name length does not fit usize"))?;
    let control_len = usize::try_from(output.controllen)
        .map_err(|_| malformed_recvmsg("recvmsg control length does not fit usize"))?;
    if name_len > name_capacity {
        return Err(malformed_recvmsg(
            "recvmsg name length exceeds configured capacity",
        ));
    }
    if control_len > control_capacity {
        return Err(malformed_recvmsg(
            "recvmsg control length exceeds configured capacity",
        ));
    }

    let payload_len = usize::try_from(output.payloadlen)
        .map_err(|_| malformed_recvmsg("recvmsg payload length does not fit usize"))?;
    if payload_len > datagram_capacity {
        return Err(UringError::InvalidInput
            .report(
                "uring.op.payload.recvmsg_multishot",
                "recvmsg payload exceeds configured datagram capacity",
            )
            .with_ctx("payload_length", payload_len)
            .with_ctx("datagram_capacity", datagram_capacity));
    }
    let payload_end = payload_start
        .checked_add(payload_len)
        .ok_or_else(|| malformed_recvmsg("recvmsg payload offset overflowed"))?;
    if payload_end > bytes.len() {
        return Err(malformed_recvmsg("recvmsg payload exceeds selected buffer"));
    }

    let addr = to_socket_addr(&bytes[name_start..name_start + name_len])?;
    Ok(ParsedUdpRecvMsg {
        addr,
        payload: &bytes[payload_start..payload_end],
        flags: output.flags,
    })
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

#[cfg(test)]
mod tests {
    use super::*;
    use veloq_buf::FixedBuf;
    use veloq_std::{
        mem::{align_of, offset_of, size_of},
        net::{Ipv4Addr, SocketAddr, SocketAddrV4},
        num::NonZeroUsize,
    };

    fn recvmsg_buffer(addr: SocketAddr, payload: &[u8], flags: u32) -> FixedBuf {
        let (storage, addr_len) = socket_addr_to_storage(addr);
        let name_capacity = size_of::<libc::sockaddr_storage>();
        let total_len = size_of::<IoUringRecvMsgOut>() + name_capacity + payload.len();
        let mut buffer = FixedBuf::alloc_heap(
            NonZeroUsize::new(total_len.max(1)).expect("fixture capacity is non-zero"),
            total_len,
        )
        .expect("fixture buffer allocation should succeed");
        let bytes = buffer.as_slice_mut();
        let output = IoUringRecvMsgOut {
            namelen: addr_len,
            controllen: 0,
            payloadlen: payload.len() as u32,
            flags,
        };
        unsafe {
            ptr::write_unaligned(bytes.as_mut_ptr().cast::<IoUringRecvMsgOut>(), output);
        }
        let name_start = size_of::<IoUringRecvMsgOut>();
        let name_bytes = unsafe {
            core::slice::from_raw_parts(ptr::addr_of!(storage.0).cast::<u8>(), name_capacity)
        };
        bytes[name_start..name_start + name_capacity].copy_from_slice(name_bytes);
        bytes[name_start + name_capacity..].copy_from_slice(payload);
        buffer
    }

    #[test]
    fn recvmsg_payload_view_has_kernel_abi_layout() {
        assert_eq!(size_of::<IoUringRecvMsgOut>(), 16);
        assert_eq!(align_of::<IoUringRecvMsgOut>(), 4);
        assert_eq!(offset_of!(IoUringRecvMsgOut, namelen), 0);
        assert_eq!(offset_of!(IoUringRecvMsgOut, controllen), 4);
        assert_eq!(offset_of!(IoUringRecvMsgOut, payloadlen), 8);
        assert_eq!(offset_of!(IoUringRecvMsgOut, flags), 12);
    }

    #[test]
    fn udp_recv_multi_payload_uses_stable_zero_length_iovec() {
        let mut payload = UdpRecvMultiPayload::new();
        let message = unsafe { Pin::new_unchecked(&mut payload).init_recv_multi() };
        let message = message.msghdr;
        assert!(message.msg_name.is_null());
        assert!(!message.msg_iov.is_null());
        assert_eq!(message.msg_iovlen, 0);
        assert!(message.msg_control.is_null());
        assert_eq!(
            message.msg_namelen as usize,
            size_of::<libc::sockaddr_storage>()
        );
        assert_eq!(message.msg_controllen, 0);
    }

    #[test]
    fn parser_returns_independent_ipv4_source_and_payload() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9001));
        let buffer = recvmsg_buffer(addr, b"datagram", 0);
        let payload = UdpRecvMultiPayload::new();
        let message = payload.message();
        let parsed =
            parse_udp_recvmsg(buffer.as_slice(), message, 1024).expect("valid recvmsg fixture");
        assert_eq!(parsed.addr, addr);
        assert_eq!(parsed.payload, b"datagram");
        assert_eq!(parsed.flags, 0);
    }

    #[test]
    fn parser_rejects_bad_lengths_and_preserves_truncation_flag() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9002));
        let mut buffer = recvmsg_buffer(addr, b"payload", libc::MSG_TRUNC as u32);
        let payload = UdpRecvMultiPayload::new();
        let message = payload.message();
        let parsed =
            parse_udp_recvmsg(buffer.as_slice(), message, 1024).expect("valid truncated fixture");
        assert_eq!(parsed.flags, libc::MSG_TRUNC as u32);

        let bytes = buffer.as_slice_mut();
        unsafe {
            (*bytes.as_mut_ptr().cast::<IoUringRecvMsgOut>()).namelen =
                (size_of::<libc::sockaddr_storage>() + 1) as u32;
        }
        assert!(parse_udp_recvmsg(buffer.as_slice(), message, 1024).is_err());

        let mut short = recvmsg_buffer(addr, b"payload", 0);
        short.set_len(size_of::<IoUringRecvMsgOut>());
        assert!(parse_udp_recvmsg(short.as_slice(), message, 1024).is_err());
    }

    #[test]
    fn parser_rejects_payload_capacity_and_buffer_overflow() {
        let addr = SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 9003));
        let buffer = recvmsg_buffer(addr, b"payload", 0);
        let payload = UdpRecvMultiPayload::new();
        let message = payload.message();
        assert!(parse_udp_recvmsg(buffer.as_slice(), message, 3).is_err());

        let mut overflow = recvmsg_buffer(addr, b"payload", 0);
        unsafe {
            (*overflow
                .as_slice_mut()
                .as_mut_ptr()
                .cast::<IoUringRecvMsgOut>())
            .payloadlen = 1000;
        }
        assert!(parse_udp_recvmsg(overflow.as_slice(), message, 1024).is_err());
    }
}
