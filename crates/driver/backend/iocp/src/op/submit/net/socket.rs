use diagweave::prelude::*;
use veloq_buf::{BufIoRangeError, FixedBuf, RegistrationCapability};
use veloq_std::{
    io,
    mem::{ManuallyDrop, size_of},
    ptr,
};
use windows_sys::Win32::Networking::WinSock::{
    SOCKADDR, SOCKADDR_STORAGE, SOCKET, WSA_IO_PENDING, WSABUF, WSAGetLastError, WSARecv,
    WSARecvFrom, WSASend,
};

use crate::{
    config::{IoFd, RawHandle},
    error::{IocpError, IocpResult},
    net::SockAddrStorage,
    op::{
        OverlappedEntry, SubmitContext,
        submit::{SubmissionOps, SubmissionResult},
    },
    rio::{RioError, SocketInflightGuard},
    win32::{Overlapped, SafeSocket},
};

pub(super) struct SocketSubmission;

pub(super) struct RawSocketRequest<'request, 'ctx> {
    header: &'request mut OverlappedEntry,
    fd: IoFd,
    raw: RawHandle,
    ctx: &'request mut SubmitContext<'ctx>,
}

impl SocketSubmission {
    pub(super) fn invalid_buffer_range(
        operation: &'static str,
        error: BufIoRangeError,
    ) -> Report<IocpError> {
        IocpError::InvalidInput
            .report(operation, error.note())
            .with_ctx("buffer_offset", error.buffer_offset())
            .with_ctx("buffer_length", error.buffer_length())
            .with_ctx("buffer_capacity", error.buffer_capacity())
            .with_ctx("buffer_bound", error.buffer_bound())
            .with_ctx("buffer_bound_kind", error.buffer_bound_kind().name())
            .with_ctx("submission_length", error.submission_length())
    }

    pub(super) fn uses_rio(raw: RawHandle, buffer: &FixedBuf) -> bool {
        raw.raw().uses_rio()
            && matches!(
                buffer.resolve_region_info().registration,
                RegistrationCapability::RegisteredChunk(_)
            )
    }

    pub(super) fn with_borrowed_socket<T>(
        raw: SOCKET,
        f: impl FnOnce(&SafeSocket) -> IocpResult<T>,
    ) -> IocpResult<T> {
        let socket = ManuallyDrop::new(SafeSocket::new(raw));
        f(&socket)
    }

    pub(super) fn mark_socket_header_in_flight(
        header: &mut OverlappedEntry,
        inflight: SocketInflightGuard<'_>,
        res: IocpResult<SubmissionResult>,
    ) -> IocpResult<SubmissionResult> {
        let res = SubmissionOps::mark_header_in_flight(header, res);
        if matches!(res, Ok(SubmissionResult::Pending)) {
            debug_assert!(
                header.socket_inflight().is_none(),
                "socket inflight token already attached to op header"
            );
            *header.socket_inflight_mut() = Some(inflight.commit());
        }
        res
    }

    pub(super) fn wsa_submit_result(
        result: i32,
        operation: &'static str,
    ) -> IocpResult<SubmissionResult> {
        if result == 0 {
            return Ok(SubmissionResult::Pending);
        }
        let error = unsafe { WSAGetLastError() };
        if error == WSA_IO_PENDING {
            Ok(SubmissionResult::Pending)
        } else {
            Err(IocpError::Submission.io_report(operation, io::Error::from_raw_os_error(error)))
        }
    }

    pub(super) fn submit_wsa_recv(
        request: RawSocketRequest<'_, '_>,
        buffer: &mut FixedBuf,
        offset: usize,
        operation: &'static str,
    ) -> IocpResult<SubmissionResult> {
        let (ptr, len) = buffer
            .checked_read_range(offset)
            .map_err(|error| Self::invalid_buffer_range(operation, error))?;
        let wsabuf = WSABUF { len, buf: ptr };
        request.submit(|socket, overlapped| {
            let mut bytes_received = 0;
            let mut flags = 0;
            let result = unsafe {
                WSARecv(
                    socket,
                    &wsabuf,
                    1,
                    &mut bytes_received,
                    &mut flags,
                    overlapped.cast(),
                    None,
                )
            };
            Self::wsa_submit_result(result, operation)
        })
    }

    pub(super) fn submit_wsa_send(
        request: RawSocketRequest<'_, '_>,
        buffer: &FixedBuf,
        offset: usize,
        operation: &'static str,
    ) -> IocpResult<SubmissionResult> {
        let (ptr, len) = buffer
            .checked_write_range(offset)
            .map_err(|error| Self::invalid_buffer_range(operation, error))?;
        let wsabuf = WSABUF {
            len,
            buf: ptr.cast_mut(),
        };
        request.submit(|socket, overlapped| {
            let mut bytes_sent = 0;
            let result = unsafe {
                WSASend(
                    socket,
                    &wsabuf,
                    1,
                    &mut bytes_sent,
                    0,
                    overlapped.cast(),
                    None,
                )
            };
            Self::wsa_submit_result(result, operation)
        })
    }

    pub(super) fn submit_wsa_recv_from(
        request: RawSocketRequest<'_, '_>,
        buffer: &mut FixedBuf,
        address: &mut SockAddrStorage,
        address_len: &mut i32,
    ) -> IocpResult<SubmissionResult> {
        let (ptr, len) = buffer
            .checked_read_range(0)
            .map_err(|error| Self::invalid_buffer_range("udp_recv_multi", error))?;
        let wsabuf = WSABUF { len, buf: ptr };
        *address = SockAddrStorage::default();
        *address_len = size_of::<SOCKADDR_STORAGE>() as i32;
        request.submit(|socket, overlapped| {
            let mut flags = 0;
            let result = unsafe {
                WSARecvFrom(
                    socket,
                    &wsabuf,
                    1,
                    ptr::null_mut(),
                    &mut flags,
                    address.as_raw_mut() as *mut SOCKADDR_STORAGE as *mut SOCKADDR,
                    address_len,
                    overlapped.cast(),
                    None,
                )
            };
            Self::wsa_submit_result(result, "udp_recv_multi")
        })
    }
}

impl<'request, 'ctx> RawSocketRequest<'request, 'ctx> {
    pub(super) fn new(
        header: &'request mut OverlappedEntry,
        fd: IoFd,
        raw: RawHandle,
        ctx: &'request mut SubmitContext<'ctx>,
    ) -> Self {
        Self {
            header,
            fd,
            raw,
            ctx,
        }
    }

    pub(super) fn submit(
        self,
        submit: impl FnOnce(SOCKET, *mut Overlapped) -> IocpResult<SubmissionResult>,
    ) -> IocpResult<SubmissionResult> {
        Self::validate_overlapped_socket(self.raw)?;
        SubmissionOps::ensure_iocp_association(
            &self.fd,
            self.raw,
            self.ctx.port.as_ref(),
            &mut *self.ctx.registered_slots,
        )?;
        let inflight = self
            .ctx
            .rio
            .try_acquire_socket_inflight_guard(self.raw.raw().actor_key())
            .attach_note("failed to acquire socket inflight slot before overlapped I/O")
            .trans()?;
        SocketSubmission::mark_socket_header_in_flight(
            self.header,
            inflight,
            submit(self.raw.raw().as_socket(), self.ctx.overlapped),
        )
    }

    fn validate_overlapped_socket(raw: RawHandle) -> IocpResult<()> {
        if raw.raw().uses_rio() {
            return IocpError::Rio(RioError::BufferRegistrationRequired)
                .with_ctx("socket_mode", "rio")
                .attach_note("RIO sockets require registered buffers for network submissions");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{RawSocketRequest, SocketSubmission};
    use crate::{
        config::{IocpHandle, RawHandle},
        error::IocpError,
        rio::RioError,
    };
    use veloq_buf::FixedBuf;
    use veloq_std::{num::NonZeroUsize, ptr::null_mut};

    #[test]
    fn unregistered_buffer_uses_overlapped_only_for_regular_socket() {
        let regular = RawHandle::new(IocpHandle::for_socket(null_mut()));
        let rio = RawHandle::new(IocpHandle::for_rio_socket(null_mut()));
        let buffer = FixedBuf::alloc_heap(NonZeroUsize::new(8).expect("non-zero capacity"), 0)
            .expect("heap buffer allocation should succeed");

        assert!(!SocketSubmission::uses_rio(regular, &buffer));
        assert!(!SocketSubmission::uses_rio(rio, &buffer));
        assert!(RawSocketRequest::validate_overlapped_socket(regular).is_ok());
        let error = RawSocketRequest::validate_overlapped_socket(rio)
            .expect_err("RIO socket must reject overlapped I/O without registration");
        assert_eq!(
            *error.inner(),
            IocpError::Rio(RioError::BufferRegistrationRequired)
        );
    }
}
