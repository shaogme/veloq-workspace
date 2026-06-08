mod blocking;

pub(crate) use blocking::{
    completion_cleanup_close_file, submit_close, submit_fallocate, submit_fallocate_raw,
    submit_fsync, submit_fsync_raw, submit_open, submit_sync_range, submit_sync_range_raw,
};

use veloq_buf::FixedBuf;
use veloq_driver_core::op::{BufIoRangeError, checked_read_buf_range, checked_write_buf_range};

use diagweave::prelude::*;

use crate::error::{IocpError, IocpResult};
use crate::op::submit::{
    SubmissionResult, ensure_iocp_association, iocp_submit_read, iocp_submit_write,
    mark_header_in_flight, resolve_fd_handle, resolve_registered_raw_file, unpack_kernel_ref,
};
use crate::op::{
    KernelRef, OverlappedEntry, ReadFixed, ReadRaw, SubmitContext, WriteFixed, WriteRaw,
};

// ============================================================================
// Macros
// ============================================================================

fn invalid_buf_io_range(scope: &'static str, err: BufIoRangeError) -> Report<IocpError> {
    IocpError::InvalidInput
        .report(scope, err.note())
        .with_ctx("buffer_offset", err.buffer_offset())
        .with_ctx("buffer_length", err.buffer_length())
        .with_ctx("buffer_capacity", err.buffer_capacity())
        .with_ctx("buffer_bound", err.buffer_bound())
        .with_ctx("buffer_bound_kind", err.buffer_bound_kind().name())
        .with_ctx("submission_length", err.submission_length())
}

fn checked_file_read_range(
    buf: &mut FixedBuf,
    buf_offset: usize,
    scope: &'static str,
) -> IocpResult<(*mut u8, u32)> {
    checked_read_buf_range(buf, buf_offset).map_err(|err| invalid_buf_io_range(scope, err))
}

fn checked_file_write_range(
    buf: &mut FixedBuf,
    buf_offset: usize,
    scope: &'static str,
) -> IocpResult<(*mut u8, u32)> {
    checked_write_buf_range(buf, buf_offset)
        .map(|(ptr, len)| (ptr as *mut u8, len))
        .map_err(|err| invalid_buf_io_range(scope, err))
}

macro_rules! submit_io_op {
    ($fn_name:ident, $field_type:ty, $wrapper_fn:ident, offset, $range_fn:ident) => {
        pub(crate) fn $fn_name(
            header: &mut OverlappedEntry,
            payload: &mut KernelRef<$field_type>,
            ctx: &mut SubmitContext,
        ) -> IocpResult<SubmissionResult> {
            // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
            let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };

            overlapped.set_offset(val.offset);

            let raw = resolve_fd_handle(&val.fd, &*ctx.registered_slots)?;
            header.resolved_handle = Some(raw);
            ensure_iocp_association(&val.fd, raw, ctx.port.as_ref(), &mut *ctx.registered_slots)
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("fd_fixed_index", val.fd.fixed_index())
                .with_ctx("fd_generation", val.fd.generation())
                .with_ctx("handle_raw", raw.as_handle() as usize)
                .with_ctx("user_data", header.token.index())
                .with_ctx("generation", header.token.generation())
                .with_ctx("offset", val.offset)
                .with_ctx("buffer_length", val.buf.len())
                .with_ctx("buffer_capacity", val.buf.capacity())?;

            // Depending on ReadFile/WriteFile sig: (handle, buf, len, bytes, overlapped)
            let (ptr, len) = $range_fn(&mut val.buf, val.buf_offset, stringify!($fn_name))?;
            let raw_handle = crate::config::RawHandle::new(raw);
            let handle = raw_handle.borrow();
            // SAFETY: Calling Win32 ReadFile/WriteFile via wrapper with valid parameters.
            let submit_res = unsafe { $wrapper_fn(handle, ptr as _, len, ctx.overlapped) }
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("fd_fixed_index", val.fd.fixed_index())
                .with_ctx("fd_generation", val.fd.generation())
                .with_ctx("handle_raw", raw.as_handle() as usize)
                .with_ctx("user_data", header.token.index())
                .with_ctx("generation", header.token.generation())
                .with_ctx("offset", val.offset)
                .with_ctx("buffer_offset", val.buf_offset)
                .with_ctx("buffer_length", len)
                .with_ctx("buffer_capacity", val.buf.capacity())
                .attach_note("file syscall submit failed");
            mark_header_in_flight(header, submit_res)
        }
    };
}

macro_rules! submit_raw_io_op {
    ($fn_name:ident, $field_type:ty, $wrapper_fn:ident, offset, $range_fn:ident) => {
        pub(crate) fn $fn_name(
            header: &mut OverlappedEntry,
            payload: &mut KernelRef<$field_type>,
            ctx: &mut SubmitContext,
        ) -> IocpResult<SubmissionResult> {
            // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
            let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };

            overlapped.set_offset(val.offset);

            let (fd, raw) = resolve_registered_raw_file(val.fd, &*ctx.registered_slots)?;
            header.resolved_handle = Some(raw);
            ensure_iocp_association(&fd, raw, ctx.port.as_ref(), &mut *ctx.registered_slots)
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .with_ctx("handle_raw", raw.as_handle() as usize)
                .with_ctx("user_data", header.token.index())
                .with_ctx("generation", header.token.generation())
                .with_ctx("offset", val.offset)
                .with_ctx("buffer_length", val.buf.len())
                .with_ctx("buffer_capacity", val.buf.capacity())?;

            let (ptr, len) = $range_fn(&mut val.buf, val.buf_offset, stringify!($fn_name))?;
            let raw_handle = crate::config::RawHandle::new(raw);
            let handle = raw_handle.borrow();
            let submit_res = unsafe { $wrapper_fn(handle, ptr as _, len, ctx.overlapped) }
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("handle_raw", raw.as_handle() as usize)
                .with_ctx("user_data", header.token.index())
                .with_ctx("generation", header.token.generation())
                .with_ctx("offset", val.offset)
                .with_ctx("buffer_offset", val.buf_offset)
                .with_ctx("buffer_length", len)
                .with_ctx("buffer_capacity", val.buf.capacity())
                .attach_note("file syscall submit failed");
            mark_header_in_flight(header, submit_res)
        }
    };
}

// ============================================================================
// Read/Write Implementation
// ============================================================================

submit_io_op!(
    submit_read_fixed,
    ReadFixed,
    iocp_submit_read,
    offset,
    checked_file_read_range
);
submit_raw_io_op!(
    submit_read_raw,
    ReadRaw,
    iocp_submit_read,
    offset,
    checked_file_read_range
);

submit_io_op!(
    submit_write_fixed,
    WriteFixed,
    iocp_submit_write,
    offset,
    checked_file_write_range
);
submit_raw_io_op!(
    submit_write_raw,
    WriteRaw,
    iocp_submit_write,
    offset,
    checked_file_write_range
);
