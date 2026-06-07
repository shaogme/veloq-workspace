use veloq_blocking::BlockingTask;
use veloq_blocking::blocking_ops::windows::{BlockingOps, CompletionInfo};
use veloq_buf::FixedBuf;

use diagweave::prelude::*;

use crate::error::{IocpError, IocpResult};
use crate::op::submit::common::{
    SubmissionResult, ensure_iocp_association, iocp_submit_read, iocp_submit_write,
    mark_header_in_flight, resolve_fd_borrowed, resolve_fd_handle, unpack_kernel_ref,
};
use crate::op::{
    Close, Fallocate, FallocateRaw, Fsync, FsyncRaw, KernelRef, OpenPayload, OverlappedEntry,
    ReadFixed, ReadRaw, SubmitContext, SyncFileRange, SyncFileRangeRaw, WriteFixed, WriteRaw,
};

// ============================================================================
// Macros
// ============================================================================

macro_rules! submit_io_op {
    ($fn_name:ident, $field_type:ty, $wrapper_fn:ident, offset, $ptr_fn:expr) => {
        pub(crate) fn $fn_name(
            header: &mut OverlappedEntry,
            payload: &mut KernelRef<$field_type>,
            ctx: &mut SubmitContext,
        ) -> IocpResult<SubmissionResult> {
            // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
            let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };

            overlapped.set_offset(val.offset);

            let handle = resolve_fd_borrowed(&val.fd, ctx.registered_files, ctx.file_generations)?;
            header.resolved_handle = Some(resolve_fd_handle(
                &val.fd,
                ctx.registered_files,
                ctx.file_generations,
            )?);
            ensure_iocp_association(handle, ctx.port)
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("fd_fixed_index", val.fd.fixed_index())
                .with_ctx("fd_generation", val.fd.generation())
                .with_ctx("handle_raw", handle.raw().as_handle() as usize)
                .with_ctx("user_data", header.user_data)
                .with_ctx("generation", header.generation)
                .with_ctx("offset", val.offset)
                .with_ctx("buffer_length", val.buf.len())?;

            // Depending on ReadFile/WriteFile sig: (handle, buf, len, bytes, overlapped)
            if val.buf_offset > val.buf.len() {
                return IocpError::InvalidInput
                    .push_ctx("scope", stringify!($fn_name))
                    .with_ctx("buffer_offset", val.buf_offset)
                    .with_ctx("buffer_length", val.buf.len())
                    .attach_note("buffer offset exceeds buffer length");
            }
            let get_ptr: fn(&mut _) -> *mut u8 = $ptr_fn;
            // SAFETY: buf_offset <= buf.len() is verified above.
            let ptr = unsafe { get_ptr(&mut val.buf).add(val.buf_offset) };
            let len = (val.buf.len().saturating_sub(val.buf_offset)) as u32;
            // SAFETY: Calling Win32 ReadFile/WriteFile via wrapper with valid parameters.
            let submit_res = unsafe { $wrapper_fn(handle, ptr as _, len, ctx.overlapped) }
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("fd_fixed_index", val.fd.fixed_index())
                .with_ctx("fd_generation", val.fd.generation())
                .with_ctx("handle_raw", handle.raw().as_handle() as usize)
                .with_ctx("user_data", header.user_data)
                .with_ctx("generation", header.generation)
                .with_ctx("offset", val.offset)
                .with_ctx("buffer_offset", val.buf_offset)
                .with_ctx("buffer_length", len)
                .attach_note("file syscall submit failed");
            mark_header_in_flight(header, submit_res)
        }
    };
}

macro_rules! submit_raw_io_op {
    ($fn_name:ident, $field_type:ty, $wrapper_fn:ident, offset, $ptr_fn:expr) => {
        pub(crate) fn $fn_name(
            header: &mut OverlappedEntry,
            payload: &mut KernelRef<$field_type>,
            ctx: &mut SubmitContext,
        ) -> IocpResult<SubmissionResult> {
            // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
            let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };

            overlapped.set_offset(val.offset);

            header.resolved_handle = Some(val.fd);
            let raw_handle = crate::config::RawHandle::new(val.fd);
            let handle = raw_handle.borrow();
            ensure_iocp_association(handle, ctx.port)
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("handle_raw", handle.raw().as_handle() as usize)
                .with_ctx("user_data", header.user_data)
                .with_ctx("generation", header.generation)
                .with_ctx("offset", val.offset)
                .with_ctx("buffer_length", val.buf.len())?;

            if val.buf_offset > val.buf.len() {
                return IocpError::InvalidInput
                    .push_ctx("scope", stringify!($fn_name))
                    .with_ctx("buffer_offset", val.buf_offset)
                    .with_ctx("buffer_length", val.buf.len())
                    .attach_note("buffer offset exceeds buffer length");
            }
            let get_ptr: fn(&mut _) -> *mut u8 = $ptr_fn;
            let ptr = unsafe { get_ptr(&mut val.buf).add(val.buf_offset) };
            let len = (val.buf.len().saturating_sub(val.buf_offset)) as u32;
            let submit_res = unsafe { $wrapper_fn(handle, ptr as _, len, ctx.overlapped) }
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("handle_raw", handle.raw().as_handle() as usize)
                .with_ctx("user_data", header.user_data)
                .with_ctx("generation", header.generation)
                .with_ctx("offset", val.offset)
                .with_ctx("buffer_offset", val.buf_offset)
                .with_ctx("buffer_length", len)
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
    |b: &mut FixedBuf| b.as_mut_ptr()
);
submit_raw_io_op!(
    submit_read_raw,
    ReadRaw,
    iocp_submit_read,
    offset,
    |b: &mut FixedBuf| b.as_mut_ptr()
);

submit_io_op!(
    submit_write_fixed,
    WriteFixed,
    iocp_submit_write,
    offset,
    |b: &mut FixedBuf| b.as_slice().as_ptr() as *mut u8
);
submit_raw_io_op!(
    submit_write_raw,
    WriteRaw,
    iocp_submit_write,
    offset,
    |b: &mut FixedBuf| b.as_slice().as_ptr() as *mut u8
);

// ============================================================================
// Blocking File Operations
// ============================================================================

fn make_blocking_completion(ctx: &SubmitContext<'_>, user_data: usize) -> CompletionInfo {
    CompletionInfo {
        port: ctx.port.as_raw() as usize,
        user_data,
        overlapped: ctx.overlapped as usize,
        store_result: crate::op::overlapped::store_blocking_result,
        clear_result: crate::op::overlapped::clear_blocking_result,
    }
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) fn submit_open(
    header: &mut OverlappedEntry,
    payload: &mut OpenPayload,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let path_ptr = user.path.as_slice().as_ptr() as usize;

    let user_data = header.user_data;
    let completion = make_blocking_completion(ctx, user_data);

    let op = BlockingOps::Open {
        path_ptr,
        flags: user.flags,
        mode: user.mode,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) fn submit_close(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Close>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files, ctx.file_generations)?;
    header.resolved_handle = Some(resolve_fd_handle(
        &user.fd,
        ctx.registered_files,
        ctx.file_generations,
    )?);

    let user_data = header.user_data;
    let completion = make_blocking_completion(ctx, user_data);

    let op = BlockingOps::Close {
        handle: handle.raw().as_handle() as usize,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) fn submit_fsync(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Fsync>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files, ctx.file_generations)?;
    header.resolved_handle = Some(resolve_fd_handle(
        &user.fd,
        ctx.registered_files,
        ctx.file_generations,
    )?);

    let user_data = header.user_data;
    let completion = make_blocking_completion(ctx, user_data);

    let op = BlockingOps::Fsync {
        handle: handle.raw().as_handle() as usize,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}

pub(crate) fn submit_fsync_raw(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<FsyncRaw>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    let user = unsafe { payload.user.as_ref() };
    header.resolved_handle = Some(user.fd);
    let raw_handle = crate::config::RawHandle::new(user.fd);
    let handle = raw_handle.borrow();

    let user_data = header.user_data;
    let completion = make_blocking_completion(ctx, user_data);

    let op = BlockingOps::Fsync {
        handle: handle.raw().as_handle() as usize,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) fn submit_sync_range(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<SyncFileRange>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files, ctx.file_generations)?;
    header.resolved_handle = Some(resolve_fd_handle(
        &user.fd,
        ctx.registered_files,
        ctx.file_generations,
    )?);

    let user_data = header.user_data;
    let completion = make_blocking_completion(ctx, user_data);

    let op = BlockingOps::SyncFileRange {
        handle: handle.raw().as_handle() as usize,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}

pub(crate) fn submit_sync_range_raw(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<SyncFileRangeRaw>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    let user = unsafe { payload.user.as_ref() };
    header.resolved_handle = Some(user.fd);
    let raw_handle = crate::config::RawHandle::new(user.fd);
    let handle = raw_handle.borrow();

    let user_data = header.user_data;
    let completion = make_blocking_completion(ctx, user_data);

    let op = BlockingOps::SyncFileRange {
        handle: handle.raw().as_handle() as usize,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) fn submit_fallocate(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Fallocate>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files, ctx.file_generations)?;
    header.resolved_handle = Some(resolve_fd_handle(
        &user.fd,
        ctx.registered_files,
        ctx.file_generations,
    )?);

    let user_data = header.user_data;
    let completion = make_blocking_completion(ctx, user_data);

    let op = BlockingOps::Fallocate {
        handle: handle.raw().as_handle() as usize,
        mode: user.mode,
        offset: user.offset,
        len: user.len,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}

pub(crate) fn submit_fallocate_raw(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<FallocateRaw>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    let user = unsafe { payload.user.as_ref() };
    header.resolved_handle = Some(user.fd);
    let raw_handle = crate::config::RawHandle::new(user.fd);
    let handle = raw_handle.borrow();

    let user_data = header.user_data;
    let completion = make_blocking_completion(ctx, user_data);

    let op = BlockingOps::Fallocate {
        handle: handle.raw().as_handle() as usize,
        mode: user.mode,
        offset: user.offset,
        len: user.len,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}
