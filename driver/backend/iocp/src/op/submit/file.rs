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

            let handle = resolve_fd_borrowed(&val.fd, ctx.registered_files)?;
            header.resolved_handle = Some(resolve_fd_handle(&val.fd, ctx.registered_files)?);
            ensure_iocp_association(
                handle,
                ctx.port,
                format!(
                    "{}: CreateIoCompletionPort failed: fd={:?}, handle={:?}, user_data={}, generation={}, offset={}, len={}",
                    stringify!($fn_name),
                    val.fd,
                    handle.raw().as_handle(),
                    header.user_data,
                    header.generation,
                    val.offset,
                    val.buf.len()
                ),
            )?;

            // Depending on ReadFile/WriteFile sig: (handle, buf, len, bytes, overlapped)
            if val.buf_offset > val.buf.len() {
                return IocpError::InvalidInput.attach_note(format!(
                    "{}: buf_offset {} exceeds buffer length {}",
                    stringify!($fn_name),
                    val.buf_offset,
                    val.buf.len()
                ));
            }
            let get_ptr: fn(&mut _) -> *mut u8 = $ptr_fn;
            // SAFETY: buf_offset <= buf.len() is verified above.
            let ptr = unsafe { get_ptr(&mut val.buf).add(val.buf_offset) };
            let len = (val.buf.len().saturating_sub(val.buf_offset)) as u32;
            // SAFETY: Calling Win32 ReadFile/WriteFile via wrapper with valid parameters.
            let submit_res = unsafe { $wrapper_fn(handle, ptr as _, len, ctx.overlapped) }
                .attach_note(format!(
                    "{}: syscall failed: fd={:?}, handle={:?}, user_data={}, generation={}, offset={}, buf_offset={}, len={}",
                    stringify!($fn_name),
                    val.fd,
                    handle.raw().as_handle(),
                    header.user_data,
                    header.generation,
                    val.offset,
                    val.buf_offset,
                    len
                ));
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
            ensure_iocp_association(
                handle,
                ctx.port,
                format!(
                    "{}: CreateIoCompletionPort failed: handle={:?}, user_data={}, generation={}, offset={}, len={}",
                    stringify!($fn_name),
                    handle.raw().as_handle(),
                    header.user_data,
                    header.generation,
                    val.offset,
                    val.buf.len()
                ),
            )?;

            if val.buf_offset > val.buf.len() {
                return IocpError::InvalidInput.attach_note(format!(
                    "{}: buf_offset {} exceeds buffer length {}",
                    stringify!($fn_name),
                    val.buf_offset,
                    val.buf.len()
                ));
            }
            let get_ptr: fn(&mut _) -> *mut u8 = $ptr_fn;
            let ptr = unsafe { get_ptr(&mut val.buf).add(val.buf_offset) };
            let len = (val.buf.len().saturating_sub(val.buf_offset)) as u32;
            let submit_res = unsafe { $wrapper_fn(handle, ptr as _, len, ctx.overlapped) }
                .map_err(|e| e.attach_note(format!(
                    "{}: syscall failed: handle={:?}, user_data={}, generation={}, offset={}, buf_offset={}, len={}",
                    stringify!($fn_name),
                    handle.raw().as_handle(),
                    header.user_data,
                    header.generation,
                    val.offset,
                    val.buf_offset,
                    len
                )));
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
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&user.fd, ctx.registered_files)?);

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
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&user.fd, ctx.registered_files)?);

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
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&user.fd, ctx.registered_files)?);

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
    let handle = resolve_fd_borrowed(&user.fd, ctx.registered_files)?;
    header.resolved_handle = Some(resolve_fd_handle(&user.fd, ctx.registered_files)?);

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
