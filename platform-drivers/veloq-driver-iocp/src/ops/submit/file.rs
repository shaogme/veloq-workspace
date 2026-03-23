use std::io;
use veloq_blocking::BlockingTask;
use veloq_blocking::blocking_ops::windows::{BlockingOps, CompletionInfo};
use veloq_buf::FixedBuf;

use crate::common::IocpErrorContext;
use crate::ops::submit::common::{
    SubmissionResult, ensure_iocp_association, iocp_submit_read, iocp_submit_write, resolve_fd,
    unpack_kernel_ref,
};
use crate::ops::{
    Close, Fallocate, Fsync, KernelRef, OpenPayload, OverlappedEntry, ReadFixed, SubmitContext,
    SyncFileRange, WriteFixed,
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
        ) -> io::Result<SubmissionResult> {
            // SAFETY: vtable submit shim guarantees payload/overlapped pointer validity.
            let (val, overlapped) = unsafe { unpack_kernel_ref(payload, ctx.overlapped) };

            overlapped.set_offset(val.offset);

            let raw_handle = resolve_fd(val.fd, ctx.registered_files)?;
            let handle = raw_handle.handle;
            ensure_iocp_association(
                handle,
                ctx.port,
                format!(
                    "{}: CreateIoCompletionPort failed: fd={:?}, handle={:?}, user_data={}, generation={}, offset={}, len={}",
                    stringify!($fn_name),
                    val.fd,
                    handle,
                    header.user_data,
                    header.generation,
                    val.offset,
                    val.buf.len()
                ),
            )?;

            // Depending on ReadFile/WriteFile sig: (handle, buf, len, bytes, overlapped)
            let get_ptr: fn(&mut _) -> *mut u8 = $ptr_fn;
            let ptr = unsafe { get_ptr(&mut val.buf).add(val.buf_offset) };
            let len = (val.buf.len().saturating_sub(val.buf_offset)) as u32;

             // SAFETY: Calling Win32 ReadFile/WriteFile via wrapper with valid parameters.
             unsafe { $wrapper_fn(handle, ptr as _, len, ctx.overlapped) }.map_err(|e| {
                 crate::common::io_error(
                     IocpErrorContext::Submission,
                     e,
                     format!(
                         "{}: syscall failed: fd={:?}, handle={:?}, user_data={}, generation={}, offset={}, buf_offset={}, len={}",
                         stringify!($fn_name),
                         val.fd,
                         handle,
                         header.user_data,
                         header.generation,
                         val.offset,
                         val.buf_offset,
                         len
                     ),
                 )
             })
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

submit_io_op!(
    submit_write_fixed,
    WriteFixed,
    iocp_submit_write,
    offset,
    |b: &mut FixedBuf| b.as_slice().as_ptr() as *mut u8
);

// ============================================================================
// Blocking File Operations
// ============================================================================

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) fn submit_open(
    header: &mut OverlappedEntry,
    payload: &mut OpenPayload,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let path_ptr = user.path.as_slice().as_ptr() as usize;

    let user_data = header.user_data;

    let completion = CompletionInfo {
        port: ctx.port.as_raw() as usize,
        user_data,
        overlapped: ctx.overlapped as usize,
    };

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
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let raw_handle = resolve_fd(user.fd, ctx.registered_files)?;
    let handle = raw_handle.handle;

    let user_data = header.user_data;

    let completion = CompletionInfo {
        port: ctx.port.as_raw() as usize,
        user_data,
        overlapped: ctx.overlapped as usize,
    };

    let op = BlockingOps::Close {
        handle: handle as usize,
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
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let raw_handle = resolve_fd(user.fd, ctx.registered_files)?;
    let handle = raw_handle.handle;

    let user_data = header.user_data;

    let completion = CompletionInfo {
        port: ctx.port.as_raw() as usize,
        user_data,
        overlapped: ctx.overlapped as usize,
    };

    let op = BlockingOps::Fsync {
        handle: handle as usize,
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
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let raw_handle = resolve_fd(user.fd, ctx.registered_files)?;
    let handle = raw_handle.handle;

    let user_data = header.user_data;

    let completion = CompletionInfo {
        port: ctx.port.as_raw() as usize,
        user_data,
        overlapped: ctx.overlapped as usize,
    };

    let op = BlockingOps::SyncFileRange {
        handle: handle as usize,
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
) -> io::Result<SubmissionResult> {
    // SAFETY: The caller guarantees that payload is valid.
    let user = unsafe { payload.user.as_ref() };
    let raw_handle = resolve_fd(user.fd, ctx.registered_files)?;
    let handle = raw_handle.handle;

    let user_data = header.user_data;

    let completion = CompletionInfo {
        port: ctx.port.as_raw() as usize,
        user_data,
        overlapped: ctx.overlapped as usize,
    };

    let op = BlockingOps::Fallocate {
        handle: handle as usize,
        mode: user.mode,
        offset: user.offset,
        len: user.len,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}
