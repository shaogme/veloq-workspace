use std::io;
use veloq_blocking::BlockingTask;
use veloq_blocking::blocking_ops::windows::{BlockingOps, CompletionInfo};
use veloq_buf::FixedBuf;

use crate::common::IocpErrorContext;
use crate::ops::submit::common::{
    SubmissionResult, ensure_iocp_association, iocp_submit_read, iocp_submit_write, resolve_fd,
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
        pub(crate) unsafe fn $fn_name(
            header: &mut OverlappedEntry,
            payload: &mut KernelRef<$field_type>,
            ctx: &mut SubmitContext,
        ) -> io::Result<SubmissionResult> {
            let val = unsafe { payload.user.as_mut() };
            // Using ctx.overlapped (Slot Overlapped)
            let overlapped = unsafe { &mut *ctx.overlapped };

            overlapped.Anonymous.Anonymous.Offset = val.offset as u32;
            overlapped.Anonymous.Anonymous.OffsetHigh = (val.offset >> 32) as u32;

            let handle = resolve_fd(val.fd, ctx.registered_files)?;
            // SAFETY: the handle is checked for validity by resolve_fd.
            unsafe {
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
                )
            }?;

            // Depending on ReadFile/WriteFile sig: (handle, buf, len, bytes, overlapped)
            let get_ptr: fn(&mut _) -> *mut u8 = $ptr_fn;
            let ptr = get_ptr(&mut val.buf);

            // SAFETY: Calling Win32 ReadFile/WriteFile via wrapper with valid parameters.
            unsafe {
                $wrapper_fn(
                    handle,
                    ptr as _,
                    val.buf.len() as u32,
                    ctx.overlapped,
                )
            }.map_err(|e| {
                crate::common::io_error(
                    IocpErrorContext::Submission,
                    e,
                    format!(
                        "{}: syscall failed: fd={:?}, handle={:?}, user_data={}, generation={}, offset={}, len={}",
                        stringify!($fn_name),
                        val.fd,
                        handle,
                        header.user_data,
                        header.generation,
                        val.offset,
                        val.buf.len()
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

pub(crate) unsafe fn submit_open(
    header: &mut OverlappedEntry,
    payload: &mut OpenPayload,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
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

pub(crate) unsafe fn submit_close(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Close>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd(user.fd, ctx.registered_files)?;

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

pub(crate) unsafe fn submit_fsync(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Fsync>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd(user.fd, ctx.registered_files)?;

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

pub(crate) unsafe fn submit_sync_range(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<SyncFileRange>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd(user.fd, ctx.registered_files)?;

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

pub(crate) unsafe fn submit_fallocate(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<Fallocate>,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let user = unsafe { payload.user.as_ref() };
    let handle = resolve_fd(user.fd, ctx.registered_files)?;

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
