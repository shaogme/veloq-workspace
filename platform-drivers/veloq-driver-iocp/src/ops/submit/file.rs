use std::io;
use veloq_blocking::BlockingTask;
use veloq_blocking::blocking_ops::windows::{BlockingOps, CompletionInfo};
use veloq_buf::FixedBuf;

use crate::common::IocpErrorContext;
use crate::ops::submit::common::{
    SubmissionResult, ensure_iocp_association, iocp_submit_read, iocp_submit_write, resolve_fd,
};
use crate::ops::{IocpOp, SubmitContext};

// ============================================================================
// Macros
// ============================================================================

macro_rules! submit_io_op {
    ($fn_name:ident, $field:ident, $wrapper_fn:ident, offset, $ptr_fn:expr) => {
        pub(crate) unsafe fn $fn_name(
            op: &mut IocpOp,
            ctx: &mut SubmitContext,
        ) -> io::Result<SubmissionResult> {
            let kernel = unsafe { &mut *op.payload.$field };
            let val = unsafe { kernel.user.as_mut() };
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
                        op.header.user_data,
                        op.header.generation,
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
                        op.header.user_data,
                        op.header.generation,
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
    read,
    iocp_submit_read,
    offset,
    |b: &mut FixedBuf| b.as_mut_ptr()
);

submit_io_op!(
    submit_write_fixed,
    write,
    iocp_submit_write,
    offset,
    |b: &mut FixedBuf| b.as_slice().as_ptr() as *mut u8
);

// ============================================================================
// Blocking File Operations
// ============================================================================

pub(crate) unsafe fn submit_open(
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let payload = unsafe { &*op.payload.open };
    let user = unsafe { payload.user.as_ref() };
    let path_ptr = user.path.as_slice().as_ptr() as usize;

    let entry = &op.header;
    let user_data = entry.user_data;

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
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let kernel = unsafe { &*op.payload.close };
    let payload = unsafe { kernel.user.as_ref() };
    let handle = resolve_fd(payload.fd, ctx.registered_files)?;

    let entry = &op.header;
    let user_data = entry.user_data;

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
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let kernel = unsafe { &*op.payload.fsync };
    let payload = unsafe { kernel.user.as_ref() };
    let handle = resolve_fd(payload.fd, ctx.registered_files)?;

    let entry = &op.header;
    let user_data = entry.user_data;

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
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let kernel = unsafe { &*op.payload.sync_range };
    let payload = unsafe { kernel.user.as_ref() };
    let handle = resolve_fd(payload.fd, ctx.registered_files)?;

    let entry = &op.header;
    let user_data = entry.user_data;

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
    op: &mut IocpOp,
    ctx: &mut SubmitContext,
) -> io::Result<SubmissionResult> {
    let kernel = unsafe { &*op.payload.fallocate };
    let payload = unsafe { kernel.user.as_ref() };
    let handle = resolve_fd(payload.fd, ctx.registered_files)?;

    let entry = &op.header;
    let user_data = entry.user_data;

    let completion = CompletionInfo {
        port: ctx.port.as_raw() as usize,
        user_data,
        overlapped: ctx.overlapped as usize,
    };

    let op = BlockingOps::Fallocate {
        handle: handle as usize,
        mode: payload.mode,
        offset: payload.offset,
        len: payload.len,
        completion,
    };
    Ok(SubmissionResult::Offload(BlockingTask::SysOp(op)))
}
