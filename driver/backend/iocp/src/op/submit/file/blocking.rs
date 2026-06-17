use veloq_blocking::BlockingTask;

use diagweave::prelude::*;
use std::{io, ptr, sync::Arc};
use windows_sys::Win32::{
    Foundation::{DUPLICATE_SAME_ACCESS, DuplicateHandle, GetLastError, INVALID_HANDLE_VALUE},
    Storage::FileSystem::{
        CreateFileW, FILE_ALLOCATION_INFO, FILE_ATTRIBUTE_NORMAL, FILE_END_OF_FILE_INFO,
        FILE_FLAG_NO_BUFFERING, FILE_FLAG_OVERLAPPED, FILE_FLAG_WRITE_THROUGH, FILE_SHARE_DELETE,
        FILE_SHARE_READ, FILE_SHARE_WRITE, FileAllocationInfo, FileEndOfFileInfo, FlushFileBuffers,
        SetFileInformationByHandle,
    },
    System::Threading::GetCurrentProcess,
};

use crate::{
    config::{BorrowedRawHandle, IocpHandle, RawHandle},
    error::{IocpError, IocpResult},
    op::{
        BlockingCompletion, BlockingSuccessCleanup, Close, Fallocate, FallocateRaw, Fsync,
        FsyncRaw, KernelRef, OpenPayload, OverlappedEntry, SubmitContext, SyncFileRange,
        SyncFileRangeRaw,
        submit::{SubmissionResult, resolve_fd_handle},
    },
    win32::OwnedHandle,
};
use veloq_driver_core::{
    RawHandleMeta,
    driver::{CompletionCleanup, CompletionCleanupGuard},
};

fn make_blocking_completion(
    header: &mut OverlappedEntry,
    ctx: &SubmitContext<'_>,
    cleanup_success: Option<BlockingSuccessCleanup>,
) -> Arc<BlockingCompletion> {
    let completion =
        BlockingCompletion::new(ctx.port.clone(), ctx.completion_token, cleanup_success);
    header.blocking_completion = Some(completion.clone());
    completion
}

fn blocking_job<F>(completion: Arc<BlockingCompletion>, f: F) -> BlockingTask
where
    F: FnOnce() -> IocpResult<usize> + Send + 'static,
{
    BlockingTask::Fn(Box::new(move || completion.complete(f())))
}

fn last_os_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { GetLastError() } as i32)
}

fn win32_err(scope: &'static str) -> Report<IocpError> {
    IocpError::Win32.io_report(scope, last_os_error())
}

fn close_unconsumed_file_handle(handle: IocpHandle) {
    handle.close();
}

pub(crate) fn completion_cleanup_close_file(result: &IocpResult<usize>) -> CompletionCleanupGuard {
    let Ok(raw) = result.as_ref().copied() else {
        return CompletionCleanupGuard::default();
    };
    CompletionCleanupGuard::new(CompletionCleanup::new(move || {
        IocpHandle::for_file(raw as _).close();
        Ok(())
    }))
}

fn duplicate_file_handle(
    handle: BorrowedRawHandle<'_>,
    scope: &'static str,
) -> IocpResult<OwnedHandle> {
    let raw = handle.raw().as_handle();
    let process = unsafe { GetCurrentProcess() };
    let mut duplicated = ptr::null_mut();
    let ret = unsafe {
        DuplicateHandle(
            process,
            raw,
            process,
            &mut duplicated,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if ret == 0 {
        Err(IocpError::Submission.io_report(scope, last_os_error()))
    } else {
        Ok(OwnedHandle(duplicated))
    }
}

fn owned_wide_path(bytes: &[u8]) -> IocpResult<Vec<u16>> {
    if !bytes.len().is_multiple_of(2) {
        return IocpError::InvalidInput
            .with_ctx("path_bytes", bytes.len())
            .attach_note("Windows open path buffer must contain UTF-16 code units");
    }

    let mut path = bytes
        .chunks_exact(2)
        .map(|chunk| u16::from_ne_bytes([chunk[0], chunk[1]]))
        .collect::<Vec<_>>();
    if path.last().copied() != Some(0) {
        path.push(0);
    }
    Ok(path)
}

fn open_file(path: Vec<u16>, flags: i32, mode: u32) -> IocpResult<IocpHandle> {
    let real_disposition = mode & 0xFF;
    const FAKE_NO_BUFFERING: u32 = 1 << 8;
    const FAKE_WRITE_THROUGH: u32 = 1 << 9;

    let mut flags_and_attributes = FILE_FLAG_OVERLAPPED | FILE_ATTRIBUTE_NORMAL;
    if (mode & FAKE_NO_BUFFERING) != 0 {
        flags_and_attributes |= FILE_FLAG_NO_BUFFERING;
    }
    if (mode & FAKE_WRITE_THROUGH) != 0 {
        flags_and_attributes |= FILE_FLAG_WRITE_THROUGH;
    }

    let handle = unsafe {
        CreateFileW(
            path.as_ptr(),
            flags as u32,
            FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
            ptr::null(),
            real_disposition,
            flags_and_attributes,
            ptr::null_mut(),
        )
    };

    if handle == INVALID_HANDLE_VALUE {
        Err(win32_err("CreateFileW"))
    } else {
        Ok(IocpHandle::for_file(handle))
    }
}

fn flush_file_buffers(handle: IocpHandle) -> IocpResult<usize> {
    let ret = unsafe { FlushFileBuffers(handle.as_handle()) };
    if ret == 0 {
        Err(win32_err("FlushFileBuffers"))
    } else {
        Ok(0)
    }
}

fn fallocate_file(handle: IocpHandle, mode: i32, offset: u64, len: u64) -> IocpResult<usize> {
    let Some(req_size) = offset.checked_add(len) else {
        return IocpError::InvalidInput.attach_note("fallocate range overflows");
    };
    let Ok(allocation_size) = i64::try_from(req_size) else {
        return IocpError::InvalidInput.attach_note("fallocate range exceeds i64");
    };

    let mut alloc_info = FILE_ALLOCATION_INFO {
        AllocationSize: allocation_size,
    };
    let ret = unsafe {
        SetFileInformationByHandle(
            handle.as_handle(),
            FileAllocationInfo,
            &mut alloc_info as *mut _ as *mut _,
            std::mem::size_of::<FILE_ALLOCATION_INFO>() as u32,
        )
    };
    if ret == 0 {
        return Err(win32_err("SetFileInformationByHandle.allocation"));
    }

    if mode == 0 {
        let mut eof_info = FILE_END_OF_FILE_INFO {
            EndOfFile: allocation_size,
        };
        let ret = unsafe {
            SetFileInformationByHandle(
                handle.as_handle(),
                FileEndOfFileInfo,
                &mut eof_info as *mut _ as *mut _,
                std::mem::size_of::<FILE_END_OF_FILE_INFO>() as u32,
            )
        };
        if ret == 0 {
            return Err(win32_err("SetFileInformationByHandle.eof"));
        }
    }

    Ok(0)
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
    let user = unsafe { payload.user.as_ref()? };
    let path = owned_wide_path(user.path.as_slice())?;
    let flags = user.flags;
    let mode = user.mode;

    let completion = make_blocking_completion(header, ctx, Some(close_unconsumed_file_handle));
    let task = blocking_job(completion, move || {
        open_file(path, flags, mode).map(|handle| handle.as_handle() as usize)
    });
    Ok(SubmissionResult::Offload(task))
}

/// # Safety
///
/// The caller must ensure that header, payload, and ctx are valid for the duration of the call.
pub(crate) fn submit_close(
    _header: &mut OverlappedEntry,
    _payload: &mut KernelRef<Close>,
    _ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    IocpError::InvalidState.attach_note("registered Close operations are handled by the driver")
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
    let user = unsafe { payload.user.as_ref()? };
    let raw = resolve_fd_handle(&user.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);

    let dup = duplicate_file_handle(raw.borrow(), "DuplicateHandle.fsync")?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || {
        flush_file_buffers(IocpHandle::for_file(dup.as_raw()))
    });
    Ok(SubmissionResult::Offload(task))
}

pub(crate) fn submit_fsync_raw(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<FsyncRaw>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    let user = unsafe { payload.user.as_ref()? };
    let raw = RawHandle::new(user.fd);
    header.resolved_handle = Some(raw);

    let dup = duplicate_file_handle(raw.borrow(), "DuplicateHandle.fsync_raw")?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || {
        flush_file_buffers(IocpHandle::for_file(dup.as_raw()))
    });
    Ok(SubmissionResult::Offload(task))
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
    let user = unsafe { payload.user.as_ref()? };
    let raw = resolve_fd_handle(&user.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);

    let dup = duplicate_file_handle(raw.borrow(), "DuplicateHandle.sync_range")?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || {
        flush_file_buffers(IocpHandle::for_file(dup.as_raw()))
    });
    Ok(SubmissionResult::Offload(task))
}

pub(crate) fn submit_sync_range_raw(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<SyncFileRangeRaw>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    let user = unsafe { payload.user.as_ref()? };
    let raw = RawHandle::new(user.fd);
    header.resolved_handle = Some(raw);

    let dup = duplicate_file_handle(raw.borrow(), "DuplicateHandle.sync_range_raw")?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || {
        flush_file_buffers(IocpHandle::for_file(dup.as_raw()))
    });
    Ok(SubmissionResult::Offload(task))
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
    let user = unsafe { payload.user.as_ref()? };
    let raw = resolve_fd_handle(&user.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);

    let mode = user.mode;
    let offset = user.offset;
    let len = user.len;
    let dup = duplicate_file_handle(raw.borrow(), "DuplicateHandle.fallocate")?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || {
        fallocate_file(IocpHandle::for_file(dup.as_raw()), mode, offset, len)
    });
    Ok(SubmissionResult::Offload(task))
}

pub(crate) fn submit_fallocate_raw(
    header: &mut OverlappedEntry,
    payload: &mut KernelRef<FallocateRaw>,
    ctx: &mut SubmitContext,
) -> IocpResult<SubmissionResult> {
    let user = unsafe { payload.user.as_ref()? };
    let raw = RawHandle::new(user.fd);
    header.resolved_handle = Some(raw);

    let mode = user.mode;
    let offset = user.offset;
    let len = user.len;
    let dup = duplicate_file_handle(raw.borrow(), "DuplicateHandle.fallocate_raw")?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || {
        fallocate_file(IocpHandle::for_file(dup.as_raw()), mode, offset, len)
    });
    Ok(SubmissionResult::Offload(task))
}
