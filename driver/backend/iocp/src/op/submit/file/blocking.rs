use veloq_blocking::BlockingTask;

use diagweave::prelude::*;
use std::io;
use std::ptr;
use std::sync::Arc;
use windows_sys::Win32::Foundation::{
    CloseHandle, DUPLICATE_SAME_ACCESS, DuplicateHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_ALLOCATION_INFO, FILE_ATTRIBUTE_NORMAL, FILE_END_OF_FILE_INFO,
    FILE_FLAG_OVERLAPPED, FILE_SHARE_DELETE, FILE_SHARE_READ, FILE_SHARE_WRITE, FileAllocationInfo,
    FileEndOfFileInfo, FlushFileBuffers, SetFileInformationByHandle,
};
use windows_sys::Win32::System::Threading::GetCurrentProcess;

use crate::error::{IocpError, IocpResult};
use crate::op::submit::{SubmissionResult, resolve_fd_handle};
use crate::op::{
    BlockingCompletion, BlockingSuccessCleanup, Close, Fallocate, FallocateRaw, Fsync, FsyncRaw,
    KernelRef, OpenPayload, OverlappedEntry, SubmitContext, SyncFileRange, SyncFileRangeRaw,
};
use veloq_driver_core::RawHandleMeta;
use veloq_driver_core::driver::{CompletionCleanup, CompletionCleanupGuard};

fn make_blocking_completion(
    header: &mut OverlappedEntry,
    ctx: &SubmitContext<'_>,
    cleanup_success: Option<BlockingSuccessCleanup>,
) -> Arc<BlockingCompletion> {
    let completion_key = ctx.completion_token.raw() as usize;
    let completion = BlockingCompletion::new(ctx.port.clone(), completion_key, cleanup_success);
    header.blocking_completion = Some(completion.clone());
    completion
}

fn blocking_job<F>(completion: Arc<BlockingCompletion>, f: F) -> BlockingTask
where
    F: FnOnce() -> io::Result<usize> + Send + 'static,
{
    BlockingTask::Fn(Box::new(move || completion.complete(f())))
}

fn last_os_error() -> io::Error {
    io::Error::from_raw_os_error(unsafe { GetLastError() } as i32)
}

fn is_valid_file_handle(handle: HANDLE) -> bool {
    !handle.is_null() && handle != INVALID_HANDLE_VALUE
}

fn close_unconsumed_file_handle(raw: usize) {
    let handle = raw as HANDLE;
    if is_valid_file_handle(handle) {
        unsafe {
            CloseHandle(handle);
        }
    }
}

pub(crate) fn completion_cleanup_close_file(result: &IocpResult<usize>) -> CompletionCleanupGuard {
    let Ok(raw) = result.as_ref().copied() else {
        return CompletionCleanupGuard::default();
    };
    CompletionCleanupGuard::new(CompletionCleanup::new(move || {
        crate::config::IocpHandle::for_file(raw as _).close();
        Ok(())
    }))
}

fn duplicate_file_handle(handle: HANDLE) -> io::Result<crate::win32::OwnedHandle> {
    let process = unsafe { GetCurrentProcess() };
    let mut duplicated = ptr::null_mut();
    let ret = unsafe {
        DuplicateHandle(
            process,
            handle,
            process,
            &mut duplicated,
            0,
            0,
            DUPLICATE_SAME_ACCESS,
        )
    };
    if ret == 0 {
        Err(last_os_error())
    } else {
        Ok(crate::win32::OwnedHandle(duplicated))
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

fn open_file(path: Vec<u16>, flags: i32, mode: u32) -> io::Result<usize> {
    let real_disposition = mode & 0xFF;
    const FAKE_NO_BUFFERING: u32 = 1 << 8;
    const FAKE_WRITE_THROUGH: u32 = 1 << 9;

    let mut flags_and_attributes = FILE_FLAG_OVERLAPPED | FILE_ATTRIBUTE_NORMAL;
    if (mode & FAKE_NO_BUFFERING) != 0 {
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_NO_BUFFERING;
        flags_and_attributes |= FILE_FLAG_NO_BUFFERING;
    }
    if (mode & FAKE_WRITE_THROUGH) != 0 {
        use windows_sys::Win32::Storage::FileSystem::FILE_FLAG_WRITE_THROUGH;
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
        Err(last_os_error())
    } else {
        Ok(handle as usize)
    }
}

fn flush_file_buffers(handle: HANDLE) -> io::Result<usize> {
    let ret = unsafe { FlushFileBuffers(handle) };
    if ret == 0 {
        Err(last_os_error())
    } else {
        Ok(0)
    }
}

fn fallocate_file(handle: HANDLE, mode: i32, offset: u64, len: u64) -> io::Result<usize> {
    let req_size = offset
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "fallocate range overflows"))?;
    let allocation_size = i64::try_from(req_size)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "fallocate range exceeds i64"))?;

    let mut alloc_info = FILE_ALLOCATION_INFO {
        AllocationSize: allocation_size,
    };
    let ret = unsafe {
        SetFileInformationByHandle(
            handle,
            FileAllocationInfo,
            &mut alloc_info as *mut _ as *mut _,
            std::mem::size_of::<FILE_ALLOCATION_INFO>() as u32,
        )
    };
    if ret == 0 {
        return Err(last_os_error());
    }

    if mode == 0 {
        let mut eof_info = FILE_END_OF_FILE_INFO {
            EndOfFile: allocation_size,
        };
        let ret = unsafe {
            SetFileInformationByHandle(
                handle,
                FileEndOfFileInfo,
                &mut eof_info as *mut _ as *mut _,
                std::mem::size_of::<FILE_END_OF_FILE_INFO>() as u32,
            )
        };
        if ret == 0 {
            return Err(last_os_error());
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
    let user = unsafe { payload.user.as_ref() };
    let path = owned_wide_path(user.path.as_slice())?;
    let flags = user.flags;
    let mode = user.mode;

    let completion = make_blocking_completion(header, ctx, Some(close_unconsumed_file_handle));
    let task = blocking_job(completion, move || open_file(path, flags, mode));
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
    let user = unsafe { payload.user.as_ref() };
    let raw = resolve_fd_handle(&user.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);

    let handle = duplicate_file_handle(raw.as_handle())
        .map_err(|e| IocpError::Submission.io_report("DuplicateHandle.fsync", e))?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || flush_file_buffers(handle.as_raw()));
    Ok(SubmissionResult::Offload(task))
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

    let handle = duplicate_file_handle(handle.raw().as_handle())
        .map_err(|e| IocpError::Submission.io_report("DuplicateHandle.fsync_raw", e))?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || flush_file_buffers(handle.as_raw()));
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
    let user = unsafe { payload.user.as_ref() };
    let raw = resolve_fd_handle(&user.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);

    let handle = duplicate_file_handle(raw.as_handle())
        .map_err(|e| IocpError::Submission.io_report("DuplicateHandle.sync_range", e))?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || flush_file_buffers(handle.as_raw()));
    Ok(SubmissionResult::Offload(task))
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

    let handle = duplicate_file_handle(handle.raw().as_handle())
        .map_err(|e| IocpError::Submission.io_report("DuplicateHandle.sync_range_raw", e))?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || flush_file_buffers(handle.as_raw()));
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
    let user = unsafe { payload.user.as_ref() };
    let raw = resolve_fd_handle(&user.fd, &*ctx.registered_slots)?;
    header.resolved_handle = Some(raw);

    let mode = user.mode;
    let offset = user.offset;
    let len = user.len;
    let handle = duplicate_file_handle(raw.as_handle())
        .map_err(|e| IocpError::Submission.io_report("DuplicateHandle.fallocate", e))?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || {
        fallocate_file(handle.as_raw(), mode, offset, len)
    });
    Ok(SubmissionResult::Offload(task))
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

    let mode = user.mode;
    let offset = user.offset;
    let len = user.len;
    let handle = duplicate_file_handle(handle.raw().as_handle())
        .map_err(|e| IocpError::Submission.io_report("DuplicateHandle.fallocate_raw", e))?;
    let completion = make_blocking_completion(header, ctx, None);
    let task = blocking_job(completion, move || {
        fallocate_file(handle.as_raw(), mode, offset, len)
    });
    Ok(SubmissionResult::Offload(task))
}
