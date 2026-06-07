use veloq_blocking::BlockingTask;
use veloq_buf::FixedBuf;

use diagweave::prelude::*;
use std::io;
use std::ptr;
use std::sync::Arc;
use windows_sys::Win32::Foundation::{
    CloseHandle, DUPLICATE_SAME_ACCESS, DuplicateHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE,
};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_ALLOCATION_INFO, FILE_ATTRIBUTE_NORMAL, FILE_END_OF_FILE_INFO,
    FILE_FLAG_OVERLAPPED, FileAllocationInfo, FileEndOfFileInfo, FlushFileBuffers,
    SetFileInformationByHandle,
};
use windows_sys::Win32::System::Threading::GetCurrentProcess;

use crate::error::{IocpError, IocpResult};
use crate::op::overlapped::{BlockingCompletion, BlockingSuccessCleanup};
use crate::op::submit::common::{
    SubmissionResult, ensure_iocp_association, iocp_submit_read, iocp_submit_write,
    mark_header_in_flight, resolve_fd_handle, resolve_registered_raw_file, unpack_kernel_ref,
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

            let raw = resolve_fd_handle(&val.fd, &*ctx.registered_slots)?;
            header.resolved_handle = Some(raw);
            ensure_iocp_association(&val.fd, raw, ctx.port.as_ref(), &mut *ctx.registered_slots)
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("fd_fixed_index", val.fd.fixed_index())
                .with_ctx("fd_generation", val.fd.generation())
                .with_ctx("handle_raw", raw.as_handle() as usize)
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
            let raw_handle = crate::config::RawHandle::new(raw);
            let handle = raw_handle.borrow();
            // SAFETY: Calling Win32 ReadFile/WriteFile via wrapper with valid parameters.
            let submit_res = unsafe { $wrapper_fn(handle, ptr as _, len, ctx.overlapped) }
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("fd_fixed_index", val.fd.fixed_index())
                .with_ctx("fd_generation", val.fd.generation())
                .with_ctx("handle_raw", raw.as_handle() as usize)
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

            let (fd, raw) = resolve_registered_raw_file(val.fd, &*ctx.registered_slots)?;
            header.resolved_handle = Some(raw);
            ensure_iocp_association(&fd, raw, ctx.port.as_ref(), &mut *ctx.registered_slots)
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("fd_fixed_index", fd.fixed_index())
                .with_ctx("fd_generation", fd.generation())
                .with_ctx("handle_raw", raw.as_handle() as usize)
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
            let raw_handle = crate::config::RawHandle::new(raw);
            let handle = raw_handle.borrow();
            let submit_res = unsafe { $wrapper_fn(handle, ptr as _, len, ctx.overlapped) }
                .push_ctx("scope", stringify!($fn_name))
                .with_ctx("handle_raw", raw.as_handle() as usize)
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

fn make_blocking_completion(
    header: &mut OverlappedEntry,
    ctx: &SubmitContext<'_>,
    cleanup_success: Option<BlockingSuccessCleanup>,
) -> Arc<BlockingCompletion> {
    let completion = BlockingCompletion::new(ctx.port.clone(), header.user_data, cleanup_success);
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
            0,
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
