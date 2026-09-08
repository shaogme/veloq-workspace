use veloq_std::{boxed::Box, io::Error as IoError, mem::size_of, ptr};

use crate::error::{IocpError, IocpResult};
use veloq_driver_core::driver::CompletionToken;
use veloq_pod::{Pod, Zeroable, zeroed};
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE, NTSTATUS, RtlNtStatusToDosError,
        WAIT_TIMEOUT,
    },
    Networking::WinSock::{
        INVALID_SOCKET, SOCKADDR, SOCKET, bind, closesocket, connect, getpeername, getsockname,
        listen, setsockopt,
    },
    System::IO::{
        CancelIoEx, CreateIoCompletionPort, GetQueuedCompletionStatusEx, OVERLAPPED,
        OVERLAPPED_ENTRY, PostQueuedCompletionStatus,
    },
};

fn last_os_error() -> IoError {
    IoError::last_os_error()
}

fn from_raw_os_error(err: i32) -> IoError {
    IoError::from_raw_os_error(err)
}

// ============================================================================
// Overlapped
// ============================================================================

/// A safe wrapper for the Windows OVERLAPPED structure.
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct Overlapped(pub OVERLAPPED);

// SAFETY: OVERLAPPED is a Win32 POD struct and can be safely zero-initialized.
unsafe impl Zeroable for Overlapped {}
// SAFETY: Overlapped is repr(transparent) and OVERLAPPED is a POD struct.
unsafe impl Pod for Overlapped {}

impl Overlapped {
    /// Creates a zero-initialized OVERLAPPED wrapper.
    pub fn zeroed() -> Self {
        zeroed()
    }

    /// Returns a pointer to the underlying OVERLAPPED structure.
    pub fn as_ptr(&self) -> *const OVERLAPPED {
        &self.0
    }

    /// Returns a mutable pointer to the underlying OVERLAPPED structure.
    pub fn as_mut_ptr(&mut self) -> *mut OVERLAPPED {
        &mut self.0
    }

    /// Sets the offset of the overlapped operation.
    pub fn set_offset(&mut self, offset: u64) {
        // SAFETY: File offset uses the documented Offset/OffsetHigh view of the OVERLAPPED union.
        let parts = unsafe { &mut self.0.Anonymous.Anonymous };
        parts.Offset = offset as u32;
        parts.OffsetHigh = (offset >> 32) as u32;
    }

    /// Returns the offset of the overlapped operation.
    pub fn offset(&self) -> u64 {
        // SAFETY: File offset uses the documented Offset/OffsetHigh view of the OVERLAPPED union.
        let parts = unsafe { self.0.Anonymous.Anonymous };
        let low = u64::from(parts.Offset);
        let high = u64::from(parts.OffsetHigh);
        low | (high << 32)
    }
}

impl Default for Overlapped {
    fn default() -> Self {
        Self::zeroed()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use veloq_std::mem::{offset_of, size_of};
    use windows_sys::Win32::System::IO::{OVERLAPPED, OVERLAPPED_0_0};

    #[test]
    fn overlapped_offset_fields_follow_win32_layout() {
        assert_eq!(offset_of!(OVERLAPPED, Anonymous), size_of::<usize>() * 2);
        assert_eq!(offset_of!(OVERLAPPED_0_0, Offset), 0);
        assert_eq!(offset_of!(OVERLAPPED_0_0, OffsetHigh), size_of::<u32>());
    }

    #[test]
    fn set_offset_writes_offset_union_without_touching_internal_status() {
        let mut overlapped = Overlapped::zeroed();
        let internal = usize::MAX;
        let internal_high = usize::MAX - 1;
        overlapped.0.Internal = internal;
        overlapped.0.InternalHigh = internal_high;

        overlapped.set_offset(0x99aa_bbcc_ddee_ff00);

        assert_eq!(overlapped.0.Internal, internal);
        assert_eq!(overlapped.0.InternalHigh, internal_high);
        assert_eq!(overlapped.offset(), 0x99aa_bbcc_ddee_ff00);

        // SAFETY: The test verifies the same documented file-offset union view used by set_offset.
        let parts = unsafe { overlapped.0.Anonymous.Anonymous };
        assert_eq!(parts.Offset, 0xddee_ff00);
        assert_eq!(parts.OffsetHigh, 0x99aa_bbcc);
    }
}

// ============================================================================
// OwnedHandle
// ============================================================================

/// A safe wrapper around a Win32 HANDLE that ensures it is closed when dropped.
#[derive(Debug)]
pub struct OwnedHandle(pub HANDLE);

impl OwnedHandle {
    /// Returns the raw HANDLE.
    pub fn as_raw(&self) -> HANDLE {
        self.0
    }

    /// Checks if the handle is valid.
    pub fn is_valid(&self) -> bool {
        !self.0.is_null() && self.0 != INVALID_HANDLE_VALUE
    }
}

impl Drop for OwnedHandle {
    fn drop(&mut self) {
        if self.is_valid() {
            // SAFETY: Handle is valid and owned by us.
            unsafe {
                CloseHandle(self.0);
            }
        }
    }
}

// SAFETY: Windows HANDLEs are pointers but can be safely transferred between threads.
unsafe impl Send for OwnedHandle {}
// SAFETY: Windows HANDLEs are pointers but can be safely shared between threads.
unsafe impl Sync for OwnedHandle {}

// ============================================================================
// SafeSocket
// ============================================================================

/// A safe wrapper around a Win32 SOCKET that ensures it is closed when dropped.
#[derive(Debug)]
pub struct SafeSocket(pub SOCKET);

impl SafeSocket {
    /// Returns the raw SOCKET.
    pub fn as_raw(&self) -> SOCKET {
        self.0
    }

    /// Checks if the socket is valid.
    pub fn is_valid(&self) -> bool {
        self.0 != INVALID_SOCKET
    }

    /// Binds the socket to a local address.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `addr` is a valid pointer to a `SOCKADDR`
    /// structure and `len` is its size.
    pub unsafe fn bind(&self, addr: *const SOCKADDR, len: i32) -> IocpResult<()> {
        // SAFETY: The caller ensures that `addr` and `len` are valid.
        let ret = unsafe { bind(self.0, addr, len) };
        if ret != 0 {
            return Err(IocpError::Socket.io_report("bind", last_os_error()));
        }
        Ok(())
    }

    /// Listens for incoming connections.
    pub fn listen(&self, backlog: i32) -> IocpResult<()> {
        // SAFETY: The socket is valid and owned by us.
        let ret = unsafe { listen(self.0, backlog) };
        if ret != 0 {
            return Err(IocpError::Socket.io_report("listen", last_os_error()));
        }
        Ok(())
    }

    /// Connects the socket to a remote address.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `addr` is a valid pointer to a `SOCKADDR`
    /// structure and `len` is its size.
    pub unsafe fn connect(&self, addr: *const SOCKADDR, len: i32) -> IocpResult<()> {
        // SAFETY: The caller ensures that `addr` and `len` are valid.
        let ret = unsafe { connect(self.0, addr, len) };
        if ret != 0 {
            return Err(IocpError::Socket.io_report("connect", last_os_error()));
        }
        Ok(())
    }

    /// Retrieves the local address of the socket.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `addr` and `len` are valid pointers.
    pub unsafe fn getsockname(&self, addr: *mut SOCKADDR, len: *mut i32) -> IocpResult<()> {
        // SAFETY: The caller ensures that `addr` and `len` are valid.
        let ret = unsafe { getsockname(self.0, addr, len) };
        if ret != 0 {
            return Err(IocpError::Socket.io_report("getsockname", last_os_error()));
        }
        Ok(())
    }

    /// Retrieves the peer address of the socket.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `addr` and `len` are valid pointers.
    pub unsafe fn getpeername(&self, addr: *mut SOCKADDR, len: *mut i32) -> IocpResult<()> {
        // SAFETY: The caller ensures that `addr` and `len` are valid.
        let ret = unsafe { getpeername(self.0, addr, len) };
        if ret != 0 {
            return Err(IocpError::Socket.io_report("getpeername", last_os_error()));
        }
        Ok(())
    }

    /// Sets a socket option.
    pub fn setsockopt<T>(&self, level: i32, optname: i32, optval: &T) -> IocpResult<()> {
        // SAFETY: `optval` is a valid reference, and its size is correctly calculated.
        let ret = unsafe {
            setsockopt(
                self.0,
                level,
                optname,
                optval as *const T as *const u8,
                size_of::<T>() as i32,
            )
        };
        if ret != 0 {
            return Err(IocpError::Socket.io_report("setsockopt", last_os_error()));
        }
        Ok(())
    }

    /// Sets a socket option with an empty payload.
    pub fn setsockopt_empty(&self, level: i32, optname: i32) -> IocpResult<()> {
        // SAFETY: Setting socket option with no payload is safe for valid options.
        let ret = unsafe { setsockopt(self.0, level, optname, ptr::null(), 0) };
        if ret != 0 {
            return Err(IocpError::Socket.io_report("setsockopt_empty", last_os_error()));
        }
        Ok(())
    }
}

impl Drop for SafeSocket {
    fn drop(&mut self) {
        if self.is_valid() {
            // SAFETY: Socket is valid and owned by us.
            unsafe {
                closesocket(self.0);
            }
        }
    }
}

// SAFETY: Windows SOCKETs are handles that can be safely transferred between threads.
unsafe impl Send for SafeSocket {}
// SAFETY: Windows SOCKETs can be safely shared between threads.
unsafe impl Sync for SafeSocket {}

// ============================================================================
// IoCompletionPort
// ============================================================================

/// A safe wrapper for an I/O Completion Port.
pub struct IoCompletionPort(OwnedHandle);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelRequestResult {
    Submitted,
    NotFound,
}

impl IoCompletionPort {
    /// Creates a new, unconnected I/O Completion Port.
    pub fn new(threads: u32) -> IocpResult<Self> {
        // SAFETY: Creating an IOCP with default parameters is safe.
        let handle =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, threads) };
        if handle.is_null() {
            return Err(IocpError::Win32.io_report("CreateIoCompletionPort.new", last_os_error()));
        }
        Ok(Self(OwnedHandle(handle)))
    }

    /// Associates a handle with this I/O Completion Port.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `handle` is valid and not already associated.
    pub unsafe fn associate(&self, handle: HANDLE, completion_key: usize) -> IocpResult<()> {
        let port = self.0.as_raw();
        let handle_raw = handle as usize;
        let port_raw = port as usize;

        // SAFETY: The caller ensures that `handle` is valid and not already associated.
        let res = unsafe { CreateIoCompletionPort(handle, port, completion_key, 0) };
        if res.is_null() {
            // SAFETY: GetLastError is safe to call after a failed Win32 API call.
            let err = unsafe { GetLastError() };
            return Err(IocpError::Win32
                .io_report(
                    "CreateIoCompletionPort.associate",
                    from_raw_os_error(err as i32),
                )
                .with_ctx("handle_raw", handle_raw)
                .with_ctx("port_raw", port_raw)
                .with_ctx("completion_key", completion_key)
                .with_ctx("os_error_code", err));
        }
        Ok(())
    }

    /// Posts a completion status to the port.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `overlapped` is valid if it is not null.
    pub unsafe fn post(
        &self,
        bytes: u32,
        key: usize,
        overlapped: *mut Overlapped,
    ) -> IocpResult<()> {
        // SAFETY: The caller ensures that `overlapped` is valid if it is not null.
        let res = unsafe {
            PostQueuedCompletionStatus(self.0.as_raw(), bytes, key, overlapped as *mut OVERLAPPED)
        };
        if res == 0 {
            return Err(IocpError::Win32.io_report("PostQueuedCompletionStatus", last_os_error()));
        }
        Ok(())
    }

    /// Notifies the completion port with a typed completion token.
    pub fn notify(&self, token: CompletionToken) -> IocpResult<()> {
        // SAFETY: Posting with a null overlapped is always safe.
        unsafe { self.post(0, token.raw() as usize, ptr::null_mut()) }
    }

    /// Cancels a pending I/O request.
    ///
    /// # Safety
    ///
    /// The caller must ensure that `handle` and `overlapped` are valid.
    pub unsafe fn cancel_request(
        handle: HANDLE,
        overlapped: *mut Overlapped,
    ) -> IocpResult<CancelRequestResult> {
        // SAFETY: The caller ensures `handle` and `overlapped` are valid.
        let res = unsafe { CancelIoEx(handle, overlapped as *mut OVERLAPPED) };
        if res == 0 {
            // SAFETY: GetLastError is safe to call after a failed Win32 API call.
            let err = unsafe { GetLastError() };
            if err == windows_sys::Win32::Foundation::ERROR_NOT_FOUND {
                return Ok(CancelRequestResult::NotFound);
            }
            return Err(IocpError::Win32.io_report("CancelIoEx", from_raw_os_error(err as i32)));
        }
        Ok(CancelRequestResult::Submitted)
    }

    /// Dequeues up to `batch.capacity()` completion statuses in a single syscall.
    ///
    /// Returns the number of entries retrieved, which is `0` when the wait timed out. The
    /// entries stay in `batch` and are decoded through [`CompletionBatch::status`].
    pub fn get_status_batch(
        &self,
        batch: &mut CompletionBatch,
        timeout_ms: u32,
    ) -> IocpResult<usize> {
        batch.len = 0;
        let capacity = batch.entries.len() as u32;
        let mut removed: u32 = 0;

        // SAFETY: `batch.entries` is a live slice of `capacity` OVERLAPPED_ENTRY values and
        // `removed` is a valid local; the port handle is owned by `self`.
        let res = unsafe {
            GetQueuedCompletionStatusEx(
                self.0.as_raw(),
                batch.entries.as_mut_ptr(),
                capacity,
                &mut removed,
                timeout_ms,
                0,
            )
        };

        if res == 0 {
            // SAFETY: GetLastError is safe to call after a failed Win32 API call.
            let err = unsafe { GetLastError() };
            if err == WAIT_TIMEOUT {
                return Ok(0);
            }
            return Err(IocpError::Win32
                .io_report("GetQueuedCompletionStatusEx", from_raw_os_error(err as i32)));
        }

        batch.len = (removed as usize).min(batch.entries.len());
        Ok(batch.len)
    }

    /// Returns the raw HANDLE of the completion port.
    pub fn as_raw(&self) -> HANDLE {
        self.0.as_raw()
    }
}

/// Reusable buffer of `OVERLAPPED_ENTRY` slots backing [`IoCompletionPort::get_status_batch`].
pub struct CompletionBatch {
    entries: Box<[OVERLAPPED_ENTRY]>,
    len: usize,
}

impl CompletionBatch {
    /// Creates a batch buffer able to hold `capacity` (at least one) completion entries.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: (0..capacity.max(1))
                .map(|_| OVERLAPPED_ENTRY::default())
                .collect(),
            len: 0,
        }
    }

    /// Decodes the `index`-th retrieved entry, or `None` when `index` is past the batch.
    pub fn status(&self, index: usize) -> Option<CompletionStatus> {
        if index >= self.len {
            return None;
        }
        self.entries.get(index).map(CompletionStatus::from_entry)
    }
}

/// Represents the status of a completed I/O operation.
#[derive(Clone, Copy)]
pub struct CompletionStatus {
    pub bytes: u32,
    pub key: usize,
    pub overlapped: *mut Overlapped,
    pub success: bool,
    pub error_code: Option<u32>,
}

impl CompletionStatus {
    /// Rebuilds the `GetQueuedCompletionStatus` view of a batched `OVERLAPPED_ENTRY`.
    ///
    /// `GetQueuedCompletionStatusEx` reports no per-entry error: the operation's `NTSTATUS`
    /// lives in the `Internal` field of the OVERLAPPED the kernel completed, and mapping it
    /// through `RtlNtStatusToDosError` is exactly what `GetQueuedCompletionStatus` does before
    /// returning `FALSE`. Entries posted by `PostQueuedCompletionStatus` carry no OVERLAPPED
    /// and therefore no status, matching the `TRUE` those posts get from the non-Ex call.
    fn from_entry(entry: &OVERLAPPED_ENTRY) -> Self {
        let overlapped = entry.lpOverlapped.cast::<Overlapped>();
        let status = if overlapped.is_null() {
            0
        } else {
            // SAFETY: a non-null `lpOverlapped` points at the OVERLAPPED the kernel just
            // completed; its owner keeps it alive until the completion is observed here.
            unsafe { (*entry.lpOverlapped).Internal as u32 as NTSTATUS }
        };

        // `NT_SUCCESS(status)` is `status >= 0`.
        let error_code = (status < 0).then(|| {
            // SAFETY: RtlNtStatusToDosError is a pure NTSTATUS -> Win32 error mapping.
            unsafe { RtlNtStatusToDosError(status) }
        });

        Self {
            bytes: entry.dwNumberOfBytesTransferred,
            key: entry.lpCompletionKey,
            overlapped,
            success: error_code.is_none(),
            error_code,
        }
    }
}

#[cfg(test)]
mod completion_status_tests {
    use super::*;
    use windows_sys::Win32::Foundation::{
        ERROR_OPERATION_ABORTED, STATUS_CANCELLED, STATUS_PENDING,
    };

    fn entry_with(overlapped: *mut OVERLAPPED, bytes: u32, key: usize) -> OVERLAPPED_ENTRY {
        OVERLAPPED_ENTRY {
            lpCompletionKey: key,
            lpOverlapped: overlapped,
            Internal: 0,
            dwNumberOfBytesTransferred: bytes,
        }
    }

    #[test]
    fn posted_entry_without_overlapped_is_a_success() {
        let status = CompletionStatus::from_entry(&entry_with(ptr::null_mut(), 0, 42));

        assert!(status.success);
        assert_eq!(status.error_code, None);
        assert_eq!(status.key, 42);
    }

    #[test]
    fn successful_overlapped_entry_reports_transferred_bytes() {
        let mut overlapped = Overlapped::zeroed();
        overlapped.0.Internal = STATUS_PENDING as u32 as usize;
        let status = CompletionStatus::from_entry(&entry_with(overlapped.as_mut_ptr(), 1024, 7));

        assert!(status.success, "a non-negative NTSTATUS is NT_SUCCESS");
        assert_eq!(status.error_code, None);
        assert_eq!(status.bytes, 1024);
    }

    #[test]
    fn cancelled_overlapped_entry_maps_to_the_win32_error_code() {
        let mut overlapped = Overlapped::zeroed();
        // The kernel stores the operation's NTSTATUS in `OVERLAPPED::Internal`; this is the
        // same source `GetQueuedCompletionStatus` converts into its `GetLastError` value.
        overlapped.0.Internal = STATUS_CANCELLED as u32 as usize;
        let status = CompletionStatus::from_entry(&entry_with(overlapped.as_mut_ptr(), 0, 7));

        assert!(!status.success);
        assert_eq!(status.error_code, Some(ERROR_OPERATION_ABORTED));
    }

    #[test]
    fn batch_only_exposes_entries_from_the_last_fill() {
        let batch = CompletionBatch::with_capacity(4);

        assert!(batch.status(0).is_none(), "a fresh batch holds no entries");
    }
}
