use std::io;
use std::ptr;
use veloq_pod::{Pod, Zeroable, zeroed};
use windows_sys::Win32::Foundation::{
    CloseHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE, WAIT_TIMEOUT,
};
use windows_sys::Win32::Networking::WinSock::{
    INVALID_SOCKET, SOCKADDR, SOCKET, bind, closesocket, getsockname, listen, setsockopt,
};
use windows_sys::Win32::System::IO::{
    CancelIoEx, CreateIoCompletionPort, GetQueuedCompletionStatus, OVERLAPPED,
    PostQueuedCompletionStatus,
};

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
    /// Returns a zero-initialized Overlapped structure.
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
        self.0.Anonymous.Anonymous.Offset = offset as u32;
        self.0.Anonymous.Anonymous.OffsetHigh = (offset >> 32) as u32;
    }

    /// Returns the offset of the overlapped operation.
    pub fn offset(&self) -> u64 {
        let low = unsafe { self.0.Anonymous.Anonymous.Offset };
        let high = unsafe { self.0.Anonymous.Anonymous.OffsetHigh };
        (low as u64) | ((high as u64) << 32)
    }
}

impl Default for Overlapped {
    fn default() -> Self {
        Self::zeroed()
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
    pub fn bind(&self, addr: *const SOCKADDR, len: i32) -> io::Result<()> {
        let ret = unsafe { bind(self.0, addr, len) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Listens for incoming connections.
    pub fn listen(&self, backlog: i32) -> io::Result<()> {
        let ret = unsafe { listen(self.0, backlog) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Retrieves the local address of the socket.
    pub fn getsockname(&self, addr: *mut SOCKADDR, len: *mut i32) -> io::Result<()> {
        let ret = unsafe { getsockname(self.0, addr, len) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Sets a socket option.
    pub fn setsockopt<T>(&self, level: i32, optname: i32, optval: &T) -> io::Result<()> {
        let ret = unsafe {
            setsockopt(
                self.0,
                level,
                optname,
                optval as *const T as *const u8,
                std::mem::size_of::<T>() as i32,
            )
        };
        if ret != 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Sets a socket option with an empty payload.
    pub fn setsockopt_empty(&self, level: i32, optname: i32) -> io::Result<()> {
        let ret = unsafe { setsockopt(self.0, level, optname, std::ptr::null(), 0) };
        if ret != 0 {
            return Err(io::Error::last_os_error());
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

impl IoCompletionPort {
    /// Creates a new, unconnected I/O Completion Port.
    pub fn new(threads: u32) -> io::Result<Self> {
        let handle =
            unsafe { CreateIoCompletionPort(INVALID_HANDLE_VALUE, ptr::null_mut(), 0, threads) };
        if handle.is_null() {
            return Err(io::Error::last_os_error());
        }
        Ok(Self(OwnedHandle(handle)))
    }

    /// Associates a handle with this I/O Completion Port.
    pub unsafe fn associate(&self, handle: HANDLE, completion_key: usize) -> io::Result<()> {
        let res = unsafe { CreateIoCompletionPort(handle, self.0.as_raw(), completion_key, 0) };
        if res.is_null() {
            let err = unsafe { GetLastError() };
            if err == windows_sys::Win32::Foundation::ERROR_INVALID_PARAMETER {
                return Ok(());
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        }
        Ok(())
    }

    /// Posts a completion status to the port.
    pub unsafe fn post(
        &self,
        bytes: u32,
        key: usize,
        overlapped: *mut Overlapped,
    ) -> io::Result<()> {
        let res = unsafe {
            PostQueuedCompletionStatus(self.0.as_raw(), bytes, key, overlapped as *mut OVERLAPPED)
        };
        if res == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    /// Notifies the completion port with a user-defined completion key.
    pub fn notify(&self, user_data: usize) -> io::Result<()> {
        unsafe { self.post(0, user_data, ptr::null_mut()) }
    }

    /// Cancels a pending I/O request.
    pub unsafe fn cancel_request(handle: HANDLE, overlapped: *mut Overlapped) -> io::Result<()> {
        let res = unsafe { CancelIoEx(handle, overlapped as *mut OVERLAPPED) };
        if res == 0 {
            let err = unsafe { GetLastError() };
            if err == windows_sys::Win32::Foundation::ERROR_NOT_FOUND {
                return Ok(());
            }
            return Err(io::Error::from_raw_os_error(err as i32));
        }
        Ok(())
    }

    /// Retrieves a completion status from the port.
    pub fn get_status(&self, timeout_ms: u32) -> io::Result<CompletionStatus> {
        let mut bytes = 0;
        let mut key = 0;
        let mut overlapped = ptr::null_mut();

        let res = unsafe {
            GetQueuedCompletionStatus(
                self.0.as_raw(),
                &mut bytes,
                &mut key,
                &mut overlapped,
                timeout_ms,
            )
        };

        if res == 0 {
            let err = unsafe { GetLastError() };
            if overlapped.is_null() {
                if err == WAIT_TIMEOUT {
                    return Ok(CompletionStatus::Timeout);
                }
                return Err(io::Error::from_raw_os_error(err as i32));
            } else {
                return Ok(CompletionStatus::Completed {
                    bytes,
                    key,
                    overlapped: overlapped as *mut Overlapped,
                    success: false,
                    error_code: Some(err),
                });
            }
        }

        Ok(CompletionStatus::Completed {
            bytes,
            key,
            overlapped: overlapped as *mut Overlapped,
            success: true,
            error_code: None,
        })
    }

    /// Returns the raw HANDLE of the completion port.
    pub fn as_raw(&self) -> HANDLE {
        self.0.as_raw()
    }
}

/// Represents the status of a completed I/O operation.
pub enum CompletionStatus {
    Completed {
        bytes: u32,
        key: usize,
        overlapped: *mut Overlapped,
        success: bool,
        error_code: Option<u32>,
    },
    Timeout,
}

// ============================================================================
// OverlappedId
// ============================================================================

/// A handle to an overlapped operation, represented as a slot index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OverlappedId(pub usize);

impl OverlappedId {
    /// Recovers the OverlappedId from a raw pointer to an Overlapped structure.
    ///
    /// # Safety
    ///
    /// The pointer must be a valid pointer to an Overlapped structure that is
    /// embedded at the start of an OverlappedEntry.
    pub unsafe fn from_ptr(ptr: *const Overlapped) -> Self {
        use crate::ops::OverlappedEntry;
        // SAFETY: The `inner` field is at the start of `OverlappedEntry` due to `#[repr(C)]`.
        let user_data = unsafe { (*(ptr as *const OverlappedEntry)).user_data };
        Self(user_data)
    }

    /// Returns the raw index.
    pub fn as_usize(&self) -> usize {
        self.0
    }
}
