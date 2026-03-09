use std::io;
use windows_sys::Win32::Foundation::{CloseHandle, GetLastError, HANDLE, INVALID_HANDLE_VALUE};
use windows_sys::Win32::Storage::FileSystem::{
    CreateFileW, FILE_ALLOCATION_INFO, FILE_ATTRIBUTE_NORMAL, FILE_END_OF_FILE_INFO,
    FILE_FLAG_OVERLAPPED, FileAllocationInfo, FileEndOfFileInfo, FlushFileBuffers,
    SetFileInformationByHandle,
};
use windows_sys::Win32::System::IO::{OVERLAPPED, PostQueuedCompletionStatus};

#[derive(Debug, Clone, Copy)]
pub struct CompletionInfo {
    pub port: usize,
    pub user_data: usize,
    pub overlapped: usize,
}

#[repr(C)]
pub struct OverlappedEntry {
    pub inner: OVERLAPPED,
    pub user_data: usize,
    pub generation: u32,
    pub blocking_result: Option<io::Result<usize>>,
}

impl CompletionInfo {
    pub fn complete(self, result: io::Result<usize>) {
        if self.overlapped == 0 {
            return;
        }
        let ptr = self.overlapped as *mut OverlappedEntry;
        unsafe {
            (*ptr).blocking_result = Some(result);
            PostQueuedCompletionStatus(
                self.port as HANDLE,
                0,
                self.user_data,
                ptr as *mut OVERLAPPED,
            );
        }
    }
}

pub enum BlockingOps {
    Open {
        path_ptr: usize,
        flags: i32,
        mode: u32,
        completion: CompletionInfo,
    },
    Close {
        handle: usize,
        completion: CompletionInfo,
    },
    Fsync {
        handle: usize,
        completion: CompletionInfo,
    },
    SyncFileRange {
        handle: usize,
        completion: CompletionInfo,
    },
    Fallocate {
        handle: usize,
        mode: i32,
        offset: u64,
        len: u64,
        completion: CompletionInfo,
    },
}

impl BlockingOps {
    pub fn run(self) {
        match self {
            BlockingOps::Open {
                path_ptr,
                flags,
                mode,
                completion,
            } => {
                let real_disposition = mode & 0xFF;
                // Bits 8, 9 are used for Buffering Mode flags
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
                        path_ptr as *const u16,
                        flags as u32,
                        0,
                        std::ptr::null(),
                        real_disposition,
                        flags_and_attributes,
                        std::ptr::null_mut(),
                    )
                };

                let result = if handle == INVALID_HANDLE_VALUE {
                    let err = unsafe { GetLastError() };
                    Err(std::io::Error::from_raw_os_error(err as i32))
                } else {
                    Ok(handle as usize)
                };
                completion.complete(result);
            }
            BlockingOps::Close { handle, completion } => {
                let ret = unsafe { CloseHandle(handle as HANDLE) };
                let result = if ret == 0 {
                    let err = unsafe { GetLastError() };
                    Err(std::io::Error::from_raw_os_error(err as i32))
                } else {
                    Ok(0)
                };
                completion.complete(result);
            }
            BlockingOps::Fsync { handle, completion } => {
                let ret = unsafe { FlushFileBuffers(handle as HANDLE) };
                let result = if ret == 0 {
                    let err = unsafe { GetLastError() };
                    Err(std::io::Error::from_raw_os_error(err as i32))
                } else {
                    Ok(0)
                };
                completion.complete(result);
            }
            BlockingOps::SyncFileRange { handle, completion } => {
                // Windows doesn't support fine-grained sync_file_range.
                // Fallback to FlushFileBuffers equivalent to Fsync.
                let ret = unsafe { FlushFileBuffers(handle as HANDLE) };
                let result = if ret == 0 {
                    let err = unsafe { GetLastError() };
                    Err(std::io::Error::from_raw_os_error(err as i32))
                } else {
                    Ok(0)
                };
                completion.complete(result);
            }
            BlockingOps::Fallocate {
                handle,
                mode,
                offset,
                len,
                completion,
            } => {
                let result = (|| {
                    // Calculate required size
                    let req_size = offset + len;

                    // 1. Set allocation size (reserve space)
                    let mut alloc_info = FILE_ALLOCATION_INFO {
                        AllocationSize: req_size as i64,
                    };
                    let ret = unsafe {
                        SetFileInformationByHandle(
                            handle as HANDLE,
                            FileAllocationInfo,
                            &mut alloc_info as *mut _ as *mut _,
                            std::mem::size_of::<FILE_ALLOCATION_INFO>() as u32,
                        )
                    };
                    if ret == 0 {
                        return Err(std::io::Error::from_raw_os_error(
                            unsafe { GetLastError() } as i32
                        ));
                    }

                    // 2. If not KEEP_SIZE (mode 0), update file size
                    if mode == 0 {
                        let mut eof_info = FILE_END_OF_FILE_INFO {
                            EndOfFile: req_size as i64,
                        };
                        let ret = unsafe {
                            SetFileInformationByHandle(
                                handle as HANDLE,
                                FileEndOfFileInfo,
                                &mut eof_info as *mut _ as *mut _,
                                std::mem::size_of::<FILE_END_OF_FILE_INFO>() as u32,
                            )
                        };
                        if ret == 0 {
                            return Err(std::io::Error::from_raw_os_error(
                                unsafe { GetLastError() } as i32,
                            ));
                        }
                    }

                    Ok(0)
                })();
                completion.complete(result);
            }
        }
    }
}
