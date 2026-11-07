use veloq_std::{
    mem::zeroed,
    num::NonZeroUsize,
    ptr::{self, NonNull},
};

use windows_sys::Win32::{
    Foundation::GetLastError,
    System::{
        Memory::{
            MEM_COMMIT, MEM_LARGE_PAGES, MEM_RELEASE, MEM_RESERVE, PAGE_READWRITE, VirtualAlloc,
            VirtualFree,
        },
        SystemInformation::{GetSystemInfo, SYSTEM_INFO},
    },
};

use crate::buffer::SystemError;

pub fn page_size() -> Option<usize> {
    // SAFETY: `SYSTEM_INFO` is an output structure that Windows fills completely. / 安全性：`SYSTEM_INFO` 是由 Windows 完整写入的输出结构。
    let mut info: SYSTEM_INFO = unsafe { zeroed() };
    // SAFETY: `info` points to writable storage with the required `SYSTEM_INFO` layout. / 安全性：`info` 指向满足布局要求且可写的 `SYSTEM_INFO` 存储空间。
    unsafe { GetSystemInfo(&mut info) };
    usize::try_from(info.dwPageSize)
        .ok()
        .filter(|size| *size > 0)
}

pub unsafe fn alloc_huge_pages(size: NonZeroUsize) -> Result<*mut u8, SystemError> {
    // Windows requires the SeLockMemoryPrivilege for MEM_LARGE_PAGES to work.
    let ptr = unsafe {
        VirtualAlloc(
            ptr::null_mut(),
            size.get(),
            MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES,
            PAGE_READWRITE,
        )
    };

    if ptr.is_null() {
        let err = unsafe { GetLastError() };
        Err(SystemError::Os(err as i32))
    } else {
        Ok(ptr as *mut u8)
    }
}

pub unsafe fn alloc_pages(size: NonZeroUsize) -> Result<*mut u8, SystemError> {
    let ptr = unsafe {
        VirtualAlloc(
            ptr::null_mut(),
            size.get(),
            MEM_COMMIT | MEM_RESERVE,
            PAGE_READWRITE,
        )
    };

    if ptr.is_null() {
        let err = unsafe { GetLastError() };
        Err(SystemError::Os(err as i32))
    } else {
        Ok(ptr as *mut u8)
    }
}

pub unsafe fn free_pages(ptr: NonNull<u8>, _size: NonZeroUsize) {
    // MEM_RELEASE: "dwSize must be 0"
    unsafe {
        VirtualFree(ptr.as_ptr() as *mut _, 0, MEM_RELEASE);
    }
}

pub unsafe fn free_huge_pages(ptr: NonNull<u8>, _size: NonZeroUsize) {
    unsafe {
        VirtualFree(ptr.as_ptr() as *mut _, 0, MEM_RELEASE);
    }
}
