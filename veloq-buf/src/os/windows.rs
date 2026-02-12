use std::io;
use std::num::NonZeroUsize;
use std::ptr;
use std::ptr::NonNull;
use windows_sys::Win32::System::Memory::{
    MEM_COMMIT, MEM_LARGE_PAGES, MEM_RELEASE, MEM_RESERVE, PAGE_READWRITE, VirtualAlloc,
    VirtualFree,
};

pub unsafe fn alloc_huge_pages(size: NonZeroUsize) -> io::Result<*mut u8> {
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
        Err(io::Error::last_os_error())
    } else {
        Ok(ptr as *mut u8)
    }
}

pub unsafe fn alloc_pages(size: NonZeroUsize) -> io::Result<*mut u8> {
    let ptr = unsafe {
        VirtualAlloc(
            ptr::null_mut(),
            size.get(),
            MEM_COMMIT | MEM_RESERVE,
            PAGE_READWRITE,
        )
    };

    if ptr.is_null() {
        Err(io::Error::last_os_error())
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
