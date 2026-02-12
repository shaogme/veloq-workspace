use std::io;
use std::num::NonZeroUsize;
use std::ptr;
use std::ptr::NonNull;

// We use libc for mmap on Linux
// MAP_HUGETLB: 0x40000 (since Linux 2.6.32)
// MAP_POPULATE: 0x08000 (since Linux 2.5.46)
const MAP_HUGETLB: libc::c_int = 0x40000;
const MAP_POPULATE: libc::c_int = 0x08000;

pub unsafe fn alloc_huge_pages(size: NonZeroUsize) -> io::Result<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            size.get(),
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE,
            -1,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(ptr as *mut u8)
    }
}

pub unsafe fn alloc_pages(size: NonZeroUsize) -> io::Result<*mut u8> {
    let ptr = unsafe {
        libc::mmap(
            ptr::null_mut(),
            size.get(),
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | MAP_POPULATE,
            -1,
            0,
        )
    };

    if ptr == libc::MAP_FAILED {
        Err(io::Error::last_os_error())
    } else {
        Ok(ptr as *mut u8)
    }
}

pub unsafe fn free_pages(ptr: NonNull<u8>, size: NonZeroUsize) {
    unsafe {
        libc::munmap(ptr.as_ptr() as *mut _, size.get());
    }
}
