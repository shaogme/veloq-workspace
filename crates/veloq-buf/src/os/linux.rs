use veloq_std::{
    num::NonZeroUsize,
    ptr::{self, NonNull},
};

use libc::{
    __errno_location, _SC_PAGESIZE, MAP_ANONYMOUS, MAP_FAILED, MAP_PRIVATE, PROT_READ, PROT_WRITE,
    c_int, mmap, munmap, sysconf,
};

use crate::buffer::SystemError;

// We use libc for mmap on Linux
// MAP_HUGETLB: 0x40000 (since Linux 2.6.32)
// MAP_POPULATE: 0x08000 (since Linux 2.5.46)
const MAP_HUGETLB: c_int = 0x40000;
const MAP_POPULATE: c_int = 0x08000;

pub fn page_size() -> Option<usize> {
    // SAFETY: `sysconf` is called with the supported page-size selector. / 安全性：`sysconf` 使用受支持的页大小查询参数。
    let size = unsafe { sysconf(_SC_PAGESIZE) };
    usize::try_from(size).ok().filter(|size| *size > 0)
}

pub unsafe fn alloc_huge_pages(size: NonZeroUsize) -> Result<*mut u8, SystemError> {
    let ptr = unsafe {
        mmap(
            ptr::null_mut(),
            size.get(),
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB | MAP_POPULATE,
            -1,
            0,
        )
    };

    if ptr == MAP_FAILED {
        let err = unsafe { *__errno_location() };
        Err(SystemError::Os(err))
    } else {
        Ok(ptr as *mut u8)
    }
}

pub unsafe fn alloc_pages(size: NonZeroUsize) -> Result<*mut u8, SystemError> {
    let ptr = unsafe {
        mmap(
            ptr::null_mut(),
            size.get(),
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE,
            -1,
            0,
        )
    };

    if ptr == MAP_FAILED {
        let err = unsafe { *__errno_location() };
        Err(SystemError::Os(err))
    } else {
        Ok(ptr as *mut u8)
    }
}

pub unsafe fn free_pages(ptr: NonNull<u8>, size: NonZeroUsize) {
    unsafe {
        munmap(ptr.as_ptr() as *mut _, size.get());
    }
}

pub unsafe fn free_huge_pages(ptr: NonNull<u8>, size: NonZeroUsize) {
    unsafe {
        munmap(ptr.as_ptr() as *mut _, size.get());
    }
}
