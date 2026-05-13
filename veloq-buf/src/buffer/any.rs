//! Type-erased buffer pool.

use super::common::BufPool;
use super::handle::FixedBuf;
use std::num::NonZeroUsize;

/// 手写 VTable，用于动态分发 BufPool 的方法而不使用 dyn
pub struct BufPoolVTable {
    pub alloc: unsafe fn(*const u8, NonZeroUsize) -> Option<FixedBuf>,
    pub clone: unsafe fn(*const u8) -> AnyBufPool,
    pub drop: unsafe fn(*mut u8),
    pub fmt: unsafe fn(*const u8, &mut std::fmt::Formatter<'_>) -> std::fmt::Result,
}

/// A type-erased handle to any `BufPool`.
///
/// Designed with Small Object Optimization (SOO) to eliminate heap allocations
/// for common pool implementations (like `SlotBasedPool` which is just an `Arc`).
pub struct AnyBufPool {
    storage: [usize; 3], // 24 bytes
    vtable: &'static BufPoolVTable,
}

impl AnyBufPool {
    /// 从任意实现了 `BufPool + Clone` 的类型构造 `AnyBufPool`。
    pub fn new<P: BufPool + Clone>(pool: P) -> Self {
        // Size of the storage in bytes
        const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();

        // Check if P fits in storage (SOO)
        let is_inline = std::mem::size_of::<P>() <= STORAGE_SIZE
            && std::mem::align_of::<P>() <= std::mem::align_of::<usize>();

        unsafe fn alloc_shim<P: BufPool + Clone>(
            ptr: *const u8,
            size: NonZeroUsize,
        ) -> Option<FixedBuf> {
            const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();
            if std::mem::size_of::<P>() <= STORAGE_SIZE
                && std::mem::align_of::<P>() <= std::mem::align_of::<usize>()
            {
                let pool = unsafe { &*(ptr as *const P) };
                pool.alloc(size)
            } else {
                let pool = unsafe { &**(ptr as *const *const P) };
                pool.alloc(size)
            }
        }

        unsafe fn clone_shim<P: BufPool + Clone>(ptr: *const u8) -> AnyBufPool {
            const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();
            if std::mem::size_of::<P>() <= STORAGE_SIZE
                && std::mem::align_of::<P>() <= std::mem::align_of::<usize>()
            {
                let pool = unsafe { &*(ptr as *const P) };
                AnyBufPool::new(pool.clone())
            } else {
                let pool = unsafe { &**(ptr as *const *const P) };
                AnyBufPool::new(pool.clone())
            }
        }

        unsafe fn drop_shim<P: BufPool + Clone>(ptr: *mut u8) {
            const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();
            if std::mem::size_of::<P>() <= STORAGE_SIZE
                && std::mem::align_of::<P>() <= std::mem::align_of::<usize>()
            {
                unsafe { std::ptr::drop_in_place(ptr as *mut P) };
            } else {
                unsafe {
                    let _ = Box::from_raw(*(ptr as *mut *mut P));
                }
            }
        }

        unsafe fn fmt_shim<P: BufPool + Clone>(
            ptr: *const u8,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            const STORAGE_SIZE: usize = std::mem::size_of::<[usize; 3]>();
            if std::mem::size_of::<P>() <= STORAGE_SIZE
                && std::mem::align_of::<P>() <= std::mem::align_of::<usize>()
            {
                let pool = unsafe { &*(ptr as *const P) };
                std::fmt::Debug::fmt(pool, f)
            } else {
                let pool = unsafe { &**(ptr as *const *const P) };
                std::fmt::Debug::fmt(pool, f)
            }
        }

        struct VTableGen<P>(std::marker::PhantomData<P>);

        impl<P: BufPool + Clone> VTableGen<P> {
            const VTABLE: BufPoolVTable = BufPoolVTable {
                alloc: alloc_shim::<P>,
                clone: clone_shim::<P>,
                drop: drop_shim::<P>,
                fmt: fmt_shim::<P>,
            };
        }

        let mut storage = [0usize; 3];
        if is_inline {
            unsafe {
                std::ptr::write(storage.as_mut_ptr() as *mut P, pool);
            }
        } else {
            storage[0] = Box::into_raw(Box::new(pool)) as usize;
        }

        AnyBufPool {
            storage,
            vtable: &VTableGen::<P>::VTABLE,
        }
    }
}

impl BufPool for AnyBufPool {
    fn alloc(&self, len: NonZeroUsize) -> Option<FixedBuf> {
        unsafe { (self.vtable.alloc)(self.storage.as_ptr() as *const u8, len) }
    }
}

impl Clone for AnyBufPool {
    fn clone(&self) -> Self {
        unsafe { (self.vtable.clone)(self.storage.as_ptr() as *const u8) }
    }
}

impl Drop for AnyBufPool {
    fn drop(&mut self) {
        unsafe { (self.vtable.drop)(self.storage.as_mut_ptr() as *mut u8) }
    }
}

impl std::fmt::Debug for AnyBufPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        unsafe { (self.vtable.fmt)(self.storage.as_ptr() as *const u8, f) }
    }
}
