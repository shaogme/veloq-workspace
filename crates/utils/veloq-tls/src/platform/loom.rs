extern crate std;

use crate::{PlatformKey, TlsErrorKind};
use alloc::{boxed::Box, vec::Vec};
use core::{
    cell::RefCell,
    ptr::null_mut,
    sync::atomic::{AtomicU32, Ordering},
};
use loom::thread;

static NEXT_KEY: AtomicU32 = AtomicU32::new(0);

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct SendPtr(*mut ());

unsafe impl Send for SendPtr {}
unsafe impl Sync for SendPtr {}

type DestructorFn = unsafe fn(*mut ());

struct ThreadValue {
    ptr: SendPtr,
    destructor: DestructorFn,
}

impl Drop for ThreadValue {
    fn drop(&mut self) {
        unsafe {
            (self.destructor)(self.ptr.0);
        }
    }
}

loom::thread_local! {
    static THREAD_VALUES: RefCell<Vec<(u32, ThreadValue)>> = RefCell::new(Vec::new());
}

unsafe fn tls_destructor_shim<T>(ptr: *mut ()) {
    // 过滤哨兵指针以防堆损坏
    if !ptr.is_null() && !crate::is_sentinel(ptr) {
        unsafe {
            let _ = Box::from_raw(ptr as *mut T);
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub(crate) struct Key(u32);

impl PlatformKey for Key {
    #[inline]
    unsafe fn free(self) {
        let _ = THREAD_VALUES.try_with(|cell| {
            let mut values = cell.borrow_mut();
            if let Some(pos) = values.iter().position(|(k, _)| *k == self.0) {
                values.remove(pos);
            }
        });
    }

    #[inline]
    unsafe fn get_value<T>(self) -> *mut T {
        THREAD_VALUES
            .try_with(|cell| {
                let values = cell.borrow();
                if let Some(pair) = values.iter().find(|(k, _)| *k == self.0) {
                    pair.1.ptr.0 as *mut T
                } else {
                    null_mut()
                }
            })
            .unwrap_or(null_mut())
    }

    #[inline]
    unsafe fn set_value<T>(self, ptr: *mut T) -> Result<(), i32> {
        let res = THREAD_VALUES.try_with(|cell| {
            let mut values = cell.borrow_mut();
            if ptr.is_null() {
                if let Some(pos) = values.iter().position(|(k, _)| *k == self.0) {
                    values.remove(pos);
                }
            } else {
                let val = ThreadValue {
                    ptr: SendPtr(ptr as *mut ()),
                    destructor: tls_destructor_shim::<T>,
                };
                if let Some(pair) = values.iter_mut().find(|(k, _)| *k == self.0) {
                    pair.1 = val;
                } else {
                    values.push((self.0, val));
                }
            }
            Ok(())
        });
        match res {
            Ok(r) => r,
            Err(_) => Err(-1),
        }
    }
}

pub(crate) struct AtomicKey(AtomicU32);

impl AtomicKey {
    pub const fn new() -> Self {
        Self(AtomicU32::new(0))
    }

    #[inline]
    pub fn get<T>(&self) -> Result<Key, TlsErrorKind> {
        let mut val = self.0.load(Ordering::Acquire);
        if val > 1 {
            return Ok(Key(val - 2));
        }

        loop {
            if val == 0 {
                match self
                    .0
                    .compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed)
                {
                    Ok(_) => {
                        let k = NEXT_KEY.fetch_add(1, Ordering::Relaxed);
                        let stored = k + 2;
                        self.0.store(stored, Ordering::Release);
                        return Ok(Key(k));
                    }
                    Err(current) => {
                        val = current;
                    }
                }
            } else if val > 1 {
                return Ok(Key(val - 2));
            } else {
                thread::yield_now();
                val = self.0.load(Ordering::Acquire);
            }
        }
    }

    #[inline]
    pub fn take(&mut self) -> Option<Key> {
        let val = *self.0.get_mut();
        if val > 1 { Some(Key(val - 2)) } else { None }
    }
}
