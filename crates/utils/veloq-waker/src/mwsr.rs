use veloq_std::{
    fmt,
    mem::ManuallyDrop,
    ptr,
    sync::atomic::{
        AtomicPtr,
        Ordering::{AcqRel, Acquire, Relaxed, Release},
    },
    task::{RawWaker, RawWakerVTable, Waker},
};

const TAG_MASK: usize = 0b11;
const REGISTERED: usize = 0b01;
const WAKING: usize = 0b10;
const REGISTERING: usize = 0b11;

// A const NOOP_VTABLE as Waker::noop vtable cannot be accessed in const context.
static NOOP_VTABLE: RawWakerVTable = RawWakerVTable::new(
    |_| RawWaker::new(ptr::null(), &NOOP_VTABLE),
    |_| (),
    |_| (),
    |_| (),
);
const NOOP_PTR: *mut RawWakerVTable = &NOOP_VTABLE as *const RawWakerVTable as *mut RawWakerVTable;

trait TaggedPointerExt {
    fn set(self, tag: usize) -> Self;
    fn unset(self, tag: usize) -> Self;
    fn tag(self) -> usize;
}

impl<T> TaggedPointerExt for *mut T {
    #[inline(always)]
    fn set(self, tag: usize) -> Self {
        (((self as usize) & !TAG_MASK) | tag) as *mut T
    }
    #[inline(always)]
    fn unset(self, tag: usize) -> Self {
        ((self as usize) & !tag) as *mut T
    }
    #[inline(always)]
    fn tag(self) -> usize {
        (self as usize) & TAG_MASK
    }
}

trait WakerExt {
    fn vtable_ptr(&self) -> *mut RawWakerVTable;
}

impl WakerExt for Waker {
    #[inline(always)]
    fn vtable_ptr(&self) -> *mut RawWakerVTable {
        self.vtable() as *const RawWakerVTable as *mut RawWakerVTable
    }
}

/// A specialized synchronization primitive for task wakeup, optimized for
/// Single-Register (单注册者) and Multi-Wake (多唤醒者) scenarios.
///
/// Unlike `AtomicWaker`, `MwsrWaker` requires that at most one thread/task
/// calls `register` concurrently. This allows for simpler state transitions
/// and better performance. Because of this, `MwsrWaker::register` is marked
/// as `unsafe`.
pub struct MwsrWaker {
    vtable: AtomicPtr<RawWakerVTable>,
    data: AtomicPtr<()>,
}

impl MwsrWaker {
    /// Create an `MwsrWaker`.
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        MwsrWaker {
            vtable: AtomicPtr::new(NOOP_PTR),
            data: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Create an `MwsrWaker`.
    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        MwsrWaker {
            vtable: AtomicPtr::new(NOOP_PTR),
            data: AtomicPtr::new(ptr::null_mut()),
        }
    }

    /// Registers the waker to be notified on calls to `wake`.
    ///
    /// # Safety
    ///
    /// The caller must ensure that there are **no concurrent calls** to `register`.
    /// Calling this function concurrently from multiple threads/tasks is undefined behavior.
    /// However, it is fully safe to call `register` concurrently with `wake`.
    pub unsafe fn register(&self, waker: &Waker) {
        let mut vtable = self.vtable.load(Acquire);

        loop {
            let tag = vtable.tag();

            // 如果当前正在唤醒，为了避免丢失唤醒，必须立即唤醒新 waker 并返回
            if tag == WAKING {
                waker.wake_by_ref();
                return;
            }

            if tag == REGISTERING {
                core::hint::spin_loop();
                vtable = self.vtable.load(Acquire);
                continue;
            }

            if tag == 0 {
                // 如果是 NOOP_PTR，可以直接安全地发布注册，因为此时 take() 不会产生作用
                if vtable == NOOP_PTR {
                    let owned_waker = ManuallyDrop::new(waker.clone());
                    self.data.store(owned_waker.data() as *mut (), Release);
                    let new_vtable = owned_waker.vtable_ptr().set(REGISTERED);
                    self.vtable.store(new_vtable, Release);
                    return;
                }
            }

            if tag == REGISTERED {
                // Fast-path: check if it is the same waker.
                let clean_vtable = vtable.unset(TAG_MASK);
                let current_data = self.data.load(Acquire) as *const ();
                if waker.vtable_ptr() == clean_vtable
                    && waker.data() == current_data
                    && self.vtable.load(Relaxed) == vtable
                {
                    return;
                }

                // 尝试获取 REGISTERING 状态锁
                let registering_vtable = vtable.set(REGISTERING);
                match self
                    .vtable
                    .compare_exchange(vtable, registering_vtable, AcqRel, Acquire)
                {
                    Ok(_) => {
                        let current_data = self.data.load(Acquire) as *const ();

                        // 在锁保护下进行缓存命中的二次校验
                        if waker.vtable_ptr() == clean_vtable && waker.data() == current_data {
                            let target = clean_vtable.set(REGISTERED);
                            match self.vtable.compare_exchange(
                                registering_vtable,
                                target,
                                Release,
                                Acquire,
                            ) {
                                Ok(_) => return,
                                Err(actual) => {
                                    debug_assert_eq!(actual.tag(), WAKING);
                                    self.vtable.store(clean_vtable, Release);
                                    waker.wake_by_ref();
                                    return;
                                }
                            }
                        }

                        // Cache Miss 路径：销毁旧 Waker
                        if clean_vtable != NOOP_PTR {
                            let old_waker = unsafe { Waker::new(current_data, &*clean_vtable) };
                            drop(old_waker);
                        }

                        // 拷贝并写入新 Waker
                        let owned_waker = ManuallyDrop::new(waker.clone());
                        self.data.store(owned_waker.data() as *mut (), Release);
                        let new_vtable = owned_waker.vtable_ptr().set(REGISTERED);

                        // 尝试释放锁并发布新注册
                        match self.vtable.compare_exchange(
                            registering_vtable,
                            new_vtable,
                            Release,
                            Acquire,
                        ) {
                            Ok(_) => return,
                            Err(actual) => {
                                debug_assert_eq!(actual.tag(), WAKING);
                                self.vtable.store(clean_vtable, Release);
                                self.data.store(ptr::null_mut(), Release);
                                let raw_waker = ManuallyDrop::into_inner(owned_waker);
                                raw_waker.wake();
                                return;
                            }
                        }
                    }
                    Err(actual) => {
                        vtable = actual;
                        continue;
                    }
                }
            }

            core::hint::spin_loop();
            vtable = self.vtable.load(Acquire);
        }
    }

    /// Calls `wake` on the last `Waker` passed to `register`.
    ///
    /// If `register` has not been called yet, then this does nothing.
    pub fn wake(&self) {
        if let Some(waker) = self.take() {
            waker.wake();
        }
    }

    /// Returns the last `Waker` passed to `register`, so that the user can wake it.
    ///
    /// If a waker has not been registered, this returns `None`.
    pub fn take(&self) -> Option<Waker> {
        let mut vtable = self.vtable.load(Relaxed);
        loop {
            let tag = vtable.tag();
            if tag == REGISTERING {
                let waking_vtable = vtable.set(WAKING);
                match self
                    .vtable
                    .compare_exchange(vtable, waking_vtable, AcqRel, Acquire)
                {
                    Ok(_) => return None,
                    Err(actual) => vtable = actual,
                }
            } else if tag == REGISTERED {
                let waking_vtable = vtable.set(WAKING);
                match self
                    .vtable
                    .compare_exchange(vtable, waking_vtable, AcqRel, Acquire)
                {
                    Ok(_) => {
                        let clean = vtable.unset(TAG_MASK);
                        let data = self.data.swap(ptr::null_mut(), AcqRel) as *const ();
                        self.vtable.store(NOOP_PTR, Release);
                        let waker = unsafe { Waker::new(data, &*clean) };
                        return Some(waker);
                    }
                    Err(actual) => {
                        vtable = actual;
                    }
                }
            } else {
                return None;
            }
        }
    }
}

impl Drop for MwsrWaker {
    fn drop(&mut self) {
        let vtable = self.vtable.load(Relaxed);
        if vtable.tag() == REGISTERED {
            let clean = vtable.unset(TAG_MASK);
            let data = self.data.load(Acquire) as *const ();
            if clean != NOOP_PTR {
                let waker = unsafe { Waker::new(data, &*clean) };
                drop(waker);
            }
        }
    }
}

impl Default for MwsrWaker {
    fn default() -> Self {
        MwsrWaker::new()
    }
}

impl fmt::Debug for MwsrWaker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "MwsrWaker")
    }
}

unsafe impl Send for MwsrWaker {}
unsafe impl Sync for MwsrWaker {}
