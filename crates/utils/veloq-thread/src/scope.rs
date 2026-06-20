use crate::{PlatformImpl, traits::ThreadParkerTrait};
use alloc::sync::Arc;

#[cfg(feature = "std")]
use alloc::boxed::Box;

use core::{
    cell::UnsafeCell,
    marker::PhantomData,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[cfg(feature = "std")]
use core::{any::Any, sync::atomic::AtomicPtr};

#[cfg(feature = "std")]
use std::{
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    thread::panicking,
};

struct SafeUnsafeCell<T>(UnsafeCell<T>);
unsafe impl<T: Send> Send for SafeUnsafeCell<T> {}
unsafe impl<T: Send> Sync for SafeUnsafeCell<T> {}

impl<T> SafeUnsafeCell<T> {
    fn new(value: T) -> Self {
        Self(UnsafeCell::new(value))
    }

    unsafe fn get(&self) -> *mut T {
        self.0.get()
    }
}

#[cfg(feature = "std")]
pub(crate) struct PanicPayload {
    inner: Box<dyn Any + Send + 'static>,
}

pub(crate) struct ScopeData<P> {
    pub(crate) num_running_threads: AtomicUsize,
    pub(crate) parker: P,
    pub(crate) cancelled: AtomicBool,
    #[cfg(feature = "std")]
    pub(crate) panics: AtomicPtr<PanicPayload>,
    #[cfg(feature = "std")]
    pub(crate) has_panic: AtomicBool,
}

impl<P> ScopeData<P> {
    #[cfg(feature = "std")]
    fn pop_panic(&self) -> Option<Box<dyn Any + Send + 'static>> {
        let ptr = self.panics.swap(core::ptr::null_mut(), Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            unsafe {
                let payload = Box::from_raw(ptr);
                Some(payload.inner)
            }
        }
    }
}

impl<P> Drop for ScopeData<P> {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        {
            let ptr = self.panics.swap(core::ptr::null_mut(), Ordering::Relaxed);
            if !ptr.is_null() {
                unsafe {
                    let _ = Box::from_raw(ptr);
                }
            }
        }
    }
}

/// 结构化并发的作用域，用于管理在其中生成的线程的生命周期。
pub struct Scope<'scope, 'env: 'scope, P: PlatformImpl> {
    data: &'scope ScopeData<P::Parker>,
    _scope: PhantomData<&'scope mut &'scope ()>,
    _env: PhantomData<&'env mut &'env ()>,
}

/// 作用域内生成的线程的加入句柄，允许等待线程完成并获取其返回值。
pub struct ScopedJoinHandle<'scope, P: PlatformImpl, R> {
    thread: Option<P::Thread<'scope>>,
    result: Arc<SafeUnsafeCell<Option<R>>>,
    #[cfg(feature = "std")]
    scope_data: &'scope ScopeData<P::Parker>,
    #[cfg(not(feature = "std"))]
    _scope_data: PhantomData<&'scope ScopeData<P::Parker>>,
    _marker: PhantomData<&'scope R>,
}

// ScopedJoinHandle 可以在线程间 safe 地发送（如果 R 是 Send）
unsafe impl<'scope, P: PlatformImpl, R: Send> Send for ScopedJoinHandle<'scope, P, R> {}
unsafe impl<'scope, P: PlatformImpl, R: Sync> Sync for ScopedJoinHandle<'scope, P, R> {}

impl<'scope, 'env, P: PlatformImpl> Scope<'scope, 'env, P> {
    /// 检查当前作用域是否已被取消（例如主线程发生 panic）
    pub fn is_cancelled(&self) -> bool {
        self.data.cancelled.load(Ordering::Acquire)
    }

    /// 在当前作用域内生成一个新线程并执行闭包 `f`。
    pub fn spawn<F, R>(&'scope self, f: F) -> Result<ScopedJoinHandle<'scope, P, R>, P::Error>
    where
        F: FnOnce() -> R + Send + 'scope,
        R: Send + 'scope,
    {
        // 1. 在堆上分配一个共享槽位，用于接收子线程的返回值
        let result = Arc::new(SafeUnsafeCell::new(None));

        // 2. 包装闭包以写入返回值并通知主线程计数减少
        let scope_data = self.data;
        let result_clone = result.clone();
        let closure = move || {
            struct ThreadFinishedGuard<'a, P: ThreadParkerTrait> {
                data: &'a ScopeData<P>,
            }
            impl<P: ThreadParkerTrait> Drop for ThreadFinishedGuard<'_, P> {
                fn drop(&mut self) {
                    if self
                        .data
                        .num_running_threads
                        .fetch_sub(1, Ordering::Release)
                        == 1
                    {
                        self.data.parker.unpark();
                    }
                }
            }
            let _guard = ThreadFinishedGuard { data: scope_data };

            #[cfg(feature = "std")]
            {
                let res = catch_unwind(AssertUnwindSafe(f));
                match res {
                    Ok(r) => unsafe {
                        *result_clone.get() = Some(r);
                    },
                    Err(err) => {
                        scope_data.has_panic.store(true, Ordering::Release);
                        let payload = Box::into_raw(Box::new(PanicPayload { inner: err }));
                        if scope_data
                            .panics
                            .compare_exchange(
                                core::ptr::null_mut(),
                                payload,
                                Ordering::Release,
                                Ordering::Relaxed,
                            )
                            .is_err()
                        {
                            unsafe {
                                let _ = Box::from_raw(payload);
                            }
                        }
                    }
                }
            }
            #[cfg(not(feature = "std"))]
            {
                let res = f();
                unsafe {
                    *result_clone.get() = Some(res);
                }
            }
        };

        // 3. 原子递增运行线程计数
        self.data
            .num_running_threads
            .fetch_add(1, Ordering::Relaxed);

        // 4. 生成线程。如果在 spawn 时出错，我们需要立刻递减计数并抛出错误
        struct SpawnRollback<'a> {
            count: &'a AtomicUsize,
            active: bool,
        }
        impl Drop for SpawnRollback<'_> {
            fn drop(&mut self) {
                if self.active {
                    self.count.fetch_sub(1, Ordering::Relaxed);
                }
            }
        }
        let mut rollback = SpawnRollback {
            count: &self.data.num_running_threads,
            active: true,
        };

        let thread = P::spawn(closure)?;

        // 成功 spawn 线程，取消回滚
        rollback.active = false;

        Ok(ScopedJoinHandle {
            thread: Some(thread),
            result,
            #[cfg(feature = "std")]
            scope_data: self.data,
            #[cfg(not(feature = "std"))]
            _scope_data: PhantomData,
            _marker: PhantomData,
        })
    }
}

impl<'scope, P: PlatformImpl, R> ScopedJoinHandle<'scope, P, R> {
    /// 等待子线程执行结束并返回其结果。
    pub fn join(mut self) -> Result<R, P::Error> {
        let thread = self.thread.take().expect("thread already joined");
        P::join(thread)?;

        unsafe {
            let val = (&mut *self.result.get()).take();
            if let Some(r) = val {
                Ok(r)
            } else {
                #[cfg(feature = "std")]
                {
                    let panic_payload = self.scope_data.pop_panic();
                    if let Some(p) = panic_payload {
                        resume_unwind(p);
                    }
                }
                panic!("thread finished but no result found");
            }
        }
    }

    /// 中止子线程的执行。
    pub fn abort(&self) -> Result<(), P::Error> {
        if let Some(ref thread) = self.thread {
            P::abort(thread)?;
        }
        Ok(())
    }
}

struct ScopeGuard<'scope, P: ThreadParkerTrait> {
    data: &'scope ScopeData<P>,
    completed_successfully: bool,
}
impl<P: ThreadParkerTrait> Drop for ScopeGuard<'_, P> {
    fn drop(&mut self) {
        if !self.completed_successfully {
            self.data.cancelled.store(true, Ordering::Release);
        }

        while self.data.num_running_threads.load(Ordering::Acquire) != 0 {
            self.data.parker.park();
        }

        #[cfg(feature = "std")]
        {
            if !panicking() {
                let panic_payload = self.data.pop_panic();
                if let Some(p) = panic_payload {
                    resume_unwind(p);
                } else if self.data.has_panic.load(Ordering::Acquire) {
                    panic!("child thread panicked");
                }
            }
        }
    }
}

/// 泛型作用域入口函数，供 `PlatformImpl::scope` 调用
pub fn scope_in<'env, P, F, R>(f: F) -> R
where
    P: PlatformImpl,
    F: for<'scope> FnOnce(&'scope Scope<'scope, 'env, P>) -> R,
{
    let scope_data = ScopeData {
        num_running_threads: AtomicUsize::new(0),
        parker: P::Parker::new(),
        cancelled: AtomicBool::new(false),
        #[cfg(feature = "std")]
        panics: AtomicPtr::new(core::ptr::null_mut()),
        #[cfg(feature = "std")]
        has_panic: AtomicBool::new(false),
    };

    let scope = Scope {
        data: &scope_data,
        _scope: PhantomData,
        _env: PhantomData,
    };

    let mut guard = ScopeGuard {
        data: &scope_data,
        completed_successfully: false,
    };

    let res = f(&scope);
    guard.completed_successfully = true;
    res
}
