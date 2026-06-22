#[cfg(feature = "std")]
use crate::traits::RawThreadErrorTrait;
use crate::traits::{PlatformImpl, RawJoinHandleTrait, ThreadParkerTrait};

#[cfg(feature = "std")]
use alloc::boxed::Box;

#[cfg(feature = "std")]
use core::sync::atomic::AtomicPtr;
use core::{
    marker::PhantomData,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
};

#[cfg(feature = "std")]
use std::{
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    thread::panicking,
};

pub(crate) struct RawScopeData<P: PlatformImpl> {
    pub(crate) num_running_threads: AtomicUsize,
    pub(crate) parker: P::Parker,
    pub(crate) cancelled: AtomicBool,
    #[cfg(feature = "std")]
    pub(crate) panics: AtomicPtr<P::Error>,
    #[cfg(feature = "std")]
    pub(crate) has_panic: AtomicBool,
}

impl<P: PlatformImpl> RawScopeData<P> {
    #[cfg(feature = "std")]
    fn pop_panic(&self) -> Option<P::Error> {
        let ptr = self.panics.swap(core::ptr::null_mut(), Ordering::Acquire);
        if ptr.is_null() {
            None
        } else {
            unsafe {
                let payload = Box::from_raw(ptr);
                Some(*payload)
            }
        }
    }
}

impl<P: PlatformImpl> Drop for RawScopeData<P> {
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
pub struct RawScope<'scope, 'env: 'scope, P: PlatformImpl> {
    data: &'scope RawScopeData<P>,
    _scope: PhantomData<&'scope mut &'scope ()>,
    _env: PhantomData<&'env mut &'env ()>,
}

/// 作用域内生成的线程的加入句柄，允许等待线程完成并获取其返回值。
pub struct RawScopedJoinHandle<'scope, P: PlatformImpl, R: Send + 'scope> {
    handle: Option<P::RawJoinHandle<'scope, Option<R>>>,
    #[cfg(feature = "std")]
    scope_data: &'scope RawScopeData<P>,
    #[cfg(not(feature = "std"))]
    _scope_data: PhantomData<&'scope RawScopeData<P>>,
}

unsafe impl<'scope, P: PlatformImpl, R: Send + 'scope> Send for RawScopedJoinHandle<'scope, P, R> {}
unsafe impl<'scope, P: PlatformImpl, R: Send + Sync + 'scope> Sync
    for RawScopedJoinHandle<'scope, P, R>
{
}

impl<'scope, 'env, P: PlatformImpl> RawScope<'scope, 'env, P> {
    /// 检查当前作用域是否已被取消（例如主线程发生 panic）
    pub fn is_cancelled(&self) -> bool {
        self.data.cancelled.load(Ordering::Acquire)
    }

    /// 在当前作用域内生成一个新线程并执行闭包 `f`。
    pub fn spawn<F, R>(&'scope self, f: F) -> Result<RawScopedJoinHandle<'scope, P, R>, P::Error>
    where
        F: FnOnce() -> R + Send + 'env,
        R: Send + 'env,
    {
        let scope_data = self.data;
        let closure = move || {
            struct ThreadFinishedGuard<'a, P: PlatformImpl> {
                data: &'a RawScopeData<P>,
            }
            impl<P: PlatformImpl> Drop for ThreadFinishedGuard<'_, P> {
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
                    Ok(r) => Some(r),
                    Err(err) => {
                        scope_data.has_panic.store(true, Ordering::Release);
                        let raw_err = P::Error::from_panic(err);
                        let payload = Box::into_raw(Box::new(raw_err));
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
                        None
                    }
                }
            }
            #[cfg(not(feature = "std"))]
            {
                Some(f())
            }
        };

        self.data
            .num_running_threads
            .fetch_add(1, Ordering::Relaxed);

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

        let handle = P::spawn(closure)?;

        rollback.active = false;

        Ok(RawScopedJoinHandle {
            handle: Some(handle),
            #[cfg(feature = "std")]
            scope_data: self.data,
            #[cfg(not(feature = "std"))]
            _scope_data: PhantomData,
        })
    }
}

impl<'scope, P: PlatformImpl, R: Send + 'scope> RawScopedJoinHandle<'scope, P, R> {
    /// 等待子线程执行结束并返回其结果。
    pub fn join(mut self) -> Result<R, P::Error> {
        let handle = self.handle.take().expect("handle already joined");
        let res = handle.join()?;

        if let Some(r) = res {
            Ok(r)
        } else {
            #[cfg(feature = "std")]
            {
                let panic_err = self.scope_data.pop_panic();
                if let Some(mut err) = panic_err {
                    if let Some(p) = err.take_panic() {
                        resume_unwind(p);
                    }
                }
            }
            panic!("handle finished but no result found");
        }
    }

    /// 中止子线程的执行。
    pub fn abort(&self) -> Result<(), P::Error> {
        if let Some(ref handle) = self.handle {
            handle.abort()?;
        }
        Ok(())
    }
}

struct RawScopeGuard<'scope, P: PlatformImpl> {
    data: &'scope RawScopeData<P>,
    completed_successfully: bool,
}
impl<P: PlatformImpl> Drop for RawScopeGuard<'_, P> {
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
                let panic_err = self.data.pop_panic();
                if let Some(mut err) = panic_err {
                    if let Some(p) = err.take_panic() {
                        resume_unwind(p);
                    }
                } else if self.data.has_panic.load(Ordering::Acquire) {
                    panic!("child thread panicked");
                }
            }
        }
    }
}

pub fn scope<'env, P, F, R>(f: F) -> R
where
    P: PlatformImpl,
    F: for<'scope> FnOnce(&'scope RawScope<'scope, 'env, P>) -> R,
{
    let scope_data = RawScopeData {
        num_running_threads: AtomicUsize::new(0),
        parker: P::Parker::new(),
        cancelled: AtomicBool::new(false),
        #[cfg(feature = "std")]
        panics: AtomicPtr::new(core::ptr::null_mut()),
        #[cfg(feature = "std")]
        has_panic: AtomicBool::new(false),
    };

    let scope = RawScope {
        data: &scope_data,
        _scope: PhantomData,
        _env: PhantomData,
    };

    let mut guard = RawScopeGuard {
        data: &scope_data,
        completed_successfully: false,
    };

    let res = f(&scope);
    guard.completed_successfully = true;
    res
}
