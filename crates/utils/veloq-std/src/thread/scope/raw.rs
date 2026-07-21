use crate::{
    marker::PhantomData,
    string::String,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        sys,
    },
    thread::traits::{RawJoinHandleTrait, SystermImpl},
};

#[cfg(feature = "std")]
use crate::{
    boxed::Box,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    ptr::null_mut,
    sync::atomic::AtomicPtr,
    thread::panicking,
    thread::traits::RawThreadErrorTrait,
};

pub(crate) struct RawScopeData<P: SystermImpl> {
    pub(crate) num_running_threads: AtomicU32,
    pub(crate) cancelled: AtomicBool,
    #[cfg(feature = "std")]
    pub(crate) panics: AtomicPtr<P::Error>,
}

impl<P: SystermImpl> RawScopeData<P> {
    #[cfg(feature = "std")]
    fn pop_panic(&self) -> Option<P::Error> {
        let ptr = self.panics.swap(null_mut(), Ordering::Acquire);
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

impl<P: SystermImpl> Drop for RawScopeData<P> {
    fn drop(&mut self) {
        #[cfg(feature = "std")]
        {
            let ptr = self.panics.swap(null_mut(), Ordering::Relaxed);
            if !ptr.is_null() {
                unsafe {
                    let _ = Box::from_raw(ptr);
                }
            }
        }
    }
}

/// 结构化并发的作用域，用于管理在其中生成的线程的生命周期。
pub struct RawScope<'scope, 'env: 'scope, P: SystermImpl> {
    data: &'scope RawScopeData<P>,
    _scope: PhantomData<&'scope mut &'scope ()>,
    _env: PhantomData<&'env mut &'env ()>,
}

/// 作用域内生成的线程的加入句柄，允许等待线程完成并获取其返回值。
pub struct RawScopedJoinHandle<'scope, P: SystermImpl, R: Send + 'scope> {
    handle: Option<P::RawJoinHandle<'scope, Option<R>>>,
    #[cfg(feature = "std")]
    scope_data: &'scope RawScopeData<P>,
    #[cfg(not(feature = "std"))]
    _scope_data: PhantomData<&'scope RawScopeData<P>>,
}

unsafe impl<'scope, P: SystermImpl, R: Send + 'scope> Send for RawScopedJoinHandle<'scope, P, R> {}
unsafe impl<'scope, P: SystermImpl, R: Send + Sync + 'scope> Sync
    for RawScopedJoinHandle<'scope, P, R>
{
}

impl<'scope, 'env, P: SystermImpl> RawScope<'scope, 'env, P> {
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
        self.spawn_with(None, None, f)
    }

    /// 在当前作用域内，使用特定属性生成一个新线程并执行闭包 `f`。
    pub fn spawn_with<F, R>(
        &'scope self,
        name: Option<String>,
        stack_size: Option<usize>,
        f: F,
    ) -> Result<RawScopedJoinHandle<'scope, P, R>, P::Error>
    where
        F: FnOnce() -> R + Send + 'env,
        R: Send + 'env,
    {
        let scope_data = self.data;
        let closure = move || {
            struct ThreadFinishedGuard<'a, P: SystermImpl> {
                data: &'a RawScopeData<P>,
            }
            impl<P: SystermImpl> Drop for ThreadFinishedGuard<'_, P> {
                fn drop(&mut self) {
                    let old = self
                        .data
                        .num_running_threads
                        .fetch_sub(1, Ordering::Release);
                    if old == 1 {
                        sys::wake_by_address(&self.data.num_running_threads);
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
                        let raw_err = P::Error::from_panic(Some(err));
                        let payload = Box::into_raw(Box::new(raw_err));
                        if scope_data
                            .panics
                            .compare_exchange(
                                null_mut(),
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
            count: &'a AtomicU32,
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

        let handle = P::spawn(name, stack_size, closure)?;

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

impl<'scope, P: SystermImpl, R: Send + 'scope> RawScopedJoinHandle<'scope, P, R> {
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
                if let Some(err) = panic_err {
                    return Err(err);
                }
            }
            Err(P::Error::from_panic(Default::default()))
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

struct RawScopeGuard<'scope, P: SystermImpl> {
    data: &'scope RawScopeData<P>,
    completed_successfully: bool,
}
impl<P: SystermImpl> Drop for RawScopeGuard<'_, P> {
    fn drop(&mut self) {
        if !self.completed_successfully {
            self.data.cancelled.store(true, Ordering::Release);
        }

        let mut val;
        while {
            val = self.data.num_running_threads.load(Ordering::Acquire);
            val != 0
        } {
            sys::wait_on_address(&self.data.num_running_threads, val);
        }

        #[cfg(feature = "std")]
        {
            if !panicking() {
                let panic_err = self.data.pop_panic();
                if let Some(mut err) = panic_err
                    && let Some(p) = err.take_panic()
                {
                    resume_unwind(p);
                }
            }
        }
    }
}

pub fn scope<'env, P, F, R>(f: F) -> R
where
    P: SystermImpl,
    F: for<'scope> FnOnce(&'scope RawScope<'scope, 'env, P>) -> R,
{
    let scope_data = RawScopeData {
        num_running_threads: AtomicU32::new(0),
        cancelled: AtomicBool::new(false),
        #[cfg(feature = "std")]
        panics: AtomicPtr::new(null_mut()),
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
