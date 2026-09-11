use veloq_std::vec;
use veloq_std::{
    boxed::Box, marker::PhantomData, num::NonZeroUsize, ops::AsyncFnOnce, pin::pin, ptr, thread,
    vec::Vec,
};

use crate::{
    error::{Result, RuntimeError},
    utils::FastRand,
};
use diagweave::prelude::*;
use veloq_std::panic::{AssertUnwindSafe, catch_unwind};
use veloq_std::sync::Mutex;

pub mod cancellation;
pub mod context;
pub mod primitives;
pub mod shared;

pub use cancellation::GenericCancellationToken;
pub use context::{
    IdleDecision, IdleHook, IdleWaitStrategy, IntoRuntimeCtx, RuntimeCtx, current_scope,
};
pub(crate) use context::{RuntimeTlsInner, WorkerTickHook};
pub use shared::{EnqueuePinnedOutcome, ParkHook, RuntimeShared, RuntimeSharedBase};

use primitives::BlockOnSignal;
use shared::{BlockOnController, MAX_WORKER_COUNT, Receivers, run_worker_loop};

pub struct Runtime<'rt, 'env: 'rt, T, WF: 'rt> {
    shared: RuntimeShared<T>,
    receivers: Option<Receivers>,
    worker_factory: Option<WF>,
    _marker: PhantomData<fn(&'rt ()) -> &'env ()>,
}

pub type DefaultWorkerFactory = fn(usize, &RuntimeShared<()>) -> ();

pub type DefaultWorkerFactoryFor<T> = fn(usize, &RuntimeShared<T>) -> T;

impl<'rt, 'env: 'rt> Runtime<'rt, 'env, (), DefaultWorkerFactoryFor<()>> {
    pub fn scope<F, R>(f: F) -> Result<R>
    where
        F: for<'rt_inner> AsyncFnOnce(RuntimeCtx<'rt_inner, ()>) -> R,
    {
        RuntimeBuilder::new().scope(f)
    }

    pub fn builder() -> RuntimeBuilder<(), DefaultWorkerFactoryFor<()>> {
        RuntimeBuilder::new()
    }
}

impl<'rt, 'env: 'rt, T, WF> Runtime<'rt, 'env, T, WF> {
    pub fn worker_count(&self) -> NonZeroUsize {
        self.shared.worker_count()
    }

    /// Runs `f` to completion on this runtime.
    ///
    /// `f` is higher-ranked over the context lifetime, so `R` cannot mention it: the
    /// `RuntimeCtx` (and anything derived from it) is confined to the call. Without that
    /// bound `'rt` is picked by the caller and `async |ctx| ctx` hands back a dangling
    /// context, because the `RuntimeShared` it points at lives in this frame.
    pub fn block_on<R, F>(mut self, f: F) -> Result<R>
    where
        T: 'rt,
        WF: Fn(usize, &'rt RuntimeShared<T>) -> T + Send + Sync,
        F: for<'a> AsyncFnOnce(RuntimeCtx<'a, T>) -> R,
    {
        struct TlsCleanupGuard<'a, T>(&'a veloq_tls::Tls<T>);
        impl<'a, T> Drop for TlsCleanupGuard<'a, T> {
            fn drop(&mut self) {
                let _ = self.0.take();
            }
        }

        struct MainTlsCleanupGuard<'a, T> {
            tls: &'a veloq_tls::Tls<RuntimeTlsInner>,
            shared: &'a RuntimeShared<T>,
            completed: bool,
        }
        impl<T> MainTlsCleanupGuard<'_, T> {
            fn mark_completed(&mut self) {
                self.completed = true;
            }
        }
        impl<T> Drop for MainTlsCleanupGuard<'_, T> {
            fn drop(&mut self) {
                if !self.completed {
                    self.shared.shutdown();
                    self.shared.base.complete_shutdown(0);
                }
                let _ = self.tls.take();
            }
        }

        let shared_ref: &'rt RuntimeShared<T> = unsafe { &*ptr::from_ref(&self.shared) };
        let ctx = RuntimeCtx::new(shared_ref);

        let worker_count = shared_ref.worker_count();
        let worker_factory = self
            .worker_factory
            .take()
            .ok_or(RuntimeError::WorkerFactoryAlreadyTaken)?;
        let receivers = self
            .receivers
            .take()
            .ok_or(RuntimeError::ReceiversAlreadyTaken)?;
        let mut deques = receivers.deques;

        let thread_errors = Mutex::new(None);
        // 主线程的唤醒信号必须在 worker 线程启动**之前**建好：worker 初始化失败时要靠
        // 它把主线程从 park 里叫回来，否则错误永远不会被报告。
        let signal = BlockOnSignal::new(shared_ref.base.unparker(0).clone());

        let res: Result<R> = veloq_std::thread::scope(|scope| {
            struct ShutdownGuard<'rt, T>(&'rt RuntimeShared<T>);
            impl<'rt, T> Drop for ShutdownGuard<'rt, T> {
                fn drop(&mut self) {
                    self.0.shutdown();
                }
            }
            let shutdown_guard = ShutdownGuard(shared_ref);
            let mut started_workers = vec![false; worker_count.get()];
            // The calling thread is the worker-0 completion participant even before its TLS is
            // installed; it can perform the final barrier and global drain on setup failures.
            started_workers[0] = true;

            for worker_id in (1..worker_count.get()).rev() {
                let deque = match deques.pop() {
                    Some(d) => d,
                    None => {
                        shared_ref.shutdown();
                        shared_ref.base.mark_workers_unavailable(
                            (1..worker_count.get()).filter(|id| !started_workers[*id]),
                        );
                        shared_ref.base.complete_shutdown(0);
                        return RuntimeError::DequesExhausted { worker_id }.trans();
                    }
                };
                let worker_factory_ref = &worker_factory;
                let thread_errors_ref = &thread_errors;
                let signal_ref = &signal;

                let context = RuntimeTlsInner {
                    worker_id,
                    rand: FastRand::new(worker_id as u64),
                    worker: deque,
                };

                if let Err(e) = scope.spawn(move || {
                    let _tls_cleanup = TlsCleanupGuard(&shared_ref.base.tls);
                    let _extra_cleanup = TlsCleanupGuard(&shared_ref.extra_tls);
                    let init_res = catch_unwind(AssertUnwindSafe::new(|| {
                        shared_ref.base.tls.set_owned(context).map_err(|source| {
                            RuntimeError::TlsSetOwnedFailed {
                                worker_id,
                                source: source.kind(),
                            }
                        })?;
                        shared_ref
                            .extra_tls
                            .set_owned(worker_factory_ref(worker_id, shared_ref))
                            .map_err(|source| RuntimeError::TlsSetOwnedFailed {
                                worker_id,
                                source: source.kind(),
                            })?;
                        Ok(())
                    }))
                    .map_err(|_| {
                        RuntimeError::InvariantViolation {
                            site: "worker-initialization",
                            detail: "worker factory panicked".into(),
                        }
                        .to_report()
                    })
                    .and_then(|result| result);

                    // 该 worker 无法参与调度，必须叫停整个运行时并唤醒主线程：主线程
                    // 可能正 park 着等一个再也不会到来的事件。还要放弃自己队列里的积压
                    // 任务：它们再也不会被 poll，而某个作用域可能正在 join 它们（调度
                    // 循环只在自己正常退出时才排空）。
                    let report_fatal = |err| {
                        let mut guard = thread_errors_ref.lock().unwrap_or_else(|e| e.into_inner());
                        if guard.is_none() {
                            *guard = Some(err);
                        }
                        drop(guard);
                        signal_ref.notify();
                        shared_ref.shutdown();
                        shared_ref.base.complete_shutdown(worker_id);
                    };

                    if let Err(err) = init_res {
                        report_fatal(err);
                        return;
                    }

                    match catch_unwind(AssertUnwindSafe::new(|| shared_ref.run_worker())) {
                        Ok(Ok(())) => {}
                        Ok(Err(err)) => report_fatal(err),
                        Err(_) => report_fatal(
                            RuntimeError::InvariantViolation {
                                site: "worker-loop",
                                detail: "worker loop panicked".into(),
                            }
                            .to_report(),
                        ),
                    }
                }) {
                    shared_ref.shutdown();
                    shared_ref.base.mark_workers_unavailable(
                        (1..worker_count.get()).filter(|id| !started_workers[*id]),
                    );
                    shared_ref.base.complete_shutdown(0);
                    return RuntimeError::ThreadSpawnFailed { source: e }.trans();
                }
                started_workers[worker_id] = true;
            }

            let deque0 = match deques.pop() {
                Some(deque) => deque,
                None => {
                    shared_ref.shutdown();
                    shared_ref.base.mark_workers_unavailable(
                        (1..worker_count.get()).filter(|id| !started_workers[*id]),
                    );
                    shared_ref.base.complete_shutdown(0);
                    return RuntimeError::MainWorkerDequeExhausted.trans();
                }
            };

            let context = RuntimeTlsInner {
                worker_id: 0,
                rand: FastRand::new(0),
                worker: deque0,
            };
            let _tls_cleanup = TlsCleanupGuard(&shared_ref.base.tls);
            let _extra_cleanup = TlsCleanupGuard(&shared_ref.extra_tls);
            let mut main_cleanup = MainTlsCleanupGuard {
                tls: &shared_ref.base.tls,
                shared: shared_ref,
                completed: false,
            };

            if let Err(source) = shared_ref.base.tls.set_owned(context) {
                return RuntimeError::TlsSetOwnedFailed {
                    worker_id: 0,
                    source: source.kind(),
                }
                .trans();
            }
            let main_extra =
                match catch_unwind(AssertUnwindSafe::new(|| worker_factory(0, shared_ref))) {
                    Ok(extra) => extra,
                    Err(_) => {
                        return RuntimeError::InvariantViolation {
                            site: "worker-initialization",
                            detail: "worker factory panicked".into(),
                        }
                        .trans();
                    }
                };
            if let Err(source) = shared_ref.extra_tls.set_owned(main_extra) {
                return RuntimeError::TlsSetOwnedFailed {
                    worker_id: 0,
                    source: source.kind(),
                }
                .trans();
            }

            if let Some(err) = thread_errors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                return Err(err);
            }

            // 主线程就是 0 号 worker：外层 future 作为循环的「退出条件」交给统一调度循环
            // 驱动，不再手写第二份 pop 链。
            let mut fut = pin!(f(ctx));
            let mut controller = BlockOnController::new(fut.as_mut(), signal.clone());
            let loop_res = run_worker_loop(shared_ref, &mut controller);

            // The main worker owns the final barrier and global drain whether
            // the outer future completed normally or shutdown interrupted it.
            shared_ref.shutdown();
            shared_ref.base.complete_shutdown(0);
            main_cleanup.mark_completed();

            // worker 线程的致命错误优先于循环自身的退出原因：循环正是被它触发的 shutdown
            // 叫停的。
            if let Some(err) = thread_errors
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                drop(controller);
                drop(main_cleanup);
                drop(shutdown_guard);
                return Err(err);
            }
            loop_res?;

            match controller.take_output() {
                Some(res) => Ok(res),
                None => RuntimeError::ShutdownBeforeCompletion.trans(),
            }
        });

        res
    }
}

/// 表示尚未配置 idle 或 park hook 的 builder 状态。
#[doc(hidden)]
pub struct HooksUnconfigured;

/// 表示至少配置了一个 idle 或 park hook 的 builder 状态。
#[doc(hidden)]
pub struct HooksConfigured;

/// 用于配置并创建运行时的 builder。
///
/// `H` 是隐藏的 hook 配置状态参数。新 builder 处于 [`HooksUnconfigured`] 状态，首次
/// 设置 idle 或 park hook 时可以选择 worker extra 类型；设置任意一个 hook 后，builder
/// 处于 [`HooksConfigured`] 状态，后续 hook setter 只能接受相同的 `T`。因此，两个 hook
/// 始终与 [`RuntimeShared<T>`] 使用相同的类型，配置顺序不会静默丢弃 park hook。
pub struct RuntimeBuilder<T, WF, H = HooksUnconfigured> {
    worker_count: Option<NonZeroUsize>,
    queue_capacity: NonZeroUsize,
    topology_override: Option<Box<[usize]>>,
    worker_factory: Option<WF>,
    idle_hook: Option<IdleHook<T>>,
    park_hook: Option<ParkHook<T>>,
    worker_tick_hook: Option<WorkerTickHook>,
    _hook_state: PhantomData<fn() -> H>,
}

impl Default for RuntimeBuilder<(), DefaultWorkerFactoryFor<()>, HooksUnconfigured> {
    fn default() -> Self {
        Self::new()
    }
}

impl RuntimeBuilder<(), DefaultWorkerFactoryFor<()>, HooksUnconfigured> {
    /// 创建一个尚未配置 hook 的默认 builder。
    pub fn new() -> Self {
        RuntimeBuilder {
            worker_count: None,
            queue_capacity: NonZeroUsize::new(1024).unwrap(),
            topology_override: None,
            worker_factory: Some(|_, _| ()),
            idle_hook: None,
            park_hook: None,
            worker_tick_hook: None,
            _hook_state: PhantomData,
        }
    }
}

impl<T, WF> RuntimeBuilder<T, WF, HooksUnconfigured> {
    /// 设置 idle hook，并在此处选择 worker extra 的类型。
    ///
    /// 这是 builder 唯一可以改变 `T` 的 hook setter。配置任意一个 hook 后，后续 setter
    /// 只能接受相同的 `T`，从而保证 idle hook、park hook、worker factory 和
    /// [`RuntimeShared`] 始终使用同一个 extra 类型。
    pub fn with_idle_hook<NewT>(
        self,
        hook: IdleHook<NewT>,
    ) -> RuntimeBuilder<NewT, WF, HooksConfigured> {
        RuntimeBuilder {
            idle_hook: Some(hook),
            park_hook: None,
            worker_count: self.worker_count,
            queue_capacity: self.queue_capacity,
            topology_override: self.topology_override,
            worker_factory: self.worker_factory,
            worker_tick_hook: self.worker_tick_hook,
            _hook_state: PhantomData,
        }
    }

    /// 设置 park hook，并在此处选择 worker extra 的类型。
    ///
    /// 这是 builder 唯一可以改变 `T` 的 hook setter。之后可以继续设置同一个 `T` 的
    /// idle hook，已有 park hook 会被保留。
    pub fn with_park_hook<NewT>(
        self,
        hook: ParkHook<NewT>,
    ) -> RuntimeBuilder<NewT, WF, HooksConfigured> {
        RuntimeBuilder {
            idle_hook: None,
            park_hook: Some(hook),
            worker_count: self.worker_count,
            queue_capacity: self.queue_capacity,
            topology_override: self.topology_override,
            worker_factory: self.worker_factory,
            worker_tick_hook: self.worker_tick_hook,
            _hook_state: PhantomData,
        }
    }
}

impl<T, WF> RuntimeBuilder<T, WF, HooksConfigured> {
    /// 设置同一个 worker extra 类型的 idle hook。
    pub fn with_idle_hook(mut self, hook: IdleHook<T>) -> Self {
        self.idle_hook = Some(hook);
        self
    }

    /// 设置同一个 worker extra 类型的 park hook。
    pub fn with_park_hook(mut self, hook: ParkHook<T>) -> Self {
        self.park_hook = Some(hook);
        self
    }
}

impl<T, WF, H> RuntimeBuilder<T, WF, H> {
    pub fn with_worker_count(mut self, count: Option<NonZeroUsize>) -> Self {
        self.worker_count = count;
        self
    }

    pub fn with_queue_capacity(mut self, capacity: NonZeroUsize) -> Self {
        self.queue_capacity = capacity;
        self
    }

    /// Supplies a synthetic worker-to-group mapping for deterministic scheduler tests.
    ///
    /// This mapping describes locality hints only; it does not configure OS CPU affinity.
    #[doc(hidden)]
    pub fn with_test_topology(mut self, worker_to_group: Vec<usize>) -> Self {
        self.topology_override = Some(worker_to_group.into_boxed_slice());
        self
    }

    pub fn with_worker_tick_hook(mut self, hook: WorkerTickHook) -> Self {
        self.worker_tick_hook = Some(hook);
        self
    }

    /// 替换 worker factory，同时保留当前 hook 配置状态和两个 hook。
    pub fn with_worker_factory<NWF>(self, factory: NWF) -> RuntimeBuilder<T, NWF, H> {
        RuntimeBuilder {
            worker_count: self.worker_count,
            queue_capacity: self.queue_capacity,
            topology_override: self.topology_override,
            worker_factory: Some(factory),
            idle_hook: self.idle_hook,
            park_hook: self.park_hook,
            worker_tick_hook: self.worker_tick_hook,
            _hook_state: PhantomData,
        }
    }

    /// Builds the runtime and runs `f` on it. See [`Runtime::block_on`] for why `f` is
    /// higher-ranked over the context lifetime.
    pub fn scope<'rt, 'env: 'rt, F, R>(self, f: F) -> Result<R>
    where
        T: 'rt,
        WF: Fn(usize, &'rt RuntimeShared<T>) -> T + Send + Sync + 'rt,
        F: for<'a> AsyncFnOnce(RuntimeCtx<'a, T>) -> R,
    {
        let worker_count = self
            .worker_count
            .unwrap_or_else(|| thread::available_parallelism().unwrap_or(NonZeroUsize::MIN));
        // worker id 会被编码进 idle 栈 head 的低 32 位，必须在构造期就拒绝越界的规模，
        // 而不是让 `IdleStack` 静默截断。
        if worker_count.get() > MAX_WORKER_COUNT - 1 {
            return RuntimeError::WorkerCountTooLarge {
                worker_count: worker_count.get(),
                max_worker_count: MAX_WORKER_COUNT - 1,
            }
            .trans();
        }
        if let Some(worker_to_group) = self.topology_override.as_deref()
            && (worker_to_group.len() != worker_count.get()
                || worker_to_group
                    .iter()
                    .any(|&group_idx| group_idx >= worker_count.get()))
        {
            return RuntimeError::InvariantViolation {
                site: "RuntimeBuilder::with_test_topology",
                detail: "synthetic topology must map every worker to a valid group".into(),
            }
            .trans();
        }
        let (registry, topo, receivers) = shared::init_runtime_components_with_topology(
            worker_count,
            self.queue_capacity,
            self.topology_override.as_deref(),
        );
        let shared = RuntimeShared::new(
            registry,
            topo,
            worker_count,
            self.idle_hook,
            self.park_hook,
            self.worker_tick_hook,
        );
        let rt = Runtime {
            shared,
            receivers: Some(receivers),
            worker_factory: self.worker_factory,
            _marker: PhantomData,
        };
        rt.block_on(f)
    }
}
