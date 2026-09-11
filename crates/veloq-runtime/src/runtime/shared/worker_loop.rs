//! 调度循环的**唯一**实现。
//!
//! 统一了 worker 线程与 `block_on` 主线程的调度驱动，主 worker 同样参与 work stealing、
//! 公平性间隔与 idle 协调，无需特殊分支处理。
//!
//! 现在三种「为什么要跑循环」的差异全部收敛到 [`LoopController`]：
//!
//! - [`ShutdownController`]：worker 线程入口，跑到运行时关停。
//! - [`ScopeJoinController`]：作用域析构 join，跑到该作用域的子任务全部结束。
//! - [`BlockOnController`]：`block_on` 主线程，额外驱动外层 future，跑到它就绪。

use veloq_std::{
    future::Future,
    pin::Pin,
    sync::NativeArc as Arc,
    task::{Context, Poll, Waker},
};

use crate::{
    error::Result,
    runtime::{
        primitives::{BlockOnSignal, create_block_on_waker, create_unpark_waker},
        shared::{RuntimeShared, infra::RuntimeProgressCoordinator},
    },
    scope::{GenericScopeCompletion, ScopeCompletionRegistration},
    task::ScopeStorage,
    utils::ownership::Ownership,
};

/// 循环的退出条件，以及「循环存在的理由」本身要推进的那点工作。
pub(crate) trait LoopController {
    /// 每轮循环开头调用一次。返回 `true` 表示目标已达成，应当退出循环。
    fn poll_progress(&mut self) -> Result<bool>;

    /// 进入 idle 之前把 `waker` 挂到目标的完成信号上，避免完成事件落在 park 期间。
    fn arm(&mut self, waker: &Waker);

    /// park 之前的最后一次廉价复查：目标可能已经达成或已可推进。
    fn is_ready(&self) -> bool;
}

/// 一直跑到运行时关停。
pub(crate) struct ShutdownController;

impl LoopController for ShutdownController {
    #[inline]
    fn poll_progress(&mut self) -> Result<bool> {
        Ok(false)
    }

    #[inline]
    fn arm(&mut self, _waker: &Waker) {}

    #[inline]
    fn is_ready(&self) -> bool {
        false
    }
}

/// 跑到某个作用域的全部子任务真正结束。
///
/// 只用于**同步**的 join（作用域析构）：异步等待走 `wait_all()` 的 waker 路径，不再嵌套
/// 调度循环。
pub(crate) struct ScopeJoinController<'a, S: ScopeStorage, O: Ownership> {
    completion: &'a GenericScopeCompletion<S, O>,
    registration: Option<ScopeCompletionRegistration<'a, S, O>>,
}

impl<'a, S: ScopeStorage, O: Ownership> ScopeJoinController<'a, S, O> {
    pub(crate) fn new(completion: &'a GenericScopeCompletion<S, O>) -> Self {
        Self {
            completion,
            registration: None,
        }
    }
}

impl<'a, S: ScopeStorage, O: Ownership> LoopController for ScopeJoinController<'a, S, O> {
    #[inline]
    fn poll_progress(&mut self) -> Result<bool> {
        Ok(self.completion.is_done())
    }

    fn arm(&mut self, waker: &Waker) {
        let completion = self.completion;
        self.registration
            .get_or_insert_with(|| ScopeCompletionRegistration::new(completion, waker))
            .register(waker);
    }

    #[inline]
    fn is_ready(&self) -> bool {
        self.completion.is_done()
    }
}

/// 跑到 `block_on` 的外层 future 就绪，顺带在每轮循环里驱动它。
///
/// 外层 future 只在被唤醒过之后才重新 poll：它的 waker 落在 [`BlockOnSignal`] 上，既标记
/// 「需要重新 poll」，也把主线程从 park 里叫回来。
pub(crate) struct BlockOnController<'a, F: Future> {
    future: Pin<&'a mut F>,
    signal: Arc<BlockOnSignal>,
    waker: Waker,
    output: Option<F::Output>,
}

impl<'a, F: Future> BlockOnController<'a, F> {
    pub(crate) fn new(future: Pin<&'a mut F>, signal: Arc<BlockOnSignal>) -> Self {
        let waker = create_block_on_waker(signal.clone());
        Self {
            future,
            signal,
            waker,
            output: None,
        }
    }

    /// 取出外层 future 的返回值；`None` 表示循环是因关停退出的。
    pub(crate) fn take_output(&mut self) -> Option<F::Output> {
        self.output.take()
    }
}

impl<'a, F: Future> LoopController for BlockOnController<'a, F> {
    fn poll_progress(&mut self) -> Result<bool> {
        if !self.signal.take_ready() {
            return Ok(false);
        }

        let mut cx = Context::from_waker(&self.waker);
        match self.future.as_mut().poll(&mut cx) {
            Poll::Ready(output) => {
                self.output = Some(output);
                Ok(true)
            }
            Poll::Pending => Ok(false),
        }
    }

    #[inline]
    fn arm(&mut self, _waker: &Waker) {}

    #[inline]
    fn is_ready(&self) -> bool {
        self.signal.is_ready()
    }
}

/// 驱动当前线程所属 worker 的调度循环，直到 `controller` 宣布目标达成或运行时关停。
pub(crate) fn run_worker_loop<T, C: LoopController>(
    shared: &RuntimeShared<T>,
    controller: &mut C,
) -> Result<()> {
    let base = &shared.base;
    base.tls.with(|ctx| -> Result<()> {
        let worker_id = ctx.worker_id;
        let waker = create_unpark_waker(base.unparker(worker_id).clone());
        let worker_tick_hook = base.worker_tick_hook;
        let mut tick = 0u32;
        let mut loop_error = None;

        while !base.shutdown.is_shutdown() {
            if let Some(hook) = worker_tick_hook {
                hook();
            }

            match controller.poll_progress() {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(err) => {
                    loop_error = Some(err);
                    base.shutdown();
                    break;
                }
            }

            tick = tick.wrapping_add(1);
            match base.poll_next_task(worker_id, tick, &ctx.rand) {
                Ok(true) => continue,
                Ok(false) => {}
                Err(err) => {
                    loop_error = Some(err);
                    base.shutdown();
                    break;
                }
            }

            controller.arm(&waker);
            if let Err(err) = RuntimeProgressCoordinator::new(shared, worker_id).run(controller) {
                loop_error = Some(err);
                base.shutdown();
                break;
            }
        }

        base.complete_shutdown(worker_id);
        loop_error
            .or_else(|| base.fatal_error())
            .map_or(Ok(()), Err)
    })
}
