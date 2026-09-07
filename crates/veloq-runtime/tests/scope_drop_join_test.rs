//! 作用域析构必须 join，而不是只发一次取消信号。
//!
//! 这些用例走的是「scope future 被丢弃」这条路径：`select!` 的落败分支。旧实现在这里只调
//! 一次 `cancel()` 就返回，子任务仍在别的 worker 上持有 `'env` 借用运行；而取消是协作式
//! 的，一个已经挂起的任务在没有唤醒的情况下永远看不到取消状态。**用例同时靠 nextest 的
//! 超时兜底：只要 join 或取消唤醒任何一环缺失，它们就会挂住或断言失败。**

use std::{
    future::Future,
    hint::spin_loop,
    num::NonZeroUsize,
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    sync::atomic::{AtomicBool, Ordering},
    task::{Context, Poll},
};
use veloq_runtime::{
    runtime::{RuntimeBuilder, RuntimeShared},
    scope, select, task,
    task::{RawTask, Task, TaskError},
};

fn with_workers(count: usize) -> RuntimeBuilder<(), fn(usize, &RuntimeShared<()>)> {
    RuntimeBuilder::new().with_worker_count(NonZeroUsize::new(count))
}

/// 永久挂起且**从不自我唤醒**的 future：只有取消唤醒才能让持有它的任务结束。
struct Park;

impl Future for Park {
    type Output = ();
    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<()> {
        Poll::Pending
    }
}

/// 被 poll 指定次数之后就绪，每次都自我唤醒，用来让 `select!` 的落败分支在若干轮之后胜出。
struct ReadyAfter(u32);

impl Future for ReadyAfter {
    type Output = u32;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
        if self.0 == 0 {
            return Poll::Ready(1);
        }
        self.0 -= 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// 作用域被丢弃时，一个**已经挂起**的子任务必须被唤醒、观察到取消并真正结束。
///
/// 子任务被定向派发到 worker 1 并在那里先跑到挂起（`started` 是它已被 poll 过的证据），
/// 因此它不在任何队列里 —— 没有取消唤醒就再没有人会碰它。任务节点声明在作用域**之外**，
/// 于是析构之后还能查它的最终状态：这就是「子任务已停止」的可观测证据。
#[test]
fn dropping_a_scope_joins_a_parked_child() {
    let started = AtomicBool::new(false);

    with_workers(2)
        .scope(async |ctx| {
            task!(child, async {
                started.store(true, Ordering::Release);
                Park.await;
            });

            let winner = select! {
                ctx;
                biased;
                _ = scope!(ctx, async |s| {
                    let _child_handle = s.spawn_to::<(), _>(1, &child);
                    while !started.load(Ordering::Acquire) {
                        spin_loop();
                    }
                    // 作用域自身永不正常结束，只能被丢弃。
                    Park.await;
                }) => 0u32,
                v = ReadyAfter(8) => v,
            };
            assert_eq!(winner, 1, "落败分支应当是那个永不结束的作用域");

            assert!(
                RawTask::header(&child).is_ready(),
                "子任务在其作用域析构后仍未结束：Drop 没有 join，或取消没有唤醒它"
            );
            assert!(
                matches!(
                    Task::<()>::take_result(&child),
                    Some(Err(TaskError::Cancelled))
                ),
                "被丢弃作用域里的子任务应当以取消收尾"
            );
        })
        .unwrap();
}

/// 同一条路径下，子任务的 panic 不能丢失：作用域析构时把 payload 交给上一层作用域，由后者的
/// `wait_all()` 抛出。
#[test]
fn panic_survives_a_dropped_scope() {
    let result = catch_unwind(AssertUnwindSafe(|| {
        with_workers(1)
            .scope(async |ctx| {
                scope!(ctx, async |outer| {
                    // 内层作用域建在**任务**里，父节点才会是 outer，从而走「上交 payload」路径。
                    outer.spawn_boxed(async move {
                        let _ = select! {
                            ctx;
                            biased;
                            _ = scope!(ctx, async |inner| {
                                inner.spawn_boxed(async { panic!("BOOM") });
                                Park.await;
                            }) => 0u32,
                            v = ReadyAfter(8) => v,
                        };
                    });
                })
                .await
                .unwrap();
            })
            .unwrap();
    }));

    let payload = result.expect_err("子任务的 panic 必须传播出来");
    let msg = payload
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| payload.downcast_ref::<String>().map(String::as_str));
    assert_eq!(msg, Some("BOOM"));
}
