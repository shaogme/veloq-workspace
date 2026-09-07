//! 调度器统一改造的回归测试。
//!
//! 「等待」从嵌套调度循环改成了 waker 驱动，`block_on` 的主线程与 worker 线程共用同一份
//! 循环，默认 park 也从 `yield_now` 死转换成了真正阻塞在信号上。前两项有直接的可观测行为
//! （见下面各用例的说明）；「不再忙等」本身是结构性的（park 现在阻塞在 futex /
//! `WaitOnAddress` 上），可测的是它依赖的那些唤醒路径 —— 一旦某条唤醒丢失，从前的忙等会
//! 把它掩盖过去，现在则直接挂死，由 nextest 的 20s 超时抓住。

use std::{
    convert::Infallible,
    future::Future,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    thread::{sleep, yield_now as thread_yield},
    time::Duration,
};

use veloq_runtime::{
    Outcome,
    runtime::{RuntimeBuilder, RuntimeCtx, RuntimeShared},
    scope, select,
    task::yield_now,
};

fn with_workers(count: usize) -> RuntimeBuilder<(), fn(usize, &RuntimeShared<()>)> {
    RuntimeBuilder::new().with_worker_count(NonZeroUsize::new(count))
}

/// 被 poll 指定次数后就绪，每次都自我唤醒。
struct ReadyAfter(u32);

impl Future for ReadyAfter {
    type Output = u32;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<u32> {
        if self.0 == 0 {
            return Poll::Ready(7);
        }
        self.0 -= 1;
        cx.waker().wake_by_ref();
        Poll::Pending
    }
}

/// 只能被**运行时之外**的线程唤醒：第一次 poll 把 waker 交出去，然后挂起。
struct ForeignWake<'a> {
    slot: &'a Mutex<Option<Waker>>,
    fired: &'a AtomicBool,
}

impl Future for ForeignWake<'_> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.fired.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        *self.slot.lock().expect("waker slot") = Some(cx.waker().clone());
        // 交出 waker 与对方 `wake()` 之间的窗口：复查一次。
        if self.fired.load(Ordering::Acquire) {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }
}

/// `handle.await` 只等这**一个**任务，不等它的兄弟任务。
///
/// 旧实现里 `JoinHandle::poll` 的第一件事是跑一整个调度循环直到 `completion.is_done()`，
/// 也就是整个作用域的子任务全部结束 —— 那样 `quick.await` 会一直卡到 `blocked` 也完成，
/// 而 `blocked` 只在本用例放行之后才会完成，于是永远走不到断言。
#[test]
fn awaiting_one_handle_does_not_wait_for_its_siblings() {
    let release = AtomicBool::new(false);

    with_workers(2)
        .scope(async |ctx| {
            scope!(ctx, async |s| {
                let blocked = s.spawn_boxed(async {
                    while !release.load(Ordering::Acquire) {
                        yield_now().await;
                    }
                    1u32
                });
                let quick = s.spawn_boxed(async { 2u32 });

                assert_eq!(quick.await.unwrap(), 2);
                assert!(
                    !blocked.is_finished(),
                    "await 一个 handle 不应顺带等完整个作用域"
                );

                release.store(true, Ordering::Release);
                assert_eq!(blocked.await.unwrap(), 1);
            })
            .await
            .unwrap();
        })
        .unwrap();
}

/// 一个挂起的 `JoinHandle` 可以被 `select!` 放弃。
///
/// `biased;` 让 `parked` 成为第一个被 poll 的分支：旧实现会在那里同步跑调度循环等整个作用
/// 域结束，而 `parked` 的任务只在用例最后才被放行 —— 控制权永远回不到 `select!`，用例挂死。
#[test]
fn select_can_abandon_a_pending_join() {
    with_workers(2)
        .scope(async |ctx| {
            scope!(ctx, async |s| {
                let token = s.cancel_token().child();
                let task_token = token.clone();
                let parked = s.spawn_boxed(async move {
                    task_token.cancelled().await;
                    0u32
                });

                let winner = select! {
                    ctx;
                    biased;
                    _ = parked => 1u32,
                    v = ReadyAfter(4) => v,
                };
                assert_eq!(winner, 7, "落败分支应当是那个挂起的 join");

                // handle 已被 `select!` 消耗掉，只能靠令牌让子任务自行结束，作用域才能 join。
                token.cancel();
            })
            .await
            .unwrap();
        })
        .unwrap();
}

type BoxedU32Future<'a> = Pin<Box<dyn Future<Output = u32> + Send + 'a>>;

/// 每层：建一个子作用域 → 派生一个任务跑下一层 → await 它。
fn nested_scope_chain<'a>(ctx: RuntimeCtx<'a, ()>, depth: u32) -> BoxedU32Future<'a> {
    Box::pin(async move {
        if depth == 0 {
            return 0;
        }
        let outcome = scope!(ctx, async move |s| {
            let child = s.spawn_boxed(nested_scope_chain(ctx, depth - 1));
            Outcome::<_, Infallible>::Ok(child.await.unwrap() + 1)
        })
        .await
        .unwrap();
        match outcome {
            Outcome::Ok(value) => value,
            _ => panic!("nested scope did not return a value"),
        }
    })
}

/// 作用域嵌套深度不再映射为栈帧深度。
///
/// 旧实现每嵌套一层就多压一整套「`drive_worker` → poll 子任务 → 子任务 await 自己的作用域
/// → 再一层 `drive_worker`」的栈帧。现在每层 await 都返回 `Pending`，下一层由 worker 顶层
/// 循环从一个干净的栈上继续，深度只受任务数限制。
#[test]
fn deeply_nested_scopes_stay_flat() {
    const DEPTH: u32 = 128;

    let depth = with_workers(2)
        .scope(async |ctx| Outcome::<_, Infallible>::Ok(nested_scope_chain(ctx, DEPTH).await))
        .unwrap();
    let depth = match depth {
        Outcome::Ok(value) => value,
        _ => panic!("runtime scope did not return a value"),
    };

    assert_eq!(depth, DEPTH);
}

/// 运行时之外的线程唤醒一个挂起任务时，睡着的 worker 必须醒过来。
///
/// 外部线程刻意先睡一会儿，让所有 worker（含跑外层 future 的主线程）都真正 park 下去，
/// 之后的 `wake()` 走 `enqueue_send` → `wake_worker` → `Unparker`。旧实现在没有 `park_hook`
/// 时把 idle 退化成 `yield_now` 死转，任何丢失的 unpark 都会被忙等掩盖；现在线程是真的睡着
/// 的，唤醒链路缺一环就直接挂死。
#[test]
fn a_parked_worker_wakes_on_a_foreign_thread_wake() {
    let slot: Mutex<Option<Waker>> = Mutex::new(None);
    let fired = AtomicBool::new(false);

    std::thread::scope(|threads| {
        threads.spawn(|| {
            loop {
                let waker = slot.lock().expect("waker slot").take();
                if let Some(waker) = waker {
                    sleep(Duration::from_millis(50));
                    fired.store(true, Ordering::Release);
                    waker.wake();
                    return;
                }
                thread_yield();
            }
        });

        with_workers(2)
            .scope(async |ctx| {
                scope!(ctx, async |s| {
                    let handle = s.spawn_boxed(ForeignWake {
                        slot: &slot,
                        fired: &fired,
                    });
                    handle.await.unwrap();
                })
                .await
                .unwrap();
            })
            .unwrap();
    });
}

/// 跨 worker 的入队风暴不能丢唤醒。
///
/// 每个任务都要经历「派发到别的 worker → 多次自我唤醒重新入队 → 完成后唤醒主线程」，
/// 每一步都在 `EventCount` 序列号与 park 的竞态窗口上。序列号一旦回到「入队之前」递增，
/// worker 就可能读到新序列号、看到空队列、然后安心睡死（默认 park 现在是真的睡）。
#[test]
fn cross_worker_task_storm_does_not_lose_wakeups() {
    const TASKS: usize = 64;
    const YIELDS: usize = 4;

    let sum = with_workers(4)
        .scope(async |ctx| {
            scope!(ctx, async |s| {
                let mut handles = Vec::with_capacity(TASKS);
                for i in 0..TASKS {
                    handles.push(s.spawn_boxed(async move {
                        for _ in 0..YIELDS {
                            yield_now().await;
                        }
                        i
                    }));
                }

                let mut sum = 0usize;
                for handle in handles {
                    sum += handle.await.unwrap();
                }
                Outcome::<_, Infallible>::Ok(sum)
            })
            .await
            .unwrap()
        })
        .unwrap();
    let sum = match sum {
        Outcome::Ok(value) => value,
        _ => panic!("scope did not return a sum"),
    };

    assert_eq!(sum, (0..TASKS).sum::<usize>());
}
