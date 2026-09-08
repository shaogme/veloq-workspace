use std::{
    future::Future,
    pin::Pin,
    sync::{
        Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
    thread::yield_now as thread_yield,
};

use veloq_runtime::{
    runtime::Runtime,
    scope, scope_local,
    task::{RuntimeContextExt, yield_now},
    task_local,
};

#[test]
fn test_local_task_execution() {
    Runtime::<(), _>::scope(async |ctx| {
        task_local!(t, async { 1 + 1 });
        scope!(ctx, async |s| {
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 2);
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
fn test_local_task_with_yield() {
    Runtime::<(), _>::scope(async |ctx| {
        task_local!(t, async {
            yield_now().await;
            42
        });
        scope!(ctx, async |s| {
            let handle = s.spawn_local(&t);
            assert_eq!(handle.await.unwrap(), 42);
        })
        .await
        .unwrap();
    })
    .unwrap();
}

struct ForeignWakeLocal<'a> {
    waker_slot: &'a Mutex<Option<Waker>>,
    fired: &'a AtomicBool,
    polls: &'a AtomicUsize,
}

impl Future for ForeignWakeLocal<'_> {
    type Output = u32;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::AcqRel);
        if self.fired.load(Ordering::Acquire) {
            return Poll::Ready(7);
        }

        *self.waker_slot.lock().expect("local waker slot") = Some(cx.waker().clone());
        if self.fired.load(Ordering::Acquire) {
            Poll::Ready(7)
        } else {
            Poll::Pending
        }
    }
}

/// local waker 的 foreign callback 只能投递 owner mailbox；重复 wake 必须合并，且 foreign
/// thread 从同一个 local waker 构造的 Context 不能反查 local header。
#[test]
fn local_task_foreign_wake_uses_owner_mailbox() {
    let waker_slot = Mutex::new(None);
    let fired = AtomicBool::new(false);
    let polls = AtomicUsize::new(0);
    let foreign_context_is_cancelled = AtomicBool::new(true);
    let foreign_context_has_scope = AtomicBool::new(true);
    let stale_waker = Mutex::new(None);

    std::thread::scope(|threads| {
        threads.spawn(|| {
            loop {
                let waker = waker_slot.lock().expect("local waker slot").take();
                let Some(waker) = waker else {
                    thread_yield();
                    continue;
                };

                let foreign_context = Context::from_waker(&waker);
                foreign_context_is_cancelled
                    .store(foreign_context.is_cancelled(), Ordering::Release);
                foreign_context_has_scope.store(
                    foreign_context.scope_completion().is_some(),
                    Ordering::Release,
                );
                fired.store(true, Ordering::Release);
                for _ in 0..32 {
                    waker.wake_by_ref();
                }
                *stale_waker.lock().expect("stale local waker slot") = Some(waker);
                return;
            }
        });

        Runtime::<(), _>::scope(async |ctx| {
            scope_local!(ctx, async |local_scope| {
                let handle = local_scope.spawn_boxed_local(ForeignWakeLocal {
                    waker_slot: &waker_slot,
                    fired: &fired,
                    polls: &polls,
                });
                assert_eq!(handle.await.unwrap(), 7);
            })
            .await
            .unwrap();
        })
        .unwrap();

        if let Some(waker) = stale_waker.lock().expect("stale local waker slot").take() {
            waker.wake();
        }
    });

    assert_eq!(polls.load(Ordering::Acquire), 2);
    assert!(!foreign_context_is_cancelled.load(Ordering::Acquire));
    assert!(!foreign_context_has_scope.load(Ordering::Acquire));
}
