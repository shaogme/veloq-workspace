use std::{
    future::poll_fn,
    num::NonZeroUsize,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Waker},
};

use veloq_runtime::{
    error::Result,
    runtime::{
        RuntimeBuilder, RuntimeShared,
        context::{IdleDecision, IdleWaitStrategy},
    },
};

struct HookState {
    park_calls: AtomicUsize,
    waker: Mutex<Option<Waker>>,
}

type Extra = Arc<HookState>;

fn wait_for_work(_: &RuntimeShared<Extra>) -> Result<IdleDecision> {
    Ok(IdleDecision::wait(IdleWaitStrategy::block()))
}

fn wake_after_park(shared: &RuntimeShared<Extra>, _: IdleWaitStrategy) -> Result<()> {
    let waker = shared.extra_tls.with(|state| {
        let previous = state.park_calls.fetch_add(1, Ordering::AcqRel);
        if previous == 0 {
            state
                .waker
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .take()
        } else {
            None
        }
    });
    if let Some(waker) = waker {
        waker.wake();
    }
    Ok(())
}

fn run_until_park<F, H>(builder: RuntimeBuilder<Extra, F, H>, state: Arc<HookState>) -> usize
where
    F: for<'rt> Fn(usize, &'rt RuntimeShared<Extra>) -> Extra + Send + Sync + 'static,
{
    builder
        .scope(async move |_| {
            poll_fn(|cx: &mut Context<'_>| {
                if state.park_calls.load(Ordering::Acquire) != 0 {
                    Poll::Ready(())
                } else {
                    *state
                        .waker
                        .lock()
                        .unwrap_or_else(|poisoned| poisoned.into_inner()) =
                        Some(cx.waker().clone());
                    if state.park_calls.load(Ordering::Acquire) != 0 {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }
            })
            .await;
            state.park_calls.load(Ordering::Acquire)
        })
        .expect("runtime with recording park hook should complete")
}

fn worker_factory(state: Arc<HookState>) -> impl Fn(usize, &RuntimeShared<Extra>) -> Extra {
    move |_, _| state.clone()
}

#[test]
fn idle_first_preserves_park_hook() {
    let state = Arc::new(HookState {
        park_calls: AtomicUsize::new(0),
        waker: Mutex::new(None),
    });
    let result = run_until_park(
        RuntimeBuilder::new()
            .with_worker_count(NonZeroUsize::new(1))
            .with_idle_hook(wait_for_work)
            .with_park_hook(wake_after_park)
            .with_worker_factory(worker_factory(state.clone())),
        state,
    );

    assert_eq!(result, 1);
}

#[test]
fn park_first_preserves_park_hook_when_idle_hook_is_added() {
    let state = Arc::new(HookState {
        park_calls: AtomicUsize::new(0),
        waker: Mutex::new(None),
    });
    let result = run_until_park(
        RuntimeBuilder::new()
            .with_worker_count(NonZeroUsize::new(1))
            .with_park_hook(wake_after_park)
            .with_idle_hook(wait_for_work)
            .with_worker_factory(worker_factory(state.clone())),
        state,
    );

    assert_eq!(result, 1);
}
