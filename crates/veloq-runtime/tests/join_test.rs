use veloq_runtime::{
    join,
    runtime::Runtime,
    scope,
    scope::JoinOutcome,
    task::{TaskError, yield_now},
};
use veloq_std::{
    future::{Future, IntoFuture},
    pin::Pin,
    sync::{
        NativeArc as Arc,
        atomic::{NativeAtomicBool as AtomicBool, NativeAtomicUsize as AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};

#[cfg(feature = "std")]
use veloq_std::panic::{AssertUnwindSafe, catch_unwind};

struct ReadyFuture<T>(Option<T>);

impl<T: Unpin> Future for ReadyFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(self.0.take().expect("ready future polled after completion"))
    }
}

fn ready<T>(value: T) -> ReadyFuture<T> {
    ReadyFuture(Some(value))
}

struct IntoReady<T>(Option<T>);

struct IntoReadyFuture<T>(Option<T>);

impl<T: Unpin> Future for IntoReadyFuture<T> {
    type Output = T;

    fn poll(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Ready(
            self.0
                .take()
                .expect("into-ready future polled after completion"),
        )
    }
}

impl<T: Unpin> IntoFuture for IntoReady<T> {
    type IntoFuture = IntoReadyFuture<T>;
    type Output = T;

    fn into_future(self) -> Self::IntoFuture {
        IntoReadyFuture(self.0)
    }
}

struct Steps {
    remaining: usize,
    polls: Arc<AtomicUsize>,
    value: usize,
}

impl Future for Steps {
    type Output = usize;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        if self.remaining == 0 {
            Poll::Ready(self.value)
        } else {
            self.remaining -= 1;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

struct DropPending(Arc<AtomicBool>);

impl Future for DropPending {
    type Output = ();

    fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}

impl Drop for DropPending {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Release);
    }
}

#[test]
fn join_returns_ordered_tuples_for_ready_futures() {
    Runtime::<(), _>::scope(async |_| {
        let pair = join!(ready("left"), async { "right" });
        assert_eq!(pair, ("left", "right"));

        let single = join!(ready(7usize),);
        assert_eq!(single, (7,));
    })
    .unwrap();
}

#[test]
fn join_polls_pending_branches_without_repolling_ready_branches() {
    Runtime::<(), _>::scope(async |_| {
        let first_polls = Arc::new(AtomicUsize::new(0));
        let second_polls = Arc::new(AtomicUsize::new(0));
        let first = Steps {
            remaining: 0,
            polls: first_polls.clone(),
            value: 1,
        };
        let second = Steps {
            remaining: 2,
            polls: second_polls.clone(),
            value: 2,
        };

        assert_eq!(join!(first, second), (1, 2));
        assert_eq!(first_polls.load(Ordering::Relaxed), 1);
        assert_eq!(second_polls.load(Ordering::Relaxed), 3);
    })
    .unwrap();
}

#[test]
fn join_evaluates_inputs_once_and_accepts_into_future() {
    Runtime::<(), _>::scope(async |_| {
        let evaluations = Arc::new(AtomicUsize::new(0));
        let first_evaluations = evaluations.clone();
        let second_evaluations = evaluations.clone();

        let result = join!(
            {
                first_evaluations.fetch_add(1, Ordering::Relaxed);
                ready(3usize)
            },
            {
                second_evaluations.fetch_add(1, Ordering::Relaxed);
                IntoReady(Some(4usize))
            },
        );

        assert_eq!(result, (3, 4));
        assert_eq!(evaluations.load(Ordering::Relaxed), 2);
    })
    .unwrap();
}

#[test]
fn join_does_not_short_circuit_result_errors() {
    Runtime::<(), _>::scope(async |_| {
        let polls = Arc::new(AtomicUsize::new(0));
        let other = Steps {
            remaining: 1,
            polls: polls.clone(),
            value: 9,
        };
        let result = join!(async { Err::<usize, &'static str>("failed") }, other);

        assert_eq!(result, (Err("failed"), 9));
        assert_eq!(polls.load(Ordering::Relaxed), 2);
    })
    .unwrap();
}

#[test]
fn join_drops_unfinished_branches_when_task_is_cancelled() {
    Runtime::<(), _>::scope(async |ctx| {
        scope!(ctx, async |child_scope| {
            let dropped = Arc::new(AtomicBool::new(false));
            let dropped_by_future = dropped.clone();
            let mut handle = child_scope.spawn_boxed(async move {
                join!(DropPending(dropped_by_future), async {
                    yield_now().await;
                    1usize
                });
            });

            yield_now().await;
            handle.cancel();
            assert!(matches!(
                handle.await,
                JoinOutcome::TaskErr(TaskError::Cancelled)
            ));
            assert!(dropped.load(Ordering::Acquire));
        })
        .await
        .unwrap();
    })
    .unwrap();
}

#[test]
#[cfg(feature = "std")]
fn join_propagates_branch_panics_through_task_outcome() {
    let result = catch_unwind(AssertUnwindSafe::new(|| {
        Runtime::<(), _>::scope(async |_| {
            join!(async { panic!("join branch panic") }, async { 2usize });
        })
        .unwrap();
    }));

    assert!(result.is_err());
}
