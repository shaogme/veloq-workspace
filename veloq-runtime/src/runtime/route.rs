use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use veloq_atomic_waker::AtomicWaker;

pub struct RouteCell<T> {
    value: Mutex<Option<T>>,
    waker: AtomicWaker,
}

impl<T> RouteCell<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            value: Mutex::new(None),
            waker: AtomicWaker::new(),
        })
    }

    pub(crate) fn set(&self, value: T) {
        let mut slot = self.value.lock().expect("worker route slot poisoned");
        debug_assert!(slot.is_none(), "worker route slot already populated");
        *slot = Some(value);
        self.waker.wake();
    }

    pub(crate) fn take(&self) -> Option<T> {
        self.value
            .lock()
            .expect("worker route slot poisoned")
            .take()
    }

    pub(crate) fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }
}

pub struct RoutedFuture<'a, F> {
    slot: Arc<RouteCell<F>>,
    inner: Option<F>,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, F> RoutedFuture<'a, F> {
    pub(crate) fn new(slot: Arc<RouteCell<F>>) -> Self {
        Self {
            slot,
            inner: None,
            _marker: std::marker::PhantomData,
        }
    }
}

impl<'a, F> Future for RoutedFuture<'a, F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.inner.is_none() {
            if let Some(op) = this.slot.take() {
                this.inner = Some(op);
            } else {
                this.slot.register(cx.waker());
                if let Some(op) = this.slot.take() {
                    this.inner = Some(op);
                } else {
                    return Poll::Pending;
                }
            }
        }
        let inner = this.inner.as_mut().expect("route future missing inner op");
        unsafe { Pin::new_unchecked(inner) }.poll(cx)
    }
}
