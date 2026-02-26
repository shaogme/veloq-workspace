use criterion::{Criterion, criterion_group, criterion_main};
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Wake, Waker};
use veloq_local::mpsc;

struct MyWaker;

impl Wake for MyWaker {
    fn wake(self: Arc<Self>) {}
}

fn real_waker() -> Waker {
    Waker::from(Arc::new(MyWaker))
}

fn bench_poll_pending(c: &mut Criterion) {
    let (_tx, rx) = mpsc::new_unbounded::<i32>();
    let recv_fut = rx.recv();
    let mut pinned = Box::pin(recv_fut);
    let waker = real_waker();
    let mut cx = Context::from_waker(&waker);

    c.bench_function("mpsc_poll_pending", |b| {
        b.iter(|| {
            let _ = pinned.as_mut().poll(&mut cx);
        });
    });
}

fn bench_stream_poll_pending(c: &mut Criterion) {
    use futures_core::Stream;
    let (_tx, rx) = mpsc::new_unbounded::<i32>();
    let stream = rx.stream();
    let mut pinned = Box::pin(stream);
    let waker = real_waker();
    let mut cx = Context::from_waker(&waker);

    c.bench_function("mpsc_stream_poll_pending", |b| {
        b.iter(|| {
            let _ = pinned.as_mut().poll_next(&mut cx);
        });
    });
}

criterion_group!(benches, bench_poll_pending, bench_stream_poll_pending);
criterion_main!(benches);
