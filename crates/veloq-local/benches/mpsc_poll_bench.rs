use criterion::{Criterion, criterion_group, criterion_main};
use std::future::Future;
use std::task::{Context, Waker};
use veloq_local::mpsc;

fn bench_poll_pending(c: &mut Criterion) {
    let state = mpsc::unbounded::<i32>();
    let (_tx, rx) = state.split();
    let recv_fut = rx.recv();
    let mut pinned = Box::pin(recv_fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(&waker);

    c.bench_function("mpsc_poll_pending", |b| {
        b.iter(|| {
            let _ = pinned.as_mut().poll(&mut cx);
        });
    });
}

fn bench_stream_poll_pending(c: &mut Criterion) {
    use futures_core::Stream;
    let state = mpsc::unbounded::<i32>();
    let (_tx, rx) = state.split();
    let stream = rx.stream();
    let mut pinned = Box::pin(stream);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(&waker);

    c.bench_function("mpsc_stream_poll_pending", |b| {
        b.iter(|| {
            let _ = pinned.as_mut().poll_next(&mut cx);
        });
    });
}

criterion_group!(benches, bench_poll_pending, bench_stream_poll_pending);
criterion_main!(benches);
