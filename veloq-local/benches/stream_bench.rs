use criterion::{Criterion, criterion_group, criterion_main};
use futures_core::Stream;
use std::future::poll_fn;
use tokio::pin;
use veloq_local::mpsc;

fn bench_stream_creation(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    c.bench_function("stream_creation_and_poll", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, rx) = mpsc::new_unbounded();
            tx.send(1).await.unwrap();

            let stream = rx.stream();
            pin!(stream);

            // Poll once to ensure node is registered/used
            let item = poll_fn(|cx| stream.as_mut().poll_next(cx)).await;
            assert_eq!(item, Some(1));
        });
    });
}

criterion_group!(benches, bench_stream_creation);
criterion_main!(benches);
