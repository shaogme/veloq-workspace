use criterion::{criterion_group, criterion_main, Criterion};
use veloq_local::oneshot;

fn bench_oneshot_send_recv(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("oneshot");

    group.bench_function("send_recv", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, rx) = oneshot::channel();
            tx.send(1).unwrap();
            rx.await.unwrap();
        });
    });

    group.finish();
}

criterion_group!(benches, bench_oneshot_send_recv);
criterion_main!(benches);
