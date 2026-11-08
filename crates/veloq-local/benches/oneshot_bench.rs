use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Builder;
use veloq_local::Oneshot;

fn bench_oneshot_send_recv(c: &mut Criterion) {
    let rt = Builder::new_current_thread().enable_all().build().unwrap();

    let mut group = c.benchmark_group("oneshot");

    group.bench_function("send_recv", |b| {
        b.to_async(&rt).iter(|| async {
            Oneshot::with_borrowed_channel(async |tx, rx| {
                tx.send(1).unwrap();
                rx.await.unwrap();
            })
            .await;
        });
    });

    group.finish();
}

criterion_group!(benches, bench_oneshot_send_recv);
criterion_main!(benches);
