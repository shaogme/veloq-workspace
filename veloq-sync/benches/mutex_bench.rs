use criterion::{criterion_group, criterion_main, Criterion, black_box};
use veloq_sync::mutex::Mutex;
use std::sync::Arc;

fn bench_mutex_uncontended(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mutex = Arc::new(Mutex::new(0));

    c.bench_function("mutex_lock_unlock_uncontended", |b| {
        b.to_async(&rt).iter(|| async {
            let _guard = black_box(mutex.lock().await);
        })
    });
}

criterion_group!(benches, bench_mutex_uncontended);
criterion_main!(benches);
