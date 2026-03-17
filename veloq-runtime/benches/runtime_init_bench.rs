use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use veloq_runtime::{config::Config, runtime::RuntimeBuilder};

fn bench_runtime_init(c: &mut Criterion) {
    let config = Config::default().worker_threads(8);
    c.bench_function("runtime_init_8_workers", |b| {
        b.iter(|| {
            let runtime = RuntimeBuilder::new()
                .config(black_box(config.clone()))
                .build()
                .unwrap();
            black_box(runtime);
        });
    });
}

criterion_group!(benches, bench_runtime_init);
criterion_main!(benches);
