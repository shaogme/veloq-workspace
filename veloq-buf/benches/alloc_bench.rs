use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::sync::Arc;
use veloq_buf::heap::{GlobalAllocatorConfig, GlobalSlotPool};
use veloq_buf::{BufPool, SlotBasedPool};

fn bench_alloc(c: &mut Criterion) {
    let config = GlobalAllocatorConfig {
        total_memory: 64 * 1024 * 1024, // 64MB
    };
    let global_pool = Arc::new(GlobalSlotPool::new(config).unwrap());
    let pool = SlotBasedPool::new(global_pool);

    let mut group = c.benchmark_group("alloc_throughput");
    group.throughput(Throughput::Elements(1));

    group.bench_function("alloc_dealloc_single_thread", |b| {
        let size = NonZeroUsize::new(4096).unwrap();
        b.iter(|| {
            let buf = pool.alloc(size);
            black_box(buf);
        })
    });
    group.finish();
}

fn bench_threaded(c: &mut Criterion) {
    // Large pool for multiple threads
    let config = GlobalAllocatorConfig {
        total_memory: 256 * 1024 * 1024, // 256MB
    };
    let global_pool = Arc::new(GlobalSlotPool::new(config).unwrap());

    let mut group = c.benchmark_group("alloc_contention");
    group.throughput(Throughput::Elements(1));

    group.bench_function("alloc_dealloc_4_threads", |b| {
        b.iter_custom(|iters| {
            let start = std::time::Instant::now();
            let mut handles = Vec::with_capacity(4);
            // Ensure even distribution, prevent 0 iter threads
            let iters_per_thread = iters.div_ceil(4);

            for _ in 0..4 {
                let pool = SlotBasedPool::new(global_pool.clone());
                let size = NonZeroUsize::new(4096).unwrap();
                handles.push(std::thread::spawn(move || {
                    for _ in 0..iters_per_thread {
                        let buf = pool.alloc(size);
                        black_box(buf);
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }
            start.elapsed()
        })
    });

    group.finish();
}

criterion_group!(benches, bench_alloc, bench_threaded);
criterion_main!(benches);
