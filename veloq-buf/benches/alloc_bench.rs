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

fn find_fastest_cores(count: usize) -> Vec<core_affinity::CoreId> {
    let core_ids = core_affinity::get_core_ids().unwrap_or_default();
    if core_ids.is_empty() {
        return Vec::new();
    }

    let mut results = Vec::new();
    for id in core_ids {
        let start = std::time::Instant::now();
        let handle = std::thread::spawn(move || {
            core_affinity::set_for_current(id);
            let mut sum = 0u64;
            // A simple computation to stress the core
            for i in 0..1_000_000 {
                sum = black_box(sum.wrapping_add(i));
            }
            sum
        });
        let _ = handle.join().unwrap();
        results.push((id, start.elapsed()));
    }

    // Sort by elapsed time (ascending)
    results.sort_by_key(|&(_, elapsed)| elapsed);
    results.into_iter().take(count).map(|(id, _)| id).collect()
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
        let size = NonZeroUsize::new(4096).unwrap();
        // Select the 4 fastest cores (e.g., P-Cores on Intel hybrid) to minimize variance
        let fastest_cores = find_fastest_cores(4);

        b.iter_custom(|iters| {
            // Barrier for 4 worker threads + 1 controller thread
            let barrier = Arc::new(std::sync::Barrier::new(5));
            let mut handles = Vec::with_capacity(4);
            let iters_per_thread = iters.div_ceil(4);

            for i in 0..4 {
                let pool = SlotBasedPool::with_seed(global_pool.clone(), i);
                let b = barrier.clone();
                // Assign each thread to one of the fastest cores
                let core_id = if !fastest_cores.is_empty() {
                    Some(fastest_cores[i % fastest_cores.len()])
                } else {
                    None
                };

                handles.push(std::thread::spawn(move || {
                    if let Some(core) = core_id {
                        core_affinity::set_for_current(core);
                    }
                    let mut warmup_bufs = Vec::with_capacity(1024);
                    // Warm up: trigger page faults and populate superblocks
                    for _ in 0..1024 {
                        if let Some(buf) = pool.alloc(size) {
                            warmup_bufs.push(buf);
                        }
                    }
                    drop(warmup_bufs);

                    b.wait(); // Sync Start: Wait for all threads to be ready
                    for _ in 0..iters_per_thread {
                        let buf = pool.alloc(size);
                        black_box(buf);
                    }
                    b.wait(); // Sync End: Signal work completed
                }));
            }

            // Phase 1: Wait for workers to be ready
            barrier.wait();
            let start = std::time::Instant::now();

            // Phase 2: Wait for workers to finish
            barrier.wait();
            let elapsed = start.elapsed();

            for h in handles {
                h.join().unwrap();
            }
            elapsed
        })
    });

    group.finish();
}

criterion_group!(benches, bench_alloc, bench_threaded);
criterion_main!(benches);
