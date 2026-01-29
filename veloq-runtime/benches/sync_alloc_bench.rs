use criterion::{criterion_group, criterion_main, Criterion};
use std::future::IntoFuture;
use std::path::Path;
use std::hint::black_box;
use veloq_runtime::fs::{BufferingMode, File};
use veloq_runtime::LocalExecutor;
use veloq_runtime::config::BlockingPoolConfig;
use veloq_runtime::runtime::blocking::init_blocking_pool;
use veloq_buf::{GlobalAllocator, GlobalAllocatorConfig};
use veloq_runtime::io::buffer::RegisteredPool;
use std::num::NonZeroUsize;

fn create_local_executor() -> LocalExecutor {
    // Increase memory to 32MB to avoid OOM in BuddyPool
    let config = GlobalAllocatorConfig {
        thread_sizes: vec![NonZeroUsize::new(32 * 1024 * 1024).unwrap()],
    };
    let (mut memories, global_info) = GlobalAllocator::new(config).unwrap();
    let memory = memories.pop().unwrap();

    LocalExecutor::builder().build(move |registrar| {
        let pool = veloq_runtime::io::buffer::BuddyPool::new(memory).unwrap();
        veloq_runtime::io::buffer::AnyBufPool::new(
            RegisteredPool::new(pool, registrar, global_info)
                .expect("Failed to register buffer pool"),
        )
    })
}

fn bench_sync_into_future_alloc(c: &mut Criterion) {
    let mut exec = create_local_executor();
    init_blocking_pool(BlockingPoolConfig::default());

    // Run setup in the executor
    let file = exec.block_on(async {
        let file_path = Path::new("bench_sync_alloc.tmp");
        if file_path.exists() {
            let _ = std::fs::remove_file(file_path);
        }

        File::options()
            .write(true)
            .create(true)
            .truncate(true)
            .buffering(BufferingMode::DirectSync)
            .open(file_path)
            .await
            .expect("Failed to create file")
    });

    c.bench_function("sync_range_into_future", |b| {
        b.iter(|| {
            // Create the builder and convert to future.
            let fut = file.sync_range(black_box(0), black_box(1024)).into_future();
            let _ = black_box(fut);
        })
    });

    // Cleanup
    let _ = std::fs::remove_file("bench_sync_alloc.tmp");
}

criterion_group!(benches, bench_sync_into_future_alloc);
criterion_main!(benches);
