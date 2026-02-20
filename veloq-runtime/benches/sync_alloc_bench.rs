use criterion::{Criterion, criterion_group, criterion_main};
use std::future::IntoFuture;
use std::hint::black_box;
use std::num::NonZeroUsize;
use std::path::Path;
use veloq_buf::PoolTopology;

use veloq_runtime::LocalExecutor;
use veloq_runtime::config::BlockingPoolConfig;
use veloq_runtime::fs::{BufferingMode, File};
use veloq_runtime::runtime::blocking::init_blocking_pool;

fn create_local_executor() -> LocalExecutor {
    // Increase memory to 32MB to avoid OOM in BuddyPool
    LocalExecutor::builder().build(move |registrar| {
        use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier};

        // 16x multiplier -> 32MB (BuddyPool default block size is 2MB * 16 = 32MB)
        let multiplier = ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(16) });
        let topology = UniformSlot::new(multiplier);

        let global_pool = topology
            .create_pool(1)
            .expect("Failed to create global pool");
        topology.build(&global_pool, 0, registrar)
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
            drop(black_box(fut));
        })
    });

    // Cleanup
    let _ = std::fs::remove_file("bench_sync_alloc.tmp");
}

criterion_group!(benches, bench_sync_into_future_alloc);
criterion_main!(benches);
