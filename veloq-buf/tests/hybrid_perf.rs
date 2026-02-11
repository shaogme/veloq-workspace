use std::num::NonZeroUsize;
use std::sync::mpsc::channel;
use std::time::Instant;
use veloq_buf::{
    BufPool, BufferRegion, BufferRegistrar, FixedBuf, ThreadMemoryMultiplier, UniformBlock,
};

struct DummyRegistrar;
impl BufferRegistrar for DummyRegistrar {
    fn register(&self, regions: &[BufferRegion]) -> std::io::Result<Vec<usize>> {
        Ok(vec![0; regions.len()])
    }
}

#[test]
fn bench_hybrid_contended() {
    // 8x multiplier for ~16MB total (Hybrid needs min 14MB per instance)
    // Here we use 1 thread, so 1 instance.
    let multiplier = ThreadMemoryMultiplier(std::num::NonZeroUsize::new(8).unwrap());
    let topology = UniformBlock::hybrid(multiplier);

    // Create global pool for 1 worker
    // This creates 2 blocks (Primary + Backup) for Thread 0
    let global_pool = topology
        .create_pool(1)
        .expect("Failed to create global pool");

    // Build pool for worker 0
    let pool = topology.build_for_worker(&global_pool, 0, Box::new(DummyRegistrar));

    let iterations = 500_000;

    // Channel to pass buffers
    let (tx, rx) = channel::<FixedBuf>();

    let start = Instant::now();

    let receiver = std::thread::spawn(move || {
        // Consumer just drops buffers
        // This simulates remote deallocation
        for _ in 0..iterations {
            if let Ok(buf) = rx.recv() {
                drop(buf);
            }
        }
    });

    for _ in 0..iterations {
        let mut buf = pool.alloc(NonZeroUsize::new(4096).unwrap());
        while buf.is_none() {
            std::thread::yield_now();
            buf = pool.alloc(NonZeroUsize::new(4096).unwrap());
        }
        tx.send(buf.unwrap()).unwrap();
    }

    receiver.join().unwrap();

    let duration = start.elapsed();

    println!("HybridPool (BlockBased) Contended Performance:");
    println!("  Iterations: {}", iterations);
    println!("  Total Time: {:?}", duration);
    println!("  Avg Time: {:?} / op", duration / iterations as u32);
}
