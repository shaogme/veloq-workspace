use std::num::NonZeroUsize;
use std::sync::mpsc::channel;
use std::time::Instant;
use veloq_buf::global::{GlobalAllocator, GlobalAllocatorConfig};
use veloq_buf::{BufPool, BufferRegion, BufferRegistrar, FixedBuf, ThreadMemoryMultiplier};
use veloq_buf::{HybridPool, RegisteredPool};

struct DummyRegistrar;
impl BufferRegistrar for DummyRegistrar {
    fn register(&self, regions: &[BufferRegion]) -> std::io::Result<Vec<usize>> {
        Ok(vec![0; regions.len()])
    }
}

#[test]
fn bench_hybrid_contended() {
    let multiplier = ThreadMemoryMultiplier(NonZeroUsize::new(8).unwrap());
    let config = GlobalAllocatorConfig {
        multipliers: vec![multiplier],
    };

    let (mut memories, global_info) = match GlobalAllocator::new(config) {
        Ok(res) => res,
        Err(e) => {
            eprintln!(
                "Skipping benchmark: Failed to allocate global memory: {}",
                e
            );
            return;
        }
    };

    let memory = memories.pop().unwrap();
    let raw_pool = HybridPool::new(memory).expect("Failed to create HybridPool");
    let pool = RegisteredPool::new(raw_pool, Box::new(DummyRegistrar), global_info)
        .expect("Failed to register pool");

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

    // Producer (must be owner thread of the pool, which is the main thread here if we created it here? No, owner is thread::current().id() when new() called)
    // HybridPool::new was called in main thread. So main thread is owner.

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

    println!("HybridPool Contended Performance:");
    println!("  Iterations: {}", iterations);
    println!("  Total Time: {:?}", duration);
    println!("  Avg Time: {:?} / op", duration / iterations as u32);
}
