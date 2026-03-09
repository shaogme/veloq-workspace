use crate::runtime::Runtime;
use veloq_buf::{BufPool, UniformSlot, heap::ThreadMemoryMultiplier, nz};

// Test automatic memory expansion and registration
#[test]
fn test_memory_expansion_and_registration() {
    // 1. Configure Runtime with very small memory to trigger expansion easily.
    // Multiplier 1 means 2MB * 2 * 1 worker = 4MB total initial memory.
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .with_topology(UniformSlot::new(ThreadMemoryMultiplier(nz!(1))))
        .build()
        .unwrap();

    runtime.block_on(async move {
        // Use current_pool() and unwrap it
        let pool = crate::runtime::context::current_pool().expect("Buffer pool not found");

        println!("Initial pool allocation...");

        // 2. Allocate enough buffers to exhaust the initial memory.
        let mut bufs = Vec::new();
        // 1MB allocations
        let alloc_size = nz!(1024 * 1024);

        // Allocate 4x 1MB -> Should fill the pool (4MB)
        for i in 0..4 {
            if let Some(buf) = pool.alloc(alloc_size) {
                println!("Allocated buffer {}", i);
                bufs.push(buf);
            } else {
                panic!("Failed to allocate initial buffer {}", i);
            }
        }

        println!("Allocated {} buffers (Initial 4MB)", bufs.len());

        // 3. Allocate more to trigger expansion
        // This should trigger expansion by 64MB (default EXPANSION_SIZE in GlobalSlotPool)
        println!("Triggering expansion...");

        if let Some(mut buf) = pool.alloc(alloc_size) {
            println!("Allocated extra buffer (Expansion triggered)");

            // 4. Verify the new buffer works
            {
                let slice = buf.as_slice_mut();
                slice[0] = 42;
                slice[slice.len() - 1] = 100;

                assert_eq!(slice[0], 42);
                assert_eq!(slice[slice.len() - 1], 100);
            }

            bufs.push(buf);
        } else {
            panic!("Unified allocation failed - expansion didn't work?");
        }

        // 5. Verify registration happened (implicitly) and we can continue allocating
        // Allocate 10 more to ensure the new chunk is actually usable and big enough
        for i in 0..10 {
            if let Some(buf) = pool.alloc(alloc_size) {
                bufs.push(buf);
            } else {
                panic!("Failed to allocate post-expansion buffer {}", i);
            }
        }

        println!("Total allocated buffers: {}", bufs.len());
        assert!(
            bufs.len() >= 15,
            "Should have allocated significantly more than initial capacity"
        );

        // Optional: Yield to allow any async registration tasks to proceed (though registration is usually synchronous in notification)
        crate::runtime::context::yield_now().await;
    });
}

#[test]
fn test_multithreaded_expansion() {
    // Test Cross-Thread Registration:
    // Worker 0 triggers expansion.
    // Worker 1 must see the new chunk and register it to its own driver to use it.

    // Config: 2 Workers
    // Size: Multiplier 1 ==> 2MB * 2 * 2 workers = 8MB Total Initial.
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(2))
        .with_topology(UniformSlot::new(ThreadMemoryMultiplier(nz!(1))))
        .build()
        .unwrap();

    runtime.block_on(async move {
        let pool = crate::runtime::context::current_pool().expect("No pool");

        // 1. Worker 0 fills the pool (8MB)
        let mut holding = Vec::new();
        let chunk_size = nz!(1024 * 1024); // 1MB

        println!("Worker 0: Filling pool...");
        // Alloc 8MB (should succeed mostly)
        for i in 0..8 {
            if let Some(buf) = pool.alloc(chunk_size) {
                holding.push(buf);
            } else {
                println!("Worker 0 warning: Early alloc failure at {}", i);
            }
        }

        // 2. Trigger Expansion (Alloc 1 more)
        println!("Worker 0: Triggering expansion...");
        if let Some(buf) = pool.alloc(chunk_size) {
            // Verify it is from a new chunk (Chunk 0 has max 8MB, we just took 9th)
            let info = buf.resolve_region_info();
            assert_ne!(info.id, 0, "Should be new chunk");
            println!("Worker 0: Expansion successful (Chunk {})", info.id);
            holding.push(buf);
        } else {
            panic!("Worker 0 failed to trigger expansion");
        }

        // 3. Spawn task on (hopefully) another worker to verify visibility
        // We spawn multiple to ensure at least one hits Worker 1
        let handles: Vec<_> = (0..4)
            .map(|_i| {
                crate::runtime::context::spawn(async move {
                    let chunk_size = nz!(1024 * 1024);

                    // Allow some time for epoch synch
                    crate::runtime::context::yield_now().await;

                    let pool = crate::runtime::context::current_pool().unwrap();

                    // Try alloc. Since Main Thread holds ~9MB, and initial was ~8MB,
                    // any new alloc MUST come from the new 64MB chunk.
                    if let Some(buf) = pool.alloc(chunk_size) {
                        let info = buf.resolve_region_info();
                        // Just return the chunk ID
                        Some(info.id)
                    } else {
                        None
                    }
                })
            })
            .collect();

        for h in handles {
            let res = h.await;
            if let Some(chunk_id) = res {
                assert_eq!(chunk_id, 1, "Other workers should see Chunk 1");
            }
            // Some allocs might fail if we raced too fast? Unlikely with 64MB expansion.
        }

        println!("Cross-thread expansion test passed");
    });
}
