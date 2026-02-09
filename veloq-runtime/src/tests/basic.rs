//! Basic runtime tests for spawn and spawn_local functionality.

use crate::runtime::{LocalExecutor, Runtime};
use crate::spawn_local;
use crate::sync::mpsc;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use veloq_buf::{BufferRegion, ThreadMemoryMultiplier, UniformBlock, nz};

fn create_local_executor() -> LocalExecutor {
    let topology = UniformBlock::hybrid(ThreadMemoryMultiplier(nz!(8)));
    // We are creating a single-threaded executor for test, so worker_count = 1
    let global_pool = topology
        .create_pool(1)
        .expect("Failed to create global pool");

    // We are worker 0
    let worker_idx = 0;

    LocalExecutor::builder().build(move |registrar| {
        // Register global memory
        let info = global_pool.global_info();
        let regions = [BufferRegion::new(info.ptr, info.len)];
        registrar.register(&regions).expect("Failed to register");

        // Use topology to build pool
        topology.build_for_worker(global_pool, worker_idx, registrar)
    })
}

// ============ LocalExecutor Tests (Single Threaded) ============

/// Test that spawn_local works correctly in a basic LocalExecutor.
/// This verifies that tasks are executed on the same thread.
#[test]
fn test_spawn_local_basic() {
    let mut exec = create_local_executor();
    let result = Rc::new(RefCell::new(0));
    let result_clone = result.clone();

    exec.block_on(async move {
        // let cx = crate::runtime::context::current();
        let handle = spawn_local(async move {
            *result_clone.borrow_mut() = 42;
            "done"
        });

        assert_eq!(handle.await, "done");
    });

    assert_eq!(*result.borrow(), 42);
}

/// Test that spawn_local supports !Send futures (like Rc).
#[test]
fn test_spawn_local_not_send() {
    let mut exec = create_local_executor();
    // Rc is !Send
    let data = Rc::new(vec![1, 2, 3]);
    let data_clone = data.clone();

    exec.block_on(async move {
        // let cx = crate::runtime::context::current();
        // This would fail to compile with spawn()
        let handle = spawn_local(async move {
            assert_eq!(data_clone.len(), 3);
            data_clone[0] + data_clone[1] + data_clone[2]
        });

        assert_eq!(handle.await, 6);
    });
}

/// Test nested spawn_local calls.
#[test]
fn test_nested_spawn_local() {
    let mut exec = create_local_executor();
    let counter = Rc::new(RefCell::new(0));
    let c1 = counter.clone();

    exec.block_on(async move {
        let h1 = spawn_local(async move {
            *c1.borrow_mut() += 1;
            let c2 = c1.clone();

            let h2 = spawn_local(async move {
                *c2.borrow_mut() += 10;
            });
            h2.await;
        });
        h1.await;
    });

    assert_eq!(*counter.borrow(), 11);
}

// ============ Runtime Tests (Multi-Threaded) ============

/// Test global spawn works from within the Runtime (injecting into workers).
#[test]
fn test_runtime_global_spawn() {
    // 1 Worker
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    let (tx, rx) = std::sync::mpsc::channel();

    // Block on main thread
    runtime.block_on(async move {
        // Spawn a task globally from the main thread
        let handle = crate::runtime::context::spawn(async move { 42 });
        let res = handle.await;
        tx.send(res).unwrap();
    });

    assert_eq!(rx.recv().unwrap(), 42);
}

/// Test global spawn works from INSIDE a worker.
/// Implicitly tested by generic spawn, but let's test specific worker-to-worker or such?
/// Since we can't easily execute specific code on a specific worker unless we schedule it there:
#[test]
fn test_spawn_from_any_thread() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(2)) // 2 workers
        .build()
        .unwrap();

    let (tx, rx) = std::sync::mpsc::channel();

    runtime.block_on(async move {
        // Current context (Main) -> spawn -> (Worker 0/1)
        let handle = crate::runtime::context::spawn(async move {
            // Inside Worker
            "hello from worker"
        });

        let res = handle.await;
        tx.send(res).unwrap();
    });

    assert_eq!(rx.recv().unwrap(), "hello from worker");
}

/// Test using both spawn_local and spawn in a worker.
#[test]
fn test_mixed_spawn_in_worker() {
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    let (tx, rx) = std::sync::mpsc::channel();

    runtime.block_on(async move {
        // We are on Main Thread (participating as LocalExecutor)
        // 1. spawn_local (!Send)
        let rc_val = Rc::new(5);
        let rc_clone = rc_val.clone();
        let local_handle = crate::runtime::context::spawn_local(async move { *rc_clone * 2 });

        // 2. spawn (Send) - goes to Worker
        let global_handle = crate::runtime::context::spawn(async move { 20 });

        let v1 = local_handle.await;
        let v2 = global_handle.await;

        tx.send(v1 + v2).unwrap();
    });

    assert_eq!(rx.recv().unwrap(), 10 + 20);
}

/// Test that tasks can float between workers (basic check).
#[test]
fn test_multi_worker_throughput() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(2))
        .build()
        .unwrap();

    let counter = Arc::new(AtomicUsize::new(0));

    // Spawn 50 global tasks
    for _ in 0..50 {
        let c = counter.clone();
        runtime.spawn(async move {
            c.fetch_add(1, Ordering::SeqCst);
        });
    }

    let counter_clone = counter.clone();
    runtime.block_on(async move {
        // Wait for completion
        let start = std::time::Instant::now();
        while counter_clone.load(Ordering::SeqCst) < 50 {
            if start.elapsed() > std::time::Duration::from_secs(5) {
                break;
            }
            crate::runtime::context::yield_now().await;
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    });

    let final_count = counter.load(Ordering::SeqCst);
    assert_eq!(final_count, 50);
}

/// Test local channel functionality using the runtime.
#[test]
fn test_local_channel() {
    let mut exec = create_local_executor();

    exec.block_on(async move {
        let (tx, mut rx) = mpsc::unbounded();

        // Spawn a local task to send messages
        let tx1 = tx.clone();
        spawn_local(async move {
            tx1.send(1).unwrap();
            crate::runtime::context::yield_now().await;
            tx1.send(2).unwrap();
        });

        // Spawn another local task to send messages
        let tx2 = tx.clone();
        spawn_local(async move {
            crate::runtime::context::yield_now().await;
            tx2.send(3).unwrap();
        });

        // Drop original text to allow disconnect detection eventually (after clones drop)
        drop(tx);

        assert_eq!(rx.recv().await, Some(1));
        assert_eq!(rx.recv().await, Some(2));
        assert_eq!(rx.recv().await, Some(3));
        assert_eq!(rx.recv().await, None); // All senders dropped
    });
}

/// Test local channel disconnect behavior.
#[test]
fn test_local_channel_disconnect() {
    let mut exec = create_local_executor();

    exec.block_on(async move {
        let (tx, mut rx) = mpsc::unbounded::<i32>();

        // Drop sender immediately
        drop(tx);

        // Receiver should see disconnected
        assert_eq!(rx.recv().await, None);
    });
}
