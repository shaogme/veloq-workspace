use crate::fs::{File, LocalFile};
use crate::runtime::Runtime;
use crate::runtime::context::alloc;
use crate::runtime::executor::LocalExecutor;
use std::fs;
use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use veloq_blocking::{BlockingPoolConfig, init_blocking_pool};
use veloq_buf::{BufferRegion, PoolTopology, ThreadMemoryMultiplier, UniformSlot, nz};

fn create_local_executor() -> LocalExecutor {
    let topology = UniformSlot::new(ThreadMemoryMultiplier(nz!(8)));
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
        topology.build(&global_pool, worker_idx, registrar)
    })
}

#[test]
fn test_file_integrity() {
    init_blocking_pool(BlockingPoolConfig::default());

    for size in [8192, 16384, 65536] {
        std::thread::spawn(move || {
            println!("Testing with BufferSize: {:?}", size);
            let mut exec = create_local_executor();

            exec.block_on(async move {
                let file_path_string = format!("test_file_integrity_{:?}.tmp", size);
                let file_path = Path::new(&file_path_string);
                // Remove file if exists
                if file_path.exists() {
                    let _ = fs::remove_file(file_path);
                }

                // 1. Create and Write
                {
                    let file = LocalFile::create(&file_path)
                        .await
                        .expect("Failed to create");

                    let mut write_buf = alloc(size);
                    let data = b"Hello World!";
                    write_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);

                    let (res, _) = file.write_at(write_buf, 0).await;
                    let wrote = res.expect("Write failed");
                    assert_eq!(wrote, size);

                    file.sync_all().await.expect("Sync failed");
                }

                // 2. Open and Read
                {
                    let file = LocalFile::open(&file_path).await.expect("Failed to open");

                    let read_buf = alloc(size);

                    let (res, read_buf) = file.read_at(read_buf, 0).await;
                    let n = res.expect("Read failed");
                    assert_eq!(n, size);
                    assert_eq!(&read_buf.as_slice()[..12], b"Hello World!");
                }

                // Cleanup
                if file_path.exists() {
                    let _ = fs::remove_file(file_path);
                }
            });
        })
        .join()
        .unwrap();
    }
}

#[test]
fn test_multithread_file_ops() {
    let completion_count = Arc::new(AtomicUsize::new(0));
    const NUM_TASKS: usize = 10;
    const NUM_WORKERS: usize = 3;

    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(NUM_WORKERS))
        .build()
        .unwrap();

    let (tx, mut rx) = crate::sync::mpsc::unbounded();
    let completion_count_clone = completion_count.clone();

    runtime.block_on(async move {
        for i in 0..NUM_TASKS {
            let tx_done = tx.clone();
            let counter = completion_count_clone.clone();

            crate::runtime::context::spawn(async move {
                let file_name = format!("test_mt_fs_{}.tmp", i);
                let path = Path::new(&file_name);

                // Ensure clean start
                if path.exists() {
                    let _ = fs::remove_file(path);
                }

                let content = format!("Task {} content", i);
                let len = content.len();

                // 1. Create and Write
                {
                    let file = File::create(path).await.expect("Failed to create file");
                    let mut write_buf = alloc(len);
                    write_buf.set_len(NonZeroUsize::new(len).unwrap());
                    write_buf.as_slice_mut().copy_from_slice(content.as_bytes());

                    let (res, _) = file.write_at(write_buf, 0).await;
                    let wrote = res.expect("Write failed");
                    assert_eq!(wrote, len);

                    file.sync_all().await.expect("Sync failed");
                }

                // 2. Open and Read
                {
                    let file = File::open(path).await.expect("Failed to open file");
                    let read_buf = alloc(len);

                    let (res, read_buf) = file.read_at(read_buf, 0).await;
                    let n = res.expect("Read failed");
                    assert_eq!(n, len);
                    assert_eq!(&read_buf.as_slice()[..n], content.as_bytes());
                }

                // Cleanup
                if path.exists() {
                    let _ = fs::remove_file(path);
                }

                counter.fetch_add(1, Ordering::SeqCst);
                tx_done.send(()).unwrap();
            });
        }

        for _ in 0..NUM_TASKS {
            rx.recv().await.unwrap();
        }
    });

    assert_eq!(completion_count.load(Ordering::SeqCst), NUM_TASKS);
}
