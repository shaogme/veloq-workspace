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
use veloq_buf::nz;

fn create_local_executor() -> LocalExecutor {
    LocalExecutor::new_default()
}

#[test]
fn test_file_integrity() {
    init_blocking_pool(BlockingPoolConfig::default());

    for size in [nz!(8192), nz!(16384), nz!(65536)] {
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
                    assert_eq!(wrote, size.get());

                    file.sync_all().await.expect("Sync failed");
                }

                // 2. Open and Read
                {
                    let file = LocalFile::open(&file_path).await.expect("Failed to open");

                    let read_buf = alloc(size);

                    let (res, read_buf) = file.read_at(read_buf, 0).await;
                    let n = res.expect("Read failed");
                    assert_eq!(n, size.get());
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
                let len = NonZeroUsize::new(content.len()).unwrap();

                // 1. Create and Write
                {
                    let file = File::create(path).await.expect("Failed to create file");
                    let mut write_buf = alloc(len);
                    write_buf.set_len(len.get());
                    write_buf.as_slice_mut().copy_from_slice(content.as_bytes());

                    let (res, _) = file.write_at(write_buf, 0).await;
                    let wrote = res.expect("Write failed");
                    assert_eq!(wrote, len.get());

                    file.sync_all().await.expect("Sync failed");
                }

                // 2. Open and Read
                {
                    let file = File::open(path).await.expect("Failed to open file");
                    let read_buf = alloc(len);

                    let (res, read_buf) = file.read_at(read_buf, 0).await;
                    let n = res.expect("Read failed");
                    assert_eq!(n, len.get());
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

/// Test File read cancellation
#[test]
fn test_fs_cancel_read() {
    use crate::select;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    // Helper to yield once to allow the IO future to be polled and submitted
    struct YieldOnce(bool);
    impl Future for YieldOnce {
        type Output = ();
        fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
            if self.0 {
                Poll::Ready(())
            } else {
                self.0 = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    }

    let completion_count = Arc::new(AtomicUsize::new(0));
    let completion_count_clone = completion_count.clone();

    let runtime = Runtime::builder()
        .config(crate::config::Config::default().worker_threads(1))
        .build()
        .unwrap();

    runtime.block_on(async move {
        // Create a large file
        let path = Path::new("test_fs_cancel.tmp");
        if path.exists() {
            let _ = fs::remove_file(path);
        }

        let file = File::create(path).await.expect("Failed to create file");
        let size = nz!(65536);
        let mut write_buf = alloc(size);
        write_buf.set_len(size.get());
        // Fill properly to avoid sparse optimization if any?
        write_buf.as_slice_mut().fill(1);

        file.write_at(write_buf, 0).await.0.expect("Write failed");
        file.sync_all().await.expect("Sync failed");
        drop(file);

        // Re-open for read
        let file = File::open(path).await.expect("Failed to open file");
        let read_buf = alloc(size);

        select! {
            res = file.read_at(read_buf, 0) => {
                 let _ = res.0.expect("Read success");
                 println!("Read completed instantly - cancellation skipped");
            },
            _ = YieldOnce(false) => {
                 println!("File read cancelled successfully");
            }
        };

        // Cleanup
        drop(file);
        if path.exists() {
            let _ = fs::remove_file(path);
        }

        completion_count_clone.fetch_add(1, Ordering::SeqCst);
    });

    assert_eq!(completion_count.load(Ordering::SeqCst), 1);
}
