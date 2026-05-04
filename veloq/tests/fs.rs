use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::Once;
use std::sync::atomic::{AtomicUsize, Ordering};

use veloq::fs::{File, LocalFile};
use veloq::io::{AsyncBufRead, AsyncBufWrite};
use veloq::runtime::{Runtime, context};
use veloq_blocking::{BlockingPoolConfig, init_blocking_pool};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier, nz};
use veloq_runtime_next::scope;
use std::path::PathBuf;

struct CleanupGuard(PathBuf);

impl CleanupGuard {
    fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
        if path.exists() {
            let _ = std::fs::remove_file(&path);
        }
        Self(path)
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if self.0.exists() {
            let _ = std::fs::remove_file(&self.0);
        }
    }
}

static BLOCKING_POOL_INIT: Once = Once::new();

fn create_runtime() -> Runtime<UniformSlot> {
    BLOCKING_POOL_INIT.call_once(|| {
        init_blocking_pool(BlockingPoolConfig::default());
    });

    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(NonZeroUsize::new(1).expect("1 is non-zero"))
        .build()
        .expect("failed to build runtime")
}

#[test]
fn test_file_integrity() {
    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        let runtime = create_runtime();
        runtime.block_on(async move {
            let file_path = format!("test_file_integrity_{:?}.tmp", size);
            let _guard = CleanupGuard::new(&file_path);

            {
                let file = LocalFile::create(&file_path)
                    .await
                    .expect("Failed to create");

                let mut write_buf = context::alloc(size);
                let data = b"Hello World!";
                write_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);

                let (wrote, _) = file.write_at(write_buf, 0).await.expect("Write failed");
                assert_eq!(wrote, size.get());

                file.sync_all().await.expect("Sync failed");
            }

            {
                let file = LocalFile::open(&file_path).await.expect("Failed to open");
                let read_buf = context::alloc(size);
                let (n, read_buf) = file.read_at(read_buf, 0).await.expect("Read failed");
                assert_eq!(n, size.get());
                assert_eq!(&read_buf.as_slice()[..12], b"Hello World!");
            }
        });
    }
}

#[test]
fn test_multithread_file_ops() {
    let completion_count = Arc::new(AtomicUsize::new(0));
    const NUM_TASKS: usize = 10;

    let runtime = create_runtime();
    let completion_count_for_runtime = completion_count.clone();
    runtime.block_on(async move {
        scope!(s, {
            for i in 0..NUM_TASKS {
                let counter = completion_count_for_runtime.clone();
                s.spawn_boxed(async move {
                    let file_name = format!("test_mt_fs_{}.tmp", i);
                    let _guard = CleanupGuard::new(&file_name);

                    let content = format!("Task {} content", i);
                    let len = NonZeroUsize::new(content.len()).unwrap();

                    let file = File::options()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(&file_name)
                        .await
                        .expect("Failed to create file");
                    let mut write_buf = context::alloc(len);
                    write_buf.set_len(len.get());
                    write_buf.as_slice_mut().copy_from_slice(content.as_bytes());
                    let (wrote, _) = file.write_at(write_buf, 0).await.expect("Write failed");
                    assert_eq!(wrote, len.get());
                    file.sync_all().await.expect("Sync failed");

                    let read_buf = context::alloc(len);
                    let (n, read_buf) = file.read_at(read_buf, 0).await.expect("Read failed");
                    assert_eq!(n, len.get());
                    assert_eq!(&read_buf.as_slice()[..n], content.as_bytes());

                    drop(file);

                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
        });
    });

    assert_eq!(completion_count.load(Ordering::SeqCst), NUM_TASKS);
}

#[test]
fn test_fs_read_exact_write_all() {
    let runtime = create_runtime();

    runtime.block_on(async move {
        let path = "test_fs_exact.tmp";
        let _guard = CleanupGuard::new(path);

        let file = LocalFile::create(path)
            .await
            .expect("Failed to create file");

        const DATA: &[u8] = b"Hello Exact World!";
        let mut write_buf = context::alloc(nz!(DATA.len()));
        write_buf.as_slice_mut()[..DATA.len()].copy_from_slice(DATA);
        write_buf.set_len(DATA.len());

        file.write_all(write_buf).await.expect("write_all failed");
        file.sync_all().await.expect("Sync failed");
        drop(file);

        let file = LocalFile::open(path).await.expect("Failed to open file");
        let mut read_buf = context::alloc(nz!(DATA.len()));
        read_buf.set_len(DATA.len());
        let (n, read_buf) = file.read_exact(read_buf).await.expect("read_exact failed");
        assert_eq!(n, DATA.len());
        assert_eq!(read_buf.as_slice(), DATA);

        drop(file);
    });
}
