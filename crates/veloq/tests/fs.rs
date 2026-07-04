use std::{
    env,
    fs::remove_file,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use veloq::{
    fs::{File, LocalFile},
    io::{AsyncBufRead, AsyncBufWrite},
    nz,
    runtime::Runtime,
};
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier};

static TEMP_FILE_ID: AtomicUsize = AtomicUsize::new(0);

struct CleanupGuard(PathBuf);

impl CleanupGuard {
    fn new(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let _ = remove_file(&path);
        }
        Self(path)
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if self.0.exists() {
            let _ = remove_file(&self.0);
        }
    }
}

fn run_with_runtime<F, R>(f: F) -> R
where
    F: for<'s1, 's2> std::ops::AsyncFnOnce(veloq::runtime::context::Ctx<'s1, 's2>) -> R,
{
    Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(4))))
        .worker_count(Some(nz!(1)))
        .scope(f)
        .expect("failed to run scope")
}

fn temp_file_path(label: &str) -> PathBuf {
    let id = TEMP_FILE_ID.fetch_add(1, Ordering::Relaxed);
    env::temp_dir().join(format!("veloq-{label}-{}-{id}.tmp", process::id()))
}

#[test]
fn test_file_integrity() {
    for size in [nz!(8192), nz!(16384), nz!(65536)] {
        run_with_runtime(async |ctx| {
            let file_path = temp_file_path(&format!("file-integrity-{}", size.get()));
            let _guard = CleanupGuard::new(&file_path);

            {
                let file = LocalFile::create(ctx, &file_path)
                    .await
                    .expect("Failed to create");

                let mut write_buf = ctx.alloc(size);
                let data = b"Hello World!";
                write_buf.spare_capacity_mut()[..data.len()].copy_from_slice(data);

                let (wrote, _) = file.write_at(write_buf, 0).await.expect("Write failed");
                assert_eq!(wrote, size.get());

                file.sync_all().await.expect("Sync failed");
            }

            {
                let file = LocalFile::open(ctx, &file_path)
                    .await
                    .expect("Failed to open");
                let read_buf = ctx.alloc(size);
                let (n, read_buf) = file.read_at(read_buf, 0).await.expect("Read failed");
                assert_eq!(n, size.get());
                assert_eq!(&read_buf.as_slice()[..12], b"Hello World!");
            }
        });
    }
}

#[test]
fn test_file_can_be_reopened_while_existing_handle_is_alive() {
    run_with_runtime(async |ctx| {
        let path = temp_file_path("fs-shared-open");
        let _guard = CleanupGuard::new(&path);

        let writer = LocalFile::create(ctx, &path)
            .await
            .expect("Failed to create file");
        let reader = LocalFile::open(ctx, &path)
            .await
            .expect("Failed to reopen file while writer is alive");

        drop(reader);
        drop(writer);
    });
}

#[test]
fn test_multithread_file_ops() {
    let completion_count = Arc::new(AtomicUsize::new(0));
    const NUM_TASKS: usize = 10;

    let completion_count_for_runtime = completion_count.clone();
    run_with_runtime(async |ctx| {
        use veloq::runtime::scope;
        scope!(ctx, async |s| {
            for i in 0..NUM_TASKS {
                let counter = completion_count_for_runtime.clone();
                s.spawn_boxed(async move {
                    let file_name = temp_file_path(&format!("mt-fs-{i}"));
                    let _guard = CleanupGuard::new(&file_name);

                    let content = format!("Task {} content", i);
                    let len = NonZeroUsize::new(content.len()).unwrap();

                    let file = File::options()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(ctx, &file_name)
                        .await
                        .expect("Failed to create file");
                    let mut write_buf = ctx.alloc(len);
                    write_buf.set_len(len.get());
                    write_buf.as_slice_mut().copy_from_slice(content.as_bytes());
                    let (wrote, _) = file.write_at(write_buf, 0).await.expect("Write failed");
                    assert_eq!(wrote, len.get());
                    file.sync_all().await.expect("Sync failed");

                    let read_buf = ctx.alloc(len);
                    let (n, read_buf) = file.read_at(read_buf, 0).await.expect("Read failed");
                    assert_eq!(n, len.get());
                    assert_eq!(&read_buf.as_slice()[..n], content.as_bytes());

                    drop(file);

                    counter.fetch_add(1, Ordering::SeqCst);
                });
            }
        })
        .await
        .unwrap();
    });

    assert_eq!(completion_count.load(Ordering::SeqCst), NUM_TASKS);
}

#[test]
fn test_fs_read_exact_write_all() {
    run_with_runtime(async |ctx| {
        let path = temp_file_path("fs-exact");
        let _guard = CleanupGuard::new(&path);

        let file = LocalFile::create(ctx, &path)
            .await
            .expect("Failed to create file");

        const DATA: &[u8] = b"Hello Exact World!";
        let mut write_buf = ctx.alloc(nz!(DATA.len()));
        write_buf.as_slice_mut()[..DATA.len()].copy_from_slice(DATA);
        write_buf.set_len(DATA.len());

        file.write_all(write_buf).await.expect("write_all failed");
        file.sync_all().await.expect("Sync failed");
        drop(file);

        let file = LocalFile::open(ctx, &path)
            .await
            .expect("Failed to open file");
        let mut read_buf = ctx.alloc(nz!(DATA.len()));
        read_buf.set_len(DATA.len());
        let (n, read_buf) = file.read_exact(read_buf).await.expect("read_exact failed");
        assert_eq!(n, DATA.len());
        assert_eq!(read_buf.as_slice(), DATA);

        drop(file);
    });
}
