use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use futures_util::{FutureExt, StreamExt, future::BoxFuture, stream::FuturesUnordered};
use std::{
    cmp::min,
    collections::VecDeque,
    env,
    fs::remove_file,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    process::id,
    rc::Rc,
    time::{Duration, Instant},
};
use veloq::{
    buf::{UniformSlot, heap::ThreadMemoryMultiplier},
    error,
    fs::{BufferingMode, File},
    nz,
    runtime::{Runtime, context::Ctx, scope, scope_local},
};
use veloq_buf::FixedBuf;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchSyncMode {
    None,
    SyncRange,
    SyncAll,
    SyncData,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchPhase {
    Total,
    Write,
    Flush,
}

struct CleanupGuard(PathBuf);

impl CleanupGuard {
    fn new(path: impl Into<PathBuf>) -> Self {
        let path = path.into();
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

fn bench_buffering_mode() -> BufferingMode {
    let raw = env::var("VELOQ_BENCH_BUFFERING").unwrap_or_else(|_| "directsync".to_string());
    match raw.to_ascii_lowercase().as_str() {
        "buffered" => BufferingMode::Buffered,
        "direct" => BufferingMode::Direct,
        "directsync" => BufferingMode::DirectSync,
        other => panic!("Unsupported VELOQ_BENCH_BUFFERING: {other}"),
    }
}

fn bench_sync_mode() -> BenchSyncMode {
    let raw = env::var("VELOQ_BENCH_SYNC").unwrap_or_else(|_| "sync_range".to_string());
    match raw.to_ascii_lowercase().as_str() {
        "none" => BenchSyncMode::None,
        "sync_range" => BenchSyncMode::SyncRange,
        "sync_all" => BenchSyncMode::SyncAll,
        "sync_data" => BenchSyncMode::SyncData,
        other => panic!("Unsupported VELOQ_BENCH_SYNC: {other}"),
    }
}

fn bench_phase() -> BenchPhase {
    let raw = env::var("VELOQ_BENCH_PHASE").unwrap_or_else(|_| "total".to_string());
    match raw.to_ascii_lowercase().as_str() {
        "total" => BenchPhase::Total,
        "write" => BenchPhase::Write,
        "flush" => BenchPhase::Flush,
        other => panic!("Unsupported VELOQ_BENCH_PHASE: {other}"),
    }
}

fn bench_case_name() -> String {
    let buffering = env::var("VELOQ_BENCH_BUFFERING").unwrap_or_else(|_| "directsync".to_string());
    let sync = env::var("VELOQ_BENCH_SYNC").unwrap_or_else(|_| "sync_range".to_string());
    let phase = env::var("VELOQ_BENCH_PHASE").unwrap_or_else(|_| "total".to_string());
    format!("{buffering}_{sync}_{phase}")
}

fn bench_base_dir() -> PathBuf {
    env::var("VELOQ_BENCH_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| PathBuf::from("."))
}

fn bench_file_path(base_dir: &Path, name: impl AsRef<str>) -> PathBuf {
    base_dir.join(name.as_ref())
}

async fn apply_sync<'rt, 'reg>(file: &File<'rt, 'reg>, len: u64, mode: BenchSyncMode) {
    match mode {
        BenchSyncMode::None => {}
        BenchSyncMode::SyncRange => {
            file.sync_range(0, len)
                .wait_before(false)
                .write(true)
                .wait_after(true)
                .await
                .expect("SyncRange failed");
        }
        BenchSyncMode::SyncAll => {
            file.sync_all().await.expect("SyncAll failed");
        }
        BenchSyncMode::SyncData => {
            file.sync_data().await.expect("SyncData failed");
        }
    }
}

async fn open_file<'rt, 'reg>(
    ctx: Ctx<'rt, 'reg>,
    path: &Path,
    buffering_mode: BufferingMode,
) -> File<'rt, 'reg> {
    File::options()
        .write(true)
        .create(true)
        .truncate(true)
        .buffering(buffering_mode)
        .open(ctx, path)
        .await
        .expect("Failed to create")
}

async fn open_and_fallocate<'rt, 'reg>(
    ctx: Ctx<'rt, 'reg>,
    path: &Path,
    buffering_mode: BufferingMode,
    len: u64,
) -> File<'rt, 'reg> {
    let file = open_file(ctx, path, buffering_mode).await;
    file.fallocate(0, len).await.expect("Fallocate failed");
    file
}

async fn run_1gb_iteration<'rt, 'reg>(
    ctx: Ctx<'rt, 'reg>,
    phase: BenchPhase,
    buffering_mode: BufferingMode,
    sync_mode: BenchSyncMode,
) -> Duration {
    const TOTAL_SIZE: u64 = 1024 * 1024 * 1024;
    const CHUNK_SIZE: NonZeroUsize = nz!(4 * 1024 * 1024);

    let base_dir = bench_base_dir();
    let file_path = bench_file_path(&base_dir, format!("bench_1gb_{}.tmp", id()));
    let _guard = CleanupGuard::new(&file_path);

    let total_start = matches!(phase, BenchPhase::Total).then(Instant::now);

    let file = open_and_fallocate(ctx, &file_path, buffering_mode, TOTAL_SIZE).await;
    let file = Rc::new(file);

    let write_start = matches!(phase, BenchPhase::Write).then(Instant::now);

    let concurrency_limit = 32;

    scope_local!(ctx, async |s| {
        let mut tasks = VecDeque::new();
        let mut offset: u64 = 0;

        while offset < TOTAL_SIZE {
            if tasks.len() < concurrency_limit
                && let Some(buf) = ctx.try_alloc_from_pool(CHUNK_SIZE)
            {
                let remaining = TOTAL_SIZE - offset;
                let write_len = min(remaining, CHUNK_SIZE.get() as u64) as usize;
                let file = file.clone();
                let current_offset = offset;

                tasks.push_back(
                    s.spawn_boxed_local(async move { file.write_at(buf, current_offset).await }),
                );
                offset += write_len as u64;
                continue;
            }

            if let Some(handle) = tasks.pop_front() {
                let (n, _buf) = handle
                    .await
                    .expect("chunk write task failed")
                    .expect("Write failed");
                let _ = n;
            } else {
                panic!("Deadlock: No tasks to wait for but cannot allocate buffer");
            }
        }

        while let Some(handle) = tasks.pop_front() {
            let (n, _buf) = handle
                .await
                .expect("chunk write task failed")
                .expect("Write failed");
            let _ = n;
        }
    })
    .await
    .unwrap();

    let flush_start = matches!(phase, BenchPhase::Flush).then(Instant::now);
    apply_sync(&file, TOTAL_SIZE, sync_mode).await;

    match phase {
        BenchPhase::Total => total_start.expect("total timer missing").elapsed(),
        BenchPhase::Write => write_start.expect("write timer missing").elapsed(),
        BenchPhase::Flush => flush_start.expect("flush timer missing").elapsed(),
    }
}

async fn run_worker_iteration<'rt, 'reg>(
    ctx: Ctx<'rt, 'reg>,
    files: Vec<File<'rt, 'reg>>,
    file_size: u64,
    chunk_size: NonZeroUsize,
    sync_mode: BenchSyncMode,
) -> u64 {
    let worker_id = ctx.runtime_ctx.worker_id();
    let concurrency_limit = 8;
    let mut offsets = vec![0u64; files.len()];
    let mut current_local_idx = 0usize;
    let mut in_flight = 0usize;
    let mut tasks: FuturesUnordered<BoxFuture<'_, error::Result<(usize, FixedBuf)>>> =
        FuturesUnordered::new();
    let mut written_bytes = 0u64;

    loop {
        let all_submitted = offsets.iter().all(|&o| o >= file_size);

        while in_flight < concurrency_limit && !all_submitted {
            let mut found = None;
            for _ in 0..files.len() {
                if offsets[current_local_idx] < file_size {
                    found = Some(current_local_idx);
                    current_local_idx = (current_local_idx + 1) % files.len();
                    break;
                }
                current_local_idx = (current_local_idx + 1) % files.len();
            }

            let Some(idx) = found else {
                break;
            };
            let Some(buf) = ctx.try_alloc_from_pool(chunk_size) else {
                break;
            };

            let remaining = file_size - offsets[idx];
            let write_len = min(remaining, chunk_size.get() as u64) as usize;
            let file = &files[idx];
            let current_offset = offsets[idx];

            tasks.push(async move { file.write_at(buf, current_offset).await }.boxed());
            offsets[idx] += write_len as u64;
            in_flight += 1;
        }

        if in_flight == 0 {
            if all_submitted {
                break;
            }
            panic!(
                "Deadlock in worker {worker_id}: No tasks to wait for but cannot allocate buffer"
            );
        }

        let Some(result) = tasks.next().await else {
            if all_submitted {
                break;
            }
            panic!("Deadlock in worker {worker_id}: task queue closed unexpectedly");
        };
        let (n, _buf) = result.expect("Write failed");
        written_bytes += n as u64;
        in_flight -= 1;
    }

    for file in &files {
        apply_sync(file, file_size, sync_mode).await;
    }

    written_bytes
}

fn benchmark_1gb_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("fs_throughput");

    // 1GB Total Size
    const TOTAL_SIZE: u64 = 1024 * 1024 * 1024;

    // 设置吞吐量统计单位
    group.throughput(Throughput::Bytes(TOTAL_SIZE));
    // 1GB写入耗时较长，减少采样次数并增加单次超时时间
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(120));

    let buffering_mode = bench_buffering_mode();
    let sync_mode = bench_sync_mode();
    let phase = bench_phase();
    let bench_name = format!("write_1gb_concurrent_{}", bench_case_name());

    match phase {
        BenchPhase::Total => {
            group.bench_function(&bench_name, |b| {
                b.iter_custom(|iters| {
                    let mut total_elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let elapsed =
                            Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(128))))
                                .worker_count(Some(nz!(1)))
                                .scope(async |ctx| {
                                    run_1gb_iteration(
                                        ctx,
                                        BenchPhase::Total,
                                        buffering_mode,
                                        sync_mode,
                                    )
                                    .await
                                })
                                .unwrap();
                        total_elapsed += elapsed;
                    }
                    total_elapsed
                })
            });
        }
        BenchPhase::Write => {
            group.bench_function(&bench_name, |b| {
                b.iter_custom(|iters| {
                    let mut total_elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let elapsed =
                            Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(128))))
                                .worker_count(Some(nz!(1)))
                                .scope(async |ctx| {
                                    run_1gb_iteration(
                                        ctx,
                                        BenchPhase::Write,
                                        buffering_mode,
                                        sync_mode,
                                    )
                                    .await
                                })
                                .unwrap();
                        total_elapsed += elapsed;
                    }
                    total_elapsed
                })
            });
        }
        BenchPhase::Flush => {
            group.bench_function(&bench_name, |b| {
                b.iter_custom(|iters| {
                    let mut total_elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let elapsed =
                            Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(128))))
                                .worker_count(Some(nz!(1)))
                                .scope(async |ctx| {
                                    run_1gb_iteration(
                                        ctx,
                                        BenchPhase::Flush,
                                        buffering_mode,
                                        sync_mode,
                                    )
                                    .await
                                })
                                .unwrap();
                        total_elapsed += elapsed;
                    }
                    total_elapsed
                })
            });
        }
    }
    group.finish();
}

fn benchmark_32_files_write(c: &mut Criterion) {
    if bench_phase() != BenchPhase::Total {
        return;
    }

    let mut group = c.benchmark_group("fs_throughput_32_files");

    // 1GB Total Size
    const FILE_COUNT: usize = 32;
    const WORKER_COUNT: NonZeroUsize = nz!(4);
    const TOTAL_SIZE: u64 = 1024 * 1024 * 1024;
    const FILE_SIZE: u64 = TOTAL_SIZE / FILE_COUNT as u64;
    const FILES_PER_WORKER: usize = FILE_COUNT / WORKER_COUNT.get();
    let buffering_mode = bench_buffering_mode();
    let sync_mode = bench_sync_mode();

    // Ensure accurate division
    assert_eq!(FILES_PER_WORKER * WORKER_COUNT.get(), FILE_COUNT);

    group.throughput(Throughput::Bytes(TOTAL_SIZE));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(120));

    let bench_name = format!("write_32_files_concurrent_{}", bench_case_name());
    group.bench_function(&bench_name, |b| {
        b.iter_custom(|iters| {
            let mut total_elapsed = Duration::ZERO;
            for _ in 0..iters {
                let elapsed = Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(nz!(128))))
                    .worker_count(Some(WORKER_COUNT))
                    .scope(async |ctx| {
                        let start = Instant::now();
                        let base_dir = bench_base_dir();
                        let pid = id();

                        scope!(ctx, async |s| {
                            let mut prepare_handles = Vec::with_capacity(WORKER_COUNT.get());
                            for worker_id in 0..WORKER_COUNT.get() {
                                let prepare_path_names: Vec<PathBuf> = (0..FILES_PER_WORKER)
                                    .map(|f_idx| {
                                        bench_file_path(
                                            &base_dir,
                                            format!("bench_32_{pid}_{worker_id}_{f_idx}.tmp"),
                                        )
                                    })
                                    .collect();

                                prepare_handles.push(s.spawn_boxed_to(
                                    worker_id,
                                    async move || {
                                        let mut files = Vec::with_capacity(FILES_PER_WORKER);
                                        for path in &prepare_path_names {
                                            if path.exists() {
                                                let _ = remove_file(path);
                                            }

                                            let file = open_and_fallocate(
                                                ctx,
                                                path,
                                                buffering_mode,
                                                FILE_SIZE,
                                            )
                                            .await;
                                            files.push(file);
                                        }

                                        let bytes = run_worker_iteration(
                                            ctx,
                                            files,
                                            FILE_SIZE,
                                            nz!(4 * 1024 * 1024),
                                            sync_mode,
                                        )
                                        .await;

                                        for path in prepare_path_names {
                                            let _ = remove_file(path);
                                        }

                                        Ok::<u64, std::io::Error>(bytes)
                                    },
                                ));
                            }

                            let mut total_bytes = 0u64;
                            for handle in prepare_handles {
                                let bytes = handle
                                    .await
                                    .expect("worker task failed")
                                    .expect("worker execution failed");
                                total_bytes += bytes;
                            }

                            assert_eq!(total_bytes, TOTAL_SIZE);
                        })
                        .await
                        .unwrap();

                        start.elapsed()
                    })
                    .unwrap();
                total_elapsed += elapsed;
            }
            total_elapsed
        })
    });
    group.finish();
}

criterion_group!(benches, benchmark_1gb_write, benchmark_32_files_write);
criterion_main!(benches);
