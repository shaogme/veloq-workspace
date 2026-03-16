use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::path::Path;
use std::rc::Rc;
use std::time::{Duration, Instant};

use veloq_buf::{BufPool, nz};
use veloq_runtime::LocalExecutor;
use veloq_runtime::config::BlockingPoolConfig;
use veloq_runtime::fs::{BufferingMode, File};
use veloq_runtime::runtime::Runtime;
use veloq_runtime::runtime::blocking::init_blocking_pool;
use veloq_runtime::spawn_local;

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

fn bench_buffering_mode() -> BufferingMode {
    let raw = std::env::var("VELOQ_BENCH_BUFFERING").unwrap_or_else(|_| "directsync".to_string());
    match raw.to_ascii_lowercase().as_str() {
        "buffered" => BufferingMode::Buffered,
        "direct" => BufferingMode::Direct,
        "directsync" => BufferingMode::DirectSync,
        other => panic!("Unsupported VELOQ_BENCH_BUFFERING: {other}"),
    }
}

fn bench_sync_mode() -> BenchSyncMode {
    let raw = std::env::var("VELOQ_BENCH_SYNC").unwrap_or_else(|_| "sync_range".to_string());
    match raw.to_ascii_lowercase().as_str() {
        "none" => BenchSyncMode::None,
        "sync_range" => BenchSyncMode::SyncRange,
        "sync_all" => BenchSyncMode::SyncAll,
        "sync_data" => BenchSyncMode::SyncData,
        other => panic!("Unsupported VELOQ_BENCH_SYNC: {other}"),
    }
}

fn bench_phase() -> BenchPhase {
    let raw = std::env::var("VELOQ_BENCH_PHASE").unwrap_or_else(|_| "total".to_string());
    match raw.to_ascii_lowercase().as_str() {
        "total" => BenchPhase::Total,
        "write" => BenchPhase::Write,
        "flush" => BenchPhase::Flush,
        other => panic!("Unsupported VELOQ_BENCH_PHASE: {other}"),
    }
}

fn bench_case_name() -> String {
    let buffering =
        std::env::var("VELOQ_BENCH_BUFFERING").unwrap_or_else(|_| "directsync".to_string());
    let sync = std::env::var("VELOQ_BENCH_SYNC").unwrap_or_else(|_| "sync_range".to_string());
    let phase = std::env::var("VELOQ_BENCH_PHASE").unwrap_or_else(|_| "total".to_string());
    format!("{buffering}_{sync}_{phase}")
}

async fn apply_sync(file: &File, len: u64, mode: BenchSyncMode) {
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

fn create_local_executor() -> LocalExecutor {
    LocalExecutor::builder().build(move |registrar| {
        use veloq_buf::{PoolTopology, UniformSlot, heap::ThreadMemoryMultiplier};

        // 128x multiplier -> ~256MB
        let multiplier = ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(128) });
        let topology = UniformSlot::new(multiplier);

        let global_pool = topology
            .create_pool(1)
            .expect("Failed to create global pool");
        topology.build(&global_pool, 0, registrar)
    })
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

    let mut exec = create_local_executor();

    init_blocking_pool(BlockingPoolConfig::default());

    let pool = exec.pool();
    let buffering_mode = bench_buffering_mode();
    let sync_mode = bench_sync_mode();
    let phase = bench_phase();
    let bench_name = format!("write_1gb_concurrent_{}", bench_case_name());

    match phase {
        BenchPhase::Total => {
            group.bench_function(&bench_name, |b| {
                b.iter(|| {
                    let pool = pool.clone();
                    exec.block_on(async move {
                        const CHUNK_SIZE: NonZeroUsize = nz!(4 * 1024 * 1024);
                        let chunk_size = CHUNK_SIZE;
                        let base_dir =
                            std::env::var("VELOQ_BENCH_DIR").unwrap_or_else(|_| ".".to_string());
                        let file_path = Path::new(&base_dir).join("bench_1gb_test.tmp");

                        if file_path.exists() {
                            let _ = std::fs::remove_file(&file_path);
                        }

                        let file = File::options()
                            .write(true)
                            .create(true)
                            .truncate(true)
                            .buffering(buffering_mode)
                            .open(&file_path)
                            .await
                            .expect("Failed to create");

                        let file = Rc::new(file);
                        file.fallocate(0, TOTAL_SIZE)
                            .await
                            .expect("Fallocate failed");

                        let concurrency_limit = 32;
                        let mut tasks = VecDeque::new();
                        let mut offset: u64 = 0;

                        while offset < TOTAL_SIZE {
                            if tasks.len() < concurrency_limit
                                && let Some(buf) = pool.alloc(CHUNK_SIZE)
                            {
                                let remaining = TOTAL_SIZE - offset;
                                let write_len =
                                    std::cmp::min(remaining, chunk_size.get() as u64) as usize;

                                let file_clone = file.clone();
                                let current_offset = offset;

                                let fut =
                                    async move { file_clone.write_at(buf, current_offset).await };
                                tasks.push_back(spawn_local(fut));
                                offset += write_len as u64;
                                continue;
                            }

                            if let Some(handle) = tasks.pop_front() {
                                let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                                res.expect("Write failed");
                            } else {
                                panic!("Deadlock: No tasks to wait for but cannot allocate buffer");
                            }
                        }

                        while let Some(handle) = tasks.pop_front() {
                            let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                            res.expect("Write failed");
                        }

                        apply_sync(&file, TOTAL_SIZE, sync_mode).await;
                        drop(file);
                        let _ = std::fs::remove_file(file_path);
                    });
                })
            });
        }
        BenchPhase::Write => {
            group.bench_function(&bench_name, |b| {
                b.iter_custom(|iters| {
                    let mut total_elapsed = Duration::ZERO;
                    for _ in 0..iters {
                        let pool = pool.clone();
                        let elapsed = exec.block_on(async move {
                            const CHUNK_SIZE: NonZeroUsize = nz!(4 * 1024 * 1024);
                            let chunk_size = CHUNK_SIZE;
                            let base_dir = std::env::var("VELOQ_BENCH_DIR")
                                .unwrap_or_else(|_| ".".to_string());
                            let file_path = Path::new(&base_dir).join("bench_1gb_test.tmp");

                            if file_path.exists() {
                                let _ = std::fs::remove_file(&file_path);
                            }

                            let file = File::options()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .buffering(buffering_mode)
                                .open(&file_path)
                                .await
                                .expect("Failed to create");

                            let file = Rc::new(file);
                            file.fallocate(0, TOTAL_SIZE)
                                .await
                                .expect("Fallocate failed");

                            let start = Instant::now();
                            let concurrency_limit = 32;
                            let mut tasks = VecDeque::new();
                            let mut offset: u64 = 0;

                            while offset < TOTAL_SIZE {
                                if tasks.len() < concurrency_limit
                                    && let Some(buf) = pool.alloc(CHUNK_SIZE)
                                {
                                    let remaining = TOTAL_SIZE - offset;
                                    let write_len =
                                        std::cmp::min(remaining, chunk_size.get() as u64) as usize;

                                    let file_clone = file.clone();
                                    let current_offset = offset;

                                    let fut = async move {
                                        file_clone.write_at(buf, current_offset).await
                                    };
                                    tasks.push_back(spawn_local(fut));
                                    offset += write_len as u64;
                                    continue;
                                }

                                if let Some(handle) = tasks.pop_front() {
                                    let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                                    res.expect("Write failed");
                                } else {
                                    panic!(
                                        "Deadlock: No tasks to wait for but cannot allocate buffer"
                                    );
                                }
                            }

                            while let Some(handle) = tasks.pop_front() {
                                let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                                res.expect("Write failed");
                            }
                            let elapsed = start.elapsed();

                            drop(file);
                            let _ = std::fs::remove_file(file_path);
                            elapsed
                        });
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
                        let pool = pool.clone();
                        let elapsed = exec.block_on(async move {
                            const CHUNK_SIZE: NonZeroUsize = nz!(4 * 1024 * 1024);
                            let chunk_size = CHUNK_SIZE;
                            let base_dir = std::env::var("VELOQ_BENCH_DIR")
                                .unwrap_or_else(|_| ".".to_string());
                            let file_path = Path::new(&base_dir).join("bench_1gb_test.tmp");

                            if file_path.exists() {
                                let _ = std::fs::remove_file(&file_path);
                            }

                            let file = File::options()
                                .write(true)
                                .create(true)
                                .truncate(true)
                                .buffering(buffering_mode)
                                .open(&file_path)
                                .await
                                .expect("Failed to create");

                            let file = Rc::new(file);
                            file.fallocate(0, TOTAL_SIZE)
                                .await
                                .expect("Fallocate failed");

                            let concurrency_limit = 32;
                            let mut tasks = VecDeque::new();
                            let mut offset: u64 = 0;

                            while offset < TOTAL_SIZE {
                                if tasks.len() < concurrency_limit
                                    && let Some(buf) = pool.alloc(CHUNK_SIZE)
                                {
                                    let remaining = TOTAL_SIZE - offset;
                                    let write_len =
                                        std::cmp::min(remaining, chunk_size.get() as u64) as usize;

                                    let file_clone = file.clone();
                                    let current_offset = offset;

                                    let fut = async move {
                                        file_clone.write_at(buf, current_offset).await
                                    };
                                    tasks.push_back(spawn_local(fut));
                                    offset += write_len as u64;
                                    continue;
                                }

                                if let Some(handle) = tasks.pop_front() {
                                    let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                                    res.expect("Write failed");
                                } else {
                                    panic!(
                                        "Deadlock: No tasks to wait for but cannot allocate buffer"
                                    );
                                }
                            }

                            while let Some(handle) = tasks.pop_front() {
                                let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                                res.expect("Write failed");
                            }

                            let start = Instant::now();
                            apply_sync(&file, TOTAL_SIZE, sync_mode).await;
                            let elapsed = start.elapsed();

                            drop(file);
                            let _ = std::fs::remove_file(file_path);
                            elapsed
                        });
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
    const WORKER_COUNT: usize = 4;
    const TOTAL_SIZE: u64 = 1024 * 1024 * 1024;
    const FILE_SIZE: u64 = TOTAL_SIZE / FILE_COUNT as u64;
    const FILES_PER_WORKER: usize = FILE_COUNT / WORKER_COUNT;
    let buffering_mode = bench_buffering_mode();
    let sync_mode = bench_sync_mode();

    // Ensure accurate division
    assert_eq!(FILES_PER_WORKER * WORKER_COUNT, FILE_COUNT);

    group.throughput(Throughput::Bytes(TOTAL_SIZE));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(120));

    let bench_name = format!("write_32_files_concurrent_{}", bench_case_name());
    group.bench_function(&bench_name, |b| {
        b.iter(|| {
            let handle = std::thread::spawn(move || {
                // Initialize Runtime with 4 workers and BuddyPool
                // Re-initialized per iteration because block_on consumes the runtime.
                let runtime = Runtime::builder()
                    .config(veloq_runtime::config::Config::default().worker_threads(WORKER_COUNT))
                    .with_topology(veloq_buf::UniformSlot::new(
                        veloq_buf::heap::ThreadMemoryMultiplier(nz!(32)),
                    ))
                    .build()
                    .unwrap();

                // Block on runtime to keep 'b.iter' scope valid until all done
                runtime.block_on(async {
                    let (tx, mut rx) = veloq_runtime::sync::mpsc::unbounded();

                    for i in 0..WORKER_COUNT {
                        let tx = tx.clone();

                        // Spawn to specific worker
                        veloq_runtime::runtime::context::spawn_to(i, async move || {
                            // Get the pool bound to this worker (correctly registered)
                            let pool = veloq_runtime::runtime::context::current_pool()
                                .expect("Worker should have bound pool");

                            // Use a clone for inner loop if needed, though AnyBufPool is cheap to clone
                            // We will need to call .alloc() on it.

                            const CHUNK_SIZE: NonZeroUsize = nz!(4 * 1024 * 1024);
                            let chunk_size = CHUNK_SIZE;

                            let start_file_idx = i * FILES_PER_WORKER;
                            let end_file_idx = start_file_idx + FILES_PER_WORKER;

                            let mut files = Vec::with_capacity(FILES_PER_WORKER);
                            let mut file_paths = Vec::with_capacity(FILES_PER_WORKER);

                            let base_dir = std::env::var("VELOQ_BENCH_DIR").unwrap_or_else(|_| ".".to_string());

                            // 1. Open and Fallocate files
                            for f_idx in start_file_idx..end_file_idx {
                                let path = Path::new(&base_dir).join(format!("bench_32_{}.tmp", f_idx));

                                if path.exists() {
                                    let _ = std::fs::remove_file(&path);
                                }

                                let file = File::options()
                                    .write(true)
                                    .create(true)
                                    .truncate(true)
                                    .buffering(buffering_mode)
                                    .open(&path)
                                    .await
                                    .expect("Failed to create");
                                let file = Rc::new(file);

                                file.fallocate(0, FILE_SIZE)
                                    .await
                                    .expect("Fallocate failed");

                                files.push(file);
                                file_paths.push(path);
                            }

                            // 2. Concurrent Write Loop for this worker's files
                            let concurrency_limit = 8; // Maybe lower per worker? 32 total / 4 = 8.
                            let mut tasks = VecDeque::new();
                            let mut offsets = [0u64; FILES_PER_WORKER];
                            let mut current_local_idx = 0;

                            loop {
                                let all_submitted = offsets.iter().all(|&o| o >= FILE_SIZE);

                                if all_submitted && tasks.is_empty() {
                                    break;
                                }

                                if tasks.len() < concurrency_limit && !all_submitted {
                                    // Find next file that needs writing
                                    let mut found = None;
                                    for _ in 0..FILES_PER_WORKER {
                                        if offsets[current_local_idx] < FILE_SIZE {
                                            found = Some(current_local_idx);
                                            current_local_idx = (current_local_idx + 1) % FILES_PER_WORKER;
                                            break;
                                        }
                                        current_local_idx = (current_local_idx + 1) % FILES_PER_WORKER;
                                    }

                                    if let Some(idx) = found
                                        && let Some(buf) = pool.alloc(CHUNK_SIZE)
                                    {
                                        let remaining = FILE_SIZE - offsets[idx];
                                        let write_len = std::cmp::min(remaining, chunk_size.get() as u64) as usize;

                                        let file_clone = files[idx].clone();
                                        let current_offset = offsets[idx];

                                        let fut = async move { file_clone.write_at(buf, current_offset).await };

                                        tasks.push_back(spawn_local(fut));
                                        offsets[idx] += write_len as u64;
                                        continue;
                                    }
                                }

                                if let Some(handle) = tasks.pop_front() {
                                    let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                                    res.expect("Write failed");
                                } else if !all_submitted {
                                    panic!("Deadlock in worker {}: No tasks to wait for but cannot allocate buffer", i);
                                }
                            }

                            // 3. Sync and Close
                            for file in &files {
                                apply_sync(file, FILE_SIZE, sync_mode).await;
                            }

                            drop(files);
                            for path in file_paths {
                                let _ = std::fs::remove_file(path);
                            }

                            // Signal done
                            tx.send(()).unwrap();

                        });
                    }

                    // Wait for all workers to finish
                    for _ in 0..WORKER_COUNT {
                        rx.recv().await.unwrap();
                    }
                })
            });
            handle.join().unwrap();
        })
    });
    group.finish();
}

criterion_group!(benches, benchmark_1gb_write, benchmark_32_files_write);
criterion_main!(benches);
