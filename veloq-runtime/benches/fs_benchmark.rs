use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Duration;

use veloq_buf::{BufPool, nz};
use veloq_runtime::LocalExecutor;
use veloq_runtime::config::BlockingPoolConfig;
use veloq_runtime::fs::{BufferingMode, File};
use veloq_runtime::runtime::Runtime;
use veloq_runtime::runtime::blocking::init_blocking_pool;
use veloq_runtime::spawn_local;

fn create_local_executor() -> LocalExecutor {
    LocalExecutor::builder().build(move |registrar| {
        use veloq_buf::{ThreadMemoryMultiplier, UniformBlock};

        // 128x multiplier -> ~256MB
        let multiplier = ThreadMemoryMultiplier(unsafe { NonZeroUsize::new_unchecked(128) });
        let topology = UniformBlock::buddy(multiplier);

        let global_pool = topology
            .create_pool(1)
            .expect("Failed to create global pool");
        topology.build_for_worker(global_pool, 0, registrar)
    })
}

fn benchmark_1gb_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("fs_throughput");

    // 1GB Total Size
    const TOTAL_SIZE: u64 = 1 * 1024 * 1024 * 1024;

    // 设置吞吐量统计单位
    group.throughput(Throughput::Bytes(TOTAL_SIZE));
    // 1GB写入耗时较长，减少采样次数并增加单次超时时间
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(120));

    let mut exec = create_local_executor();

    init_blocking_pool(BlockingPoolConfig::default());

    let pool = exec.pool();

    group.bench_function("write_1gb_concurrent", |b| {
        b.iter(|| {
            let pool = pool.clone();
            // 复用 LocalExecutor 避免每次迭代创建 driver 的开销
            exec.block_on(async move {
                const CHUNK_SIZE: NonZeroUsize = nz!(4 * 1024 * 1024);
                let chunk_size = CHUNK_SIZE;
                let file_path = Path::new("bench_1gb_test.tmp");

                if file_path.exists() {
                    let _ = std::fs::remove_file(file_path);
                }

                // Use File::create which takes pool and context
                let file = File::options()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .buffering(BufferingMode::DirectSync)
                    .open(&file_path)
                    .await
                    .expect("Failed to create");

                let file = Rc::new(file);

                // Pre-allocate space to avoid metadata lock contention during extended writes
                file.fallocate(0, TOTAL_SIZE)
                    .await
                    .expect("Fallocate failed");

                // 限制并发度为 BufferPool 中该尺寸 Chunk 的最大可用数 (32)
                let concurrency_limit = 32;
                let mut tasks = VecDeque::new();
                let mut offset: u64 = 0;

                while offset < TOTAL_SIZE {
                    // 1. 尝试分配并在此窗口内提交任务
                    if tasks.len() < concurrency_limit {
                        // Use pool directly
                        if let Some(buf) = pool.alloc(CHUNK_SIZE) {
                            let remaining = TOTAL_SIZE - offset;
                            let write_len =
                                std::cmp::min(remaining, chunk_size.get() as u64) as usize;

                            let file_clone = file.clone();
                            let current_offset = offset;

                            let fut = async move { file_clone.write_at(buf, current_offset).await };

                            // Use cx.spawn_local
                            tasks.push_back(spawn_local(fut));
                            offset += write_len as u64;
                            continue;
                        }
                    }

                    // 2. 无法分配或达到并发限制，等待最早的任务完成以释放资源
                    if let Some(handle) = tasks.pop_front() {
                        let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                        res.expect("Write failed");
                    } else {
                        panic!("Deadlock: No tasks to wait for but cannot allocate buffer");
                    }
                }

                // 3. 等待剩余任务
                while let Some(handle) = tasks.pop_front() {
                    let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                    res.expect("Write failed");
                }

                // 使用 verify range 替代 sync_all
                // Optimize: Skip wait_before because we are the only writer after the loop finishes.
                file.sync_range(0, TOTAL_SIZE)
                    .wait_before(false)
                    .write(true)
                    .wait_after(true)
                    .await
                    .expect("Sync failed");

                // 清理
                drop(file);
                let _ = std::fs::remove_file(file_path);
            });
        })
    });
    group.finish();
}

fn benchmark_32_files_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("fs_throughput_32_files");

    // 1GB Total Size
    const FILE_COUNT: usize = 32;
    const WORKER_COUNT: usize = 4;
    const TOTAL_SIZE: u64 = 1 * 1024 * 1024 * 1024;
    const FILE_SIZE: u64 = TOTAL_SIZE / FILE_COUNT as u64;
    const FILES_PER_WORKER: usize = FILE_COUNT / WORKER_COUNT;

    // Ensure accurate division
    assert_eq!(FILES_PER_WORKER * WORKER_COUNT, FILE_COUNT);

    group.throughput(Throughput::Bytes(TOTAL_SIZE));
    group.sample_size(10);
    group.measurement_time(Duration::from_secs(120));

    group.bench_function("write_32_files_concurrent", |b| {
        b.iter(|| {
            let handle = std::thread::spawn(|| {
                // Initialize Runtime with 4 workers and BuddyPool
                // Re-initialized per iteration because block_on consumes the runtime.
                let runtime = Runtime::builder()
                    .config(veloq_runtime::config::Config::default().worker_threads(WORKER_COUNT))
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

                            // 1. Open and Fallocate files
                            for f_idx in start_file_idx..end_file_idx {
                                let path_str = format!("bench_32_{}.tmp", f_idx);
                                let path = PathBuf::from(path_str);

                                if path.exists() {
                                    let _ = std::fs::remove_file(&path);
                                }

                                let file = File::options()
                                    .write(true)
                                    .create(true)
                                    .truncate(true)
                                    .buffering(BufferingMode::DirectSync)
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
                            let mut offsets = vec![0u64; FILES_PER_WORKER];
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

                                    if let Some(idx) = found {
                                        if let Some(buf) = pool.alloc(CHUNK_SIZE) {
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
                                }

                                if let Some(handle) = tasks.pop_front() {
                                    let (res, _buf): (std::io::Result<usize>, _) = handle.await;
                                    res.expect("Write failed");
                                } else {
                                    if !all_submitted {
                                        panic!("Deadlock in worker {}: No tasks to wait for but cannot allocate buffer", i);
                                    }
                                }
                            }

                            // 3. Sync and Close
                            for file in &files {
                                file.sync_range(0, FILE_SIZE)
                                    .wait_before(false)
                                    .write(true)
                                    .wait_after(true)
                                    .await
                                    .expect("Sync failed");
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
