use clap::{Parser, ValueEnum};
use rand::prelude::*;
use std::collections::VecDeque;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::rc::Rc;
use std::time::{Duration, Instant};
use veloq_runtime::fs::{BufferingMode, File, OpenOptions};
use veloq_runtime::io::buffer::{BufPool, FixedBuf};
use veloq_runtime::runtime::Runtime;
use veloq_runtime::spawn_local;
use veloq_runtime::sync::mpsc;
use veloq_runtime::LocalJoinHandle;

/// 创建 NonZeroUsize 的宏
/// - 输入 0：编译失败
/// - 输入非 0 字面量/常量：编译通过，且无运行时开销
macro_rules! nz {
    ($value:expr) => {{
        // 1. 利用匿名常量强制进行编译时检查
        // 如果 $value 为 0，assert! 会 panic，导致编译中断
        const _: () = assert!($value != 0, "nz! macro: Value cannot be zero!");

        // 2. 如果上面通过了，说明 $value 肯定不为 0
        // 使用 unsafe 块调用 new_unchecked
        unsafe { NonZeroUsize::new_unchecked($value) }
    }};
}

#[derive(Clone, Copy, ValueEnum, Debug)]
enum WriteMode {
    Seq,
    Rand,
}

#[derive(Clone, Copy, ValueEnum, Debug)]
enum BlockSize {
    K4,
    K8,
    K16,
    K32,
    K64,
    K128,
    K256,
    K512,
    M1,
    M2,
    M4,
    M8,
    M16,
}

impl BlockSize {
    fn as_bytes(&self) -> NonZeroUsize {
        match self {
            BlockSize::K4 => nz!(4 * 1024),
            BlockSize::K8 => nz!(8 * 1024),
            BlockSize::K16 => nz!(16 * 1024),
            BlockSize::K32 => nz!(32 * 1024),
            BlockSize::K64 => nz!(64 * 1024),
            BlockSize::K128 => nz!(128 * 1024),
            BlockSize::K256 => nz!(256 * 1024),
            BlockSize::K512 => nz!(512 * 1024),
            BlockSize::M1 => nz!(1024 * 1024),
            BlockSize::M2 => nz!(2 * 1024 * 1024),
            BlockSize::M4 => nz!(4 * 1024 * 1024),
            BlockSize::M8 => nz!(8 * 1024 * 1024),
            BlockSize::M16 => nz!(16 * 1024 * 1024),
        }
    }
}

impl std::fmt::Display for BlockSize {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}",
            match self {
                BlockSize::K4 => "4KB",
                BlockSize::K8 => "8KB",
                BlockSize::K16 => "16KB",
                BlockSize::K32 => "32KB",
                BlockSize::K64 => "64KB",
                BlockSize::K128 => "128KB",
                BlockSize::K256 => "256KB",
                BlockSize::K512 => "512KB",
                BlockSize::M1 => "1MB",
                BlockSize::M2 => "2MB",
                BlockSize::M4 => "4MB",
                BlockSize::M8 => "8MB",
                BlockSize::M16 => "16MB",
            }
        )
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Write mode: sequential or random
    #[arg(long, value_enum)]
    mode: WriteMode,

    /// Number of worker threads
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Queue depth (concurrent tasks per thread)
    #[arg(long, default_value_t = 32)]
    qdepth: usize,

    /// Duration in seconds to run the benchmark
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Minimum number of iterations
    #[arg(long, default_value_t = 3)]
    iterations: usize,

    /// Block size (chunk size) for I/O operations
    #[arg(long, value_enum, default_value_t = BlockSize::M1)]
    block_size: BlockSize,
}

// 1GB data per thread for benchmarking
const FILE_SIZE_PER_THREAD: u64 = 1 * 1024 * 1024 * 1024;

struct IterationResult {
    bytes: u64,
    duration: Duration,
}

/// Single write operation definition
#[derive(Clone, Debug, Copy)]
struct WriteOp {
    offset: u64,
}

/// Workload configuration for a thread
struct ThreadConfig {
    thread_index: usize,
    file_path: PathBuf,
    ops: Vec<WriteOp>,
    block_size: NonZeroUsize,
}

/// Helper to generate filenames
fn get_file_path(t_idx: usize) -> PathBuf {
    PathBuf::from(format!("bench_test_{}.data", t_idx))
}

/// Prepare files Phase: Create and fallocate files
/// This runs effectively in parallel per thread, but outside the measurement loop.
async fn prepare_files_for_thread(file_size: u64, t_idx: usize) {
    let path = get_file_path(t_idx);
    if path.exists() {
        let _ = std::fs::remove_file(&path);
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .buffering(BufferingMode::DirectSync)
        .open_local(&path)
        .await
        .expect("Failed to create file during preparation");

    // Fallocate is slow, so we do it here.
    file.fallocate(0, file_size)
        .await
        .expect("Fallocate failed");

    // We close the file after preparation
}

/// Cleanup Phase
fn cleanup_files(threads: usize) {
    for t_idx in 0..threads {
        let path = get_file_path(t_idx);
        if path.exists() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

async fn run_iteration_measured(
    qdepth: usize,
    file: Rc<File>,
    ops: &[WriteOp],
    block_size: NonZeroUsize,
    available_buffers: &mut Vec<FixedBuf>,
    pending_tasks: &mut VecDeque<LocalJoinHandle<(std::io::Result<usize>, FixedBuf)>>,
) -> IterationResult {
    let pool =
        veloq_runtime::runtime::context::current_pool().expect("Worker should have bound pool");

    // We need ownership of buffers to submit them.
    // We will collect them back as tasks finish.

    // Safety check
    if ops.is_empty() {
        return IterationResult {
            bytes: 0,
            duration: Duration::ZERO,
        };
    }

    let start_time = Instant::now();

    let total_ops = ops.len();
    let mut current_op_idx = 0;
    let mut written_bytes = 0u64;

    loop {
        // 1. Submit tasks up to qdepth if we have buffers and ops
        while pending_tasks.len() < qdepth && current_op_idx < total_ops {
            let buf = if let Some(b) = available_buffers.pop() {
                b
            } else if let Some(b) = pool.alloc(block_size) {
                // Fallback if initial set wasn't enough (shouldn't happen if properly sized)
                b
            } else {
                // No buffers available, stop submitting and wait for completions
                break;
            };

            let op = ops[current_op_idx];
            let file_clone = file.clone();

            // Spawn task
            let fut = async move { file_clone.write_at(buf, op.offset).await };
            pending_tasks.push_back(spawn_local(fut));

            current_op_idx += 1;
        }

        // 2. Check for completion or exit
        if pending_tasks.is_empty() {
            if current_op_idx >= total_ops {
                break; // DONE
            } else {
                // Deadlock check: We have ops left, but no pending tasks and no buffers?
                // This means we failed to alloc and have no tasks to return buffers.
                panic!("Stall: No buffers available and no pending tasks.");
            }
        }

        // 3. Wait for ONE task
        let handle = pending_tasks.pop_front().unwrap();
        let (res, buf) = handle.await;

        match res {
            Ok(n) => written_bytes += n as u64,
            Err(e) => panic!("IO Error at index {}: {}", current_op_idx, e),
        }

        // Recycle buffer
        available_buffers.push(buf);
    }

    let duration = start_time.elapsed();

    IterationResult {
        bytes: written_bytes,
        duration,
    }
}

async fn run_worker(
    qdepth: usize,
    min_duration: Duration,
    min_iters: usize,
    config: ThreadConfig,
) -> std::io::Result<Vec<IterationResult>> {
    let pool = veloq_runtime::runtime::context::current_pool().expect("No pool");

    // 1. Open File (Persistent handle for benchmarking)
    let file = OpenOptions::new()
        .write(true)
        .create(false) // already created
        .buffering(BufferingMode::DirectSync)
        .open(&config.file_path)
        .await
        .expect("Failed to open file in worker");
    let file = Rc::new(file);

    // 2. Pre-allocate Buffers
    // We allocate QDEPTH buffers once and reuse them across ALL iterations.
    // This mimics static buffer pools in standard benchmarks.
    let mut reuse_buffers = Vec::with_capacity(qdepth);
    for _ in 0..qdepth {
        if let Some(buf) = pool.alloc(config.block_size) {
            reuse_buffers.push(buf);
        } else {
            panic!("Failed to allocate initial buffers for queue depth coverage");
        }
    }

    // Pre-allocate pending tasks queue
    let mut pending_tasks = VecDeque::with_capacity(qdepth);

    let mut results = Vec::new();
    let start_total = Instant::now();
    let mut iter_count = 0;

    // 3. Benchmark Loop
    loop {
        if iter_count >= min_iters && start_total.elapsed() >= min_duration {
            break;
        }

        let res = run_iteration_measured(
            qdepth,
            file.clone(),
            &config.ops,
            config.block_size,
            &mut reuse_buffers,
            &mut pending_tasks,
        )
        .await;

        results.push(res);
        iter_count += 1;
    }

    Ok(results)
}

fn filter_outliers(mut data: Vec<f64>) -> Vec<f64> {
    if data.len() < 4 {
        return data;
    }
    data.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let q1 = data[data.len() / 4];
    let q3 = data[data.len() * 3 / 4];
    let iqr = q3 - q1;
    let lower_bound = q1 - 1.5 * iqr;
    let upper_bound = q3 + 1.5 * iqr;
    data.into_iter()
        .filter(|&x| x >= lower_bound && x <= upper_bound)
        .collect()
}

fn main() {
    let args = Args::parse();
    let block_size_bytes = args.block_size.as_bytes();

    println!("Starting Disk Benchmark");
    println!(
        "Mode: {:?}, Threads: {}, QD: {}",
        args.mode, args.threads, args.qdepth
    );
    println!(
        "Block Size: {} ({} Bytes)",
        args.block_size, block_size_bytes
    );
    println!("Pre-allocating files...");

    // Setup filenames and configs
    let file_size_per_file = FILE_SIZE_PER_THREAD;

    let mut configs = Vec::with_capacity(args.threads);
    for t_idx in 0..args.threads {
        let file_path = get_file_path(t_idx);

        // Generate Ops
        let num_blocks = file_size_per_file / block_size_bytes.get() as u64;
        let mut ops: Vec<WriteOp> = (0..num_blocks)
            .map(|i| WriteOp {
                offset: i * block_size_bytes.get() as u64,
            })
            .collect();

        if matches!(args.mode, WriteMode::Rand) {
            let mut rng = rand::rng();
            ops.shuffle(&mut rng);
        }

        configs.push(ThreadConfig {
            thread_index: t_idx,
            file_path,
            ops,
            block_size: block_size_bytes,
        });
    }

    // Initialize Runtime
    let runtime = Runtime::builder()
        .config(veloq_runtime::config::Config::default().worker_threads(args.threads))
        .build()
        .expect("Failed to build Runtime");

    // Execute
    runtime.block_on(async {
        // 1. Preparation Phase (Create & Fallocate)
        println!("Initializing disk files (creating & fallocating)... This may take a while.");
        let mut handles = Vec::new();
        for t_idx in 0..args.threads {
            // Spawn to workers to do it in parallel
            handles.push(spawn_local(async move {
                prepare_files_for_thread(FILE_SIZE_PER_THREAD, t_idx).await;
            }));
        }
        for h in handles {
            h.await;
        }
        println!("Files prepared. Starting Benchmark Measurement...");

        // 2. Measurement Phase
        let (tx, mut rx) = mpsc::unbounded();
        let duration_limit = Duration::from_secs(args.duration);
        let min_iters = args.iterations;

        for config in configs {
            let tx = tx.clone();
            let qdepth = args.qdepth;
            let t_idx = config.thread_index; // needed for placement

            veloq_runtime::runtime::context::spawn_to(t_idx, async move || {
                let res = run_worker(qdepth, duration_limit, min_iters, config)
                    .await
                    .expect("Worker Failed");
                tx.send(res).unwrap();
            });
        }
        drop(tx);

        // 3. Aggregate
        let mut all_throughputs = Vec::new();
        let mut total_bytes_all = 0;
        let bench_start = Instant::now(); // Approx wall time for throughput calc if we want system-wide

        for _ in 0..args.threads {
            let worker_results = rx.recv().await.expect("Failed to receive");

            // Calculate thread average
            let mut thread_bytes = 0;
            let mut thread_time = Duration::ZERO;

            for r in worker_results {
                thread_bytes += r.bytes;
                thread_time += r.duration;

                let secs = r.duration.as_secs_f64();
                if secs > 0.0 {
                    let mb = r.bytes as f64 / 1024.0 / 1024.0;
                    all_throughputs.push(mb / secs);
                }
            }
            total_bytes_all += thread_bytes;
        }

        let elapsed = bench_start.elapsed();
        let total_mb = total_bytes_all as f64 / 1024.0 / 1024.0;
        let system_throughput = if elapsed.as_secs_f64() > 0.0 {
            total_mb / elapsed.as_secs_f64()
        } else {
            0.0
        };

        println!("--------------------------------------------------");
        println!("Benchmark Completed.");
        println!("Total Data Measured: {:.2} MB", total_mb);
        println!("Wall Time (Approx):  {:.2} s", elapsed.as_secs_f64());
        println!("System Throughput:   {:.2} MB/s", system_throughput);
        println!("--------------------------------------------------");

        if !all_throughputs.is_empty() {
            let avg_raw: f64 = all_throughputs.iter().sum::<f64>() / all_throughputs.len() as f64;
            println!("Average Iteration Throughput: {:.2} MB/s", avg_raw);

            let filtered = filter_outliers(all_throughputs);
            if !filtered.is_empty() {
                let avg_filt: f64 = filtered.iter().sum::<f64>() / filtered.len() as f64;
                println!("Filtered Avg Throughput (IQR): {:.2} MB/s", avg_filt);
            }
        } else {
            println!("No data collected.");
        }
    });

    // 4. Cleanup Phase
    println!("Cleaning up files...");
    cleanup_files(args.threads);
    println!("Done.");
}
