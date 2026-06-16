use clap::{Parser, ValueEnum};
use rand::prelude::*;
use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use veloq::fs::{BufferingMode, File, OpenOptions};
use veloq::io::buffer::FixedBuf;
use veloq::runtime::Runtime;
use veloq::runtime::context::RuntimeContext;
use veloq::sync::mpsc;
use veloq_buf::{UniformSlot, heap::ThreadMemoryMultiplier, nz};

#[derive(Clone, Copy, ValueEnum, Debug)]
enum WriteMode {
    Seq,
    Rand,
}

#[derive(Clone, Copy, ValueEnum, Debug)]
enum IoBuffering {
    Buffered,
    Direct,
    DirectSync,
}

impl IoBuffering {
    fn into_runtime_mode(self) -> BufferingMode {
        match self {
            IoBuffering::Buffered => BufferingMode::Buffered,
            IoBuffering::Direct => BufferingMode::Direct,
            IoBuffering::DirectSync => BufferingMode::DirectSync,
        }
    }
}

#[derive(Clone, Copy, ValueEnum, Debug)]
enum SyncMode {
    None,
    SyncRange,
    SyncAll,
    SyncData,
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
    /// Write order for file offsets (sequential scan or shuffled order)
    #[arg(long, value_enum)]
    mode: WriteMode,

    /// Number of runtime worker threads and benchmarked files
    #[arg(long, default_value_t = 1)]
    threads: usize,

    /// Maximum in-flight write tasks per worker
    #[arg(long, default_value_t = 32)]
    qdepth: usize,

    /// Minimum wall-clock duration in seconds for each worker
    #[arg(long, default_value_t = 10)]
    duration: u64,

    /// Minimum benchmark iterations per worker before it may stop
    #[arg(long, default_value_t = 3)]
    iterations: usize,

    /// I/O block size as a ValueEnum token such as k4, m1, or m16
    #[arg(long, value_enum, default_value_t = BlockSize::M1)]
    block_size: BlockSize,

    /// File buffering mode: buffered, direct, or direct-sync
    #[arg(long, value_enum, default_value_t = IoBuffering::Direct)]
    buffering: IoBuffering,

    /// Post-write durability sync: none, sync-range, sync-all, or sync-data
    #[arg(long, value_enum, default_value_t = SyncMode::SyncAll)]
    sync: SyncMode,
}

// 1GB data per thread for benchmarking
const FILE_SIZE_PER_THREAD: u64 = 1024 * 1024 * 1024;

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
async fn prepare_files_for_thread<'a, 'ctx>(
    ctx: RuntimeContext<'a, 'ctx>,
    file_size: u64,
    t_idx: usize,
    buffering_mode: BufferingMode,
) {
    let path = get_file_path(t_idx);
    if path.exists() {
        let _ = std::fs::remove_file(&path);
    }

    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .buffering(buffering_mode)
        .open(ctx, &path)
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

async fn apply_sync<'a, 'ctx>(file: &File<'a, 'ctx>, mode: SyncMode, bytes: u64) {
    match mode {
        SyncMode::None => {}
        SyncMode::SyncRange => {
            file.sync_range(0, bytes)
                .wait_before(false)
                .write(true)
                .wait_after(true)
                .await
                .expect("sync_range failed");
        }
        SyncMode::SyncAll => {
            file.sync_all().await.expect("sync_all failed");
        }
        SyncMode::SyncData => {
            file.sync_data().await.expect("sync_data failed");
        }
    }
}

async fn run_iteration_measured<'a, 'ctx>(
    ctx: RuntimeContext<'a, 'ctx>,
    qdepth: usize,
    file: &File<'a, 'ctx>,
    ops: &[WriteOp],
    block_size: NonZeroUsize,
    sync_mode: SyncMode,
    available_buffers: &mut Vec<FixedBuf>,
) -> IterationResult {
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
    let state = mpsc::unbounded();
    let (tx, mut rx) = state.split();

    ctx.scope(async |s| {
        let mut in_flight = 0usize;
        loop {
            // 1. Submit tasks up to qdepth if we have buffers and ops
            while in_flight < qdepth && current_op_idx < total_ops {
                let buf = if let Some(b) = available_buffers.pop() {
                    b
                } else if let Ok(b) = ctx.try_alloc(block_size) {
                    // Fallback if initial set wasn't enough (shouldn't happen if properly sized)
                    b
                } else {
                    // No buffers available, stop submitting and wait for completions
                    break;
                };

                let op = ops[current_op_idx];
                let tx = tx.clone();

                // Spawn task and report completion through the channel.
                let fut = async move {
                    let res = file.write_at(buf, op.offset).await;
                    let _ = tx.send(res);
                };
                s.spawn_boxed(fut);

                current_op_idx += 1;
                in_flight += 1;
            }

            // 2. Check for completion or exit
            if in_flight == 0 {
                if current_op_idx >= total_ops {
                    break; // DONE
                }
                // Deadlock check: We have ops left, but no in-flight tasks and no buffers?
                // This means we failed to alloc and have no tasks to return buffers.
                panic!("Stall: No buffers available and no pending tasks.");
            }

            // 3. Wait for ONE task
            let result = rx.recv().await.expect("completion channel closed");
            let (n, buf) = result.expect("Write op failed");
            written_bytes += n as u64;

            // Recycle buffer
            available_buffers.push(buf);
            in_flight -= 1;
        }
    })
    .await
    .unwrap();

    apply_sync(file, sync_mode, written_bytes).await;

    let duration = start_time.elapsed();

    IterationResult {
        bytes: written_bytes,
        duration,
    }
}

async fn run_worker<'a, 'ctx>(
    ctx: RuntimeContext<'a, 'ctx>,
    qdepth: usize,
    min_duration: Duration,
    min_iters: usize,
    buffering_mode: BufferingMode,
    sync_mode: SyncMode,
    config: ThreadConfig,
) -> std::io::Result<Vec<IterationResult>> {
    // 1. Open File (Persistent handle for benchmarking)
    let file = OpenOptions::new()
        .write(true)
        .create(false) // already created
        .buffering(buffering_mode)
        .open(ctx, &config.file_path)
        .await
        .expect("Failed to open file in worker");

    // 2. Pre-allocate Buffers
    // We allocate QDEPTH buffers once and reuse them across ALL iterations.
    // This mimics static buffer pools in standard benchmarks.
    let mut reuse_buffers = Vec::with_capacity(qdepth);
    for _ in 0..qdepth {
        if let Ok(buf) = ctx.try_alloc(config.block_size) {
            reuse_buffers.push(buf);
        } else {
            panic!("Failed to allocate initial buffers for queue depth coverage");
        }
    }

    let mut results = Vec::new();
    let start_total = Instant::now();
    let mut iter_count = 0;

    // 3. Benchmark Loop
    loop {
        if iter_count >= min_iters && start_total.elapsed() >= min_duration {
            break;
        }

        let res = run_iteration_measured(
            ctx, // Pass it down
            qdepth,
            &file,
            &config.ops,
            config.block_size,
            sync_mode,
            &mut reuse_buffers,
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
    println!("Buffering: {:?}, Sync: {:?}", args.buffering, args.sync);
    println!("Pre-allocating files...");
    let buffering_mode = args.buffering.into_runtime_mode();
    let sync_mode = args.sync;

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
    let worker_count = NonZeroUsize::new(args.threads);
    let buffer_multiplier_bytes = args
        .qdepth
        .saturating_mul(block_size_bytes.get())
        .saturating_mul(2);
    let multiplier_units =
        (buffer_multiplier_bytes.saturating_add(4 * 1024 * 1024 - 1)) / (4 * 1024 * 1024);
    let multiplier =
        NonZeroUsize::new(multiplier_units.max(1)).expect("multiplier must be non-zero");

    let runtime = Runtime::builder(UniformSlot::new(ThreadMemoryMultiplier(multiplier)))
        .worker_count(worker_count)
        .build()
        .expect("Failed to build Runtime");

    // Execute
    runtime
        .block_on(async |ctx| {
            ctx.scope(async |s| {
                // 1. Preparation Phase (Create & Fallocate)
                println!(
                    "Initializing disk files (creating & fallocating)... This may take a while."
                );
                let mut prepare_handles = Vec::with_capacity(args.threads);
                for t_idx in 0..args.threads {
                    let prepare_buffering = buffering_mode;
                    prepare_handles.push(s.spawn_boxed_to(t_idx, async move || {
                        prepare_files_for_thread(
                            ctx,
                            FILE_SIZE_PER_THREAD,
                            t_idx,
                            prepare_buffering,
                        )
                        .await;
                    }));
                }
                for handle in prepare_handles {
                    handle.await.expect("Preparation task failed");
                }
                println!("Files prepared. Starting Benchmark Measurement...");

                // 2. Measurement Phase
                let duration_limit = Duration::from_secs(args.duration);
                let min_iters = args.iterations;
                let mut worker_handles = Vec::with_capacity(args.threads);

                for config in configs {
                    let qdepth = args.qdepth;
                    let worker_buffering = buffering_mode;
                    let worker_sync = sync_mode;
                    let t_idx = config.thread_index;

                    worker_handles.push(s.spawn_boxed_to(t_idx, async move || {
                        run_worker(
                            ctx,
                            qdepth,
                            duration_limit,
                            min_iters,
                            worker_buffering,
                            worker_sync,
                            config,
                        )
                        .await
                    }));
                }

                // 3. Aggregate
                let mut all_throughputs = Vec::new();
                let mut total_bytes_all = 0;
                let bench_start = Instant::now(); // Approx wall time for throughput calc if we want system-wide

                for handle in worker_handles {
                    let worker_results = handle
                        .await
                        .expect("Worker task failed")
                        .expect("Worker execution failed");

                    // Calculate thread average
                    let mut thread_bytes = 0;

                    for r in worker_results {
                        thread_bytes += r.bytes;

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
                    let avg_raw: f64 =
                        all_throughputs.iter().sum::<f64>() / all_throughputs.len() as f64;
                    println!("Average Iteration Throughput: {:.2} MB/s", avg_raw);

                    let filtered = filter_outliers(all_throughputs);
                    if !filtered.is_empty() {
                        let avg_filt: f64 = filtered.iter().sum::<f64>() / filtered.len() as f64;
                        println!("Filtered Avg Throughput (IQR): {:.2} MB/s", avg_filt);
                    }
                } else {
                    println!("No data collected.");
                }
            })
            .await
            .unwrap();
        })
        .unwrap();

    // 4. Cleanup Phase
    println!("Cleaning up files...");
    cleanup_files(args.threads);
    println!("Done.");
}
