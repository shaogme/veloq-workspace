use core::{hash::Hasher, hint::black_box};
use rustc_hash::FxHasher;
use std::{collections::hash_map::DefaultHasher, time::Instant};
use veloq_hash::{VeloqFastHasher, VeloqHasher};

fn run_perf_bench<H, F>(name: &str, sizes: &[usize], mut build_hasher: F)
where
    H: Hasher,
    F: FnMut() -> H,
{
    println!("\n=== {} 性能测试 ===", name);
    for &size in sizes {
        let data = vec![0u8; size];

        // 预热
        let mut hasher = build_hasher();
        hasher.write(&data);
        let _ = black_box(hasher.finish());

        // 根据大小调整迭代次数
        let iterations = match size {
            s if s <= 8 => 10_000_000,
            s if s <= 64 => 5_000_000,
            s if s <= 1024 => 1_000_000,
            s if s <= 65536 => 20_000,
            _ => 1_000,
        };

        let start = Instant::now();
        for _ in 0..iterations {
            let mut hasher = build_hasher();
            hasher.write(&data);
            let _ = black_box(hasher.finish());
        }
        let duration = start.elapsed();
        let total_ns = duration.as_nanos() as f64;
        let ns_per_op = total_ns / (iterations as f64);
        let total_bytes = (size * iterations) as f64;
        let gb_per_sec = (total_bytes / (1024.0 * 1024.0 * 1024.0)) / duration.as_secs_f64();

        println!(
            "大小: {:>7} B | {:>10.2} ns/op | {:>10.4} GB/s",
            size, ns_per_op, gb_per_sec
        );
    }
}

fn run_collision_test<H, F, K>(name: &str, keys: &[K], mut build_hasher: F)
where
    H: Hasher,
    F: FnMut() -> H,
    K: AsRef<[u8]>,
{
    let count = keys.len();
    let mut hashes = Vec::with_capacity(count);

    for key in keys {
        let mut hasher = build_hasher();
        hasher.write(key.as_ref());
        hashes.push(hasher.finish());
    }

    // 1. 完全 64 位碰撞检测
    let mut sorted_hashes = hashes.clone();
    sorted_hashes.sort_unstable();
    let mut full_collisions = 0;
    for i in 1..sorted_hashes.len() {
        if sorted_hashes[i] == sorted_hashes[i - 1] {
            full_collisions += 1;
        }
    }

    // 2. 16位桶分布与均匀性分析 (桶大小 N = 65536)
    const BUCKETS: usize = 65536;
    let mut bucket_counts = vec![0u32; BUCKETS];
    for &h in &hashes {
        let bucket = (h as usize) & (BUCKETS - 1);
        bucket_counts[bucket] += 1;
    }

    let mut empty_buckets = 0;
    let mut max_bucket_size = 0;
    for &cnt in &bucket_counts {
        if cnt == 0 {
            empty_buckets += 1;
        }
        if cnt > max_bucket_size {
            max_bucket_size = cnt;
        }
    }

    // 计算标准差和卡方检验值
    let expected = (count as f64) / (BUCKETS as f64);
    let mut variance_sum = 0.0;
    let mut chi_square = 0.0;
    for &cnt in &bucket_counts {
        let diff = (cnt as f64) - expected;
        variance_sum += diff * diff;
        chi_square += (diff * diff) / expected;
    }
    let std_dev = (variance_sum / (BUCKETS as f64)).sqrt();

    println!("\n=== {} 碰撞与分布测试 ===", name);
    println!("完全 64 位冲突数: {}", full_collisions);
    println!("16位低位映射 (共 {} 桶):", BUCKETS);
    println!(
        "  空桶数: {:>6} ({:.2}%)",
        empty_buckets,
        (empty_buckets as f64 * 100.0) / (BUCKETS as f64)
    );
    println!("  最大桶大小: {}", max_bucket_size);
    println!("  标准差 (StdDev): {:.4}", std_dev);
    println!(
        "  卡方检验值 (Chi-Square): {:.2} (理论期望值: {})",
        chi_square, BUCKETS
    );
}

fn main() {
    let sizes = [2, 4, 8, 12, 16, 24, 32, 64, 1024, 65536, 1048576];

    println!("==================================================");
    println!("               性能对比测试 (Throughput)          ");
    println!("==================================================");
    run_perf_bench("VeloqHasher (本库实现)", &sizes, || {
        VeloqHasher::new(42)
    });
    run_perf_bench(
        "VeloqFastHasher (极速非安全版)",
        &sizes,
        VeloqFastHasher::default,
    );
    run_perf_bench("FxHasher (rustc-hash)", &sizes, FxHasher::default);
    run_perf_bench("DefaultHasher (SipHash)", &sizes, DefaultHasher::new);

    println!("\n==================================================");
    println!("               碰撞度与分布质量测试              ");
    println!("==================================================");

    // 1. 连续序列 (如自增ID)
    println!("\n>>> 模式 1: 连续递增整数序列 (100,000个)");
    let seq_keys: Vec<Vec<u8>> = (0..100_000u64).map(|i| i.to_ne_bytes().to_vec()).collect();
    run_collision_test("VeloqHasher (本库实现)", &seq_keys, || {
        VeloqHasher::new(42)
    });
    run_collision_test(
        "VeloqFastHasher (极速非安全版)",
        &seq_keys,
        VeloqFastHasher::default,
    );
    run_collision_test("FxHasher (rustc-hash)", &seq_keys, FxHasher::default);
    run_collision_test("DefaultHasher (SipHash)", &seq_keys, DefaultHasher::new);

    // 2. 局部微小差异 (雪崩测试/结构化变化)
    println!("\n>>> 模式 2: 前缀相同且微小差异的文本序列 (100,000个)");
    let text_keys: Vec<Vec<u8>> = (0..100_000)
        .map(|i| format!("user_id_prefix_constant_string_{:010}", i).into_bytes())
        .collect();
    run_collision_test("VeloqHasher (本库实现)", &text_keys, || {
        VeloqHasher::new(42)
    });
    run_collision_test(
        "VeloqFastHasher (极速非安全版)",
        &text_keys,
        VeloqFastHasher::default,
    );
    run_collision_test("FxHasher (rustc-hash)", &text_keys, FxHasher::default);
    run_collision_test("DefaultHasher (SipHash)", &text_keys, DefaultHasher::new);
}
