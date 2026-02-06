use criterion::{Criterion, criterion_group, criterion_main};
use std::hint::black_box;
use std::time::Duration;
use veloq_wheel::{Wheel, WheelConfig};

fn bench_wheel_advance(c: &mut Criterion) {
    let mut group = c.benchmark_group("wheel_advance");

    group.bench_function("advance_expiry", |b| {
        b.iter_batched(
            || {
                let mut config = WheelConfig::default();
                config.l0_tick_duration = Duration::from_millis(1);
                let mut wheel = Wheel::new(config);
                // Insert tasks that will expire sequentially
                for i in 0..1000 {
                    wheel.insert(i, Duration::from_millis(i as u64 + 1));
                }
                (wheel, Vec::new())
            },
            |(mut wheel, mut expired)| {
                // Advance 1ms at a time, 1000 times.
                for _ in 0..1000 {
                    wheel.advance(Duration::from_millis(1), &mut expired);
                    black_box(&expired);
                    expired.clear(); // Reuse buffer
                }
            },
            criterion::BatchSize::SmallInput,
        );
    });

    group.finish();
}

criterion_group!(benches, bench_wheel_advance);
criterion_main!(benches);
