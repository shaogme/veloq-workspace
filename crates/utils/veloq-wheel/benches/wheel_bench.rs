use criterion::{Criterion, criterion_group, criterion_main};
use veloq_std::hint::black_box;
use veloq_std::time::Duration;
use veloq_wheel::{Expired, Wheel, WheelConfig};

fn bench_wheel_advance(c: &mut Criterion) {
    let mut group = c.benchmark_group("wheel_advance");

    group.bench_function("advance_expiry", |b| {
        b.iter_batched(
            || {
                let config = WheelConfig::builder()
                    .base_tick(Duration::from_millis(1))
                    .level_slots(512)
                    .level_slots(64)
                    .level_slots(64)
                    .build()
                    .expect("benchmark wheel configuration");
                let mut wheel = Wheel::new(config);
                // Insert tasks that will expire sequentially
                for i in 0..1000 {
                    wheel
                        .insert(i, Duration::from_millis(i as u64 + 1))
                        .expect("benchmark timer insertion");
                }
                (wheel, Vec::<Expired<i32>>::new())
            },
            |(mut wheel, mut expired)| {
                // Advance 1ms at a time, 1000 times.
                for _ in 0..1000 {
                    wheel
                        .advance_by(Duration::from_millis(1), &mut expired)
                        .expect("benchmark advance");
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
