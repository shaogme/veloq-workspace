use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use veloq_local::spsc;

fn bench_spsc_bounded(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("spsc_bounded");

    for size in [16, 1024, 65536] {
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(BenchmarkId::from_parameter(size), &size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let (tx, rx) = spsc::bounded(size);

                let rx_fut = async move {
                    let mut count = 0;
                    while rx.recv().await.is_some() {
                        count += 1;
                    }
                    count
                };

                let tx_fut = async move {
                    for i in 0..size {
                        tx.send(i).await.unwrap();
                    }
                    drop(tx);
                };

                let (_, count) = tokio::join!(tx_fut, rx_fut);
                assert_eq!(count, size);
            });
        });
    }
    group.finish();
}

fn bench_spsc_unbounded(c: &mut Criterion) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();

    let mut group = c.benchmark_group("spsc_unbounded");

    // Test for a fixed number of elements sent through unbounded channel
    let count = 10000;
    group.throughput(Throughput::Elements(count as u64));

    group.bench_function("send_recv_10k", |b| {
        b.to_async(&rt).iter(|| async {
            let (tx, rx) = spsc::unbounded();

            let rx_fut = async move {
                let mut c = 0;
                while rx.recv().await.is_some() {
                    c += 1;
                }
                c
            };

            let tx_fut = async move {
                for i in 0..count {
                    tx.send(i).await.unwrap();
                }
                drop(tx);
            };

            let (_, c) = tokio::join!(tx_fut, rx_fut);
            assert_eq!(c, count);
        });
    });
    group.finish();
}

criterion_group!(benches, bench_spsc_bounded, bench_spsc_unbounded);
criterion_main!(benches);
