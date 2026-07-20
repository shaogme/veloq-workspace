use criterion::{Criterion, criterion_group, criterion_main};
use veloq_std::task::Waker;
use veloq_waker::{AtomicWaker, MwsrWaker};

fn bench_register(c: &mut Criterion) {
    let waker = Waker::noop();

    c.bench_function("atomic_waker_register", |b| {
        let aw = AtomicWaker::new();
        b.iter(|| {
            aw.register(waker);
        });
    });

    c.bench_function("mwsr_waker_register", |b| {
        let mw = MwsrWaker::new();
        b.iter(|| {
            unsafe { mw.register(waker) };
        });
    });
}

fn bench_wake(c: &mut Criterion) {
    let waker = Waker::noop();

    c.bench_function("atomic_waker_wake", |b| {
        let aw = AtomicWaker::new();
        aw.register(waker);
        b.iter(|| {
            aw.wake();
        });
    });

    c.bench_function("mwsr_waker_wake", |b| {
        let mw = MwsrWaker::new();
        unsafe { mw.register(waker) };
        b.iter(|| {
            mw.wake();
        });
    });
}

fn bench_register_and_wake(c: &mut Criterion) {
    let waker = Waker::noop();

    c.bench_function("atomic_waker_register_and_wake", |b| {
        let aw = AtomicWaker::new();
        b.iter(|| {
            aw.register(waker);
            aw.wake();
        });
    });

    c.bench_function("mwsr_waker_register_and_wake", |b| {
        let mw = MwsrWaker::new();
        b.iter(|| {
            unsafe { mw.register(waker) };
            mw.wake();
        });
    });
}

criterion_group!(benches, bench_register, bench_wake, bench_register_and_wake);
criterion_main!(benches);
