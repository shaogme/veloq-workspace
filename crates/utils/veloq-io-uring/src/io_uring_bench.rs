use std::{fs::File, hint::black_box, os::fd::AsRawFd, ptr};

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use veloq_io_uring::{
    IoUring, ResourceRegistrationState, cqueue, opcode,
    types::{BufRingEntry, BufRingItem, Fd, ProvidedBufRing},
};

const SMALL_RING: u32 = 8;
const STEADY_RING: u32 = 64;
const RESOURCE_SLOTS: usize = 64;
const PROVIDED_ENTRIES: u16 = 64;

fn build_ring(entries: u32, populate: bool) -> Option<IoUring> {
    let mut builder = IoUring::builder();
    builder.mmap_populate(populate);
    builder.build(entries).ok()
}

fn require_ring(entries: u32, populate: bool) -> bool {
    if build_ring(entries, populate).is_some() {
        true
    } else {
        eprintln!(
            "skip io_uring benchmark for entries={entries}, MAP_POPULATE={populate}: \
             io_uring_setup or mapping is unavailable"
        );
        false
    }
}

fn nop_entry(user_data: u64) -> veloq_io_uring::squeue::Entry {
    opcode::Nop::new()
        .build()
        .expect("NOP construction cannot fail")
        .user_data(user_data)
}

fn enqueue_nops(ring: &mut IoUring, count: usize) {
    let entry = nop_entry(0xface);
    let mut submission = ring.submission();
    for _ in 0..count {
        unsafe { submission.push(&entry) }.expect("benchmark ring must have SQ capacity");
    }
}

fn prepared_submission(entries: u32, count: usize) -> IoUring {
    let mut ring = build_ring(entries, true).expect("ring availability was checked");
    enqueue_nops(&mut ring, count);
    ring
}

fn drain_completions(ring: &mut IoUring) -> usize {
    let mut completion = ring.completion();
    completion.sync();
    let mut count = 0;
    while completion.next().is_some() {
        count += 1;
    }
    count
}

fn prepared_completions(entries: u32, count: usize) -> IoUring {
    let mut ring = prepared_submission(entries, count);
    ring.submitter()
        .submit_and_wait(count)
        .expect("NOP batch must submit during benchmark setup");
    ring
}

fn bench_sqe_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("sqe_build");
    group.bench_function("nop", |bencher| {
        bencher.iter(|| black_box(opcode::Nop::new().build().expect("NOP is infallible")));
    });
    group.finish();
}

fn bench_submission_push(c: &mut Criterion) {
    let mut group = c.benchmark_group("sq_push");
    if !require_ring(STEADY_RING, true) {
        group.finish();
        return;
    }

    for count in [1_usize, 8, 32, STEADY_RING as usize] {
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &count,
            |bencher, &count| {
                bencher.iter_batched(
                    || build_ring(STEADY_RING, true).expect("ring availability was checked"),
                    |mut ring| {
                        enqueue_nops(&mut ring, count);
                        black_box(())
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_submit(c: &mut Criterion) {
    let mut group = c.benchmark_group("submit");
    if !require_ring(STEADY_RING, true) {
        group.finish();
        return;
    }

    for count in [1_usize, 8, 32, STEADY_RING as usize] {
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &count,
            |bencher, &count| {
                bencher.iter_batched(
                    || prepared_submission(STEADY_RING, count),
                    |mut ring| {
                        let receipt = ring.submit().expect("NOP batch must submit");
                        black_box(receipt);
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_cq_drain(c: &mut Criterion) {
    let mut group = c.benchmark_group("cq_drain");
    if !require_ring(STEADY_RING, true) {
        group.finish();
        return;
    }

    for count in [1_usize, 8, 32, STEADY_RING as usize] {
        group.bench_with_input(
            BenchmarkId::from_parameter(count),
            &count,
            |bencher, &count| {
                bencher.iter_batched(
                    || prepared_completions(STEADY_RING, count),
                    |mut ring| {
                        black_box(drain_completions(&mut ring));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_nop_roundtrip(c: &mut Criterion) {
    let mut group = c.benchmark_group("steady_state_nop_roundtrip");
    for entries in [8_u32, 64, 256] {
        if !require_ring(entries, true) {
            continue;
        }
        let count = entries as usize;
        group.bench_with_input(
            BenchmarkId::from_parameter(entries),
            &entries,
            |bencher, &entries| {
                bencher.iter_batched(
                    || prepared_submission(entries, count),
                    |mut ring| {
                        let receipt = ring
                            .submitter()
                            .submit_and_wait(count)
                            .expect("NOP batch must submit");
                        let completions = drain_completions(&mut ring);
                        black_box((receipt, completions));
                    },
                    BatchSize::SmallInput,
                );
            },
        );
    }
    group.finish();
}

fn bench_ring_startup(c: &mut Criterion) {
    let mut group = c.benchmark_group("ring_startup");
    for entries in [8_u32, 64, 256, 1024] {
        for populate in [false, true] {
            if !require_ring(entries, populate) {
                continue;
            }
            let label = format!("entries={entries}/populate={populate}");
            group.bench_function(label, |bencher| {
                bencher.iter(|| {
                    let ring =
                        build_ring(entries, populate).expect("ring availability was checked");
                    black_box(ring);
                });
            });
        }
    }
    group.finish();
}

fn map_and_unmap(length: usize, dontfork: bool) -> bool {
    let address = unsafe {
        libc::mmap(
            ptr::null_mut(),
            length,
            libc::PROT_READ | libc::PROT_WRITE,
            libc::MAP_PRIVATE | libc::MAP_ANONYMOUS,
            -1,
            0,
        )
    };
    if address == libc::MAP_FAILED {
        return false;
    }

    let advice_ok =
        !dontfork || unsafe { libc::madvise(address, length, libc::MADV_DONTFORK) == 0 };
    let unmap_ok = unsafe { libc::munmap(address, length) == 0 };
    advice_ok && unmap_ok
}

fn bench_mapping_policy(c: &mut Criterion) {
    let mut group = c.benchmark_group("mapping_policy");
    let length = 4096;
    if !map_and_unmap(length, false) || !map_and_unmap(length, true) {
        eprintln!("skip mapping policy benchmark: mmap or MADV_DONTFORK is unavailable");
        group.finish();
        return;
    }

    for dontfork in [false, true] {
        let label = if dontfork { "mmap+dontfork" } else { "mmap" };
        group.bench_function(label, |bencher| {
            bencher.iter(|| black_box(map_and_unmap(length, dontfork)));
        });
    }
    group.finish();
}

fn sparse_buffers_available() -> bool {
    let Some(mut ring) = build_ring(SMALL_RING, true) else {
        return false;
    };
    let Ok(mut registration) = ring.submitter().register_buffers_sparse(1) else {
        return false;
    };
    ring.submitter()
        .unregister_buffers(&mut registration)
        .is_ok()
}

fn contiguous_buffers_available() -> bool {
    let Some(mut ring) = build_ring(SMALL_RING, true) else {
        return false;
    };
    let mut buffer = [0_u8; 64];
    let iovec = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    let Ok(mut registration) = (unsafe {
        ring.submitter()
            .register_buffers(std::slice::from_ref(&iovec))
    }) else {
        return false;
    };
    ring.submitter()
        .unregister_buffers(&mut registration)
        .is_ok()
}

fn bench_registration(c: &mut Criterion) {
    let mut group = c.benchmark_group("registration");
    if sparse_buffers_available() {
        group.bench_function("sparse_buffers_register_unregister", |bencher| {
            bencher.iter_batched(
                || build_ring(SMALL_RING, true).expect("ring availability was checked"),
                |mut ring| {
                    let mut registration = ring
                        .submitter()
                        .register_buffers_sparse(RESOURCE_SLOTS)
                        .expect("sparse buffer registration was checked");
                    ring.submitter()
                        .unregister_buffers(&mut registration)
                        .expect("sparse buffer unregistration must succeed");
                    black_box(registration.state() == ResourceRegistrationState::Unregistered);
                },
                BatchSize::SmallInput,
            );
        });
    } else {
        eprintln!("skip sparse registration benchmark: REGISTER_BUFFERS2 is unavailable");
    }

    if contiguous_buffers_available() {
        group.bench_function("contiguous_buffers_register_unregister", |bencher| {
            bencher.iter_batched(
                || {
                    (
                        build_ring(SMALL_RING, true).expect("ring availability was checked"),
                        vec![0_u8; 4096],
                    )
                },
                |(mut ring, mut buffer)| {
                    let iovec = libc::iovec {
                        iov_base: buffer.as_mut_ptr().cast(),
                        iov_len: buffer.len(),
                    };
                    let mut registration = unsafe {
                        ring.submitter()
                            .register_buffers(std::slice::from_ref(&iovec))
                    }
                    .expect("contiguous buffer registration was checked");
                    ring.submitter()
                        .unregister_buffers(&mut registration)
                        .expect("contiguous buffer unregistration must succeed");
                    black_box(registration.state() == ResourceRegistrationState::Unregistered);
                },
                BatchSize::SmallInput,
            );
        });
    } else {
        eprintln!("skip contiguous registration benchmark: REGISTER_BUFFERS is unavailable");
    }
    group.finish();
}

fn bench_provided_refill(c: &mut Criterion) {
    let mut group = c.benchmark_group("provided_refill");
    let mut backing = vec![0_u8; PROVIDED_ENTRIES as usize * 64];
    let items: Vec<BufRingItem> = (0..PROVIDED_ENTRIES)
        .map(|bid| {
            let address = unsafe { backing.as_mut_ptr().add(bid as usize * 64) } as u64;
            unsafe { BufRingItem::new(address, 64, bid) }.expect("benchmark descriptor is valid")
        })
        .collect();

    group.bench_function("batch_one_tail_store", |bencher| {
        bencher.iter(|| {
            let mut storage = [BufRingEntry::default(); PROVIDED_ENTRIES as usize];
            let mut ring = unsafe {
                ProvidedBufRing::from_raw_parts(storage.as_mut_ptr(), PROVIDED_ENTRIES)
                    .expect("benchmark ring shape is valid")
            };
            ring.publish(&items)
                .expect("batch publication must succeed");
            black_box(ring.entries());
        });
    });

    group.bench_function("single_item_tail_store", |bencher| {
        bencher.iter(|| {
            let mut storage = [BufRingEntry::default(); PROVIDED_ENTRIES as usize];
            let mut ring = unsafe {
                ProvidedBufRing::from_raw_parts(storage.as_mut_ptr(), PROVIDED_ENTRIES)
                    .expect("benchmark ring shape is valid")
            };
            for item in &items {
                ring.publish(std::slice::from_ref(item))
                    .expect("single-item publication must succeed");
            }
            black_box(ring.entries());
        });
    });
    group.finish();
}

fn bench_fixed_buffer_lookup(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_buffer_lookup");
    for bid in [0_u16, 7, 63, u16::MAX] {
        let flags = 1_u32 | (u32::from(bid) << 16);
        group.bench_with_input(
            BenchmarkId::from_parameter(bid),
            &flags,
            |bencher, &flags| {
                bencher.iter(|| black_box(cqueue::buffer_select(black_box(flags))));
            },
        );
    }
    group.finish();
}

fn bench_fixed_buffer_sqe(c: &mut Criterion) {
    let mut group = c.benchmark_group("fixed_buffer_sqe");
    let mut buffer = [0_u8; 64];
    let file = File::open("/dev/null").expect("/dev/null must be available on Linux");
    group.bench_function("read_fixed", |bencher| {
        bencher.iter(|| {
            let entry = unsafe {
                opcode::ReadFixed::new(
                    Fd(file.as_raw_fd()),
                    buffer.as_mut_ptr(),
                    buffer.len() as u32,
                    7,
                )
            }
            .build()
            .expect("fixed-buffer SQE fields are valid");
            black_box(entry);
        });
    });
    group.finish();
}

criterion_group!(
    benches,
    bench_sqe_build,
    bench_submission_push,
    bench_submit,
    bench_cq_drain,
    bench_nop_roundtrip,
    bench_ring_startup,
    bench_mapping_policy,
    bench_registration,
    bench_provided_refill,
    bench_fixed_buffer_lookup,
    bench_fixed_buffer_sqe,
);
criterion_main!(benches);
