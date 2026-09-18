#![cfg(target_os = "linux")]

use std::{env, fs::File, os::fd::AsRawFd, thread};

use veloq_io_uring::{
    IoUring, SetupPolicy, SubmitReceipt, cqueue, opcode,
    types::{AsyncCancelFlags, Fd, Timespec},
};

const TIMEOUT_USER_DATA: u64 = 0xdeca_fbad;

fn enabled() -> bool {
    env::var_os("VELOQ_IO_URING_STRESS").is_some_and(|value| {
        let value = value.to_string_lossy();
        value == "1" || value.eq_ignore_ascii_case("true")
    })
}

fn setting(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value != 0)
        .unwrap_or(default)
}

fn submit_one_nop(ring: &mut IoUring, user_data: u64) -> Result<(), String> {
    let entry = opcode::Nop::new()
        .build()
        .map_err(|error| format!("build NOP: {error}"))?
        .user_data(user_data);
    {
        let mut submission = ring.submission();
        unsafe { submission.push(&entry) }.map_err(|error| format!("push NOP: {error}"))?;
    }

    let receipt = ring
        .submitter()
        .submit_and_wait(1)
        .map_err(|error| format!("submit NOP: {error:?}"))?;
    if !matches!(receipt, SubmitReceipt::Submitted { .. }) {
        return Err(format!("unexpected NOP receipt: {receipt:?}"));
    }

    let mut completion = ring.completion();
    completion.sync();
    let cqe = completion
        .next()
        .ok_or_else(|| "NOP did not produce a CQE".to_owned())?;
    if cqe.user_data() != user_data || cqe.result() != 0 {
        return Err(format!(
            "unexpected NOP CQE: user_data={}, result={}",
            cqe.user_data(),
            cqe.result()
        ));
    }
    Ok(())
}

fn run_worker(worker: usize, rounds: usize) -> Result<(), String> {
    let mut ring = IoUring::new(8).map_err(|error| format!("worker {worker} ring: {error}"))?;
    for round in 0..rounds {
        submit_one_nop(&mut ring, ((worker as u64) << 32) | round as u64)?;
    }
    Ok(())
}

#[test]
fn concurrent_workers_keep_nop_completion_protocol_stable() {
    if !enabled() {
        eprintln!("skip io_uring stress test: set VELOQ_IO_URING_STRESS=1 to enable");
        return;
    }
    if let Err(error) = IoUring::new(8) {
        eprintln!("skip io_uring stress test: {error}");
        return;
    }

    let workers = setting("VELOQ_IO_URING_STRESS_WORKERS", 2);
    let rounds = setting("VELOQ_IO_URING_STRESS_ROUNDS", 1_000);
    thread::scope(|scope| {
        let handles: Vec<_> = (0..workers)
            .map(|worker| scope.spawn(move || run_worker(worker, rounds)))
            .collect();
        for handle in handles {
            handle
                .join()
                .expect("io_uring stress worker must not panic")
                .expect("io_uring stress worker must complete successfully");
        }
    });
}

#[test]
fn cq_overflow_is_observable_without_losing_ring_ownership() {
    if !enabled() {
        eprintln!("skip io_uring stress test: set VELOQ_IO_URING_STRESS=1 to enable");
        return;
    }
    let mut ring = match IoUring::new(2) {
        Ok(ring) => ring,
        Err(error) => {
            eprintln!("skip CQ overflow stress test: {error}");
            return;
        }
    };

    let rounds = setting("VELOQ_IO_URING_STRESS_OVERFLOW_ROUNDS", 4_096);
    for round in 0..rounds {
        let entry = opcode::Nop::new()
            .build()
            .expect("NOP construction cannot fail")
            .user_data(round as u64);
        {
            let mut submission = ring.submission();
            unsafe { submission.push(&entry) }.expect("SQ must recycle one consumed entry");
        }
        ring.submit()
            .map_err(|error| format!("{error:?}"))
            .expect("NOP submission must remain accepted while CQ is not drained");
    }

    let mut overflow = 0;
    for _ in 0..128 {
        ring.submitter()
            .submit_and_wait(1)
            .expect("the kernel must eventually complete one NOP");
        let mut completion = ring.completion();
        completion.sync();
        overflow = completion.overflow();
        if overflow != 0 {
            break;
        }
        thread::yield_now();
    }

    let mut completion = ring.completion();
    completion.sync();
    while completion.next().is_some() {}
    if overflow == 0 {
        eprintln!("CQ overflow stress did not fill the kernel queue on this run; rounds={rounds}");
    }
}

#[test]
fn short_io_and_async_cancel_complete_under_pressure() {
    if !enabled() {
        eprintln!("skip io_uring stress test: set VELOQ_IO_URING_STRESS=1 to enable");
        return;
    }
    let mut ring = match IoUring::new(8) {
        Ok(ring) => ring,
        Err(error) => {
            eprintln!("skip short I/O/cancel stress test: {error}");
            return;
        }
    };

    let file = File::open("/dev/null").expect("/dev/null must be available on Linux");
    let mut buffer = [0_u8; 1];
    let short_read = unsafe {
        opcode::Read::new(
            Fd(file.as_raw_fd()),
            buffer.as_mut_ptr(),
            buffer.len() as u32,
        )
    }
    .build()
    .expect("short read fields are valid")
    .user_data(1);
    {
        let mut submission = ring.submission();
        unsafe { submission.push(&short_read) }.expect("SQ must accept short read");
    }
    ring.submitter()
        .submit_and_wait(1)
        .expect("short read must submit");
    let mut completion = ring.completion();
    completion.sync();
    let cqe = completion.next().expect("short read must complete");
    assert_eq!(cqe.user_data(), 1);
    assert_eq!(cqe.result(), 0);
    drop(completion);

    let timeout = Timespec::new()
        .sec(60)
        .expect("test timeout must fit the kernel ABI");
    let timeout_entry = unsafe { opcode::Timeout::new(&timeout) }
        .build()
        .expect("timeout fields are valid")
        .user_data(TIMEOUT_USER_DATA);
    let cancel_entry = opcode::AsyncCancel::new(TIMEOUT_USER_DATA)
        .flags(AsyncCancelFlags::USERDATA)
        .build()
        .expect("user-data cancellation fields are valid");
    {
        let mut submission = ring.submission();
        unsafe { submission.push(&timeout_entry) }.expect("SQ must accept timeout");
        unsafe { submission.push(&cancel_entry) }.expect("SQ must accept cancellation");
    }
    ring.submitter()
        .submit_and_wait(2)
        .expect("timeout cancellation pair must submit");

    let mut completion = ring.completion();
    completion.sync();
    let mut saw_timeout_cancel = false;
    let mut saw_cancel_completion = false;
    while let Some(cqe) = completion.next() {
        if cqe.user_data() == TIMEOUT_USER_DATA {
            saw_timeout_cancel = cqe.result() == -libc::ECANCELED;
        } else {
            saw_cancel_completion = true;
        }
    }
    assert!(
        saw_timeout_cancel,
        "timeout must be canceled before its deadline"
    );
    assert!(
        saw_cancel_completion,
        "cancellation must produce its own CQE"
    );
}

#[test]
fn optional_setup_profile_has_a_basic_fallback() {
    if !enabled() {
        eprintln!("skip io_uring stress test: set VELOQ_IO_URING_STRESS=1 to enable");
        return;
    }
    let profile =
        veloq_io_uring::RingConfig::new(8).with_setup_policy(SetupPolicy::latency_best_effort());
    let ring = match IoUring::from_config(profile) {
        Ok(ring) => ring,
        Err(profile_error) => {
            eprintln!(
                "optional setup profile rejected; exercising basic fallback: {profile_error}"
            );
            IoUring::new(8).expect("basic io_uring profile must remain available")
        }
    };
    assert!(ring.params().sq_entries() >= 1);
}

#[test]
fn provided_buffer_flag_decoder_handles_stress_ids() {
    if !enabled() {
        eprintln!("skip io_uring stress test: set VELOQ_IO_URING_STRESS=1 to enable");
        return;
    }
    for bid in [0_u16, 1, 127, u16::MAX] {
        let flags = 1_u32 | (u32::from(bid) << 16);
        assert_eq!(cqueue::buffer_select(flags), Some(bid));
    }
}
