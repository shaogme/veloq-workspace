#![cfg(target_os = "linux")]

use veloq_io_uring::{IoUring, SubmitReceipt, opcode, squeue};

#[test]
fn nop_produces_a_real_completion() {
    let mut ring = match IoUring::new(2) {
        Ok(ring) => ring,
        Err(error) => {
            eprintln!("skip NOP integration test: {error}");
            return;
        }
    };

    let (submitter, mut submission, mut completion) = ring.split();
    let entry = opcode::Nop::new()
        .build()
        .expect("NOP has no fallible fields")
        .user_data(0xfeed);
    unsafe { submission.push(&entry) }.expect("the empty submission queue has capacity");
    drop(submission);

    let receipt = submitter
        .submit_and_wait(1)
        .expect("NOP submission must succeed");
    assert!(matches!(
        receipt,
        SubmitReceipt::Submitted {
            requested: 1,
            submitted: 1,
            ..
        }
    ));

    completion.sync();
    let cqe = completion.next().expect("NOP must produce one CQE");
    assert_eq!(cqe.user_data(), 0xfeed);
    assert_eq!(cqe.result(), 0);
    assert_eq!(cqe.flags(), 0);
    assert!(squeue::Flags::empty().is_known());
}
