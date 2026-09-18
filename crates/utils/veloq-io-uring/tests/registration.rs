#![cfg(target_os = "linux")]

use std::{fs::File, os::fd::AsRawFd};

use veloq_io_uring::{IoUring, ResourceKind, ResourceLayout, ResourceRegistrationState};

#[test]
fn sparse_buffer_registration_has_a_bounded_update_range() {
    let mut ring = match IoUring::new(8) {
        Ok(ring) => ring,
        Err(error) => {
            eprintln!("skip sparse buffer registration test: {error}");
            return;
        }
    };
    let submitter = ring.submitter();
    let mut registration = match submitter.register_buffers_sparse(4) {
        Ok(registration) => registration,
        Err(error) => {
            eprintln!("skip sparse buffer registration test: {error}");
            return;
        }
    };

    assert_eq!(registration.kind(), ResourceKind::Buffers);
    assert_eq!(registration.layout(), ResourceLayout::Sparse);
    assert_eq!(registration.capacity(), 4);

    let mut buffer = [0_u8; 8];
    let iovec = libc::iovec {
        iov_base: buffer.as_mut_ptr().cast(),
        iov_len: buffer.len(),
    };
    let updated = unsafe { submitter.register_buffers_update(&registration, 3, &[iovec]) }
        .expect("the last sparse buffer slot should be updateable");
    assert_eq!(updated, 1);

    let error = unsafe { submitter.register_buffers_update(&registration, 4, &[iovec]) }
        .expect_err("an update past the table capacity must be rejected before syscall");
    assert_eq!(error.raw_os_error(), Some(libc::EINVAL));

    submitter
        .unregister_buffers(&mut registration)
        .expect("sparse buffer table should unregister after its update");
    assert_eq!(
        registration.state(),
        ResourceRegistrationState::Unregistered
    );
}

#[test]
fn sparse_file_registration_uses_files2_and_tracks_capacity() {
    let mut ring = match IoUring::new(8) {
        Ok(ring) => ring,
        Err(error) => {
            eprintln!("skip sparse file registration test: {error}");
            return;
        }
    };
    let file = File::open("/dev/null").expect("/dev/null must be available on Linux");
    let submitter = ring.submitter();
    let mut registration = match submitter.register_files_sparse(2) {
        Ok(registration) => registration,
        Err(error) => {
            eprintln!("skip sparse file registration test: {error}");
            return;
        }
    };

    assert_eq!(registration.kind(), ResourceKind::Files);
    assert_eq!(registration.layout(), ResourceLayout::Sparse);
    assert_eq!(registration.capacity(), 2);
    assert_eq!(
        submitter
            .register_files_update(&registration, 0, &[file.as_raw_fd()])
            .expect("sparse file slot should accept an update"),
        1
    );

    submitter
        .unregister_files(&mut registration)
        .expect("sparse file table should unregister after its update");
    assert_eq!(
        registration.state(),
        ResourceRegistrationState::Unregistered
    );
}
