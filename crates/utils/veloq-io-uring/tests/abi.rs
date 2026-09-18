#![cfg(any(target_os = "linux", target_os = "android"))]

use core::mem::{align_of, size_of};

use veloq_io_uring::{IoUring, SUPPORTED_ABI_ARCHITECTURES, cqueue, squeue};

const _: () = {
    assert!(size_of::<squeue::Entry>() == 64);
    assert!(align_of::<squeue::Entry>() == 8);
    assert!(size_of::<cqueue::Entry>() == 16);
    assert!(align_of::<cqueue::Entry>() == 8);
};

#[test]
fn public_entry_sizes_are_frozen() {
    assert_eq!(size_of::<squeue::Entry>(), 64);
    assert_eq!(align_of::<squeue::Entry>(), 8);
    assert_eq!(size_of::<cqueue::Entry>(), 16);
    assert_eq!(align_of::<cqueue::Entry>(), 8);
}

#[test]
fn current_target_is_in_the_checked_abi_matrix() {
    let target_arch = if cfg!(target_arch = "x86_64") {
        "x86_64"
    } else if cfg!(target_arch = "aarch64") {
        "aarch64"
    } else if cfg!(target_arch = "riscv64") {
        "riscv64"
    } else if cfg!(target_arch = "loongarch64") {
        "loongarch64"
    } else {
        "powerpc64"
    };

    assert!(SUPPORTED_ABI_ARCHITECTURES.contains(&target_arch));
}

#[test]
fn ring_mappings_are_not_inherited_after_fork() {
    let ring = match IoUring::new(8) {
        Ok(ring) => ring,
        Err(error) => {
            eprintln!("skip ring fork test: io_uring is unavailable: {error}");
            return;
        }
    };
    let layout = ring.layout();
    let mappings = [
        (layout.sq_ring_address(), layout.sq_ring_length()),
        (layout.cq_ring_address(), layout.cq_ring_length()),
        (layout.sqe_address(), layout.sqe_length()),
    ];

    let child = unsafe { libc::fork() };
    assert!(
        child >= 0,
        "fork failed: {:?}",
        std::io::Error::last_os_error()
    );
    if child == 0 {
        let inherited = mappings.iter().any(|&(address, length)| {
            let mut residency = 0_u8;
            unsafe {
                libc::mincore(
                    address as *mut libc::c_void,
                    length,
                    &mut residency as *mut u8,
                ) == 0
            }
        });
        unsafe { libc::_exit(i32::from(inherited)) };
    }

    let mut status = 0;
    assert_eq!(unsafe { libc::waitpid(child, &mut status, 0) }, child);
    assert!(libc::WIFEXITED(status));
    assert_eq!(libc::WEXITSTATUS(status), 0);
}
