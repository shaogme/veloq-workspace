#![cfg(any(target_os = "linux", target_os = "android"))]

use core::mem::{align_of, size_of};

use veloq_io_uring::{cqueue, squeue};

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
