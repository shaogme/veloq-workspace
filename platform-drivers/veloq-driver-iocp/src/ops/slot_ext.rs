use std::io;
use veloq_driver_core::slot::{ErasedPayload, SlotEntry};
use windows_sys::Win32::System::IO::OVERLAPPED;
use crate::ops::{IocpOp, OverlappedEntry};

/// Extension trait for `SlotEntry` to provide safe(r) access to IOCP operation data.
pub(crate) trait IocpSlotExt {
    /// Initializes the slot with a new operation and sets up the sidecar.
    ///
    /// # Safety
    ///
    /// The caller must ensure that the slot is exclusively owned and not currently
    /// being processed by the OS kernel (i.e., it must be in `Pending` state).
    unsafe fn init_op(&self, op: IocpOp, user_data: usize, generation: u32);

    /// Returns a raw pointer to the `OVERLAPPED` structure within the sidecar.
    ///
    /// # Safety
    ///
    /// The sidecar must remain valid for the duration of the asynchronous I/O operation.
    unsafe fn overlapped_ptr(&self) -> *mut OVERLAPPED;

    /// Provides mutable access to the sidecar (OverlappedEntry).
    ///
    /// # Safety
    ///
    /// The caller must ensure there are no concurrent accesses to the sidecar.
    unsafe fn with_sidecar_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut OverlappedEntry) -> R;

    /// Takes the operation out of the slot.
    ///
    /// # Safety
    ///
    /// The caller must ensure the operation is no longer in-flight or that it's safe to take the op
    /// (e.g., during cancellation or after completion).
    unsafe fn take_op(&self) -> Option<IocpOp>;

    /// Takes completion data (payload and result) from the slot.
    ///
    /// # Safety
    ///
    /// This should only be called once when an operation completes or is aborted.
    unsafe fn take_completion_data(&self) -> (Option<ErasedPayload>, Option<io::Result<usize>>);
}

impl IocpSlotExt for SlotEntry<IocpOp, OverlappedEntry> {
    unsafe fn init_op(&self, op: IocpOp, user_data: usize, generation: u32) {
        // SAFETY: External synchronization is provided by the OpRegistry/SlotTable lifecycle.
        unsafe {
            *self.op.get() = Some(op);
            let sidecar = &mut *self.sidecar.get();
            sidecar.user_data = user_data;
            sidecar.generation = generation;
            sidecar.blocking_result = None;
        }
    }

    unsafe fn overlapped_ptr(&self) -> *mut OVERLAPPED {
        // SAFETY: `sidecar` is guaranteed to be valid for the lifetime of the SlotEntry.
        unsafe { &mut (*self.sidecar.get()).inner as *mut _ }
    }

    unsafe fn with_sidecar_mut<F, R>(&self, f: F) -> R
    where
        F: FnOnce(&mut OverlappedEntry) -> R,
    {
        // SAFETY: Sidecar access is protected by the driver's internal state management.
        unsafe { f(&mut *self.sidecar.get()) }
    }

    unsafe fn take_op(&self) -> Option<IocpOp> {
        // SAFETY: Accessing the UnsafeCell content.
        unsafe { (*self.op.get()).take() }
    }

    unsafe fn take_completion_data(&self) -> (Option<ErasedPayload>, Option<io::Result<usize>>) {
        // SAFETY: Taking values from UnsafeCells after ensuring the IO is finished.
        unsafe {
            let payload = (*self.payload.get()).take();
            let detail = (*self.result.get()).take();
            (payload, detail)
        }
    }
}
