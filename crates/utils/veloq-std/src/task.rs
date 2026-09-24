use core::{marker::PhantomData, mem::ManuallyDrop};

use crate::sync::NativeArc;

pub use core::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

/// 可转换为任务唤醒器的用户回调。
///
/// 回调只借用实现对象，所有权由 `veloq_std` 内部的 `NativeArc` 管理，
/// 因此该接口不会要求调用方接触 `alloc::sync::Arc`。
pub trait Wake: Send + Sync + 'static {
    fn wake(&self);

    fn wake_by_ref(&self) {
        self.wake();
    }
}

struct WakeVTable<W>(PhantomData<fn() -> W>);

impl<W: Wake> WakeVTable<W> {
    const VTABLE: RawWakerVTable = RawWakerVTable::new(
        clone_waker::<W>,
        wake_waker::<W>,
        wake_by_ref_waker::<W>,
        drop_waker::<W>,
    );
}

fn wake_vtable<W: Wake>() -> &'static RawWakerVTable {
    &WakeVTable::<W>::VTABLE
}

unsafe fn clone_waker<W: Wake>(data: *const ()) -> RawWaker {
    unsafe { NativeArc::<W>::increment_strong_count(data.cast()) };
    RawWaker::new(data, wake_vtable::<W>())
}

unsafe fn wake_waker<W: Wake>(data: *const ()) {
    let waker = unsafe { NativeArc::<W>::from_raw(data.cast()) };
    waker.wake();
}

unsafe fn wake_by_ref_waker<W: Wake>(data: *const ()) {
    let waker = ManuallyDrop::new(unsafe { NativeArc::<W>::from_raw(data.cast()) });
    waker.wake_by_ref();
}

unsafe fn drop_waker<W: Wake>(data: *const ()) {
    drop(unsafe { NativeArc::<W>::from_raw(data.cast()) });
}

impl<W: Wake> From<NativeArc<W>> for Waker {
    fn from(waker: NativeArc<W>) -> Self {
        let data = NativeArc::into_raw(waker).cast();
        let raw = RawWaker::new(data, wake_vtable::<W>());
        unsafe { Waker::from_raw(raw) }
    }
}
