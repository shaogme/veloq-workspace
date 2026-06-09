use crate::driver::DriverCompletionDiagnostics;
use crate::slot::core::SlotData;
use crate::slot::{SlotCompletion, SlotError, SlotSpec};
use crossbeam_utils::CachePadded;
use veloq_shim::atomic::{AtomicUsize, Ordering};

pub type SlotEntry<Spec> = CachePadded<SlotData<Spec>>;
pub type SlotEntries<Spec> = Box<[SlotEntry<Spec>]>;

pub struct SlotTable<Spec: SlotSpec> {
    pub slots: SlotEntries<Spec>,
    pub remote_free_head: AtomicUsize,
    ready_completion_count: AtomicUsize,
    pub(crate) diagnostics: DriverCompletionDiagnostics,
}

unsafe impl<Spec> Sync for SlotTable<Spec>
where
    Spec: SlotSpec,
    SlotCompletion<Spec>: Send,
    SlotError<Spec>: Send,
{
}

impl<Spec: SlotSpec> SlotTable<Spec> {
    pub const NULL_INDEX: usize = usize::MAX;

    pub fn new(capacity: usize) -> Self {
        let mut slots = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            slots.push(CachePadded::new(SlotData::new()));
        }
        Self {
            slots: slots.into_boxed_slice(),
            remote_free_head: AtomicUsize::new(Self::NULL_INDEX),
            ready_completion_count: AtomicUsize::new(0),
            diagnostics: DriverCompletionDiagnostics::default(),
        }
    }

    #[inline]
    pub fn completion_diagnostics(&self) -> DriverCompletionDiagnostics {
        self.diagnostics.clone()
    }

    pub fn push_free(&self, idx: usize) {
        let slot = &self.slots[idx];
        let mut head = self.remote_free_head.load(Ordering::Relaxed);
        loop {
            slot.next_free.store(head, Ordering::Relaxed);
            match self.remote_free_head.compare_exchange_weak(
                head,
                idx,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(current) => head = current,
            }
        }
    }

    pub fn pop_all(&self) -> usize {
        self.remote_free_head
            .swap(Self::NULL_INDEX, Ordering::Acquire)
    }

    /// 检查是否存在已完成但尚未被消费的完成项。
    #[inline]
    pub fn has_ready_completion(&self) -> bool {
        self.ready_completion_count.load(Ordering::Acquire) > 0
    }

    #[inline]
    pub(crate) fn note_ready_completion(&self) {
        self.ready_completion_count.fetch_add(1, Ordering::Release);
    }

    #[inline]
    pub(crate) fn clear_ready_completion(&self) {
        let mut current = self.ready_completion_count.load(Ordering::Acquire);
        loop {
            if current == 0 {
                return;
            }
            match self.ready_completion_count.compare_exchange_weak(
                current,
                current - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return,
                Err(next) => current = next,
            }
        }
    }
}
