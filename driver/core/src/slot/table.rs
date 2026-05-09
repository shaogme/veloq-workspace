use crate::SlotSidecar;
use crate::slot::core::{SlotData, SlotState};
use crossbeam_utils::CachePadded;
use veloq_shim::atomic::{AtomicU64, AtomicUsize, Ordering};

pub type SlotEntry<Op, UP, S, R = usize> = CachePadded<SlotData<Op, UP, S, R>>;

pub struct DetachedCancelTable {
    slot_count: usize,
    cancel_words: Box<[CachePadded<AtomicU64>]>,
    cancel_generations: Box<[CachePadded<AtomicU64>]>,
}

impl DetachedCancelTable {
    pub fn new(capacity: usize) -> Self {
        let word_count = capacity.div_ceil(64);
        let mut cancel_words = Vec::with_capacity(word_count);
        for _ in 0..word_count {
            cancel_words.push(CachePadded::new(AtomicU64::new(0)));
        }
        let mut cancel_generations = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            cancel_generations.push(CachePadded::new(AtomicU64::new(0)));
        }
        Self {
            slot_count: capacity,
            cancel_words: cancel_words.into_boxed_slice(),
            cancel_generations: cancel_generations.into_boxed_slice(),
        }
    }

    #[inline]
    pub fn request_cancel(&self, token: u64) {
        let (idx, generation) = crate::driver::decode_completion_token(token);
        if idx >= self.slot_count {
            return;
        }

        let generation = generation as u64;
        let cell = &self.cancel_generations[idx];
        let mut current = cell.load(Ordering::Acquire);
        while generation > current {
            match cell.compare_exchange_weak(
                current,
                generation,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(next) => current = next,
            }
        }

        let word_idx = idx / 64;
        let bit_idx = idx % 64;
        self.cancel_words[word_idx].fetch_or(1u64 << bit_idx, Ordering::Release);
    }

    #[inline]
    pub fn cancel_word_count(&self) -> usize {
        self.cancel_words.len()
    }

    #[inline]
    pub fn take_cancel_word(&self, word_idx: usize) -> u64 {
        self.cancel_words[word_idx].fetch_and(0, Ordering::AcqRel)
    }

    #[inline]
    pub(crate) fn cancel_generation(&self, idx: usize) -> u64 {
        self.cancel_generations[idx].load(Ordering::Acquire)
    }
}

pub struct SlotTable<Op, UP, S: SlotSidecar, R = usize> {
    pub slots: Box<[SlotEntry<Op, UP, S, R>]>,
    pub remote_free_head: AtomicUsize,
    ready_completion_count: AtomicUsize,
}

unsafe impl<Op, UP, S: SlotSidecar, R> Sync for SlotTable<Op, UP, S, R> {}

impl<Op, UP, S: SlotSidecar, R> SlotTable<Op, UP, S, R> {
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
        }
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

    #[inline]
    pub(crate) fn slot_snapshot(&self, idx: usize) -> Option<(u32, SlotState)> {
        self.slots.get(idx).map(|slot| {
            let core = slot.load_core_state(Ordering::Acquire);
            (core.generation(), core.state())
        })
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
