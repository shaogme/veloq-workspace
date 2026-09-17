use alloc::vec::Vec;
use slotmap::DefaultKey;

pub(crate) struct Slot {
    pub(crate) head: Option<DefaultKey>,
    pub(crate) tail: Option<DefaultKey>,
}

impl Slot {
    pub(crate) fn empty() -> Self {
        Self {
            head: None,
            tail: None,
        }
    }
}

pub(crate) struct Level {
    pub(crate) unit_ticks: u64,
    pub(crate) span_ticks: u64,
    pub(crate) mask: usize,
    pub(crate) slots: Vec<Slot>,
}

impl Level {
    pub(crate) fn new(unit_ticks: u64, span_ticks: u64, slot_count: usize, mask: usize) -> Self {
        Self {
            unit_ticks,
            span_ticks,
            mask,
            slots: (0..slot_count).map(|_| Slot::empty()).collect(),
        }
    }

    pub(crate) fn clear_slots(&mut self) {
        for slot in &mut self.slots {
            slot.head = None;
            slot.tail = None;
        }
    }
}
