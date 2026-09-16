use alloc::vec;
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
    occupied: Vec<u64>,
}

impl Level {
    pub(crate) fn new(unit_ticks: u64, span_ticks: u64, slot_count: usize, mask: usize) -> Self {
        Self {
            unit_ticks,
            span_ticks,
            mask,
            slots: (0..slot_count).map(|_| Slot::empty()).collect(),
            occupied: vec![0; slot_count.div_ceil(64)],
        }
    }

    pub(crate) fn set_occupied(&mut self, slot: usize) {
        self.occupied[slot / 64] |= 1u64 << (slot % 64);
    }

    pub(crate) fn clear_occupied(&mut self, slot: usize) {
        self.occupied[slot / 64] &= !(1u64 << (slot % 64));
    }

    pub(crate) fn is_occupied(&self, slot: usize) -> bool {
        (self.occupied[slot / 64] & (1u64 << (slot % 64))) != 0
    }

    pub(crate) fn next_occupied(&self, start: usize) -> Option<(usize, usize)> {
        let word_count = self.occupied.len();
        if word_count == 0 {
            return None;
        }

        let start_word = start / 64;
        let start_bit = start % 64;
        if let Some(slot) = self.first_occupied_in_word(start_word, u64::MAX << start_bit) {
            return Some((slot, slot - start));
        }
        for word_index in (start_word + 1)..word_count {
            if let Some(slot) = self.first_occupied_in_word(word_index, u64::MAX) {
                return Some((slot, self.slots.len() - start + slot));
            }
        }
        for word_index in 0..start_word {
            if let Some(slot) = self.first_occupied_in_word(word_index, u64::MAX) {
                return Some((slot, self.slots.len() - start + slot));
            }
        }
        if start_bit != 0
            && let Some(slot) = self.first_occupied_in_word(start_word, (1u64 << start_bit) - 1)
        {
            return Some((slot, self.slots.len() - start + slot));
        }
        None
    }

    fn first_occupied_in_word(&self, word_index: usize, mask: u64) -> Option<usize> {
        let mut bits = self.occupied[word_index] & mask;
        if word_index == self.occupied.len() - 1 {
            let valid_bits = self.slots.len() % 64;
            if valid_bits != 0 {
                bits &= (1u64 << valid_bits) - 1;
            }
        }
        (bits != 0).then(|| word_index * 64 + bits.trailing_zeros() as usize)
    }

    pub(crate) fn clear_slots(&mut self) {
        for slot in &mut self.slots {
            slot.head = None;
            slot.tail = None;
        }
        self.occupied.fill(0);
    }
}
