use crate::DriverResult;
use crate::SlotSidecar;
use crate::driver::PlatformOp;
use crate::slot::{SlotEntry, SlotState, SlotStorage, SlotTable};
use std::ops::{Index, IndexMut};
use std::sync::Arc;
use veloq_shim::atomic::Ordering;

pub struct OpEntry<P> {
    pub platform_data: P,
}

impl<P> OpEntry<P> {
    pub fn new(platform_data: P) -> Self {
        Self { platform_data }
    }
}

pub struct LocalSlot<Op, UP, P, S: SlotSidecar, E, R = usize> {
    pub(crate) op: Option<Op>,
    pub entry: OpEntry<P>,
    pub storage: SlotStorage<Op, UP, S, E, R>,
}

impl<Op, UP, P: Default, S: SlotSidecar, E, R> LocalSlot<Op, UP, P, S, E, R> {
    #[inline]
    fn new() -> Self {
        Self {
            op: None,
            entry: OpEntry {
                platform_data: P::default(),
            },
            storage: SlotStorage::<Op, UP, S, E, R>::new(),
        }
    }
}

pub type LocalSlots<Op, UP, P, S, E, R = usize> = Box<[LocalSlot<Op, UP, P, S, E, R>]>;

pub struct OpRegistry<Op: PlatformOp, UP, P, S: SlotSidecar, E, R = usize> {
    pub shared: Arc<SlotTable<Op, UP, S, E, R>>,
    pub local: LocalSlots<Op, UP, P, S, E, R>,
    local_free_head: usize,
    active_count: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpHandle {
    pub index: usize,
    pub generation: u32,
}

pub struct AllocResult {
    pub handle: OpHandle,
}

pub type SlotEntryOpBundle<'a, Op, UP, P, S, E, R = usize> = (
    &'a SlotEntry<Op, UP, S, E, R>,
    &'a mut OpEntry<P>,
    &'a mut Option<Op>,
    &'a mut SlotStorage<Op, UP, S, E, R>,
);

pub type SlotEntryAndOpEntry<'a, Op, UP, P, S, E, R = usize> =
    (&'a SlotEntry<Op, UP, S, E, R>, &'a mut OpEntry<P>);

impl<Op: PlatformOp, UP: Send, P: Default, S: SlotSidecar, E, R>
    OpRegistry<Op, UP, P, S, E, R>
{
    pub fn new(capacity: usize) -> Self {
        let shared = Arc::new(SlotTable::new(capacity));
        let mut local: Vec<LocalSlot<Op, UP, P, S, E, R>> = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            local.push(LocalSlot::new());
        }
        for i in (0..capacity).rev() {
            shared.push_free(i);
        }

        Self {
            shared,
            local: local.into_boxed_slice(),
            local_free_head: SlotTable::<Op, UP, S, R>::NULL_INDEX,
            active_count: 0,
        }
    }

    pub fn alloc(&mut self, data: P) -> Result<AllocResult, P> {
        if self.local_free_head == SlotTable::<Op, UP, S, E, R>::NULL_INDEX {
            self.local_free_head = self.shared.pop_all();
        }

        let mut deferred_non_idle = Vec::new();
        while self.local_free_head != SlotTable::<Op, UP, S, E, R>::NULL_INDEX {
            let idx = self.local_free_head;
            self.local_free_head = self.shared.slots[idx].next_free.load(Ordering::Relaxed);

            let slot = &self.shared.slots[idx];
            let state = slot.state(Ordering::Acquire);
            if state != SlotState::Idle {
                // Detached completions may temporarily keep slots in READY/WAITING states.
                // Those slots are not safe to recycle yet.
                deferred_non_idle.push(idx);
                continue;
            }

            let new_gen = slot.generation(Ordering::Relaxed).wrapping_add(1);
            slot.reset(new_gen);
            slot.set_state(crate::slot::SlotState::Reserved, Ordering::Release);

            self.local[idx].op = None;
            self.local[idx].entry.platform_data = data;
            self.local[idx].storage.reset();
            self.active_count += 1;

            for deferred_idx in deferred_non_idle {
                self.shared.push_free(deferred_idx);
            }

            return Ok(AllocResult {
                handle: OpHandle {
                    index: idx,
                    generation: new_gen,
                },
            });
        }

        for deferred_idx in deferred_non_idle {
            self.shared.push_free(deferred_idx);
        }

        Err(data)
    }

    pub fn insert(&mut self, entry: OpEntry<P>) -> Result<OpHandle, OpEntry<P>> {
        match self.alloc(entry.platform_data) {
            Ok(res) => Ok(res.handle),
            Err(data) => Err(OpEntry {
                platform_data: data,
            }),
        }
    }

    pub fn get_mut(&mut self, user_data: usize) -> Option<&mut OpEntry<P>> {
        self.local.get_mut(user_data).map(|v| &mut v.entry)
    }

    pub fn get_slot_and_entry_mut(
        &mut self,
        user_data: usize,
    ) -> Option<SlotEntryAndOpEntry<'_, Op, UP, P, S, E, R>> {
        if user_data < self.local.len() {
            Some((
                &self.shared.slots[user_data],
                &mut self.local[user_data].entry,
            ))
        } else {
            None
        }
    }

    pub fn get_slot_entry_op_storage_and_entry_mut(
        &mut self,
        user_data: usize,
    ) -> Option<SlotEntryOpBundle<'_, Op, UP, P, S, E, R>> {
        if user_data < self.local.len() {
            let local = &mut self.local[user_data];
            Some((
                &self.shared.slots[user_data],
                &mut local.entry,
                &mut local.op,
                &mut local.storage,
            ))
        } else {
            None
        }
    }

    #[inline]
    pub fn with_slot_storage_mut<F, X>(&mut self, user_data: usize, f: F) -> Option<X>
    where
        F: FnOnce(&mut Option<Op>, &mut Option<DriverResult<R, E>>, &mut Option<UP>, &mut S) -> X,
    {
        self.local
            .get_mut(user_data)
            .map(|local| local.storage.with_mut(f))
    }

    pub fn slot_storage_mut(
        &mut self,
        user_data: usize,
    ) -> Option<&mut SlotStorage<Op, UP, S, E, R>> {
        if user_data < self.local.len() {
            Some(&mut self.local[user_data].storage)
        } else {
            None
        }
    }

    pub fn contains(&self, user_data: usize) -> bool {
        user_data < self.local.len()
    }

    pub fn remove(&mut self, user_data: usize) -> OpEntry<P> {
        assert!(user_data < self.local.len(), "Invalid user_data for remove");

        let local = &mut self.local[user_data];
        let _ = local.op.take();
        let data = std::mem::take(&mut local.entry.platform_data);
        local.storage.reset();
        self.shared.slots[user_data].free();
        self.shared.push_free(user_data);
        self.active_count -= 1;

        OpEntry {
            platform_data: data,
        }
    }

    pub fn recycle(&mut self, user_data: usize, generation: u32) {
        assert!(
            user_data < self.local.len(),
            "Invalid user_data for recycle"
        );

        let local = &mut self.local[user_data];
        let _ = local.op.take();
        let _ = std::mem::take(&mut local.entry.platform_data);
        local.storage.reset();

        if self.shared.slots[user_data].state(Ordering::Acquire) == SlotState::InFlightReady {
            self.shared.clear_ready_completion();
        }
        self.shared.slots[user_data].reset(generation);
        self.shared.push_free(user_data);
        self.active_count -= 1;
    }

    pub fn get_page_slice(&self, page_idx: usize) -> Option<(*const u8, usize)> {
        if page_idx == 0 {
            let ptr = self.local.as_ptr() as *const u8;
            let len = std::mem::size_of_val(&*self.local);
            Some((ptr, len))
        } else {
            None
        }
    }

    #[inline]
    pub fn has_active_ops(&self) -> bool {
        self.active_count > 0
    }
}

impl<Op: PlatformOp, UP, P, S: SlotSidecar, E, R> Index<usize>
    for OpRegistry<Op, UP, P, S, E, R>
{
    type Output = OpEntry<P>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.local[index].entry
    }
}

impl<Op: PlatformOp, UP, P, S: SlotSidecar, E, R> IndexMut<usize>
    for OpRegistry<Op, UP, P, S, E, R>
{
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.local[index].entry
    }
}
