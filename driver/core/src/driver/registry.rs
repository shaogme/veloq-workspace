use crate::DriverResult;
use crate::driver::OpToken;
use crate::slot::{
    SlotCompletion, SlotEntry, SlotError, SlotOp, SlotPayload, SlotPlatformData, SlotSidecarData,
    SlotSpec, SlotState, SlotStorage, SlotTable,
};
use std::sync::Arc;
use veloq_shim::atomic::Ordering;

pub type RegistryOp<T> = SlotOp<T>;
pub type RegistryPayload<T> = SlotPayload<T>;
pub type RegistryPlatformData<T> = SlotPlatformData<T>;
pub type RegistrySidecar<T> = SlotSidecarData<T>;
pub type RegistryError<T> = SlotError<T>;
pub type RegistryCompletion<T> = SlotCompletion<T>;
pub type SlotEntryOf<T> = SlotEntry<T>;
pub type SlotStorageOf<T> = SlotStorage<T>;
pub type SlotTableOf<T> = SlotTable<T>;

pub struct OpEntry<P> {
    pub platform_data: P,
}

impl<P> OpEntry<P> {
    pub fn new(platform_data: P) -> Self {
        Self { platform_data }
    }
}

pub struct LocalSlot<Spec: SlotSpec> {
    pub(crate) op: Option<RegistryOp<Spec>>,
    pub entry: OpEntry<RegistryPlatformData<Spec>>,
    pub storage: SlotStorageOf<Spec>,
}

impl<Spec: SlotSpec> LocalSlot<Spec> {
    #[inline]
    fn new() -> Self {
        Self {
            op: None,
            entry: OpEntry {
                platform_data: RegistryPlatformData::<Spec>::default(),
            },
            storage: SlotStorageOf::<Spec>::new(),
        }
    }
}

pub type LocalSlots<Spec> = Box<[LocalSlot<Spec>]>;

pub struct OpRegistry<Spec: SlotSpec> {
    pub shared: Arc<SlotTableOf<Spec>>,
    pub(crate) local: LocalSlots<Spec>,
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

pub type SlotEntryOpBundle<'a, Spec> = (
    &'a SlotEntryOf<Spec>,
    &'a mut OpEntry<RegistryPlatformData<Spec>>,
    &'a mut Option<RegistryOp<Spec>>,
    &'a mut SlotStorageOf<Spec>,
);

pub type SlotEntryAndOpEntry<'a, Spec> = (
    &'a SlotEntryOf<Spec>,
    &'a mut OpEntry<RegistryPlatformData<Spec>>,
);

impl<Spec: SlotSpec> OpRegistry<Spec> {
    pub fn new(capacity: usize) -> Self {
        let shared = Arc::new(SlotTableOf::<Spec>::new(capacity));
        let mut local: Vec<LocalSlot<Spec>> = Vec::with_capacity(capacity);

        for _ in 0..capacity {
            local.push(LocalSlot::new());
        }
        for i in (0..capacity).rev() {
            shared.push_free(i);
        }

        Self {
            shared,
            local: local.into_boxed_slice(),
            local_free_head: SlotTableOf::<Spec>::NULL_INDEX,
            active_count: 0,
        }
    }

    pub fn alloc(
        &mut self,
        data: RegistryPlatformData<Spec>,
    ) -> Result<AllocResult, RegistryPlatformData<Spec>> {
        if self.local_free_head == SlotTableOf::<Spec>::NULL_INDEX {
            self.local_free_head = self.shared.pop_all();
        }

        let mut deferred_non_idle = Vec::new();
        while self.local_free_head != SlotTableOf::<Spec>::NULL_INDEX {
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

    pub fn insert(
        &mut self,
        entry: OpEntry<RegistryPlatformData<Spec>>,
    ) -> Result<OpHandle, OpEntry<RegistryPlatformData<Spec>>> {
        match self.alloc(entry.platform_data) {
            Ok(res) => Ok(res.handle),
            Err(data) => Err(OpEntry {
                platform_data: data,
            }),
        }
    }

    fn get_mut(&mut self, user_data: usize) -> Option<&mut OpEntry<RegistryPlatformData<Spec>>> {
        self.local.get_mut(user_data).map(|v| &mut v.entry)
    }

    pub fn platform_mut_token(
        &mut self,
        token: OpToken,
    ) -> Option<&mut RegistryPlatformData<Spec>> {
        if !self.contains(token) {
            return None;
        }
        self.get_mut(token.index())
            .map(|entry| &mut entry.platform_data)
    }

    fn get_slot_and_entry_mut(
        &mut self,
        user_data: usize,
    ) -> Option<SlotEntryAndOpEntry<'_, Spec>> {
        if user_data < self.local.len() {
            Some((
                &self.shared.slots[user_data],
                &mut self.local[user_data].entry,
            ))
        } else {
            None
        }
    }

    pub fn get_slot_and_entry_mut_token(
        &mut self,
        token: OpToken,
    ) -> Option<SlotEntryAndOpEntry<'_, Spec>> {
        if !self.contains(token) {
            return None;
        }
        self.get_slot_and_entry_mut(token.index())
    }

    pub(crate) fn get_slot_entry_op_storage_and_entry_mut(
        &mut self,
        user_data: usize,
    ) -> Option<SlotEntryOpBundle<'_, Spec>> {
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

    pub fn get_slot_entry_op_storage_and_entry_mut_token(
        &mut self,
        token: OpToken,
    ) -> Option<SlotEntryOpBundle<'_, Spec>> {
        if !self.contains(token) {
            return None;
        }
        self.get_slot_entry_op_storage_and_entry_mut(token.index())
    }

    #[inline]
    pub(crate) fn with_slot_storage_mut<F, X>(&mut self, user_data: usize, f: F) -> Option<X>
    where
        F: FnOnce(
            &mut Option<DriverResult<RegistryCompletion<Spec>, RegistryError<Spec>>>,
            &mut Option<RegistryPayload<Spec>>,
            &mut RegistrySidecar<Spec>,
        ) -> X,
    {
        self.local
            .get_mut(user_data)
            .map(|local| local.storage.with_mut(f))
    }

    #[inline]
    pub fn with_slot_storage_mut_token<F, X>(&mut self, token: OpToken, f: F) -> Option<X>
    where
        F: FnOnce(
            &mut Option<DriverResult<RegistryCompletion<Spec>, RegistryError<Spec>>>,
            &mut Option<RegistryPayload<Spec>>,
            &mut RegistrySidecar<Spec>,
        ) -> X,
    {
        if !self.contains(token) {
            return None;
        }
        self.with_slot_storage_mut(token.index(), f)
    }

    fn slot_storage_mut(&mut self, user_data: usize) -> Option<&mut SlotStorageOf<Spec>> {
        if user_data < self.local.len() {
            Some(&mut self.local[user_data].storage)
        } else {
            None
        }
    }

    pub fn slot_storage_mut_token(&mut self, token: OpToken) -> Option<&mut SlotStorageOf<Spec>> {
        if !self.contains(token) {
            return None;
        }
        self.slot_storage_mut(token.index())
    }

    pub fn contains(&self, token: OpToken) -> bool {
        let (user_data, generation) = token.parts();
        let Some(slot) = self.shared.slots.get(user_data) else {
            return false;
        };
        let core = slot.load_core_state(Ordering::Acquire);
        core.generation() == generation && core.state() != SlotState::Idle
    }

    #[inline]
    pub fn current_token_at_index(&self, index: usize) -> Option<OpToken> {
        let slot = self.shared.slots.get(index)?;
        let core = slot.load_core_state(Ordering::Acquire);
        if core.state() == SlotState::Idle {
            return None;
        }
        Some(OpToken::new(index, core.generation()))
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.local.len()
    }

    fn remove_index(&mut self, user_data: usize) -> OpEntry<RegistryPlatformData<Spec>> {
        let local = &mut self.local[user_data];
        let _ = local.op.take();
        let data = std::mem::take(&mut local.entry.platform_data);
        local.storage.reset();
        self.shared.slots[user_data].free();
        self.shared.push_free(user_data);
        self.active_count = self.active_count.saturating_sub(1);

        OpEntry {
            platform_data: data,
        }
    }

    pub fn remove_token(&mut self, token: OpToken) -> Option<OpEntry<RegistryPlatformData<Spec>>> {
        let (user_data, generation) = token.parts();
        let slot = self.shared.slots.get(user_data)?;
        let core = slot.load_core_state(Ordering::Acquire);
        if core.state() == SlotState::Idle || core.generation() != generation {
            return None;
        }

        Some(self.remove_index(user_data))
    }

    #[inline]
    pub fn finalize_token_checked(
        &mut self,
        token: OpToken,
    ) -> Option<OpEntry<RegistryPlatformData<Spec>>> {
        self.remove_token(token)
    }

    fn recycle_index(&mut self, user_data: usize, generation: u32) {
        let local = &mut self.local[user_data];
        let _ = local.op.take();
        let _ = std::mem::take(&mut local.entry.platform_data);
        local.storage.reset();

        if self.shared.slots[user_data].state(Ordering::Acquire) == SlotState::InFlightReady {
            self.shared.clear_ready_completion();
        }
        self.shared.slots[user_data].reset(generation);
        self.shared.push_free(user_data);
        self.active_count = self.active_count.saturating_sub(1);
    }

    pub fn recycle_token(&mut self, token: OpToken, next_generation: u32) -> bool {
        let (user_data, generation) = token.parts();
        let Some(slot) = self.shared.slots.get(user_data) else {
            return false;
        };
        let core = slot.load_core_state(Ordering::Acquire);
        if core.state() == SlotState::Idle || core.generation() != generation {
            return false;
        }

        self.recycle_index(user_data, next_generation);
        true
    }

    #[inline]
    pub fn finalize_waiting_completion(
        &mut self,
        token: OpToken,
    ) -> Option<OpEntry<RegistryPlatformData<Spec>>> {
        self.remove_token(token)
    }

    #[inline]
    pub fn finalize_orphaned_completion(
        &mut self,
        token: OpToken,
    ) -> Option<OpEntry<RegistryPlatformData<Spec>>> {
        self.remove_token(token)
    }

    #[inline]
    pub fn finalize_corrupt_slot(
        &mut self,
        snapshot: crate::slot::SlotSnapshot,
    ) -> Option<OpEntry<RegistryPlatformData<Spec>>> {
        self.remove_token(OpToken::new(snapshot.index, snapshot.generation))
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

    #[inline]
    pub fn active_count(&self) -> usize {
        self.active_count
    }
}
