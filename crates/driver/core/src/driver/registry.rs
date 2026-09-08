use crate::{
    DriverResult,
    driver::OpToken,
    slot::{
        Generation, SlotCompletion, SlotEntry, SlotError, SlotOp, SlotPayload, SlotPlatformData,
        SlotSidecarData, SlotSnapshot, SlotSpec, SlotState, SlotStorage, SlotTable,
    },
};
use veloq_std::{
    boxed::Box,
    mem,
    sync::{Arc, atomic::Ordering},
    vec::Vec,
};

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
    pub(crate) active: bool,
    pub(crate) generation: Generation,
}

impl<Spec: SlotSpec> LocalSlot<Spec> {
    fn new() -> Self {
        Self {
            op: None,
            entry: OpEntry {
                platform_data: RegistryPlatformData::<Spec>::default(),
            },
            storage: SlotStorageOf::<Spec>::new(),
            active: false,
            generation: Generation::ZERO,
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
    pub generation: Generation,
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
            if !slot.status(Ordering::Acquire).is_idle() {
                // 仍在途、或信箱里压着一条未被消费的完成（detached future 随时可能来
                // 取），两种情况都还不能复用这个 slot。
                deferred_non_idle.push(idx);
                continue;
            }

            let new_gen = slot.generation(Ordering::Relaxed).next();
            slot.reset(new_gen);
            slot.set_state(SlotState::Reserved, Ordering::Release);

            self.local[idx].active = true;
            self.local[idx].generation = new_gen;
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

    pub fn platform_mut(&mut self, token: OpToken) -> Option<&mut RegistryPlatformData<Spec>> {
        self.active_slot_bundle_mut(token)
            .map(|(_, entry, _, _)| &mut entry.platform_data)
    }

    pub fn active_slot_and_entry_mut(
        &mut self,
        token: OpToken,
    ) -> Option<SlotEntryAndOpEntry<'_, Spec>> {
        if !self.is_current_active(token) {
            return None;
        }

        let index = token.index();
        let slot = self.shared.slots.get(index)?;
        let local = self.local.get_mut(index)?;
        Some((slot, &mut local.entry))
    }

    pub fn slot_bundle_by_index_mut(
        &mut self,
        index: usize,
    ) -> Option<SlotEntryOpBundle<'_, Spec>> {
        let slot = self.shared.slots.get(index)?;
        let local = self.local.get_mut(index)?;
        Some((slot, &mut local.entry, &mut local.op, &mut local.storage))
    }

    pub fn active_slot_bundle_mut(
        &mut self,
        token: OpToken,
    ) -> Option<SlotEntryOpBundle<'_, Spec>> {
        if !self.is_current_active(token) {
            return None;
        }

        self.slot_bundle_by_index_mut(token.index())
    }

    pub fn with_slot_storage_mut<F, X>(&mut self, token: OpToken, f: F) -> Option<X>
    where
        F: FnOnce(
            &mut Option<DriverResult<RegistryCompletion<Spec>, RegistryError<Spec>>>,
            &mut Option<RegistryPayload<Spec>>,
            &mut RegistrySidecar<Spec>,
        ) -> X,
    {
        if !self.is_current_active(token) {
            return None;
        }
        self.local
            .get_mut(token.index())
            .map(|local| local.storage.with_mut(f))
    }

    pub fn slot_storage_mut(&mut self, token: OpToken) -> Option<&mut SlotStorageOf<Spec>> {
        if !self.is_current_active(token) {
            return None;
        }
        self.local
            .get_mut(token.index())
            .map(|local| &mut local.storage)
    }

    pub fn is_current_active(&self, token: OpToken) -> bool {
        let (user_data, generation) = token.parts();
        let Some(slot) = self.shared.slots.get(user_data) else {
            return false;
        };
        let core = slot.load_core_state(Ordering::Acquire);
        core.generation() == generation && !core.status().is_idle()
    }

    pub fn active_tokens(&self) -> impl Iterator<Item = OpToken> + '_ {
        self.shared
            .slots
            .iter()
            .enumerate()
            .filter_map(|(index, slot)| {
                let core = slot.load_core_state(Ordering::Acquire);
                (!core.status().is_idle())
                    .then(|| OpToken::from_registry_parts(index, core.generation()).ok())
                    .flatten()
            })
    }

    pub fn capacity(&self) -> usize {
        self.local.len()
    }

    fn remove_at_index(&mut self, user_data: usize) -> OpEntry<RegistryPlatformData<Spec>> {
        let local = &mut self.local[user_data];
        local.active = false;
        let _ = local.op.take();
        let data = mem::take(&mut local.entry.platform_data);
        local.storage.reset();
        self.shared.slots[user_data].free();
        self.shared.push_free(user_data);
        self.active_count = self.active_count.saturating_sub(1);

        OpEntry {
            platform_data: data,
        }
    }

    pub fn remove(&mut self, token: OpToken) -> Option<OpEntry<RegistryPlatformData<Spec>>> {
        let (user_data, generation) = token.parts();
        let local = self.local.get(user_data)?;
        if !local.active || local.generation != generation {
            return None;
        }

        Some(self.remove_at_index(user_data))
    }

    fn recycle_at_index(&mut self, user_data: usize, generation: Generation) {
        let local = &mut self.local[user_data];
        local.active = false;
        let _ = local.op.take();
        let _ = mem::take(&mut local.entry.platform_data);
        local.storage.reset();

        let cell = &self.shared.slots[user_data];
        if cell.status(Ordering::Acquire).ready {
            // `reset` 会清掉 `ready` 标志位，但不动信箱本身。已就绪的完成必须在这里
            // 取出并清理，否则其 payload（含 `FixedBuf`）会一直压在 slot 里，直到该
            // slot 下一次被 `record_completion` 覆盖才释放——slot 若不再被复用就是
            // 永久泄漏。multishot 可能压着不止一条，所以是排空而不是取一条。
            self.shared.drain_mailbox_cleanup(cell);
        }
        self.shared.slots[user_data].reset(generation);
        self.shared.push_free(user_data);
        self.active_count = self.active_count.saturating_sub(1);
    }

    /// 强制把 slot 收回 `Idle` 并推进到 `next_generation`，**丢弃**其上任何已就绪的
    /// 完成（连同其 payload 与 cleanup）。
    ///
    /// 与 [`Self::remove`] 的区别是后者只把生命周期归还成 `Idle`、保留信箱上的 `ready`
    /// 标志位，让已到达的完成仍可被 detached future 消费。释放一个尚未提交的预留 slot
    /// 应当用 `remove`。
    pub fn recycle(&mut self, token: OpToken, next_generation: Generation) -> bool {
        let (user_data, generation) = token.parts();
        let local = match self.local.get(user_data) {
            Some(v) => v,
            None => return false,
        };
        if !local.active || local.generation != generation {
            return false;
        }

        self.recycle_at_index(user_data, next_generation);
        true
    }

    /// 最终化一个处于等待状态的 Operation 完成，将其从注册表中移除。
    pub fn finalize_waiting_completion(
        &mut self,
        token: OpToken,
    ) -> Option<OpEntry<RegistryPlatformData<Spec>>> {
        self.remove(token)
    }

    /// 最终化一个处于孤立（Orphaned）状态的 Operation 完成，将其从注册表中移除。
    pub fn finalize_orphaned_completion(
        &mut self,
        token: OpToken,
    ) -> Option<OpEntry<RegistryPlatformData<Spec>>> {
        self.remove(token)
    }

    pub fn finalize_corrupt_slot(
        &mut self,
        snapshot: SlotSnapshot,
    ) -> Option<OpEntry<RegistryPlatformData<Spec>>> {
        self.remove(OpToken::from_registry_parts(snapshot.index, snapshot.generation).ok()?)
    }

    pub fn has_active_ops(&self) -> bool {
        self.active_count > 0
    }

    pub fn active_count(&self) -> usize {
        self.active_count
    }
}

#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod tests {
    use super::*;
    use crate::{DriverCoreError, DriverError, driver::PlatformOp};
    use diagweave::prelude::*;
    use veloq_std::{error::Error, fmt, vec};

    struct DummyPlatformOp;

    impl PlatformOp for DummyPlatformOp {
        type CleanupContext<'a> = ();
    }

    struct DummySlotSpec;

    #[derive(Debug, Copy, Clone, PartialEq, Eq)]
    struct DummyError;

    impl fmt::Display for DummyError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "dummy error")
        }
    }

    impl Error for DummyError {}

    impl DriverError for DummyError {
        #[inline]
        fn from_core_report(report: Report<DriverCoreError>) -> Report<Self> {
            report.map_err(|_| DummyError)
        }
    }

    impl SlotSpec for DummySlotSpec {
        type Op = DummyPlatformOp;
        type UserPayload = ();
        type PlatformData = ();
        type Sidecar = ();
        type Error = DummyError;
        type Completion = usize;
        type CompletionDiagnostics = ();
    }

    #[test]
    fn active_tokens_iterates_non_idle_slots() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(3);
        let first = registry.alloc(()).expect("first slot").handle;
        let second = registry.alloc(()).expect("second slot").handle;
        let first_token = OpToken::from_registry_parts(first.index, first.generation)
            .expect("first token should be encodable");
        let second_token = OpToken::from_registry_parts(second.index, second.generation)
            .expect("second token should be encodable");

        let tokens = registry.active_tokens().collect::<Vec<_>>();

        assert_eq!(tokens.len(), 2);
        assert!(tokens.contains(&first_token));
        assert!(tokens.contains(&second_token));

        let _ = registry.remove(first_token);
        let tokens = registry.active_tokens().collect::<Vec<_>>();

        assert_eq!(tokens, vec![second_token]);
    }

    #[test]
    fn idempotent_remove_prevents_double_free() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(3);
        let alloc_res = registry.alloc(()).expect("alloc slot");
        let token =
            OpToken::from_registry_parts(alloc_res.handle.index, alloc_res.handle.generation)
                .expect("token");

        // 第一次 remove 成功，返回 platform_data
        let first_remove = registry.remove(token);
        assert!(first_remove.is_some());

        // 第二次 remove 由于已非 active，应返回 None，且不会导致重复 push_free
        let second_remove = registry.remove(token);
        assert!(second_remove.is_none());
    }

    #[test]
    fn finalize_waiting_completion_is_noop_after_remove() {
        let mut registry = OpRegistry::<DummySlotSpec>::new(3);
        let alloc_res = registry.alloc(()).expect("alloc slot");
        let token =
            OpToken::from_registry_parts(alloc_res.handle.index, alloc_res.handle.generation)
                .expect("token");

        // 先通过 remove 回收
        assert!(registry.remove(token).is_some());

        // 随后再调用 finalize_waiting_completion 应该幂等返回 None
        assert!(registry.finalize_waiting_completion(token).is_none());
    }
}
