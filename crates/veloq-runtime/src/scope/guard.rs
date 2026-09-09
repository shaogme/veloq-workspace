use super::completion::GenericScopeCompletion;
use crate::{
    task::{GenericTaskHeader, ScopeStorage},
    utils::ownership::Ownership,
};
use veloq_storage::Storage;

/// 一次 spawn 在 scope 上的待完成义务，通过 RAII 保证 `add_task` 与 `task_done` 配对。
pub(crate) struct ScopeTaskGuard<S: ScopeStorage, O: Ownership> {
    completion: O::Shared<GenericScopeCompletion<S, O>>,
    armed: bool,
}

impl<S: ScopeStorage, O: Ownership> ScopeTaskGuard<S, O> {
    pub(crate) fn new(completion: &O::Shared<GenericScopeCompletion<S, O>>) -> Self {
        completion.register_task();
        Self {
            completion: completion.clone(),
            armed: true,
        }
    }

    pub(crate) fn completion(&self) -> &O::Shared<GenericScopeCompletion<S, O>> {
        &self.completion
    }

    pub(crate) fn is_armed(&self) -> bool {
        self.armed
    }

    /// 将义务移交给已初始化的 task header；之后由终态状态机结算。
    pub(crate) fn handoff_to<H: Storage>(&mut self, header: &GenericTaskHeader<H>) {
        debug_assert!(self.armed, "scope guard already disarmed");
        header.claim_scope_obligation();
        self.armed = false;
    }

    /// 早退路径：无 header 或 header 不会 ack 时，由 guard 直接结算 scope。
    pub(crate) fn settle(&mut self) {
        if self.armed {
            self.armed = false;
            self.completion.settle_task();
        }
    }
}

impl<S: ScopeStorage, O: Ownership> Drop for ScopeTaskGuard<S, O> {
    fn drop(&mut self) {
        if self.armed {
            self.completion.settle_task();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::utils::ownership::ArcOwnership;
    use veloq_storage::AtomicStorage;

    #[test]
    fn guard_settles_on_drop() {
        let completion = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        {
            let _guard = ScopeTaskGuard::<AtomicStorage, ArcOwnership>::new(&completion);
            assert!(!completion.is_done());
        }
        assert!(completion.is_done());
    }

    #[test]
    fn guard_settle_is_idempotent() {
        let completion = GenericScopeCompletion::<AtomicStorage, ArcOwnership>::new(None);
        let mut guard = ScopeTaskGuard::<AtomicStorage, ArcOwnership>::new(&completion);
        guard.settle();
        assert!(completion.is_done());
        guard.settle();
        assert!(completion.is_done());
    }
}
