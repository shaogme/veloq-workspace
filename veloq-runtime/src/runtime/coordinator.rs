use super::context::{IdleDecision, IdleWaitStrategy};
use super::primitives::Parker;
use super::shared::RuntimeShared;
use crate::scope::GenericScopeCompletion;
use crate::utils::ownership::Ownership;
use crate::utils::storage::Storage;
use std::time::Duration;

pub(crate) struct RuntimeProgressCoordinator<'a, T> {
    shared: &'a RuntimeShared<T>,
    worker_id: usize,
}

impl<'a, T: crate::runtime::context::RuntimeContextExtra> RuntimeProgressCoordinator<'a, T> {
    pub(crate) fn new(shared: &'a RuntimeShared<T>, worker_id: usize) -> Self {
        Self { shared, worker_id }
    }

    pub(crate) fn run<S: Storage, O: Ownership>(
        &self,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) {
        let idle_decision = self
            .shared
            .idle_hook
            .map(|h| h(self.shared))
            .unwrap_or(IdleDecision::wait(IdleWaitStrategy::Block));
        if idle_decision.is_continue() {
            std::thread::yield_now();
            return;
        }

        let base = &self.shared.base;
        let group_idx = base.topo.worker_to_group[self.worker_id];
        let group = &base.topo.groups[group_idx];
        let seq = base.idle.event_count.load();

        base.idle.idle_mask.set(self.worker_id);
        group.idle_stack.push(self.worker_id, &base.topo.next_idle);

        if self.should_retry(seq, completion) {
            self.leave_idle(group_idx);
            return;
        }

        if let Some(task) = base.scheduler.pop_global() {
            self.leave_idle(group_idx);
            base.poll_send_task(self.worker_id, task);
            return;
        }

        self.park(idle_decision, completion);
        self.leave_idle(group_idx);
    }

    fn should_retry<S: Storage, O: Ownership>(
        &self,
        seq: usize,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) -> bool {
        let base = &self.shared.base;
        base.idle.event_count.load() != seq
            || base.has_work(self.worker_id)
            || base.shutdown.load(std::sync::atomic::Ordering::Acquire)
            || completion.map(|c| c.is_done()).unwrap_or(false)
    }

    fn park<S: Storage, O: Ownership>(
        &self,
        idle_decision: IdleDecision,
        completion: Option<&GenericScopeCompletion<S, O>>,
    ) {
        let base = &self.shared.base;
        let parker = Parker::from_inner(base.registry.parker_inners[self.worker_id].clone());
        match idle_decision {
            IdleDecision::Wait(IdleWaitStrategy::Timeout(duration)) => {
                let _ = parker.park_timeout(duration);
            }
            IdleDecision::Wait(IdleWaitStrategy::Block) => {
                if completion.is_some() {
                    let _ = parker.park_timeout(Duration::from_millis(1));
                } else {
                    parker.park();
                }
            }
            IdleDecision::Continue => unreachable!(),
        }
    }

    fn leave_idle(&self, group_idx: usize) {
        let base = &self.shared.base;
        let _ = base.topo.groups[group_idx]
            .idle_stack
            .pop(&base.topo.next_idle);
        base.idle.idle_mask.clear(self.worker_id);
    }
}
