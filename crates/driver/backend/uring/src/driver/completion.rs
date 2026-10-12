use crate::{
    config::UringDriveLimits,
    driver::{context::CompletionContext, control::UringPostCompletionEffects, drive::RoundBudget},
    error::{UringResult, uring_report_to_event_res},
};

use veloq_driver_core::driver::{
    AnomalyAttach, CompletionAnomalyKind, CompletionFlowOutcome, SyntheticCompletionSource,
    UserCompletionEvent,
};

mod hooks;
mod ports;
mod slot;

pub(super) use hooks::{
    COMP_BACKEND_URING, CompletionProgress, UringBackendEffect, UringBackendIngress,
};
#[cfg(test)]
pub(crate) use hooks::WakerRecovery;
pub(crate) use hooks::{UringCompletionHooks, UringSyntheticCompletion};
pub(crate) use ports::CompletionBatchProgress;
pub(super) use ports::CompletionScratch;
pub(super) use slot::CompletionSlotRouter;

pub(crate) struct CompletionEngine {
    scratch: CompletionScratch,
}

impl CompletionEngine {
    pub(crate) fn with_capacity(
        cqe_capacity: usize,
        effect_capacity: usize,
        limits: UringDriveLimits,
    ) -> Self {
        Self {
            scratch: CompletionScratch::with_capacity(cqe_capacity, effect_capacity, limits),
        }
    }

    pub(crate) fn has_pending_cqes(&self) -> bool {
        self.scratch.has_pending_cqes()
    }

    pub(crate) const fn drive_limits(&self) -> UringDriveLimits {
        self.scratch.drive_limits()
    }

    pub(crate) fn take_effects(&mut self) -> (UringPostCompletionEffects, bool) {
        self.scratch.take_effects()
    }

    pub(crate) fn recycle_effects(&mut self, effects: UringPostCompletionEffects) {
        self.scratch.recycle_effects(effects);
    }

    pub(crate) fn pending_effects_len(&self) -> usize {
        self.scratch.pending_effects_len()
    }

    pub(crate) fn process_completion_batch(
        &mut self,
        context: &mut CompletionContext<'_, '_, '_, '_>,
        budget: &mut RoundBudget,
    ) -> UringResult<CompletionBatchProgress> {
        self.scratch.process_completion_batch(context, budget)
    }

    pub(crate) fn accept_synthetic_completion(
        &mut self,
        context: &mut CompletionContext<'_, '_, '_, '_>,
        event: UserCompletionEvent,
        source: SyntheticCompletionSource,
        synthetic: UringSyntheticCompletion,
    ) -> UringResult<CompletionFlowOutcome> {
        self.scratch
            .accept_synthetic_completion(context, event, source, synthetic)
    }

    pub(crate) fn accept_completion_anomaly_kind(
        &mut self,
        context: &mut CompletionContext<'_, '_, '_, '_>,
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    ) -> UringResult<CompletionFlowOutcome> {
        self.scratch
            .accept_completion_anomaly_kind(context, kind, attach)
    }

    pub(super) fn driver_result_to_event_res(res: &UringResult<usize>) -> i32 {
        match res {
            Ok(value) => (*value).min(i32::MAX as usize) as i32,
            Err(error) => uring_report_to_event_res(error),
        }
    }
}

#[cfg(all(test, not(feature = "loom")))]
mod tests {
    use crate::config::UringDriveLimits;
    use crate::driver::{
        drive::WaitBudget,
        env::{CompletionLedgerParts, CompletionMetadata},
        registration::{ProvidedBufGroup, ProvidedBufPort},
        submission::WaitBudgetSource,
    };
    use crate::op::{
        Accept, AcceptMulti, Open, ReadRaw, Recv, UringOpRegistry, UringOperationDescriptor,
        WriteRaw,
    };
    use crate::test_alloc::measure;
    use crate::{
        diagnostics::UringCompletionDiagnostics,
        driver::{
            control::{
                ControlPlaneEvent, ControlPlaneObserver, PendingCancel, UringControlEffectKind,
                UringPostCompletionEffects,
            },
            env::CompletionControlView,
        },
        op::{CompletionCleanupHintFn, UringSlotSpec},
    };
    use crate::{
        driver::completion::{
            COMP_BACKEND_URING, CompletionEngine, UringBackendEffect, UringCompletionHooks,
            UringSyntheticCompletion, WakerRecovery,
        },
        error::UringError,
    };
    use veloq_driver_core::driver::{
        CancelTicket, CompletionBackendHooks, CompletionEnvelope, CompletionFlowExt,
        CompletionFlowOutcome,
        CompletionIngress, CompletionSettlement, CompletionToken, DriverCompletionDiagnostics,
        OpToken, RawCompletion, SharedCompletionTable,
    };
    use veloq_driver_core::slot::Generation;
    use veloq_std::{
        collections::HashMap,
        time::Duration,
    };
    use libc::{close, fcntl, pipe};

    #[test]
    fn completion_effect_scratch_recycles_without_allocating() {
        let mut engine = CompletionEngine::with_capacity(8, 8, UringDriveLimits::for_entries(8));
        engine.scratch.prepare_recycle_test(8);

        let (_, counts) = measure(|| {
            let (effects, _) = engine.take_effects();
            engine.recycle_effects(effects);
        });

        assert_eq!(counts.dynamic_allocations(), 0);
        assert_eq!(counts.deallocations, 0);
    }

    struct CompletionHookTestParts<'a> {
        diagnostics: &'a DriverCompletionDiagnostics<UringCompletionDiagnostics>,
        pending_cancel_cqes: &'a mut HashMap<CancelTicket, PendingCancel>,
        completion_cleanup_hints: &'a mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        provided_buffers: Option<&'a mut ProvidedBufGroup>,
        observer: &'a mut ControlPlaneObserver,
        post: &'a mut UringPostCompletionEffects,
    }

    fn test_hooks(parts: CompletionHookTestParts<'_>) -> UringCompletionHooks<'_, '_> {
        let CompletionHookTestParts {
            diagnostics,
            pending_cancel_cqes,
            completion_cleanup_hints,
            provided_buffers,
            observer,
            post,
        } = parts;
        let control = CompletionControlView::new(
            CompletionLedgerParts::new(
                pending_cancel_cqes,
                completion_cleanup_hints,
                observer,
                post,
            ),
            CompletionMetadata::new(8, 0),
        );
        UringCompletionHooks::new(
            diagnostics,
            control,
            provided_buffers.map(ProvidedBufPort::new),
            UringSyntheticCompletion::None,
        )
    }

    fn accept_corrupt(
        registry: &mut UringOpRegistry,
        sidecar: &mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
        token: OpToken,
        result: i32,
        flags: u32,
    ) -> CompletionFlowOutcome {
        let diagnostics = registry.shared.completion_diagnostics();
        let table: SharedCompletionTable<UringSlotSpec> = registry.shared_table();
        let mut pending_cancel_cqes = HashMap::default();
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        for token in sidecar.keys().copied() {
            observer.record(ControlPlaneEvent::CleanupHintInsert(token));
        }
        let mut hooks = test_hooks(CompletionHookTestParts {
            diagnostics: &diagnostics,
            pending_cancel_cqes: &mut pending_cancel_cqes,
            completion_cleanup_hints: sidecar,
            provided_buffers: None,
            observer: &mut observer,
            post: &mut post,
        });
        let envelope = CompletionEnvelope::from_raw_parts(
            COMP_BACKEND_URING,
            CompletionToken::user(token).raw(),
            result,
            flags,
        );
        registry
            .accept_completion(
                &table,
                &diagnostics,
                &mut hooks,
                CompletionIngress::Kernel(envelope),
            )
            .expect("corrupt completion should be handled")
    }

    fn open_test_fds() -> [i32; 2] {
        let mut fds = [-1; 2];
        assert_eq!(unsafe { pipe(fds.as_mut_ptr()) }, 0);
        assert!(fds[0] > 2 && fds[1] > 2);
        fds
    }

    fn assert_closed(fd: i32) {
        assert_eq!(unsafe { fcntl(fd, libc::F_GETFD) }, -1);
    }

    #[test]
    fn waker_control_records_unexpected_byte_count_as_error() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut hooks = test_hooks(CompletionHookTestParts {
            diagnostics: &diagnostics,
            pending_cancel_cqes: &mut pending_cancel_cqes,
            completion_cleanup_hints: &mut completion_cleanup_hints,
            provided_buffers: None,
            observer: &mut observer,
            post: &mut post,
        });
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), 4, 0);

        let outcome = hooks.handle_waker_control(raw);
        let (error, effect) = match outcome {
            CompletionSettlement::TerminalFailure { failure } => (failure.error, failure.effect),
            _ => panic!("unexpected byte count must produce a failed outcome"),
        };
        hooks
            .finish_backend_effect(effect)
            .expect("waker recovery effect should be recorded");

        assert_eq!(*error.inner(), UringError::CompletionWait);
        drop(hooks);
        assert!(
            post.effects()
                .iter()
                .any(|effect| matches!(effect.kind(), UringControlEffectKind::WakerRebuild { .. }))
        );
        assert!(
            post.effects()
                .iter()
                .any(|effect| matches!(effect.kind(), UringControlEffectKind::WakerRearm { .. }))
        );
        assert_eq!(diagnostics.snapshot().backend.waker_error, 1);
    }

    #[test]
    fn waker_control_rearms_successful_completion() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut hooks = test_hooks(CompletionHookTestParts {
            diagnostics: &diagnostics,
            pending_cancel_cqes: &mut pending_cancel_cqes,
            completion_cleanup_hints: &mut completion_cleanup_hints,
            provided_buffers: None,
            observer: &mut observer,
            post: &mut post,
        });
        let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), 8, 0);

        let outcome = hooks.handle_waker_control(raw);
        let effect = match outcome {
            CompletionSettlement::ControlHandled { effect } => effect,
            _ => panic!("a valid eventfd read must be handled successfully"),
        };
        hooks
            .finish_backend_effect(effect)
            .expect("waker rearm effect should be recorded");

        drop(hooks);
        assert!(
            post.effects()
                .iter()
                .any(|effect| matches!(effect.kind(), UringControlEffectKind::WakerRearm { .. }))
        );
        assert!(
            !post
                .effects()
                .iter()
                .any(|effect| matches!(effect.kind(), UringControlEffectKind::WakerRebuild { .. }))
        );
        assert_eq!(diagnostics.snapshot().backend.waker_ok, 1);
        assert_eq!(diagnostics.snapshot().backend.waker_error, 0);
    }

    #[test]
    fn recoverable_waker_errors_still_rearm() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        for res in [-libc::EAGAIN, -libc::EINTR] {
            let mut pending_cancel_cqes = HashMap::default();
            let mut completion_cleanup_hints = HashMap::default();
            let mut observer = ControlPlaneObserver::default();
            let mut post = UringPostCompletionEffects::default();
            let mut hooks = test_hooks(CompletionHookTestParts {
                diagnostics: &diagnostics,
                pending_cancel_cqes: &mut pending_cancel_cqes,
                completion_cleanup_hints: &mut completion_cleanup_hints,
                provided_buffers: None,
                observer: &mut observer,
                post: &mut post,
            });
            let raw = RawCompletion::new(COMP_BACKEND_URING, CompletionToken::waker(0), res, 0);

            let outcome = hooks.handle_waker_control(raw);
            let effect = match outcome {
                CompletionSettlement::ControlHandled { effect } => effect,
                _ => panic!("recoverable eventfd errors must be rearmed"),
            };
            hooks
                .finish_backend_effect(effect)
                .expect("waker rearm effect should be recorded");
            drop(hooks);

        }

        let snapshot = diagnostics.snapshot().backend;
        assert_eq!(snapshot.waker_error, 2);
        assert_eq!(snapshot.waker_rearm, 0);
    }

    #[test]
    fn waker_rearm_effect_is_recorded_for_the_control_executor() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut hooks = test_hooks(CompletionHookTestParts {
            diagnostics: &diagnostics,
            pending_cancel_cqes: &mut pending_cancel_cqes,
            completion_cleanup_hints: &mut completion_cleanup_hints,
            provided_buffers: None,
            observer: &mut observer,
            post: &mut post,
        });

        hooks
            .finish_backend_effect(UringBackendEffect::Waker {
                recovery: WakerRecovery::Rearm,
                generation: 0,
            })
            .expect("waker rearm effect should be recorded");
        drop(hooks);
        assert_eq!(post.effects().len(), 2);
        assert!(matches!(
            post.effects()[0].kind(),
            UringControlEffectKind::WakerRearm { generation: 0 }
        ));
        assert!(matches!(
            post.effects()[1].kind(),
            UringControlEffectKind::BacklogKick
        ));
    }

    #[test]
    fn wait_budget_keeps_the_explicit_deadline_source() {
        assert_eq!(
            WaitBudget::resolve(
                Some(Duration::from_secs(2)),
                Some(Duration::from_secs(3)),
                Duration::from_secs(1),
            ),
            WaitBudget::new(Duration::from_secs(1), WaitBudgetSource::Probe)
        );
        assert_eq!(
            WaitBudget::resolve(
                Some(Duration::from_millis(3)),
                Some(Duration::from_millis(7)),
                Duration::from_secs(1),
            ),
            WaitBudget::new(Duration::from_millis(3), WaitBudgetSource::External,)
        );
        assert_eq!(
            WaitBudget::resolve(None, Some(Duration::from_millis(5)), Duration::from_secs(1),),
            WaitBudget::new(Duration::from_millis(5), WaitBudgetSource::Timer)
        );
    }

    #[test]
    fn untracked_cancel_cqe_is_anomaly_not_user_completion() {
        let diagnostics = DriverCompletionDiagnostics::<UringCompletionDiagnostics>::default();
        let mut pending_cancel_cqes = HashMap::default();
        let mut completion_cleanup_hints = HashMap::default();
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut hooks = test_hooks(CompletionHookTestParts {
            diagnostics: &diagnostics,
            pending_cancel_cqes: &mut pending_cancel_cqes,
            completion_cleanup_hints: &mut completion_cleanup_hints,
            provided_buffers: None,
            observer: &mut observer,
            post: &mut post,
        });
        let cancel_ticket = CancelTicket::try_new(7).expect("test ticket");
        let raw = RawCompletion::new(
            COMP_BACKEND_URING,
            CompletionToken::cancel(cancel_ticket),
            0,
            0,
        );

        let outcome = hooks.handle_cancel_control(cancel_ticket, raw);

        let err = match outcome {
            CompletionSettlement::TerminalFailure { failure } => {
                assert!(matches!(failure.effect, UringBackendEffect::None));
                failure.error
            }
            _ => panic!("untracked cancel must produce a failed outcome"),
        };
        assert_eq!(*err.inner(), UringError::InvalidState);
        assert_eq!(diagnostics.snapshot().backend.cancel_untracked_cqe, 1);
    }

    #[test]
    fn raw_fd_cleanup_hint_is_exposed_only_for_fd_producing_ops() {
        assert!(
            <Open as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_some()
        );
        assert!(
            <Accept as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_some()
        );
        assert!(
            <AcceptMulti as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_some()
        );
        assert!(
            <ReadRaw as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_none()
        );
        assert!(
            <WriteRaw as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_none()
        );
        assert!(
            <Recv as UringOperationDescriptor>::descriptor()
                .erased
                .completion_cleanup_hint
                .is_none()
        );
    }

    #[test]
    fn stale_fd_completion_is_closed_and_final_hint_is_removed() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();
        let fd = fd_pair[0];
        let hint = <Open as UringOperationDescriptor>::descriptor()
            .erased
            .completion_cleanup_hint
            .expect("open must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), Some(hint));

        let outcome = accept_corrupt(&mut registry, &mut sidecar, token, fd, 0);

        assert_eq!(outcome.anomaly, 1);
        assert!(sidecar.is_empty());
        assert_closed(fd);
        unsafe { close(fd_pair[1]) };
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 1);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 1);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 0);
    }

    #[test]
    fn negative_fd_completion_does_not_close_or_retain_hint() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();
        let hint = <Accept as UringOperationDescriptor>::descriptor()
            .erased
            .completion_cleanup_hint
            .expect("accept must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), Some(hint));

        accept_corrupt(&mut registry, &mut sidecar, token, -libc::ECANCELED, 0);

        assert!(sidecar.is_empty());
        assert_ne!(unsafe { fcntl(fd_pair[0], libc::F_GETFD) }, -1);
        unsafe {
            close(fd_pair[0]);
            close(fd_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 1);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 0);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 0);
    }

    #[test]
    fn corrupt_completion_returns_selected_provided_buffer() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let mut sidecar = HashMap::default();
        let diagnostics = registry.shared.completion_diagnostics();
        let mut pending_cancel_cqes = HashMap::default();
        let mut observer = ControlPlaneObserver::default();
        let mut post = UringPostCompletionEffects::default();
        let mut provided_buffers = ProvidedBufGroup::for_test(2);
        let before = provided_buffers.stats();
        let table: SharedCompletionTable<UringSlotSpec> = registry.shared_table();
        let mut hooks = test_hooks(CompletionHookTestParts {
            diagnostics: &diagnostics,
            pending_cancel_cqes: &mut pending_cancel_cqes,
            completion_cleanup_hints: &mut sidecar,
            provided_buffers: Some(&mut provided_buffers),
            observer: &mut observer,
            post: &mut post,
        });
        let flags = 1 | (1 << 16);
        let envelope = CompletionEnvelope::from_raw_parts(
            COMP_BACKEND_URING,
            CompletionToken::user(token).raw(),
            7,
            flags,
        );

        let outcome = registry
            .accept_completion(
                &table,
                &diagnostics,
                &mut hooks,
                CompletionIngress::Kernel(envelope),
            )
            .expect("corrupt completion should be handled");

        assert_eq!(outcome.anomaly, 1);
        assert_eq!(provided_buffers.stats().returned(), before.returned() + 1);
        assert_eq!(provided_buffers.stats().available(), before.available());
    }

    #[test]
    fn stale_non_fd_completion_does_not_close_a_positive_result() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();
        let fd = fd_pair[0];
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), None);

        let outcome = accept_corrupt(&mut registry, &mut sidecar, token, fd, 0);

        assert_eq!(outcome.anomaly, 1);
        assert!(sidecar.is_empty());
        assert_ne!(unsafe { fcntl(fd, libc::F_GETFD) }, -1);
        unsafe {
            close(fd);
            close(fd_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 0);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 0);
    }

    #[test]
    fn missing_fd_cleanup_hint_does_not_guess_close() {
        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let fd_pair = open_test_fds();

        accept_corrupt(&mut registry, &mut HashMap::default(), token, fd_pair[0], 0);

        assert_ne!(unsafe { fcntl(fd_pair[0], libc::F_GETFD) }, -1);
        unsafe {
            close(fd_pair[0]);
            close(fd_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 0);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 0);
        assert_eq!(snapshot.corrupt_cleanup_hint_missing, 1);
    }

    #[test]
    fn multishot_corrupt_completions_close_each_fd_and_retain_hint_until_final() {
        const CQE_MORE: u32 = 2;

        let mut registry = UringOpRegistry::new(1);
        let token = OpToken::from_registry_parts(0, Generation::new(1)).expect("test token");
        let first_pair = open_test_fds();
        let second_pair = open_test_fds();
        let hint = <AcceptMulti as UringOperationDescriptor>::descriptor()
            .erased
            .completion_cleanup_hint
            .expect("accept multi must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(token), Some(hint));

        let first = accept_corrupt(&mut registry, &mut sidecar, token, first_pair[0], CQE_MORE);
        assert_eq!(first.anomaly, 1);
        assert_eq!(sidecar.len(), 1);
        assert_closed(first_pair[0]);

        let final_outcome = accept_corrupt(&mut registry, &mut sidecar, token, second_pair[0], 0);
        assert_eq!(final_outcome.anomaly, 1);
        assert!(sidecar.is_empty());
        assert_closed(second_pair[0]);
        unsafe {
            close(first_pair[1]);
            close(second_pair[1]);
        }
        let snapshot = registry.shared.completion_diagnostics().snapshot().backend;
        assert_eq!(snapshot.corrupt_cleanup_attempts, 2);
        assert_eq!(snapshot.corrupt_raw_fd_cleanups, 2);
    }

    #[test]
    fn generation_reuse_keeps_old_and_new_cleanup_hints_separate() {
        let mut registry = UringOpRegistry::new(1);
        let old_token = OpToken::from_registry_parts(0, Generation::new(1)).expect("old token");
        let new_token = OpToken::from_registry_parts(0, Generation::new(2)).expect("new token");
        let old_pair = open_test_fds();
        let new_pair = open_test_fds();
        let hint = <Accept as UringOperationDescriptor>::descriptor()
            .erased
            .completion_cleanup_hint
            .expect("accept must expose a cleanup hint");
        let mut sidecar = HashMap::default();
        sidecar.insert(CompletionToken::user(old_token), Some(hint));
        sidecar.insert(CompletionToken::user(new_token), Some(hint));

        accept_corrupt(&mut registry, &mut sidecar, old_token, old_pair[0], 0);
        assert_closed(old_pair[0]);
        assert_eq!(sidecar.len(), 1);
        assert!(sidecar.contains_key(&CompletionToken::user(new_token)));

        accept_corrupt(&mut registry, &mut sidecar, new_token, new_pair[0], 0);
        assert_closed(new_pair[0]);
        assert!(sidecar.is_empty());
        unsafe {
            close(old_pair[1]);
            close(new_pair[1]);
        }
    }
}
