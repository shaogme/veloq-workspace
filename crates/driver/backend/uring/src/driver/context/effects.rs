use veloq_std::{format, time::Instant};

use diagweave::prelude::*;
use tracing::trace;

use crate::{
    config::{IoFd, RawHandle, UringRawHandle},
    driver::{
        completion::CompletionEngine,
        context::{DrivePortAccess, DrivePorts},
        control::{
            ControlPlaneEvent, DeferredCancelReconcile, UringControlEffectKind,
            UringPostCompletionEffects, UringWakerManager,
        },
        lifecycle::{CancellationPhase, LifecycleEngine, SubmissionPhase},
        registration::RegistrationPorts,
    },
    error::{UringError, UringResult},
    op::{CheckedSlotView, RecordPolicy, SlotView, UringOpRegistryExt},
};

pub(crate) struct CompletionEffectExecutor;

struct CompletionDrainState<'completion> {
    completion: &'completion CompletionEngine,
    collector_exhausted: bool,
}

impl CompletionEffectExecutor {
    /// 按固定资源顺序执行 completion ingress 产生的 effects。
    /// Execute effects from completion ingress in a fixed resource order.
    ///
    /// completion 已结算 slot 并交出 effect batch，因此可以更新资源 owner 而不持有 CQE 或 operation 借用。
    /// Completion has settled the slot and returned the effect batch, so resource owners can change without a live CQE or operation borrow.
    pub(crate) fn execute(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        completion: &CompletionEngine,
        effects: &UringPostCompletionEffects,
        collector_exhausted: bool,
    ) -> UringResult<()> {
        let mut first_error = None;
        if effects.is_overflowed() {
            ports.diagnostics.backend().inc_completion_effect_overflow();
            Self::remember_first_error(
                &mut first_error,
                UringError::InvalidState
                    .report(
                        "uring.drive.completion_effects",
                        "completion effect batch capacity was exhausted",
                    )
                    .attach_note("completion effects were truncated; the driver entered fail-stop"),
            );
        }

        Self::execute_bookkeeping_effects(
            ports,
            lifecycle,
            effects,
            CompletionDrainState {
                completion,
                collector_exhausted,
            },
            &mut first_error,
        );
        Self::execute_resource_effects(ports, effects, &mut first_error);
        Self::execute_close_effects(ports, effects, &mut first_error);
        Self::execute_waker_effects(ports, effects, &mut first_error);
        if effects.has_backlog_kick() {
            trace!("completion batch requested a bounded backlog submit round");
        }

        if let Some(error) = first_error {
            Err(error)
        } else {
            Ok(())
        }
    }

    fn execute_bookkeeping_effects(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        effects: &UringPostCompletionEffects,
        drain: CompletionDrainState<'_>,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects.iter() {
            match effect.kind() {
                UringControlEffectKind::CancelAck {
                    cancel_ticket,
                    phase,
                } => {
                    if let Some(target) = effect.token()
                        && let Err(error) = DrivePortAccess::with_lifecycle(
                            ports,
                            lifecycle,
                            |lifecycle, context| {
                                lifecycle.set_cancel_phase(context, target, phase);
                                Ok(())
                            },
                        )
                    {
                        Self::remember_first_error(first_error, error);
                    }
                    if let Some(target) = effect.token() {
                        let _ = ports
                            .control
                            .cancel_ticket_for_completion(cancel_ticket, target);
                    }
                }
                UringControlEffectKind::CancelReconcile {
                    cancel_ticket,
                    request,
                    raw,
                } => {
                    ports.diagnostics.backend().inc_cancel_reconcile_deferred();
                    if let Err(()) =
                        ports
                            .control
                            .defer_cancel_reconcile(cancel_ticket, request, raw)
                    {
                        Self::remember_first_error(
                            first_error,
                            UringError::InvalidState
                                .report(
                                    "uring.cancel.reconcile",
                                    "cancel reconcile ledger is full",
                                )
                                .attach_note(
                                    "the target remains quarantined because its ENOENT could not be tracked",
                                ),
                        );
                    }
                }
                _ => {}
            }
        }
        Self::reconcile_deferred_cancels(
            ports,
            lifecycle,
            drain.completion,
            drain.collector_exhausted,
            first_error,
        );
    }

    fn execute_resource_effects(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        effects: &UringPostCompletionEffects,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects.iter() {
            let UringControlEffectKind::UdpRearm {
                logical_receiver_generation,
            } = effect.kind()
            else {
                continue;
            };
            let Some(token) = effect.token() else {
                Self::remember_first_error(
                    first_error,
                    UringError::InvalidState.report(
                        "uring.drive.udp_rearm",
                        "UDP rearm effect has no operation token",
                    ),
                );
                continue;
            };
            let valid = match ports.ops.checked_slot_view(token) {
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(mut slot))) => {
                    let generation_matches = slot
                        .with_access_mut(|access| {
                            let descriptor = access.operation().get_ref().descriptor();
                            descriptor.record_policy == RecordPolicy::UdpMultishot
                                && unsafe { (descriptor.receive_generation)(access) }
                                    == Some(logical_receiver_generation)
                        })
                        .unwrap_or(false);
                    generation_matches
                        && slot.platform().submission_phase() == SubmissionPhase::KernelOutstanding
                }
                Ok(CheckedSlotView::Valid(SlotView::Reserved(_)))
                | Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(_)))
                | Ok(CheckedSlotView::Empty(_))
                | Ok(CheckedSlotView::Missing { .. })
                | Ok(CheckedSlotView::Stale(_))
                | Err(_) => false,
            };
            if !valid {
                continue;
            }
            if let Err(report) = ports.ops.transition_submission(
                token,
                SubmissionPhase::Reserved,
                "UDP receive permit became available",
                ports.control.observer_mut(),
            ) {
                Self::remember_first_error(first_error, report);
                continue;
            }
            if let Err(error) = ports.control.push_backlog(token) {
                Self::remember_first_error(
                    first_error,
                    UringError::InvalidState
                        .report("uring.drive.udp_rearm", "failed to queue UDP rearm")
                        .with_ctx("backlog_error", format!("{error:?}")),
                );
            }
        }
    }

    fn execute_close_effects(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        effects: &UringPostCompletionEffects,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects.iter() {
            let UringControlEffectKind::CloseUnregister { ticket } = effect.kind() else {
                continue;
            };
            if effect.token() != Some(ticket.operation()) {
                Self::remember_first_error(
                    first_error,
                    UringError::InvalidState
                        .report(
                            "uring.drive.close_effect",
                            "close ownership ticket does not match effect token",
                        )
                        .attach_note("late close completion cannot consume another operation's fd"),
                );
                continue;
            }
            let submitter = ports.ring.submitter();
            let registration_ports =
                RegistrationPorts::new(&submitter, ports.diagnostics.backend());
            if let Err(report) = ports
                .registration
                .unregister_close_owned_fd(&registration_ports, ticket.fd())
            {
                Self::remember_first_error(first_error, report);
            }
        }
    }

    fn execute_waker_effects(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        effects: &UringPostCompletionEffects,
        first_error: &mut Option<Report<UringError>>,
    ) {
        for effect in effects.iter() {
            if let UringControlEffectKind::WakerRebuild { generation } = effect.kind() {
                if generation != ports.control.waker().armed_generation() {
                    continue;
                }
                ports.diagnostics.backend().inc_waker_rebuild();
                if let Err(report) =
                    Self::rebuild_waker_fd(ports).attach_note("failed to rebuild eventfd waker")
                {
                    Self::remember_first_error(first_error, report);
                }
            }
        }

        for effect in effects.iter() {
            if let UringControlEffectKind::WakerRearm { generation } = effect.kind()
                && ports.control.waker_mut().prepare_rearm(generation)
            {
                ports
                    .control
                    .record(ControlPlaneEvent::WakerArm { armed: false });
                ports.control.record(ControlPlaneEvent::WakerRearmRequested);
                ports.control.set_waker_stage_pending(true);
                ports.diagnostics.backend().inc_waker_rearm();
            }
        }
    }

    fn rebuild_waker_fd(ports: &mut DrivePorts<'_, '_, '_, '_>) -> UringResult<()> {
        if ports.registration.file_table_is_poisoned() {
            return Err(ports.registration.file_table_poisoned_report(
                "driver.rebuild_waker_fd",
                ports.control.waker().registered_fd(),
            ));
        }
        let new_fd = UringWakerManager::create_event_fd("driver.rebuild_waker_fd.eventfd")?;
        let raw = RawHandle::new(UringRawHandle::for_file(new_fd.raw().as_fd()));
        let registered_fd = ports.control.waker().registered_fd();
        let state = ports.control.waker().state();

        state.with_lock(|current_fd| {
            if ports.control.waker().has_pending_notification() {
                UringWakerManager::write_event_fd(&new_fd)?;
            }

            match registered_fd {
                Some(fd @ IoFd::Registered { .. }) => {
                    let submitter = ports.ring.submitter();
                    let registration_ports =
                        RegistrationPorts::new(&submitter, ports.diagnostics.backend());
                    ports
                        .registration
                        .replace_registered_fixed_fd(&registration_ports, fd, raw)?;
                }
                Some(IoFd::Direct(_)) => {
                    ports
                        .control
                        .waker_mut()
                        .set_registered_fd(Some(IoFd::direct(raw.raw())));
                }
                Some(IoFd::OwnedDirect { .. }) => {
                    return Err(UringError::InvalidState
                        .report(
                            "uring.rebuild_waker_fd",
                            "owned direct waker descriptors are unsupported",
                        )
                        .with_ctx("fd", format!("{registered_fd:?}")));
                }
                None => {}
            }

            *current_fd = new_fd.clone();
            Ok(())
        })
    }

    fn reconcile_deferred_cancels(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        completion: &CompletionEngine,
        collector_exhausted: bool,
        first_error: &mut Option<Report<UringError>>,
    ) {
        let timeout = completion.drive_limits().cancel_reconcile_timeout;
        let mut index = 0;
        while index < ports.control.deferred_cancel_reconciles().len() {
            let entry = ports.control.deferred_cancel_reconciles()[index];
            let request = entry.request();
            let active = match ports.ops.checked_slot_view(request.target()) {
                Ok(CheckedSlotView::Valid(SlotView::Reserved(slot))) => Some(slot.snapshot()),
                Ok(CheckedSlotView::Valid(SlotView::InFlightWaiting(slot))) => {
                    Some(slot.snapshot())
                }
                Ok(CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot))) => {
                    Some(slot.snapshot())
                }
                Ok(CheckedSlotView::Empty(_))
                | Ok(CheckedSlotView::Missing { .. })
                | Ok(CheckedSlotView::Stale(_)) => None,
                Err(report) => {
                    Self::remember_first_error(first_error, report);
                    index += 1;
                    continue;
                }
            };
            let watchdog_expired =
                Instant::now().saturating_duration_since(entry.since()) >= timeout;
            if !collector_exhausted && !watchdog_expired {
                index += 1;
                continue;
            }

            if active.is_none() {
                let _ = ports.control.remove_deferred_cancel_reconcile(index);
                let _ = ports
                    .control
                    .cancel_ticket_for_completion(entry.cancel_ticket(), request.target());
                continue;
            }
            if watchdog_expired && !collector_exhausted {
                ports.diagnostics.backend().inc_cancel_reconcile_timeout();
            }
            match Self::quarantine_cancel_target(ports, lifecycle, entry) {
                Ok(()) => {
                    let _ = ports.control.remove_deferred_cancel_reconcile(index);
                    let _ = ports
                        .control
                        .cancel_ticket_for_completion(entry.cancel_ticket(), request.target());
                }
                Err(report) => {
                    Self::remember_first_error(first_error, report);
                    index += 1;
                }
            }
        }
    }

    fn quarantine_cancel_target(
        ports: &mut DrivePorts<'_, '_, '_, '_>,
        lifecycle: &mut LifecycleEngine,
        entry: DeferredCancelReconcile,
    ) -> UringResult<()> {
        let request = entry.request();
        let snapshot = match ports.ops.checked_slot_view(request.target())? {
            CheckedSlotView::Valid(SlotView::InFlightWaiting(slot)) => {
                let snapshot = slot.snapshot();
                if !ports
                    .completion_table
                    .mark_orphaned(request.target())
                    .is_applied()
                {
                    return Err(UringError::InvalidState.report(
                        "uring.cancel.reconcile.quarantine",
                        "unable to move active cancel target to orphaned state",
                    ));
                }
                snapshot
            }
            CheckedSlotView::Valid(SlotView::InFlightOrphaned(slot)) => slot.snapshot(),
            CheckedSlotView::Valid(SlotView::Reserved(slot)) => slot.snapshot(),
            CheckedSlotView::Empty(_)
            | CheckedSlotView::Missing { .. }
            | CheckedSlotView::Stale(_) => return Ok(()),
        };
        ports.diagnostics.backend().inc_cancel_ack_enoent_active();
        ports.control.quarantine_token(request.target());
        DrivePortAccess::with_lifecycle(ports, lifecycle, |lifecycle, context| {
            lifecycle.set_cancel_phase(context, request.target(), CancellationPhase::NotFound);
            Ok(())
        })?;
        Err(UringError::InvalidState
            .report(
                "uring.cancel.reconcile",
                "io_uring cancel returned ENOENT while target remained active",
            )
            .with_ctx("cancel_ticket", entry.cancel_ticket().raw())
            .with_ctx("expected_index", request.target().index())
            .with_ctx("expected_generation", request.target().generation())
            .with_ctx("actual_index", snapshot.index)
            .with_ctx("actual_generation", snapshot.generation)
            .with_ctx("slot_status", format!("{:?}", snapshot.status))
            .with_ctx("raw_cqe_res", entry.raw().res)
            .with_ctx("raw_cqe_flags", entry.raw().flags)
            .attach_note(
                "The target was moved to orphan cleanup and will not be reused until its final CQE arrives.",
            ))
    }

    fn remember_first_error(
        first_error: &mut Option<Report<UringError>>,
        error: Report<UringError>,
    ) {
        if first_error.is_none() {
            *first_error = Some(error);
        }
    }
}
