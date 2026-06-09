use super::event::unknown_completion_anomaly;
use super::routing::{finalize_corrupt_checked, finalize_waiting_checked};
use super::token::CONTROL_TOKEN_INDEX;
use super::*;
use crate::driver::PlatformOp;
use crate::driver::registry::OpRegistry;
use crate::slot::{self, SlotRegistryExt};

#[test]
fn op_token_try_new_rejects_control_index() {
    let err = OpToken::try_new(CONTROL_TOKEN_INDEX as usize, 1)
        .expect_err("control index must not be a user token");
    assert_eq!(
        err,
        OpTokenError::ReservedControlIndex {
            index: CONTROL_TOKEN_INDEX as usize
        }
    );
}

#[test]
fn classify_user_token_does_not_panic() {
    let token = OpToken::from_registry_parts((CONTROL_TOKEN_INDEX - 1) as usize, 7)
        .expect("max user index should be encodable");
    assert_eq!(
        CompletionToken::user(token).classify(),
        CompletionTokenClass::User(token)
    );
}

#[test]
fn rio_context_anomaly_keeps_backend_context_separate() {
    let raw_context = 0xa700_0001_0000_002a;
    let anomaly = CompletionAnomaly::rio_malformed_context_raw(raw_context);

    assert_eq!(anomaly.token, CompletionToken::rio_wake(0));
    assert_eq!(anomaly.backend_context, Some(raw_context));
}

#[test]
fn user_completion_event_rejects_mismatched_raw_token() {
    let expected = OpToken::from_registry_parts(1, 7).expect("test token");
    let actual = OpToken::from_registry_parts(2, 7).expect("test token");
    let raw = RawCompletion::new(CompletionBackend::Core, CompletionToken::user(actual), 3, 0);

    let err = UserCompletionEvent::try_new(expected, raw)
        .expect_err("mismatched user completion must not be constructible");

    assert_eq!(err.token, expected);
    assert_eq!(err.expected, CompletionToken::user(expected));
    assert_eq!(err.actual, CompletionToken::user(actual));
}

#[test]
fn completion_packet_uses_user_completion_event_token() {
    let token = OpToken::from_registry_parts(3, 9).expect("test token");
    let event = UserCompletionEvent::from_parts(CompletionBackend::Core, token, 11, 5);
    let packet = CompletionPacket::<DummySlotSpec>::user(event, (), None);

    assert_eq!(packet.token(), token);
    assert_eq!(
        packet.completion_event().token,
        CompletionToken::user(token)
    );
    assert_eq!(packet.completion_event().res, 11);
    assert_eq!(packet.completion_event().flags, 5);
}

#[test]
fn sidecar_completion_envelope_keeps_queue_key_as_source_only() {
    let token = OpToken::from_registry_parts(3, 9).expect("test token");
    let envelope =
        CompletionEnvelope::from_sidecar_user_token(CompletionBackend::Iocp, token, 77, 11, 5);

    assert_eq!(envelope.identity, CompletionIdentity::User(token));
    assert_eq!(
        envelope.source,
        CompletionIdentitySource::SidecarTokenWithQueueKey { queue_key: 77 }
    );
    assert_eq!(envelope.raw.token, CompletionToken::user(token));
    assert_eq!(envelope.raw.res, 11);
    assert_eq!(envelope.raw.flags, 5);
}

#[test]
fn unknown_completion_anomaly_preserves_backend_context() {
    let envelope = CompletionEnvelope::backend_context(CompletionBackend::Iocp, 0, -5, 0);

    let anomaly = unknown_completion_anomaly(envelope);

    assert_eq!(
        anomaly.reason,
        CompletionAnomalyReason::BackendContextUnknown
    );
    assert_eq!(anomaly.backend, Some(CompletionBackend::Iocp));
    assert_eq!(anomaly.backend_context, Some(0));
    assert_eq!(anomaly.raw_result, Some(-5));
}

#[test]
fn cancel_target_anomaly_classifies_missing_target_without_user_completion() {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let token = OpToken::from_registry_parts(4, 1).expect("test token");

    let (reason, anomaly) = cancel_target_anomaly(
        CompletionBackend::Core,
        token,
        -1,
        0,
        registry.checked_slot_view(token),
    );

    assert_eq!(reason, CancelTargetGoneReason::Missing);
    assert_eq!(anomaly.reason, CompletionAnomalyReason::UnknownSlot);
    assert_eq!(anomaly.token, CompletionToken::user(token));
}

struct DummyPlatformOp;

impl PlatformOp for DummyPlatformOp {
    type CleanupContext<'a> = ();
}

struct DummySlotSpec;

impl slot::SlotSpec for DummySlotSpec {
    type Op = DummyPlatformOp;
    type UserPayload = ();
    type PlatformData = ();
    type Sidecar = ();
    type Error = ();
    type Completion = usize;
    type CompletionDiagnostics = ();
}

#[test]
fn finalize_waiting_checked_records_missing_slot() {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let diagnostics = registry.shared.completion_diagnostics();
    let token = OpToken::from_registry_parts(4, 1).expect("test token");

    let outcome = finalize_waiting_checked(
        &mut registry,
        &diagnostics,
        CompletionBackend::Core,
        token,
        -5,
        0,
    );

    assert!(matches!(
        outcome,
        FinalizeOutcome::Missing(anomaly)
            if anomaly.reason == CompletionAnomalyReason::UnknownSlot
    ));
    assert_eq!(diagnostics.snapshot().unknown_completion, 1);
}

#[test]
fn finalize_corrupt_checked_records_unencodable_snapshot() {
    let mut registry = OpRegistry::<DummySlotSpec>::new(1);
    let diagnostics = registry.shared.completion_diagnostics();
    let snapshot = slot::SlotSnapshot {
        index: u32::MAX as usize,
        generation: 1,
        state: slot::SlotState::InFlightWaiting,
        has_op: false,
        has_payload: false,
    };

    let outcome = finalize_corrupt_checked(
        &mut registry,
        &diagnostics,
        CompletionBackend::Core,
        snapshot,
        -5,
        0,
    );

    assert!(matches!(
        outcome,
        FinalizeOutcome::Missing(anomaly)
            if anomaly.reason == CompletionAnomalyReason::FinalizeFailed
    ));
    assert_eq!(diagnostics.snapshot().internal_unknown, 1);
}
