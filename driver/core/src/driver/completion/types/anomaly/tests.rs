use std::mem::size_of;

use super::{
    AnomalyAttach, AnomalyOutcome, CompletionAnomaly, CompletionAnomalyKind,
    CompletionMutationOutcome,
};

#[test]
fn completion_anomaly_is_compact_enough_to_copy() {
    assert!(size_of::<CompletionAnomaly>() <= 72);
}

#[test]
fn completion_anomaly_kind_is_lightweight() {
    assert!(size_of::<CompletionAnomalyKind>() <= 40);
}

#[test]
fn anomaly_outcome_is_compact() {
    assert!(size_of::<AnomalyOutcome>() <= 48);
    assert!(size_of::<CompletionMutationOutcome>() <= 48);
}

#[test]
fn unavailable_completion_attach_is_compact() {
    assert!(size_of::<(CompletionAnomalyKind, AnomalyAttach)>() <= 56);
}
