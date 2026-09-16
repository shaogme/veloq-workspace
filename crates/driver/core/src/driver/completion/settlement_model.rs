#![allow(dead_code)]

use super::CompletionContinuation;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HookFailure {
    OnComplete,
    RecordItem,
    PayloadMissing,
    MoreRecordMismatch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettlementOutcome {
    Completed,
    TerminalFailure,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SlotDisposition {
    Released,
    Retained,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ResourceDisposition {
    Absent,
    Released,
    Retained,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CompletionSettlement {
    outcome: SettlementOutcome,
    slot: SlotDisposition,
    operation: ResourceDisposition,
    completion_payload: ResourceDisposition,
    cleanup_runs: usize,
    provided_buffer_returns: usize,
    error: Option<HookFailure>,
}

#[derive(Debug, Clone, Copy)]
struct ConsumedCompletion {
    continuation: CompletionContinuation,
    failure: Option<HookFailure>,
    has_payload: bool,
    has_provided_buffer: bool,
}

#[derive(Debug, Default)]
struct ProvidedBufferGuard {
    claimed: bool,
    returned: bool,
}

impl ProvidedBufferGuard {
    fn claim() -> Self {
        Self {
            claimed: true,
            returned: false,
        }
    }

    fn return_once(&mut self) -> bool {
        if !self.claimed || self.returned {
            return false;
        }
        self.returned = true;
        true
    }
}

/// CQE 已被消费后的 settlement 模型。
///
/// 它把“错误”与 slot、操作载荷、当前完成 payload 和 buffer return 放在同一个结果里，
/// 用来约束阶段 2 的 core contract：错误不能在 `?` 之后丢失清理责任。
fn settle_completion(input: ConsumedCompletion) -> CompletionSettlement {
    let is_final = input.continuation.is_final();
    let outcome = match (input.failure, is_final) {
        (None, _) => SettlementOutcome::Completed,
        (Some(_), true) => SettlementOutcome::TerminalFailure,
        (Some(_), false) => SettlementOutcome::Quarantined,
    };
    let slot = match outcome {
        SettlementOutcome::Completed if is_final => SlotDisposition::Released,
        SettlementOutcome::Completed => SlotDisposition::Retained,
        SettlementOutcome::TerminalFailure => SlotDisposition::Released,
        SettlementOutcome::Quarantined => SlotDisposition::Quarantined,
    };
    let operation = if is_final {
        ResourceDisposition::Released
    } else {
        ResourceDisposition::Retained
    };
    let completion_payload = if input.has_payload {
        ResourceDisposition::Released
    } else {
        ResourceDisposition::Absent
    };
    let provided_buffer_returns = if input.has_provided_buffer {
        let mut guard = ProvidedBufferGuard::claim();
        usize::from(guard.return_once())
    } else {
        0
    };

    CompletionSettlement {
        outcome,
        slot,
        operation,
        completion_payload,
        cleanup_runs: 1,
        provided_buffer_returns,
        error: input.failure,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn final_on_complete_failure_is_a_terminal_settlement() {
        let settlement = settle_completion(ConsumedCompletion {
            continuation: CompletionContinuation::Final,
            failure: Some(HookFailure::OnComplete),
            has_payload: true,
            has_provided_buffer: true,
        });

        assert_eq!(settlement.outcome, SettlementOutcome::TerminalFailure);
        assert_eq!(settlement.slot, SlotDisposition::Released);
        assert_eq!(settlement.operation, ResourceDisposition::Released);
        assert_eq!(settlement.completion_payload, ResourceDisposition::Released);
        assert_eq!(settlement.cleanup_runs, 1);
        assert_eq!(settlement.provided_buffer_returns, 1);
        assert_eq!(settlement.error, Some(HookFailure::OnComplete));
    }

    #[test]
    fn more_failure_quarantines_the_operation_but_settles_this_cqe() {
        let settlement = settle_completion(ConsumedCompletion {
            continuation: CompletionContinuation::More,
            failure: Some(HookFailure::RecordItem),
            has_payload: true,
            has_provided_buffer: true,
        });

        assert_eq!(settlement.outcome, SettlementOutcome::Quarantined);
        assert_eq!(settlement.slot, SlotDisposition::Quarantined);
        assert_eq!(settlement.operation, ResourceDisposition::Retained);
        assert_eq!(settlement.completion_payload, ResourceDisposition::Released);
        assert_eq!(settlement.cleanup_runs, 1);
        assert_eq!(settlement.provided_buffer_returns, 1);
        assert_eq!(settlement.error, Some(HookFailure::RecordItem));
    }

    #[test]
    fn final_success_and_failure_each_return_a_provided_buffer_once() {
        for failure in [None, Some(HookFailure::OnComplete)] {
            let settlement = settle_completion(ConsumedCompletion {
                continuation: CompletionContinuation::Final,
                failure,
                has_payload: true,
                has_provided_buffer: true,
            });
            assert_eq!(settlement.provided_buffer_returns, 1);
            assert_eq!(settlement.cleanup_runs, 1);
        }
    }

    #[test]
    fn provided_buffer_guard_rejects_a_second_return() {
        let mut guard = ProvidedBufferGuard::claim();
        assert!(guard.return_once());
        assert!(!guard.return_once());
    }

    #[test]
    fn missing_payload_is_a_terminal_failure_with_no_payload_to_release() {
        let settlement = settle_completion(ConsumedCompletion {
            continuation: CompletionContinuation::Final,
            failure: Some(HookFailure::PayloadMissing),
            has_payload: false,
            has_provided_buffer: false,
        });

        assert_eq!(settlement.outcome, SettlementOutcome::TerminalFailure);
        assert_eq!(settlement.slot, SlotDisposition::Released);
        assert_eq!(settlement.operation, ResourceDisposition::Released);
        assert_eq!(settlement.completion_payload, ResourceDisposition::Absent);
        assert_eq!(settlement.cleanup_runs, 1);
        assert_eq!(settlement.provided_buffer_returns, 0);
        assert_eq!(settlement.error, Some(HookFailure::PayloadMissing));
    }

    #[test]
    fn record_hook_failure_does_not_leave_a_final_slot_active() {
        let settlement = settle_completion(ConsumedCompletion {
            continuation: CompletionContinuation::Final,
            failure: Some(HookFailure::RecordItem),
            has_payload: true,
            has_provided_buffer: false,
        });

        assert_eq!(settlement.outcome, SettlementOutcome::TerminalFailure);
        assert_eq!(settlement.slot, SlotDisposition::Released);
        assert_eq!(settlement.error, Some(HookFailure::RecordItem));
    }

    #[test]
    fn more_record_mismatch_is_isolated_without_releasing_kernel_resources() {
        let settlement = settle_completion(ConsumedCompletion {
            continuation: CompletionContinuation::More,
            failure: Some(HookFailure::MoreRecordMismatch),
            has_payload: false,
            has_provided_buffer: true,
        });

        assert_eq!(settlement.outcome, SettlementOutcome::Quarantined);
        assert_eq!(settlement.slot, SlotDisposition::Quarantined);
        assert_eq!(settlement.operation, ResourceDisposition::Retained);
        assert_eq!(settlement.completion_payload, ResourceDisposition::Absent);
        assert_eq!(settlement.provided_buffer_returns, 1);
    }
}
