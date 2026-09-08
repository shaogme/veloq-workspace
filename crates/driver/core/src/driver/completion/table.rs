use crate::DriverError;
use crate::slot::{self, Generation};
use diagweave::prelude::*;
use veloq_std::{
    boxed::Box,
    hint::spin_loop,
    sync::{Arc, atomic::Ordering},
    task::Waker,
};

#[cfg(any(test, feature = "loom"))]
use veloq_std::string::String;

#[cfg(feature = "loom")]
use veloq_std::thread::yield_now;

use super::{
    AnomalyAttach, AnomalyOutcome, CompletionAnomalyKind, CompletionInput, CompletionPacket,
    CompletionRecord, CompletionWritePermit, DriverCompletionDiagnosticsBackend, OpToken,
    RecordCompletionOutcome, RecordCompletionResult, UserCompletionEvent, run_completion_cleanup,
    types::CompletionMutationOutcome,
};

pub type SharedCompletionTable<Spec> = Arc<dyn CompletionAccess<Spec>>;

/// Result of a completion poll, enabling detection of recycled slots.
pub enum PollRecordResult<Spec: slot::SlotSpec> {
    /// Operation completed successfully or with an error.
    Ready(CompletionRecord<Spec>),
    /// Operation completion became unavailable; materialize at the poll boundary.
    Unavailable {
        kind: CompletionAnomalyKind,
        attach: AnomalyAttach,
    },
    /// Operation is still in flight.
    Pending,
}

pub trait CompletionAccess<Spec: slot::SlotSpec>: Send + Sync {
    fn record_completion(
        &self,
        permit: CompletionWritePermit,
        packet: CompletionPacket<Spec>,
    ) -> RecordCompletionResult<Spec>;

    fn try_take_record(
        &self,
        token: OpToken,
    ) -> Result<PollRecordResult<Spec>, Report<Spec::Error>>;

    fn register_waker(&self, token: OpToken, waker: &Waker) -> CompletionMutationOutcome;

    fn mark_waiting(&self, token: OpToken) -> CompletionMutationOutcome;

    /// 丢弃信箱里**全部**未消费的记录并跑各自的 cleanup。
    ///
    /// 对仍在途的 multishot 只清空信箱，不归还 slot——操作还没结束。
    fn discard_ready_records(&self, token: OpToken) -> CompletionMutationOutcome;

    fn mark_orphaned(&self, token: OpToken) -> CompletionMutationOutcome;

    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, idx: usize) -> u8;

    /// 完整的 [`slot::SlotStatus`] 渲染，给失败断言提供比单个 `u8` 更多的线索。
    #[cfg(any(test, feature = "loom"))]
    fn debug_status_string(&self, idx: usize) -> String;
}

pub const CELL_STATE_IDLE: u8 = 0;
pub const CELL_STATE_WAITING: u8 = 1;
pub const CELL_STATE_READY: u8 = 2;
pub const CELL_STATE_ORPHANED: u8 = 3;
pub const CELL_STATE_BUSY: u8 = 4;

#[inline(always)]
fn spin_yield() {
    #[cfg(feature = "loom")]
    let _ = yield_now();
    #[cfg(not(feature = "loom"))]
    spin_loop();
}

#[inline]
fn recorded_outcome<Spec: slot::SlotSpec>(
    input: &CompletionInput<Spec>,
) -> RecordCompletionOutcome {
    match input {
        CompletionInput::User(_) => RecordCompletionOutcome::RecordedUser,
    }
}

#[inline]
fn mutation_missing(token: OpToken) -> CompletionMutationOutcome {
    let (idx, generation) = token.parts();
    CompletionMutationOutcome::Rejected(AnomalyOutcome::Missing(
        CompletionAnomalyKind::unknown_slot(idx, generation),
    ))
}

#[inline]
fn mutation_generation_mismatch(
    idx: usize,
    expected_generation: Generation,
    actual_generation: Generation,
    status: slot::SlotStatus,
) -> CompletionMutationOutcome {
    if actual_generation.is_newer_than(expected_generation) {
        CompletionMutationOutcome::Rejected(AnomalyOutcome::Stale(CompletionAnomalyKind::stale(
            idx,
            expected_generation,
            actual_generation,
            status,
        )))
    } else {
        CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(
            CompletionAnomalyKind::non_active(idx, expected_generation, status),
        ))
    }
}

#[inline]
fn mutation_non_active(
    idx: usize,
    generation: Generation,
    status: slot::SlotStatus,
) -> CompletionMutationOutcome {
    CompletionMutationOutcome::Rejected(AnomalyOutcome::NonActive(
        CompletionAnomalyKind::non_active(idx, generation, status),
    ))
}

impl<Spec> slot::SlotTable<Spec>
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
    Spec::CompletionDiagnostics: DriverCompletionDiagnosticsBackend,
{
    #[inline]
    fn recorded_completion(
        &self,
        outcome: RecordCompletionOutcome,
    ) -> RecordCompletionResult<Spec> {
        self.diagnostics.record_completion_outcome(&outcome);
        RecordCompletionResult::Recorded(outcome)
    }

    #[inline]
    fn rejected_completion(
        &self,
        outcome: RecordCompletionOutcome,
        event: UserCompletionEvent,
        packet: CompletionPacket<Spec>,
    ) -> RecordCompletionResult<Spec> {
        if let RecordCompletionOutcome::Rejected(anomaly_outcome) = outcome {
            self.diagnostics.record_anomaly_outcome(
                anomaly_outcome,
                AnomalyAttach::from_raw_completion(event.raw()),
            );
        }
        self.diagnostics.record_completion_outcome(&outcome);
        RecordCompletionResult::Rejected {
            outcome,
            packet: Box::new(packet),
        }
    }

    #[inline]
    fn recorded_mutation(
        &self,
        token: OpToken,
        outcome: CompletionMutationOutcome,
    ) -> CompletionMutationOutcome {
        if let Some(anomaly_outcome) = outcome.anomaly_outcome() {
            self.diagnostics
                .record_anomaly_outcome(anomaly_outcome, AnomalyAttach::from_op_token(token));
        }
        outcome
    }

    /// 丢弃一条已就绪但不会再被消费的完成：归还 payload、丢掉 detail，并执行
    /// `cleanup`（完成式 I/O 下内核可能仍持有其中的资源）。
    #[inline]
    fn run_discarded_record_cleanup(&self, record: slot::MailboxRecord<Spec>) {
        let slot::MailboxRecord {
            event: _,
            payload,
            detail,
            mut cleanup,
            continuation: _,
        } = record;
        drop(payload);
        drop(detail);
        let _ = run_completion_cleanup(&self.diagnostics, &mut cleanup);
    }

    /// 排空一个 slot 的整个信箱，逐条跑 cleanup，并把全局就绪计数扣回去。
    ///
    /// 调用方必须已经持有该 cell 的 `finalizing`（或确知没有并发写入者，例如
    /// `OpRegistry::recycle_at_index` 那种 driver 线程独占的场景）。
    pub(crate) fn drain_mailbox_cleanup(&self, cell: &slot::SlotEntry<Spec>) {
        loop {
            let Some(record) = cell.with_mailbox(slot::CompletionMailbox::pop) else {
                break;
            };
            self.clear_ready_completion();
            self.run_discarded_record_cleanup(record);
        }
    }

    /// 抢占信箱、丢弃其中全部记录，然后按 `target` 与 `streaming` 决定 slot 的去向。
    ///
    /// 仍在途的 multishot（`streaming`）**不推进 generation**：内核还会投递完成，token
    /// 必须保持有效，否则后续 CQE 一律判 `Stale` 被丢，`orphan_cleanup` 再也跑不到，
    /// 其中的 fd 与 provided buffer 直接泄漏。
    fn drain_and_settle(&self, token: OpToken, target: SettleTarget) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return self.recorded_mutation(token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        let claimed = loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let status = current.status();
            let cell_gen = current.generation();

            if status.finalizing {
                spin_yield();
                continue;
            }
            if cell_gen != generation {
                return self.recorded_mutation(
                    token,
                    mutation_generation_mismatch(idx, generation, cell_gen, status),
                );
            }
            if !status.ready {
                return self.recorded_mutation(token, mutation_non_active(idx, generation, status));
            }

            match cell.core_state.compare_exchange(
                current,
                current.with_finalizing(true),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break current.with_finalizing(true),
                Err(_) => continue,
            }
        };

        self.drain_mailbox_cleanup(cell);

        publish_mailbox_release(cell, claimed, |current| {
            let next = current.with_finalizing(false).with_ready(false);
            match (current.streaming(), target) {
                (true, SettleTarget::Orphan) => next.with_state(slot::SlotState::InFlightOrphaned),
                // 仍在途但只是丢弃已发布的记录：生命周期不动，等操作自己终止。
                (true, SettleTarget::Release) => next,
                (false, _) => next
                    .with_state(slot::SlotState::Idle)
                    .with_generation(generation.next()),
            }
        });

        self.recorded_mutation(token, CompletionMutationOutcome::Applied)
    }
}

/// 释放 `finalizing` 并发布新状态。
///
/// **必须是 CAS 循环而不是盲 store。** `finalizing` 只保护信箱（谁能 push/pop），它拦不住
/// driver 线程的 `SlotData::free` / `set_state`——那两个是不检查标志位的 CAS，会在远端消费
/// 者持有 `finalizing` 期间照常改动生命周期位。盲 store 会把它们的写入整段吞掉：实测
/// 表现为 `free` 把 slot 归还成 `Idle` 之后，消费方的 store 把 `finalizing` 又按回去，
/// cell 永久停在 `Idle+ready+finalizing`，此后所有 mutator 都在自旋等一个再也不会被清掉
/// 的标志位。
///
/// `compute` 拿到的是**最新**的状态，所以重试时它是在别人刚写完的值上重新决定去向，而
/// 不是在陈旧的快照上。
#[inline]
fn publish_mailbox_release(
    cell: &slot::SlotEntry<impl slot::SlotSpec>,
    claimed: slot::PackedCoreState,
    compute: impl Fn(slot::PackedCoreState) -> slot::PackedCoreState,
) {
    let mut current = claimed;
    loop {
        let next = compute(current);
        match cell.core_state.compare_exchange_weak(
            current,
            next,
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return,
            Err(actual) => current = actual,
        }
    }
}

/// 排空信箱之后，slot 该往哪去。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SettleTarget {
    /// 归还 slot（操作已终止时）。
    Release,
    /// 标记为已放弃：用户不再消费，但内核可能还会投递完成。
    Orphan,
}

impl<Spec> CompletionAccess<Spec> for slot::SlotTable<Spec>
where
    Spec: slot::SlotSpec,
    slot::SlotPayload<Spec>: Send,
    slot::SlotError<Spec>: Send,
    slot::SlotCompletion<Spec>: Send,
{
    fn record_completion(
        &self,
        _permit: CompletionWritePermit,
        packet: CompletionPacket<Spec>,
    ) -> RecordCompletionResult<Spec> {
        let op_token = packet.token();
        let event = packet.event;
        let (idx, generation) = op_token.parts();
        let success_outcome = recorded_outcome(&packet.input);
        if idx >= self.slots.len() {
            return self.rejected_completion(
                RecordCompletionOutcome::Rejected(AnomalyOutcome::Missing(
                    CompletionAnomalyKind::unknown_slot(idx, generation),
                )),
                event,
                packet,
            );
        }
        let cell = &self.slots[idx];

        // 判定顺序统一为 generation → finalizing → ready → state：前两项是「这条完成
        // 该不该由我处理」，`ready` 是信箱维度，最后才轮到 slot 的生命周期。
        let claimed = loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let status = current.status();
            let cell_gen = current.generation();

            if generation.is_older_than(cell_gen) {
                return self.rejected_completion(
                    RecordCompletionOutcome::Rejected(AnomalyOutcome::Stale(
                        CompletionAnomalyKind::stale(idx, generation, cell_gen, status),
                    )),
                    event,
                    packet,
                );
            }
            if generation.is_newer_than(cell_gen) {
                let outcome = if status.is_idle() {
                    RecordCompletionOutcome::Rejected(AnomalyOutcome::NonActive(
                        CompletionAnomalyKind::non_active(idx, generation, status),
                    ))
                } else {
                    RecordCompletionOutcome::Rejected(AnomalyOutcome::Stale(
                        CompletionAnomalyKind::stale(idx, generation, cell_gen, status),
                    ))
                };
                return self.rejected_completion(outcome, event, packet);
            }

            if status.finalizing {
                spin_yield();
                continue;
            }
            // 信箱里已经压着未消费的记录时，只有 multishot 允许再追加一条。单发操作
            // 走到这里说明同一个 slot 收到了第二条完成——那是损坏，仍旧拒绝，不能因为
            // 信箱变成队列就把这道防御一起放开。
            if status.ready && !status.streaming {
                return self.rejected_completion(
                    RecordCompletionOutcome::Rejected(AnomalyOutcome::NonActive(
                        CompletionAnomalyKind::non_active(idx, generation, status),
                    )),
                    event,
                    packet,
                );
            }

            match status.state {
                slot::SlotState::InFlightWaiting => match cell.core_state.compare_exchange(
                    current,
                    current.with_finalizing(true).with_generation(generation),
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => break current.with_finalizing(true),
                    Err(_) => continue,
                },
                slot::SlotState::Idle | slot::SlotState::Reserved => {
                    return self.rejected_completion(
                        RecordCompletionOutcome::Rejected(AnomalyOutcome::NonActive(
                            CompletionAnomalyKind::non_active(idx, generation, status),
                        )),
                        event,
                        packet,
                    );
                }
                slot::SlotState::InFlightOrphaned => {
                    return self.rejected_completion(
                        RecordCompletionOutcome::OrphanedDropped,
                        event,
                        packet,
                    );
                }
            }
        };

        let input = packet.input;
        let continuation = packet.continuation;
        cell.with_mailbox(|mailbox| {
            let record = match input {
                CompletionInput::User(completion) => slot::MailboxRecord {
                    event,
                    payload: completion.payload,
                    detail: completion.detail,
                    cleanup: completion.cleanup,
                    continuation,
                },
            };
            mailbox.push(record);
        });
        cell.completion_res.store(event.res(), Ordering::Release);
        cell.completion_flags
            .store(event.flags(), Ordering::Release);
        self.note_ready_completion();
        // 发布：清掉 finalizing、立起 ready，并按这条完成的 continuation 更新 streaming。
        // 生命周期状态**保持不变**——slot 的归还由 driver 线程随后的 `finalize_*` →
        // `free()` 负责（multishot 的 `More` 完成不触发它），两件事互不依赖。
        cell.core_state.store(
            claimed
                .with_finalizing(false)
                .with_ready(true)
                .with_streaming(continuation.is_more())
                .with_generation(generation),
            Ordering::Release,
        );
        #[cfg(feature = "loom")]
        eprintln!(
            "TRACE record_completion store {:?}",
            cell.load_core_state(Ordering::Acquire).status()
        );

        cell.completion_waker.wake();
        self.recorded_completion(success_outcome)
    }

    fn try_take_record(
        &self,
        token: OpToken,
    ) -> Result<PollRecordResult<Spec>, Report<Spec::Error>> {
        let attach = AnomalyAttach::from_op_token(token);
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            let kind = CompletionAnomalyKind::unknown_slot(idx, generation);
            self.diagnostics.record_anomaly_kind(kind, attach);
            return Ok(PollRecordResult::Unavailable { kind, attach });
        }
        let cell = &self.slots[idx];

        // 消费方也要抢 `finalizing`：能否终态收尾取决于「pop 之后信箱是否还非空」，
        // 而这件事只有锁住信箱才知道，没法像单槽时代那样先用一次 CAS 把结论定死。
        // 抢占期间 `record_completion` 与三个远端 mutator 都会自旋等待，窗口是一次
        // `pop` 加一次 store。
        let claimed = loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let status = current.status();
            let cell_gen = current.generation();

            if cell_gen.is_newer_than(generation) {
                let kind = CompletionAnomalyKind::stale(idx, generation, cell_gen, status);
                self.diagnostics.record_anomaly_kind(kind, attach);
                return Ok(PollRecordResult::Unavailable { kind, attach });
            }

            if cell_gen.is_older_than(generation) {
                let kind = CompletionAnomalyKind::non_active(idx, generation, status);
                self.diagnostics.record_anomaly_kind(kind, attach);
                return Ok(PollRecordResult::Unavailable { kind, attach });
            }

            if status.finalizing {
                spin_yield();
                continue;
            }

            if !status.ready {
                // 信箱空：仍在途（含未终止的 multishot）就是 Pending，发布完成后
                // `record_completion` 会唤醒已注册的 waker。
                return if status.streaming || status.state == slot::SlotState::InFlightWaiting {
                    Ok(PollRecordResult::Pending)
                } else {
                    let kind = CompletionAnomalyKind::non_active(idx, generation, status);
                    self.diagnostics.record_anomaly_kind(kind, attach);
                    Ok(PollRecordResult::Unavailable { kind, attach })
                };
            }

            match cell.core_state.compare_exchange(
                current,
                current.with_finalizing(true),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break current.with_finalizing(true),
                Err(_) => continue,
            }
        };

        let (record, remaining) = cell.with_mailbox(|mailbox| (mailbox.pop(), !mailbox.is_empty()));

        // 终态收尾只在「信箱已空」且「不会再有完成」时发生。两个条件缺一不可：信箱里
        // 排着 `[More, Final]` 时，取走第一条的瞬间 `streaming` 已经是 false（第二条
        // 发布时清掉了），只看 streaming 会推进 generation 把第二条永久锁死。
        publish_mailbox_release(cell, claimed, |current| {
            let next = current.with_finalizing(false).with_ready(remaining);
            if !remaining && !current.streaming() {
                next.with_state(slot::SlotState::Idle)
                    .with_generation(generation.next())
            } else {
                next
            }
        });

        match record {
            Some(record) => {
                self.clear_ready_completion();
                let slot::MailboxRecord {
                    event,
                    payload,
                    detail,
                    cleanup,
                    continuation,
                } = record;
                Ok(PollRecordResult::Ready(CompletionRecord {
                    event,
                    payload,
                    detail,
                    cleanup,
                    continuation,
                }))
            }
            None => {
                let report = crate::DriverCoreError::Internal
                    .to_report()
                    .push_ctx("scope", "try_take_record")
                    .attach_note(veloq_std::format!(
                        "corrupt slot state: mailbox marked ready but holds no record. index: {}, generation: {}",
                        idx, generation
                    ));
                Err(Spec::Error::from_core_report(report))
            }
        }
    }

    fn register_waker(&self, token: OpToken, waker: &Waker) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return self.recorded_mutation(token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        let current = cell.load_core_state(Ordering::Acquire);
        let status = current.status();
        let cell_gen = current.generation();

        if cell_gen != generation {
            return self.recorded_mutation(
                token,
                mutation_generation_mismatch(idx, generation, cell_gen, status),
            );
        }

        cell.completion_waker.register(waker);

        let current_after = cell.load_core_state(Ordering::Acquire);
        let status_after = current_after.status();
        let generation_after = current_after.generation();
        if generation_after != generation {
            return self.recorded_mutation(
                token,
                mutation_generation_mismatch(idx, generation, generation_after, status_after),
            );
        }
        if status_after.ready {
            // 注册与发布竞速：记录已就绪，自己唤醒自己，避免丢唤醒。
            waker.wake_by_ref();
            return self.recorded_mutation(token, CompletionMutationOutcome::Applied);
        }

        let outcome =
            if status_after.finalizing || status_after.state == slot::SlotState::InFlightWaiting {
                CompletionMutationOutcome::Applied
            } else {
                mutation_non_active(idx, generation, status_after)
            };
        self.recorded_mutation(token, outcome)
    }

    fn mark_waiting(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return self.recorded_mutation(token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let status = current.status();
            let cell_generation = current.generation();

            if cell_generation != generation {
                return self.recorded_mutation(
                    token,
                    mutation_generation_mismatch(idx, generation, cell_generation, status),
                );
            }

            if status.finalizing {
                spin_yield();
                continue;
            }
            if status.ready {
                return self.recorded_mutation(token, CompletionMutationOutcome::Applied);
            }

            return match status.state {
                slot::SlotState::InFlightWaiting => {
                    self.recorded_mutation(token, CompletionMutationOutcome::Applied)
                }
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightOrphaned => {
                    self.recorded_mutation(token, mutation_non_active(idx, generation, status))
                }
            };
        }
    }

    fn discard_ready_records(&self, token: OpToken) -> CompletionMutationOutcome {
        self.drain_and_settle(token, SettleTarget::Release)
    }

    fn mark_orphaned(&self, token: OpToken) -> CompletionMutationOutcome {
        let (idx, generation) = token.parts();
        if idx >= self.slots.len() {
            return self.recorded_mutation(token, mutation_missing(token));
        }
        let cell = &self.slots[idx];

        loop {
            let current = cell.load_core_state(Ordering::Acquire);
            let status = current.status();
            let cell_gen = current.generation();

            if status.finalizing {
                spin_yield();
                continue;
            }
            if cell_gen != generation {
                return self.recorded_mutation(
                    token,
                    mutation_generation_mismatch(idx, generation, cell_gen, status),
                );
            }
            // 完成已经发布：放弃它等于丢弃信箱里的记录，走 drain 才能跑 cleanup。仍在
            // 途的 multishot 由 `SettleTarget::Orphan` 收进 `InFlightOrphaned`，内核后续
            // 的完成才有 slot 可落、`orphan_cleanup` 才跑得到。
            if status.ready {
                return self.drain_and_settle(token, SettleTarget::Orphan);
            }

            match status.state {
                slot::SlotState::InFlightWaiting => {
                    if cell
                        .core_state
                        .compare_exchange(
                            current,
                            current
                                .with_state(slot::SlotState::InFlightOrphaned)
                                .with_generation(generation),
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                        .is_ok()
                    {
                        return self.recorded_mutation(token, CompletionMutationOutcome::Applied);
                    }
                }
                slot::SlotState::Idle
                | slot::SlotState::Reserved
                | slot::SlotState::InFlightOrphaned => {
                    return self
                        .recorded_mutation(token, mutation_non_active(idx, generation, status));
                }
            }
        }
    }

    #[cfg(any(test, feature = "loom"))]
    fn debug_status_string(&self, idx: usize) -> String {
        veloq_std::format!(
            "{:?}",
            self.slots[idx].load_core_state(Ordering::Acquire).status()
        )
    }

    #[cfg(any(test, feature = "loom"))]
    fn debug_get_state(&self, idx: usize) -> u8 {
        let status = self.slots[idx].load_core_state(Ordering::Acquire).status();
        if status.finalizing {
            return CELL_STATE_BUSY;
        }
        if status.ready {
            return CELL_STATE_READY;
        }
        match status.state {
            slot::SlotState::InFlightWaiting => CELL_STATE_WAITING,
            slot::SlotState::InFlightOrphaned => CELL_STATE_ORPHANED,
            slot::SlotState::Idle | slot::SlotState::Reserved => CELL_STATE_IDLE,
        }
    }
}

#[cfg(test)]
#[cfg(not(feature = "loom"))]
mod tests;
