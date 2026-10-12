//! Actor coordination and completion routing for RIO. / RIO actor 协调与完成路由。

mod model;
mod multishot;
mod replacement;
mod single;

use self::model::{RioCompletionHooks, RioIngress, RioResultData};
use crate::{
    IoFd,
    config::{BorrowedRawHandle, SocketKey},
    driver::{COMP_BACKEND_RIO, IocpDriverCompletionDiagnostics, RIO_EVENT_TOKEN},
    error::IocpResult,
    ext::Extensions,
    op::{IocpOpRegistry, IocpSlotSpec},
    rio::{
        RioEnv, RioRuntime, RioSocketActor,
        core::{RioCompletionKind, RioCore, RioRequestContextDecode},
        error::{RioError, RioResult},
    },
};
use diagweave::prelude::*;
use veloq_buf::BufferRegistrar;
use veloq_driver_core::{
    driver::{
        AnomalyAttach, CompletionAnomalyKind, CompletionFlowExt, CompletionIngress, RawCompletion,
        SharedCompletionTable,
    },
    slot::Generation,
};
use veloq_std::{mem::zeroed, string::ToString};
use windows_sys::Win32::Networking::WinSock::{RIO_CORRUPT_CQ, RIORESULT};

pub(crate) struct RioCompletionAnomaly;

impl RioCompletionAnomaly {
    pub(crate) const MALFORMED: u16 = 1;
    pub(crate) const MISSING: u16 = 2;
    pub(crate) const STALE: u16 = 3;

    pub(crate) fn malformed(raw_context: u64) -> CompletionAnomalyKind {
        CompletionAnomalyKind::backend_specific(Self::MALFORMED, COMP_BACKEND_RIO, raw_context)
    }

    pub(crate) fn missing(
        raw_context: u64,
        index: usize,
        generation: Generation,
    ) -> CompletionAnomalyKind {
        CompletionAnomalyKind::backend_specific_missing(
            Self::MISSING,
            COMP_BACKEND_RIO,
            raw_context,
            index,
            generation,
        )
    }

    pub(crate) fn stale(
        raw_context: u64,
        index: usize,
        expected_generation: Generation,
        actual_generation: Generation,
    ) -> CompletionAnomalyKind {
        CompletionAnomalyKind::backend_specific_stale(
            Self::STALE,
            COMP_BACKEND_RIO,
            raw_context,
            index,
            expected_generation,
            actual_generation,
        )
    }
}

fn rio_result_attach(res: RioResultData) -> AnomalyAttach {
    AnomalyAttach::from_raw_completion(RawCompletion::new(
        COMP_BACKEND_RIO,
        RIO_EVENT_TOKEN,
        res.raw_res(),
        0,
    ))
}

pub(crate) struct RioCompletionService;

pub(crate) struct CompletionPollArgs<'a> {
    pub(crate) ops: &'a mut IocpOpRegistry,
    pub(crate) ext: &'a Extensions,
    pub(crate) registrar: &'a dyn BufferRegistrar,
    pub(crate) completion_table: &'a SharedCompletionTable<IocpSlotSpec>,
    pub(crate) diagnostics: &'a mut IocpDriverCompletionDiagnostics,
}

struct CompletionBatchOutcome {
    completed_count: usize,
    outstanding_count: usize,
    completions_found: bool,
}

impl RioCompletionService {
    pub(crate) fn ensure_actor<'a>(
        core: &mut RioCore,
        runtime: &'a mut RioRuntime,
        target: (IoFd, BorrowedRawHandle<'_>),
        env: RioEnv<'_>,
    ) -> RioResult<&'a mut RioSocketActor> {
        let (fd, handle) = target;
        let socket_key = handle.raw().actor_key();
        if runtime.is_submissions_closed() {
            return RioError::InvalidInput
                .with_ctx("socket_raw", socket_key.as_handle() as usize)
                .with_ctx("rio_outstanding_count", core.outstanding_count())
                .attach_note("RIO runtime is shutting down; rejecting actor creation");
        }

        if runtime.actor_index_contains(socket_key) {
            return runtime
                .actor_mut_for_handle(socket_key)
                .ok_or(RioError::Internal)
                .attach_note("failed to retrieve indexed actor");
        }

        let rq_depth = core.registry().rq_depth();
        let rq = core
            .registry_mut()
            .create_rq((handle, fd), env)
            .push_ctx("scope", "rio.runtime.control_flow.ensure_actor")
            .with_ctx("fd", fd.to_string())
            .with_ctx("handle_raw", handle.raw().as_handle() as usize)
            .with_ctx("socket_raw", handle.raw().as_handle() as usize)
            .with_ctx("rq_depth", rq_depth)
            .with_ctx("max_outstanding_recvs", rq_depth)
            .with_ctx("max_outstanding_sends", rq_depth)
            .with_ctx("max_receive_data_buffers", 1_u32)
            .with_ctx("max_send_data_buffers", 1_u32)
            .with_ctx("rio_outstanding_count", core.outstanding_count())
            .with_ctx("actors_len", runtime.actor_count())
            .with_ctx("actor_index_hit", runtime.actor_index_contains(socket_key))
            .attach_note("RIOCreateRequestQueue failed")?;

        let actor = RioSocketActor::new(rq);
        let is_first = runtime.insert_actor(socket_key, actor);
        if is_first && !core.cq_is_armed() {
            core.kernel_mut().rearm_notify().trans()?;
            core.mark_cq_armed();
        }
        runtime
            .actor_mut_for_handle(socket_key)
            .ok_or(RioError::Internal)
            .trans()
            .attach_note("failed to retrieve inserted actor")
    }

    pub(crate) fn shutdown_actor(runtime: &mut RioRuntime, socket_key: SocketKey) {
        runtime.remove_actor(socket_key);
    }

    pub(crate) fn process_completions(
        core: &mut RioCore,
        runtime: &mut RioRuntime,
        args: CompletionPollArgs<'_>,
    ) -> IocpResult<usize> {
        let CompletionPollArgs {
            ops,
            ext,
            registrar,
            completion_table,
            diagnostics,
        } = args;
        const MAX_RIO_RESULTS: usize = 128;
        let mut results: [RIORESULT; MAX_RIO_RESULTS] = unsafe { zeroed() };
        if core
            .kernel()
            .env(registrar, core.registration_mode())
            .is_none()
        {
            return Ok(0);
        }
        let outcome = {
            let mut completions_found = false;
            let mut hooks = RioCompletionHooks::new(core, runtime, registrar, ext);

            loop {
                let count = hooks.core_mut().kernel_mut().dequeue(&mut results);
                if count == RIO_CORRUPT_CQ {
                    return RioError::Internal
                        .attach_note("RIO completion queue is corrupt (RIO_CORRUPT_CQ)")
                        .trans();
                }
                if count == 0 {
                    break;
                }
                completions_found = true;

                for res in results.iter().take(count as usize) {
                    let result = RioResultData::from_result(res);
                    match hooks
                        .core_mut()
                        .registry_mut()
                        .decode_request_context_checked(result.request_context())
                    {
                        RioRequestContextDecode::Valid(kind) => {
                            let RioCompletionKind::Op {
                                init,
                                context: _completed_context,
                            } = *kind;
                            let _ = ops.accept_completion(
                                completion_table,
                                diagnostics,
                                &mut hooks,
                                CompletionIngress::Backend(RioIngress { init, result }),
                            )?;
                        }
                        RioRequestContextDecode::Malformed { raw } => {
                            let _ = ops.accept_completion(
                                completion_table,
                                diagnostics,
                                &mut hooks,
                                CompletionIngress::Anomaly {
                                    kind: RioCompletionAnomaly::malformed(raw),
                                    attach: rio_result_attach(result),
                                },
                            )?;
                        }
                        RioRequestContextDecode::Missing { id } => {
                            let _ = ops.accept_completion(
                                completion_table,
                                diagnostics,
                                &mut hooks,
                                CompletionIngress::Anomaly {
                                    kind: RioCompletionAnomaly::missing(
                                        result.request_context(),
                                        id.index(),
                                        id.generation(),
                                    ),
                                    attach: rio_result_attach(result),
                                },
                            )?;
                        }
                        RioRequestContextDecode::Stale {
                            id,
                            actual_generation,
                        } => {
                            let _ = ops.accept_completion(
                                completion_table,
                                diagnostics,
                                &mut hooks,
                                CompletionIngress::Anomaly {
                                    kind: RioCompletionAnomaly::stale(
                                        result.request_context(),
                                        id.index(),
                                        id.generation(),
                                        actual_generation,
                                    ),
                                    attach: rio_result_attach(result),
                                },
                            )?;
                        }
                    }
                }

                if count < MAX_RIO_RESULTS as u32 {
                    break;
                }
            }

            CompletionBatchOutcome {
                completed_count: hooks.completed_count(),
                outstanding_count: hooks.core_mut().outstanding_count(),
                completions_found,
            }
        };

        if outcome.completions_found {
            core.mark_cq_disarmed();
        }

        if runtime.has_actors() && !core.cq_is_armed() {
            core.kernel_mut().rearm_notify().trans()?;
            core.mark_cq_armed();
        }

        if outcome.outstanding_count == 0 {
            let (registry, env) = core.registry_with_env(registrar).into_parts();
            if let Some(env) = env {
                registry.flush_deregs(env);
            }
        }
        Ok(outcome.completed_count)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        model::{RioCompletionResult, RioResultData},
        single::RioSingleCompletion,
    };
    use crate::error::IocpError;
    use diagweave::prelude::*;
    use veloq_driver_core::platform::receive_pump::ReceivePumpError;
    use windows_sys::Win32::Networking::WinSock::{WSA_OPERATION_ABORTED, WSAENOBUFS};

    fn result(status: i32, bytes: u32) -> RioResultData {
        RioResultData::new(0, status, bytes)
    }

    #[test]
    fn receive_status_preserves_os_code_and_categories() {
        assert_eq!(
            RioSingleCompletion::receive_completion_result(result(0, 17), false),
            Ok(17)
        );
        assert_eq!(
            RioSingleCompletion::receive_completion_result(result(0, 17), true),
            Err(ReceivePumpError::ReceiveCancelled)
        );
        assert_eq!(
            RioSingleCompletion::receive_completion_result(result(WSA_OPERATION_ABORTED, 0), false),
            Err(ReceivePumpError::ReceiveCancelled)
        );
        assert_eq!(
            RioSingleCompletion::receive_completion_result(result(WSAENOBUFS, 0), false),
            Err(ReceivePumpError::ReceiveBufferExhausted)
        );
        assert_eq!(
            RioSingleCompletion::receive_completion_result(result(10035, 0), true),
            Err(ReceivePumpError::ReceiveOsError { code: 10035 })
        );
    }

    #[test]
    fn rio_result_translation_behaviour() {
        assert_eq!(RioCompletionResult::event_code(&Ok(5)), 5);
        assert_eq!(
            RioCompletionResult::event_code(&Ok((i32::MAX as usize) + 10)),
            i32::MAX
        );
        let err = IocpError::Internal
            .to_report()
            .push_ctx("scope", "rio.core.tests")
            .set_error_code(10022)
            .attach_note("invalid argument");
        assert_eq!(RioCompletionResult::event_code(&Err(err)), -10022);
    }
}
