use crate::{
    driver::{CqeEnv, ProvidedBufLeaseAction},
    error::{UringError, UringResult},
    op::{
        Accept, AcceptPayload, AcceptedSocket, Open, OpenPayload, ProvidedBuf, SendTo,
        SendToPayload, Timeout, TimeoutPayload, UdpPendingLease, UdpRecvMsgParser, UdpRecvMulti,
        UdpRecvMultiPayload, UringRecordItem, UringUserPayload, UringUserPayloadStorage, Wakeup,
        WakeupPayload, spec::UringOpSpec,
    },
};
use diagweave::prelude::Report;
use libc::{ENOBUFS, MSG_TRUNC};
use veloq_buf::{FixedBuf, heap::ChunkId};
use veloq_driver_core::{
    CompletionCleanupGuard, OpToken, ReceivePumpError, ReceivePumpEvent, UdpRecvMultiBackend,
};
use veloq_io_uring::cqueue;
use veloq_std::{format, num::NonZeroUsize, pin::Pin, time::Duration};

pub(crate) struct RecordItemContext<'borrow, 'provided, 'diagnostics> {
    pub(crate) result: i32,
    pub(crate) flags: u32,
    pub(crate) env: &'borrow mut CqeEnv<'provided, 'diagnostics>,
}

pub(super) struct DescriptorCallbacks;

impl DescriptorCallbacks {
    pub(super) fn new_accept_kernel(_user: &Accept) -> AcceptPayload {
        AcceptPayload::new()
    }

    pub(super) fn new_open_kernel(_user: &Open) -> OpenPayload {
        OpenPayload::new()
    }

    pub(super) fn new_send_to_kernel(_user: &SendTo) -> SendToPayload {
        SendToPayload::new()
    }

    pub(super) fn new_udp_recv_multi_kernel(_user: &UdpRecvMulti) -> UdpRecvMultiPayload {
        UdpRecvMultiPayload::new()
    }

    pub(super) fn new_wakeup_kernel(_user: &Wakeup) -> WakeupPayload {
        WakeupPayload::new()
    }

    pub(super) fn new_timeout_kernel(_user: &Timeout) -> TimeoutPayload {
        TimeoutPayload::new()
    }

    pub(super) unsafe fn descriptor_on_complete_default<S>(
        _kernel: Pin<&mut S::KernelPayload>,
        _payload: &mut S,
        result: i32,
    ) -> UringResult<usize>
    where
        S: UringOpSpec,
    {
        if result >= 0 {
            Ok(result as usize)
        } else {
            Err(UringError::CompletionWait
                .report(
                    "uring.op.spec.descriptor_on_complete_default",
                    "kernel completion returned error",
                )
                .set_error_code(-result))
        }
    }

    pub(super) fn descriptor_get_timeout_default<S>(
        _kernel: Pin<&S::KernelPayload>,
        _payload: &S,
    ) -> UringResult<Option<Duration>>
    where
        S: UringOpSpec,
    {
        Ok(None)
    }

    pub(super) fn descriptor_get_timeout_timeout(
        _kernel: Pin<&TimeoutPayload>,
        payload: &Timeout,
    ) -> UringResult<Option<Duration>> {
        Ok(Some(payload.duration))
    }

    pub(super) fn descriptor_resolve_chunks_default<S>(
        _kernel: Pin<&S::KernelPayload>,
        _payload: &S,
        _chunks: &mut [ChunkId],
    ) -> usize
    where
        S: UringOpSpec,
    {
        0
    }

    pub(super) fn descriptor_record_item_submit<S>(
        _kernel: Pin<&mut S::KernelPayload>,
        _payload: &mut S,
        _context: RecordItemContext<'_, '_, '_>,
    ) -> UringResult<UringRecordItem>
    where
        S: UringOpSpec,
    {
        Ok(UringRecordItem::use_submit_payload())
    }

    pub(super) fn descriptor_resume_item_none<S>(
        _kernel: Pin<&mut S::KernelPayload>,
        _payload: &mut S,
        _token: OpToken,
        _logical_receiver_generation: u32,
        _env: &mut CqeEnv<'_, '_>,
    ) -> UringResult<UringRecordItem>
    where
        S: UringOpSpec,
    {
        Err(UringError::InvalidState.report(
            "uring.op.spec.resume_item",
            "operation does not support backend resume ingress",
        ))
    }

    pub(super) fn descriptor_receive_generation_none<S>(_payload: &S) -> Option<u32>
    where
        S: UringOpSpec,
    {
        None
    }

    pub(super) fn descriptor_receive_generation_udp(user: &UdpRecvMulti) -> Option<u32> {
        Some(user.receive_pump().logical_receiver_generation())
    }

    pub(super) fn descriptor_record_item_provided<S>(
        _kernel: Pin<&mut S::KernelPayload>,
        _payload: &mut S,
        context: RecordItemContext<'_, '_, '_>,
    ) -> UringResult<UringRecordItem>
    where
        S: UringOpSpec,
    {
        let RecordItemContext {
            result, flags, env, ..
        } = context;
        let Some(mut lease) = env.take_selected_lease(flags, result)? else {
            return Ok(UringRecordItem::new(UringUserPayload::from_storage(
                UringUserPayloadStorage::ProvidedBuf(ProvidedBuf { buf: None }),
            )));
        };
        env.handoff_selected_lease(&mut lease)?;
        env.begin_selected_delivery(&mut lease)?;
        let lease_len = lease.len();
        let buf = lease.take_buffer_for_delivery();
        env.settle_selected_lease(&mut lease, ProvidedBufLeaseAction::Republish)?;
        debug_assert_eq!(buf.as_ref().map(FixedBuf::len), Some(lease_len));
        Ok(UringRecordItem::new(UringUserPayload::from_storage(
            UringUserPayloadStorage::ProvidedBuf(ProvidedBuf { buf }),
        )))
    }

    pub(super) fn descriptor_record_item_accepted<S>(
        _kernel: Pin<&mut S::KernelPayload>,
        _payload: &mut S,
        _context: RecordItemContext<'_, '_, '_>,
    ) -> UringResult<UringRecordItem>
    where
        S: UringOpSpec,
    {
        Ok(UringRecordItem::new(UringUserPayload::from_storage(
            UringUserPayloadStorage::AcceptedSocket(AcceptedSocket),
        )))
    }

    pub(super) fn udp_receive_pump_report(
        scope: &'static str,
        error: ReceivePumpError,
    ) -> Report<UringError> {
        UringError::InvalidState
            .report(scope, "UDP receive pump rejected a multishot transition")
            .with_ctx("pump_error", format!("{error:?}"))
    }

    pub(super) fn descriptor_finish_udp_packet(
        kernel: Pin<&mut UdpRecvMultiPayload>,
        user: &mut UdpRecvMulti,
        env: &mut CqeEnv<'_, '_>,
        rearm_generation: Option<u32>,
    ) -> UringResult<UringRecordItem> {
        const SCOPE: &str = "uring.op.spec.finish_udp_recv_multi";
        let kernel = unsafe { kernel.get_unchecked_mut() };
        let Some(delivery) = user.receive_pump_mut().next_datagram_delivery() else {
            if let Some(generation) = rearm_generation
                && user.receive_pump().pending_datagrams() == 0
                && user.receive_pump().permits_available() > 0
            {
                return Ok(UringRecordItem::retained(Some(generation)));
            }
            return Ok(UringRecordItem::retained(None));
        };
        let key = delivery.key();
        let Some(mut pending) = kernel.take_pending_by_key(key) else {
            drop(delivery);
            return Err(UringError::InvalidState.report(
                SCOPE,
                "UDP delivery permit had no matching provided-buffer lease",
            ));
        };

        if let Err(report) = env.begin_selected_delivery(&mut pending.lease) {
            let _ =
                env.settle_selected_lease(&mut pending.lease, ProvidedBufLeaseAction::Quarantine);
            drop(delivery);
            return Err(report);
        }
        let datagram_capacity = user.receive_pump().config().datagram_capacity().get();
        let parsed = match pending.lease.as_slice() {
            Some(bytes) => UdpRecvMsgParser::parse(bytes, kernel.message(), datagram_capacity),
            None => Err(UringError::InvalidState.report(SCOPE, "pending UDP lease has no buffer")),
        };
        let parsed = match parsed {
            Ok(parsed) if parsed.flags & MSG_TRUNC as u32 == 0 => parsed,
            Ok(parsed) => {
                let report = UringError::InvalidInput
                    .report(SCOPE, "UDP datagram was truncated by the selected buffer")
                    .with_ctx("payload_length", parsed.payload.len())
                    .with_ctx("datagram_capacity", datagram_capacity);
                let _ = env
                    .settle_selected_lease(&mut pending.lease, ProvidedBufLeaseAction::Quarantine);
                drop(delivery);
                return Err(report);
            }
            Err(report) => {
                let _ = env
                    .settle_selected_lease(&mut pending.lease, ProvidedBufLeaseAction::Quarantine);
                drop(delivery);
                return Err(report);
            }
        };
        if parsed.payload.len() != pending.len || parsed.addr != pending.addr {
            let report = UringError::InvalidState
                .report(
                    SCOPE,
                    "pending UDP lease metadata no longer matches recvmsg payload",
                )
                .with_ctx("metadata_length", pending.len)
                .with_ctx("payload_length", parsed.payload.len());
            let _ =
                env.settle_selected_lease(&mut pending.lease, ProvidedBufLeaseAction::Quarantine);
            drop(delivery);
            return Err(report);
        }

        let mut output = FixedBuf::alloc_heap(
            NonZeroUsize::new(datagram_capacity).expect("validated datagram capacity is non-zero"),
            0,
        )
        .map_err(|_| UringError::Internal.report(SCOPE, "UDP output buffer allocation failed"))?;
        output.spare_capacity_mut()[..parsed.payload.len()].copy_from_slice(parsed.payload);
        output.set_len(parsed.payload.len());
        if let Err(report) =
            env.settle_selected_lease(&mut pending.lease, ProvidedBufLeaseAction::Republish)
        {
            drop(delivery);
            return Err(report);
        }
        let packet = user
            .receive_pump_mut()
            .finish_datagram_delivery(delivery, output)
            .map_err(|error| Self::udp_receive_pump_report(SCOPE, error))?;
        let item = UringUserPayload::from_storage(UringUserPayloadStorage::UdpRecvPacket(packet));
        if let Some(generation) = rearm_generation
            && user.receive_pump().pending_datagrams() == 0
            && user.receive_pump().permits_available() > 0
        {
            return Ok(UringRecordItem::new_with_rearm(item, generation));
        }
        Ok(UringRecordItem::new(item))
    }

    pub(super) fn descriptor_record_item_udp_packet(
        mut kernel: Pin<&mut UdpRecvMultiPayload>,
        user: &mut UdpRecvMulti,
        context: RecordItemContext<'_, '_, '_>,
    ) -> UringResult<UringRecordItem> {
        const SCOPE: &str = "uring.op.spec.record_item_udp_recv_multi";
        let RecordItemContext {
            result, flags, env, ..
        } = context;
        let rearm_generation = if result == -ENOBUFS {
            let event = user
                .receive_pump_mut()
                .retain_for_rearm()
                .map_err(|error| Self::udp_receive_pump_report(SCOPE, error))?;
            match event {
                ReceivePumpEvent::Retained {
                    logical_receiver_generation,
                } => Some(logical_receiver_generation),
                _ => None,
            }
        } else {
            None
        };
        if result == -ENOBUFS {
            return Self::descriptor_finish_udp_packet(kernel, user, env, rearm_generation);
        }
        let Some(mut lease) = env.take_selected_lease(flags, result)? else {
            let report = if result < 0 {
                UringError::CompletionWait
                    .report(SCOPE, "UDP recvmsg completion selected no provided buffer")
                    .set_error_code(-result)
            } else {
                UringError::InvalidInput.report(SCOPE, "UDP recvmsg completion selected no buffer")
            };
            return Err(report);
        };

        if let Err(report) = env.handoff_selected_lease(&mut lease) {
            let _ = env.settle_selected_lease(&mut lease, ProvidedBufLeaseAction::Quarantine);
            return Err(report);
        }
        if result < 0 {
            let report = UringError::CompletionWait
                .report(SCOPE, "UDP recvmsg completion returned an error")
                .set_error_code(-result);
            let _ = env.settle_selected_lease(&mut lease, ProvidedBufLeaseAction::Quarantine);
            return Err(report);
        }

        let datagram_capacity = user.receive_pump().config().datagram_capacity().get();
        let parsed = match lease.as_slice() {
            Some(bytes) => UdpRecvMsgParser::parse(
                bytes,
                kernel.as_ref().get_ref().message(),
                datagram_capacity,
            ),
            None => Err(UringError::InvalidState.report(SCOPE, "selected lease has no buffer")),
        }?;
        if parsed.flags & MSG_TRUNC as u32 != 0 {
            let report = UringError::InvalidInput
                .report(SCOPE, "UDP datagram was truncated by the selected buffer")
                .with_ctx("payload_length", parsed.payload.len())
                .with_ctx("datagram_capacity", datagram_capacity);
            let _ = env.settle_selected_lease(&mut lease, ProvidedBufLeaseAction::Quarantine);
            return Err(report);
        }
        let admission = match user
            .receive_pump_mut()
            .admit_datagram(parsed.payload.len(), parsed.addr)
        {
            Ok(admission) => admission,
            Err(error) => {
                let _ = env.settle_selected_lease(&mut lease, ProvidedBufLeaseAction::Quarantine);
                return Err(Self::udp_receive_pump_report(SCOPE, error));
            }
        };
        unsafe { kernel.as_mut().get_unchecked_mut() }.push_pending(UdpPendingLease {
            key: admission.key(),
            addr: parsed.addr,
            len: parsed.payload.len(),
            lease,
        });

        let rearm_generation = if !cqueue::Flags::more(flags) {
            let event = user
                .receive_pump_mut()
                .retain_for_rearm()
                .map_err(|error| Self::udp_receive_pump_report(SCOPE, error))?;
            match event {
                ReceivePumpEvent::Retained {
                    logical_receiver_generation,
                } => Some(logical_receiver_generation),
                _ => None,
            }
        } else {
            None
        };
        Self::descriptor_finish_udp_packet(kernel, user, env, rearm_generation)
    }

    pub(super) fn descriptor_resume_item_udp_packet(
        kernel: Pin<&mut UdpRecvMultiPayload>,
        user: &mut UdpRecvMulti,
        _token: OpToken,
        logical_receiver_generation: u32,
        env: &mut CqeEnv<'_, '_>,
    ) -> UringResult<UringRecordItem> {
        if user.receive_pump().logical_receiver_generation() != logical_receiver_generation {
            return Ok(UringRecordItem::ignored());
        }
        let rearm_generation = user
            .receive_pump()
            .rearm_pending()
            .then(|| user.receive_pump().logical_receiver_generation());
        Self::descriptor_finish_udp_packet(kernel, user, env, rearm_generation)
    }

    pub(super) fn descriptor_cleanup_none(_result: i32) -> CompletionCleanupGuard {
        CompletionCleanupGuard::default()
    }
}
