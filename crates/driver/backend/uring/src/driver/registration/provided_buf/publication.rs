use veloq_std::{boxed::Box, collections::BitSet, io, vec::Vec};

use tracing::debug;
use veloq_buf::{AnyBufPool, FixedBuf};
use veloq_io_uring::{Submitter, types::BufRingItem};

use crate::{
    config::{MAX_PROVIDED_BUF_ENTRIES, ProvidedBufConfig},
    driver::registration::RingLifetimeToken,
    error::UringError,
};

use super::{
    ProvidedBufGroup,
    lease::{ProvidedBufInitializationFailure, ProvidedBufInitializationResult},
    lifetime::RingMappingOwner,
    snapshot::ProvidedBufStats,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProvidedBufRegistrationState {
    Unregistered,
    RegisteredButNotPublished,
    Registered,
    UnregisterUnknown,
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProvidedBufBidState {
    DriverOwnedUnpublished,
    KernelPublished { publish_seq: u64 },
    SelectedLease { publish_seq: u64 },
    PendingLease { publish_seq: u64 },
    DeliveryLease { publish_seq: u64 },
    Vacant,
    Quarantined,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProvidedBufBidAnomaly {
    pub(super) bid: u16,
    pub(super) cqe_flags: Option<u32>,
    pub(super) last_publish_sequence: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ProvidedBufGroupHealth {
    Healthy,
    Degraded,
    Quarantined,
}

pub(super) struct ProvidedBufInitializationParams<'submitter, 'ring, F, P, U> {
    pub(super) submitter: &'submitter Submitter<'ring>,
    pub(super) config: ProvidedBufConfig,
    pub(super) pool: AnyBufPool,
    pub(super) ring_lifetime_token: RingLifetimeToken,
    pub(super) allocate: F,
    pub(super) publish: P,
    pub(super) unregister: U,
}

pub(super) struct ProvidedBufPublication {
    pending_bids: Vec<u16>,
    pending_bid_bits: BitSet,
    pending_refill_bits: BitSet,
    publish_items: Vec<BufRingItem>,
    next_publish_sequence: u64,
}

pub(super) struct ProvidedBufPublicationParts<'a> {
    pub(super) ring: &'a mut RingMappingOwner,
    pub(super) buffers: &'a [Option<FixedBuf>],
    pub(super) bid_states: &'a mut [ProvidedBufBidState],
    pub(super) last_publish_sequences: &'a mut [Option<u64>],
    pub(super) stats: &'a mut ProvidedBufStats,
}

impl ProvidedBufPublication {
    pub(super) fn new(entries: u16) -> Self {
        Self {
            pending_bids: Vec::new(),
            pending_bid_bits: BitSet::new(entries as usize),
            pending_refill_bits: BitSet::new(entries as usize),
            publish_items: Vec::with_capacity(entries as usize),
            next_publish_sequence: 0,
        }
    }

    pub(super) fn stage(
        &mut self,
        bid: u16,
        is_refill: bool,
        can_stage: bool,
        state: Option<ProvidedBufBidState>,
    ) -> Result<(), &'static str> {
        if !can_stage || state != Some(ProvidedBufBidState::DriverOwnedUnpublished) {
            return Err("published a provided bid from an invalid state");
        }
        let index = bid as usize;
        if !self.pending_bid_bits.get(index).unwrap_or(false) {
            if self.pending_bid_bits.set(index).is_err() {
                return Err("staged a provided bid outside the publication ledger");
            }
            self.pending_bids.push(bid);
        }
        if is_refill {
            debug_assert!(self.pending_refill_bits.set(index).is_ok());
        }
        Ok(())
    }

    pub(super) fn flush<P>(
        &mut self,
        parts: ProvidedBufPublicationParts<'_>,
        mut publish: P,
    ) -> Result<(), (u16, &'static str)>
    where
        P: FnMut(&mut RingMappingOwner, &[BufRingItem]) -> io::Result<()>,
    {
        if self.pending_bids.is_empty() {
            return Ok(());
        }
        let ProvidedBufPublicationParts {
            ring,
            buffers,
            bid_states,
            last_publish_sequences,
            stats,
        } = parts;
        self.publish_items.clear();
        for index in 0..self.pending_bids.len() {
            let bid = self.pending_bids[index];
            let Some(Some(buffer)) = buffers.get(bid as usize) else {
                return Err((bid, "published a provided bid without a buffer owner"));
            };
            let Ok(len) = u32::try_from(buffer.capacity()) else {
                return Err((bid, "provided buffer capacity exceeds the ring ABI"));
            };
            // SAFETY: the group retains each buffer owner until the kernel consumes its entry.
            // 安全性：内核消费对应条目前，group 始终持有每个 buffer 的所有权。
            let Ok(item) = (unsafe { BufRingItem::new(buffer.as_ptr() as u64, len, bid) }) else {
                return Err((bid, "provided buffer descriptor is invalid"));
            };
            self.publish_items.push(item);
        }

        if publish(ring, &self.publish_items).is_err() {
            return Err((0, "provided buffer batch publication was rejected"));
        }

        let mut refilled = 0_u64;
        for index in 0..self.pending_bids.len() {
            let bid = self.pending_bids[index];
            let state_index = bid as usize;
            let publish_sequence = self.next_publish_sequence;
            self.next_publish_sequence = self.next_publish_sequence.saturating_add(1);
            if let Some(last_publish_sequence) = last_publish_sequences.get_mut(state_index) {
                *last_publish_sequence = Some(publish_sequence);
            }
            if let Some(state) = bid_states.get_mut(state_index) {
                *state = ProvidedBufBidState::KernelPublished {
                    publish_seq: publish_sequence,
                };
            }
            if self.pending_refill_bits.get(state_index).unwrap_or(false) {
                refilled = refilled.saturating_add(1);
            }
            debug_assert!(self.pending_bid_bits.clear(state_index).is_ok());
            debug_assert!(self.pending_refill_bits.clear(state_index).is_ok());
        }
        let published = self.pending_bids.len();
        self.pending_bids.clear();
        stats.note_published(published as u16, refilled);
        Ok(())
    }

    pub(super) fn initialize<F, P, U>(
        params: ProvidedBufInitializationParams<'_, '_, F, P, U>,
    ) -> ProvidedBufInitializationResult
    where
        F: Fn(&ProvidedBufGroup) -> Option<FixedBuf>,
        P: FnMut(&mut RingMappingOwner, &[BufRingItem]) -> io::Result<()>,
        U: FnOnce(u16) -> Result<(), i32>,
    {
        let ProvidedBufInitializationParams {
            submitter,
            config,
            pool,
            ring_lifetime_token,
            allocate,
            mut publish,
            unregister,
        } = params;
        let entries = config.entries.get();
        if !entries.is_power_of_two() || entries > MAX_PROVIDED_BUF_ENTRIES {
            let report = UringError::InvalidInput
                .report(
                    "uring.provided_buf.new",
                    "provided buffer ring entries are invalid",
                )
                .with_ctx("entries", entries)
                .with_ctx("max_entries", MAX_PROVIDED_BUF_ENTRIES);
            return Err(Box::new(ProvidedBufInitializationFailure::new(
                report, None,
            )));
        }

        let ring = match RingMappingOwner::new(entries, ring_lifetime_token.clone()) {
            Ok(ring) => ring,
            Err(report) => {
                return Err(Box::new(ProvidedBufInitializationFailure::new(
                    report, None,
                )));
            }
        };
        let mut group =
            ProvidedBufGroup::new_unregistered(ring, ring_lifetime_token, entries, config, pool);

        for bid in 0..entries {
            let buffer = allocate(&group);
            group.store_initial_buffer(bid, buffer);
        }
        if !group.has_initial_buffer(entries) {
            let report = UringError::Registration
                .report(
                    "uring.provided_buf.new",
                    "buffer pool could not fill a single provided buffer",
                )
                .with_ctx("entries", entries)
                .with_ctx("buf_size", config.buf_size.get());
            return Err(Box::new(ProvidedBufInitializationFailure::new(
                report,
                Some(group),
            )));
        }

        if let Err(report) = group.register_initial_ring(submitter, entries) {
            return Err(Box::new(ProvidedBufInitializationFailure::new(
                report,
                Some(group),
            )));
        }
        group.stage_initial_buffers(entries);
        if !group.publish_initial_batch(&mut publish) {
            let report = UringError::Registration
                .report(
                    "uring.provided_buf.new",
                    "provided buffer ring batch publication failed",
                )
                .with_ctx("entries", entries)
                .with_ctx("publication_failed", true);
            let publication_errno = report
                .error_code()
                .and_then(|code| i32::try_from(code).ok());
            return match group.try_unregister_with(unregister) {
                Ok(()) => Err(Box::new(ProvidedBufInitializationFailure::new(
                    report, None,
                ))),
                Err(failure) => {
                    let (unregister_report, group) = (*failure).into_parts();
                    let unregister_report = match publication_errno {
                        Some(errno) => unregister_report
                            .with_ctx("publication_errno", errno)
                            .with_ctx("publication_failed", true),
                        None => unregister_report,
                    };
                    Err(Box::new(ProvidedBufInitializationFailure::new(
                        unregister_report,
                        Some(group),
                    )))
                }
            };
        }
        group.finish_initial_publication();
        debug!(
            bgid = group.group_id(),
            entries,
            filled = group.available_buffers(),
            buf_size = config.buf_size.get(),
            "registered provided buffer ring"
        );
        Ok(group)
    }
}
