//! provided buffer ring（`IORING_REGISTER_PBUF_RING`，Linux 5.19+）。
//!
//! 环里的每个条目是 `(addr, len, bid)`。CQE 选中条目后，buffer 先进入显式
//! [`ProvidedBufLease`]。lease settle 以前，原 bid 不得重新发布；这条规则对 UDP 的
//! bounded pending 尤其重要，因为 datagram 可能暂时没有 packet permit。
//!
//! TCP 的兼容路径仍然可以把 lease 中的 buffer 移交给 record，但也必须在同一个显式
//! settlement transaction 中为原 bid 准备 replacement。UDP 则可以把 lease 保留在
//! pending ledger 中，直到 output buffer 和 permit 都可用。
//! This module owns the provided-buffer ring. A selected BID stays in an explicit lease until settlement; TCP may replace it after delivery, while UDP can retain it in a bounded pending ledger.

use veloq_std::{boxed::Box, collections::BitSet, io, mem, num::NonZeroUsize, vec::Vec};

use tracing::warn;
use veloq_buf::{AnyBufPool, BufPool, FixedBuf};
use veloq_io_uring::{Submitter, cqueue, types::BufRingItem};

mod lease;
mod lifetime;
mod publication;
mod snapshot;

pub(crate) use lease::{
    ProvidedBufInitializationResult, ProvidedBufLease, ProvidedBufLeaseAction,
    ProvidedBufLeaseError, ProvidedBufLeasePhase, ProvidedBufPort, ProvidedBufUnregisterFailure,
    ProvidedBufUnregisterResult,
};
use lifetime::RingMappingOwner;
#[cfg(test)]
use lifetime::RingMappingOwnerState;
pub(crate) use lifetime::{PROVIDED_BUF_GROUP_ID, RingLifetimeOwner, RingLifetimeToken};
use publication::{
    ProvidedBufBidAnomaly, ProvidedBufBidState, ProvidedBufGroupHealth,
    ProvidedBufInitializationParams, ProvidedBufPublication, ProvidedBufPublicationParts,
    ProvidedBufRegistrationState,
};
pub use snapshot::ProvidedBufferSnapshot;
use snapshot::{ProvidedBufOwnershipParts, ProvidedBufStats};

use crate::{
    config::ProvidedBufConfig,
    driver::env::ProvidedBufSqeInfo,
    error::{UringError, UringResult},
};

pub(crate) struct ProvidedBufGroup {
    bgid: u16,
    ring: RingMappingOwner,
    /// The token is kept next to the resource ledger so every bid owner can assert that the ring
    /// lifetime still covers its mapping. The driver-owned copy is dropped only after `IoUring`.
    /// lifetime token 与资源账本相邻保存，用于校验映射始终由 ring 生命周期覆盖；驱动持有的副本仅在 `IoUring` 之后释放。
    ring_lifetime_token: RingLifetimeToken,
    registration_state: ProvidedBufRegistrationState,
    health: ProvidedBufGroupHealth,
    /// `bufs` 保存实际的 `FixedBuf` owner；`bid_states` 才说明这个 owner 是否已发布、被
    /// kernel 选中、交给用户或暂时空缺。两者必须通过状态方法一起变更。
    /// Published `FixedBuf` owners are leaked together with the mapping if ring teardown is not
    /// proven complete. This prevents the kernel from observing freed buffer addresses.
    bufs: mem::ManuallyDrop<Box<[Option<FixedBuf>]>>,
    bid_states: Box<[ProvidedBufBidState]>,
    /// 每个 bid 最近一次成功发布的序号；即使 bid 已经转为 vacant/user-owned，异常记录仍
    /// 能保留它最后一次进入内核可见状态的上下文。
    /// Stores the last successful publication sequence per BID so anomaly reports retain its latest kernel-visible context.
    last_publish_sequences: Box<[Option<u64>]>,
    /// 补充失败留下的空洞，等后续任一次完成顺手重试。 Vacant bids left by a failed refill are retried on a later completion.
    vacant: Vec<u16>,
    /// `retry_vacant` 的消费 scratch，避免每次重试都通过 `mem::take` 丢失容量。
    /// Scratch space consumed by `retry_vacant`, preserving capacity across refill attempts.
    vacant_retry: Vec<u16>,
    vacant_bids: BitSet,
    /// 待批量发布的 bid，membership 由位图保证，避免重复 CQE/取消路径重复入队。
    /// Staged BIDs are batch-published; the membership bitset prevents duplicate enqueueing from CQE or cancellation paths.
    publication: ProvidedBufPublication,
    /// 处于 selected lease 状态的 bid 数量，避免析构/取消检查扫描整张表。
    /// Counts selected leases so drop and cancellation checks avoid scanning the full BID table.
    selected_bids: BitSet,
    selected_bid_count: usize,
    buf_size: NonZeroUsize,
    pool: AnyBufPool,
    stats: ProvidedBufStats,
    bid_anomalies: u64,
    last_bid_anomaly: Option<ProvidedBufBidAnomaly>,
}

impl ProvidedBufGroup {
    /// 注册一组 provided buffer 并把它填满。
    ///
    /// 失败一律**降级而不是致命**（调用方据此把 `provided_buffers` 能力留在 `false`）：
    /// 5.6–5.18 的内核根本没有 `IORING_REGISTER_PBUF_RING`，那是仓库声明支持的区间。
    /// Registers and fills a provided-buffer group. Unsupported kernels degrade this capability while retaining the resource error for operations that require it.
    pub(crate) fn new(
        submitter: &Submitter<'_>,
        config: ProvidedBufConfig,
        pool: AnyBufPool,
        ring_lifetime_token: RingLifetimeToken,
    ) -> ProvidedBufInitializationResult {
        ProvidedBufPublication::initialize(ProvidedBufInitializationParams {
            submitter,
            config,
            pool,
            ring_lifetime_token,
            allocate: |group: &Self| group.alloc_buf(),
            publish: |ring: &mut RingMappingOwner, items: &[BufRingItem]| ring.publish(items),
            unregister: |bgid| {
                submitter
                    .unregister_buf_ring(bgid)
                    .map_err(|err| err.raw_os_error().unwrap_or(libc::EIO))
            },
        })
    }

    fn new_unregistered(
        ring: RingMappingOwner,
        ring_lifetime_token: RingLifetimeToken,
        entries: u16,
        config: ProvidedBufConfig,
        pool: AnyBufPool,
    ) -> Self {
        Self {
            bgid: PROVIDED_BUF_GROUP_ID,
            ring,
            ring_lifetime_token,
            registration_state: ProvidedBufRegistrationState::Unregistered,
            health: ProvidedBufGroupHealth::Healthy,
            bufs: mem::ManuallyDrop::new((0..entries).map(|_| None).collect()),
            bid_states: (0..entries).map(|_| ProvidedBufBidState::Vacant).collect(),
            last_publish_sequences: (0..entries).map(|_| None).collect(),
            vacant: Vec::new(),
            vacant_retry: Vec::new(),
            vacant_bids: BitSet::new(entries as usize),
            publication: ProvidedBufPublication::new(entries),
            selected_bids: BitSet::new(entries as usize),
            selected_bid_count: 0,
            buf_size: config.buf_size,
            pool,
            stats: ProvidedBufStats::default(),
            bid_anomalies: 0,
            last_bid_anomaly: None,
        }
    }

    pub(super) fn store_initial_buffer(&mut self, bid: u16, buffer: Option<FixedBuf>) {
        match buffer {
            Some(buffer) => self.store_driver_owned(bid, buffer),
            None => {
                self.stats.note_refill_failed();
                self.queue_vacant(bid);
            }
        }
    }

    pub(super) fn has_initial_buffer(&self, entries: u16) -> bool {
        (0..entries).any(|bid| self.has_unpublished_buf(bid))
    }

    pub(super) fn register_initial_ring(
        &mut self,
        submitter: &Submitter<'_>,
        entries: u16,
    ) -> UringResult<()> {
        self.ring.register(submitter, self.bgid, entries)?;
        self.registration_state = ProvidedBufRegistrationState::RegisteredButNotPublished;
        Ok(())
    }

    pub(super) fn stage_initial_buffers(&mut self, entries: u16) {
        for bid in 0..entries {
            if self.has_unpublished_buf(bid) {
                self.stage_driver_owned(bid, true);
            }
        }
    }

    fn publish_initial_batch<P>(&mut self, publish: P) -> bool
    where
        P: FnMut(&mut RingMappingOwner, &[BufRingItem]) -> io::Result<()>,
    {
        self.flush_pending_publish_with(publish)
    }

    pub(super) fn finish_initial_publication(&mut self) {
        self.mark_published();
        self.stats.set_initial_level();
    }

    pub(super) const fn group_id(&self) -> u16 {
        self.bgid
    }

    pub(super) fn available_buffers(&self) -> u16 {
        self.stats.available()
    }

    #[inline]
    pub(crate) fn sqe_info(&self) -> Option<ProvidedBufSqeInfo> {
        self.is_usable()
            .then(|| ProvidedBufSqeInfo::new(self.bgid, self.buf_size.get() as u32))
    }

    #[inline]
    pub(crate) fn stats(&self) -> ProvidedBufferSnapshot {
        self.stats.snapshot()
    }

    /// 内核报「环里没 buffer 了」。
    /// Records that the kernel reported an empty buffer ring.
    #[inline]
    pub(crate) fn note_exhausted(&mut self) {
        self.stats.note_exhausted();
    }

    /// Claim the buffer selected by this CQE without publishing a replacement.
    ///
    /// `res` is the kernel-written length. Negative results still carry a selected bid on some
    /// error paths, so their lease length is zero and the lease must still be settled.
    /// 取得 CQE 选中的 buffer，但不立即补入 ring。负结果仍可能带有 BID，因此 lease 长度为零也必须结算。
    pub(crate) fn claim_selected_lease(
        &mut self,
        flags: u32,
        res: i32,
    ) -> Result<Option<ProvidedBufLease>, ProvidedBufLeaseError> {
        let Some(bid) = cqueue::buffer_select(flags) else {
            return Ok(None);
        };
        if !self.is_usable() {
            return Err(ProvidedBufLeaseError::InvalidState);
        }
        let Some((publish_seq, mut buf)) = self.claim_kernel_selected(bid, flags) else {
            self.quarantine();
            return Err(ProvidedBufLeaseError::InvalidState);
        };
        let filled = usize::try_from(res).unwrap_or(0).min(buf.capacity());
        buf.set_len(filled);
        Ok(Some(ProvidedBufLease::new(
            self.bgid,
            bid,
            publish_seq,
            buf,
            filled,
        )))
    }

    /// Claim and hand a selected buffer to the TCP compatibility record path.
    ///
    /// This is intentionally a wrapper around the explicit lease API. UDP must retain the lease
    /// until its copy/delivery transaction has settled.
    /// 将选中 buffer 交给 TCP 兼容记录路径；UDP 必须保留 lease，直到复制或投递事务完成。
    #[cfg(test)]
    pub(crate) fn take_selected_for_test(&mut self, flags: u32, res: i32) -> Option<FixedBuf> {
        let mut lease = self.claim_selected_lease(flags, res).ok().flatten()?;
        self.begin_delivery(&mut lease).ok()?;
        let buf = lease.take_buffer_for_delivery()?;
        self.settle_selected_lease(&mut lease, ProvidedBufLeaseAction::Republish)
            .ok()?;
        Some(buf)
    }

    /// This completion must be discarded without taking ownership of its payload. The selected
    /// buffer remains in the group and is republished by the same settlement transaction.
    /// 丢弃此完成时不接管 payload；buffer 继续由 group 持有，并在同一结算事务中重新发布。
    pub(crate) fn discard_selected(&mut self, flags: u32) -> bool {
        let Some(bid) = cqueue::buffer_select(flags) else {
            return false;
        };
        if !self.is_usable() {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "discarded completion arrived after provided group quarantine",
            );
            return true;
        }
        let Some(publish_seq) = self.prepare_kernel_return(bid, flags) else {
            self.quarantine();
            return true;
        };
        let result = self.republish_claimed(bid, publish_seq, None);
        if result.is_ok() {
            self.stats.note_returned();
            false
        } else {
            self.quarantine();
            true
        }
    }

    /// 尝试反注册。**顺序不能反**：内核在反注册之前仍可能往环里读写。
    ///
    /// 成功才消费 group；失败会把 group 原样放入错误载体，调用方必须继续持有它。
    /// Unregisters before releasing the group, because the kernel may access the ring until unregister completes; failures return ownership to the caller.
    pub(crate) fn try_unregister(self, submitter: &Submitter<'_>) -> ProvidedBufUnregisterResult {
        self.try_unregister_with(|bgid| {
            submitter
                .unregister_buf_ring(bgid)
                .map_err(|err| err.raw_os_error().unwrap_or(libc::EIO))
        })
    }

    /// 反注册的最小注入点。生产路径通过 [`Self::try_unregister`] 使用 io_uring syscall；
    /// 测试可以传入闭包验证失败时 group 没有被消费，不需要全局可变 syscall hook。
    /// Provides a narrow syscall injection point so tests can verify that a failed unregister preserves the group without a global hook.
    pub(crate) fn try_unregister_with<F>(mut self, unregister: F) -> ProvidedBufUnregisterResult
    where
        F: FnOnce(u16) -> Result<(), i32>,
    {
        if self.selected_bid_count != 0 {
            let report = UringError::InvalidState.report(
                "uring.provided_buf.unregister.leases",
                "provided buffer group still has unsettled leases",
            );
            return Err(Box::new(ProvidedBufUnregisterFailure::new(report, self)));
        }
        if !self.ring.is_registered() {
            let report = UringError::InvalidState.report(
                "uring.provided_buf.unregister.state",
                "provided buffer ring was not kernel-registered",
            );
            return Err(Box::new(ProvidedBufUnregisterFailure::new(report, self)));
        }
        let bgid = self.bgid;
        match unregister(bgid) {
            Ok(()) => {
                self.ring.mark_released();
                self.registration_state = ProvidedBufRegistrationState::Released;
                self.bid_states
                    .iter_mut()
                    .for_each(|state| *state = ProvidedBufBidState::Retired);
                Ok(())
            }
            Err(err) => {
                let mut group = self;
                group.registration_state = ProvidedBufRegistrationState::UnregisterUnknown;
                group.ring.mark_unregister_unknown();
                let report = UringError::Registration
                    .io_report(
                        "uring.provided_buf.unregister",
                        io::Error::from_raw_os_error(err),
                    )
                    .with_ctx("bgid", bgid);
                Err(Box::new(ProvidedBufUnregisterFailure::new(report, group)))
            }
        }
    }

    /// Put a selected lease into the backend pending ledger. 将选中 lease 放入后端待处理账本。
    pub(crate) fn handoff_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> Result<(), ProvidedBufLeaseError> {
        lease.validate_with(self)?;
        if lease.phase() != ProvidedBufLeasePhase::Selected {
            return Err(ProvidedBufLeaseError::InvalidState);
        }
        self.set_bid_state(
            lease.bid(),
            ProvidedBufBidState::PendingLease {
                publish_seq: lease.publish_seq(),
            },
        );
        lease.set_phase(ProvidedBufLeasePhase::Pending);
        self.refresh_ownership_snapshot();
        Ok(())
    }

    /// Mark a selected or pending lease as being copied to an independent output buffer. 标记选中或待处理 lease 正在复制到独立输出 buffer。
    pub(crate) fn begin_delivery(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> Result<(), ProvidedBufLeaseError> {
        lease.validate_with(self)?;
        if !matches!(
            lease.phase(),
            ProvidedBufLeasePhase::Selected | ProvidedBufLeasePhase::Pending
        ) {
            return Err(ProvidedBufLeaseError::InvalidState);
        }
        self.set_bid_state(
            lease.bid(),
            ProvidedBufBidState::DeliveryLease {
                publish_seq: lease.publish_seq(),
            },
        );
        lease.set_phase(ProvidedBufLeasePhase::Delivery);
        self.refresh_ownership_snapshot();
        Ok(())
    }

    /// Settle a lease exactly once. Republish may reuse the selected buffer or allocate a
    /// replacement after a TCP delivery took the original buffer.
    /// lease 只能结算一次；若 TCP 投递已取走原 buffer，重新发布时可分配替代 buffer。
    pub(crate) fn settle_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
        action: ProvidedBufLeaseAction,
    ) -> Result<(), ProvidedBufLeaseError> {
        lease.validate_with(self)?;
        if matches!(
            lease.phase(),
            ProvidedBufLeasePhase::Settled | ProvidedBufLeasePhase::Quarantined
        ) {
            return Err(ProvidedBufLeaseError::AlreadySettled);
        }

        match action {
            ProvidedBufLeaseAction::Quarantine => {
                self.quarantine_lease(lease);
                Ok(())
            }
            ProvidedBufLeaseAction::Republish => {
                let Some(buf) = lease.take_owned_buffer().or_else(|| self.alloc_buf()) else {
                    self.quarantine_lease(lease);
                    return Err(ProvidedBufLeaseError::NoReplacementBuffer);
                };
                let bid = lease.bid();
                let Some(slot) = self.bufs.get_mut(bid as usize) else {
                    self.quarantine_lease(lease);
                    return Err(ProvidedBufLeaseError::BidMismatch);
                };
                if slot.is_some() {
                    self.quarantine_lease(lease);
                    return Err(ProvidedBufLeaseError::InvalidState);
                }
                *slot = Some(buf);
                self.set_bid_state(bid, ProvidedBufBidState::DriverOwnedUnpublished);
                self.clear_selected(bid);
                if !self.stage_driver_owned(bid, true) || !self.flush_pending_publish() {
                    self.quarantine_bid_after_failed_publish(bid);
                    lease.set_phase(ProvidedBufLeasePhase::Quarantined);
                    self.refresh_ownership_snapshot();
                    return Err(ProvidedBufLeaseError::PublicationFailed);
                }
                self.retry_vacant();
                if !self.flush_pending_publish() {
                    self.quarantine();
                    lease.set_phase(ProvidedBufLeasePhase::Quarantined);
                    self.refresh_ownership_snapshot();
                    return Err(ProvidedBufLeaseError::PublicationFailed);
                }
                if lease.user_buffer_taken() {
                    self.stats.note_handed_out();
                }
                lease.set_phase(ProvidedBufLeasePhase::Settled);
                self.refresh_ownership_snapshot();
                Ok(())
            }
        }
    }

    fn quarantine_lease(&mut self, lease: &mut ProvidedBufLease) {
        let bid = lease.bid();
        if let Some(slot) = self.bufs.get_mut(bid as usize)
            && slot.is_none()
        {
            *slot = lease.take_owned_buffer();
        }
        self.set_bid_state(bid, ProvidedBufBidState::Quarantined);
        self.clear_selected(bid);
        self.health = ProvidedBufGroupHealth::Quarantined;
        lease.set_phase(ProvidedBufLeasePhase::Quarantined);
        self.refresh_ownership_snapshot();
    }

    fn quarantine_bid_after_failed_publish(&mut self, bid: u16) {
        self.set_bid_state(bid, ProvidedBufBidState::Quarantined);
        self.health = ProvidedBufGroupHealth::Quarantined;
    }

    /// Quarantine a claimed lease when completion cleanup lost the lease owner before settlement. 完成清理在结算前丢失 lease owner 时隔离已领取的 lease。
    pub(crate) fn quarantine_claimed(&mut self, bid: u16, publish_seq: u64) -> bool {
        if self.bid_states.get(bid as usize)
            != Some(&ProvidedBufBidState::SelectedLease { publish_seq })
        {
            self.note_bid_anomaly(bid, "quarantined a provided bid without its selected lease");
            self.quarantine();
            return true;
        }
        self.set_bid_state(bid, ProvidedBufBidState::Quarantined);
        self.clear_selected(bid);
        self.health = ProvidedBufGroupHealth::Quarantined;
        self.refresh_ownership_snapshot();
        true
    }

    fn republish_claimed(
        &mut self,
        bid: u16,
        publish_seq: u64,
        buf: Option<FixedBuf>,
    ) -> Result<(), ProvidedBufLeaseError> {
        if self.bid_states.get(bid as usize)
            != Some(&ProvidedBufBidState::SelectedLease { publish_seq })
        {
            return Err(ProvidedBufLeaseError::PublishSequenceMismatch);
        }
        let Some(slot) = self.bufs.get_mut(bid as usize) else {
            return Err(ProvidedBufLeaseError::BidMismatch);
        };
        if slot.is_none() {
            return Err(ProvidedBufLeaseError::InvalidState);
        }
        if let Some(buf) = buf {
            *slot = Some(buf);
        }
        self.set_bid_state(bid, ProvidedBufBidState::DriverOwnedUnpublished);
        self.clear_selected(bid);
        if !self.stage_driver_owned(bid, false) || !self.flush_pending_publish() {
            self.quarantine_bid_after_failed_publish(bid);
            self.refresh_ownership_snapshot();
            return Err(ProvidedBufLeaseError::PublicationFailed);
        }
        self.retry_vacant();
        if !self.flush_pending_publish() {
            self.quarantine();
            self.refresh_ownership_snapshot();
            return Err(ProvidedBufLeaseError::PublicationFailed);
        }
        self.refresh_ownership_snapshot();
        Ok(())
    }

    /// Take the buffer owner out while retaining the publish sequence for lease validation. 取出 buffer owner，同时保留发布序号供 lease 校验。
    fn claim_kernel_selected(&mut self, bid: u16, flags: u32) -> Option<(u64, FixedBuf)> {
        let Some(slot) = self.bufs.get_mut(bid as usize) else {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "kernel selected a provided buffer id out of range",
            );
            return None;
        };
        let Some(ProvidedBufBidState::KernelPublished { publish_seq }) =
            self.bid_states.get(bid as usize).copied()
        else {
            self.note_cqe_bid_anomaly(bid, flags, "kernel selected a bid that was not published");
            return None;
        };
        let Some(buf) = slot.take() else {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "kernel selected a provided buffer we do not hold",
            );
            return None;
        };
        self.set_bid_state(bid, ProvidedBufBidState::SelectedLease { publish_seq });
        self.mark_selected(bid);
        self.note_consumed();
        self.refresh_ownership_snapshot();
        Some((publish_seq, buf))
    }

    /// 从池里取一个新 buffer 填进 `bid` 并发布。失败时 `bid` 留空，由 [`Self::retry_vacant`]
    /// 后续捡回来。
    /// Allocates a replacement buffer from the pool and publishes it; failed refill leaves the BID vacant for a later retry.
    fn refill(&mut self, bid: u16) -> bool {
        if !matches!(
            self.bid_states.get(bid as usize),
            Some(ProvidedBufBidState::Vacant)
        ) {
            self.note_bid_anomaly(bid, "refill attempted for a non-vacant provided bid");
            return false;
        }
        let Some(buf) = self.alloc_buf() else {
            self.stats.note_refill_failed();
            return false;
        };
        let Some(slot) = self.bufs.get_mut(bid as usize) else {
            return false;
        };
        *slot = Some(buf);
        self.set_bid_state(bid, ProvidedBufBidState::DriverOwnedUnpublished);
        if !self.stage_driver_owned(bid, true) {
            self.bufs[bid as usize] = None;
            self.set_bid_state(bid, ProvidedBufBidState::Vacant);
            return false;
        }
        true
    }

    fn retry_vacant(&mut self) {
        if self.vacant.is_empty() {
            return;
        }
        self.vacant_retry.clear();
        mem::swap(&mut self.vacant, &mut self.vacant_retry);
        while let Some(bid) = self.vacant_retry.pop() {
            self.clear_vacant(bid);
            if !self.refill(bid) {
                self.queue_vacant(bid);
            }
        }
    }

    fn alloc_buf(&self) -> Option<FixedBuf> {
        // 池空了就退回堆分配：provided buffer 不需要被注册进内核的固定缓冲表（环条目带的
        // 是裸地址），所以堆上的那一个一样能用，只是走不了 fixed-buffer 快路径。
        // A provided ring stores raw addresses and does not require fixed-buffer registration, so a heap buffer remains usable but misses the fixed-buffer fast path.
        self.pool
            .alloc_full(self.buf_size)
            .or_else(|| FixedBuf::alloc_heap_full(self.buf_size).ok())
    }

    #[inline]
    fn store_driver_owned(&mut self, bid: u16, buf: FixedBuf) {
        let Some(slot) = self.bufs.get_mut(bid as usize) else {
            self.note_bid_anomaly(bid, "stored a buffer for an out-of-range bid");
            return;
        };
        if !matches!(
            self.bid_states.get(bid as usize),
            Some(ProvidedBufBidState::Vacant)
        ) {
            self.note_bid_anomaly(bid, "stored a buffer in a non-vacant bid");
            return;
        }
        debug_assert!(slot.is_none());
        *slot = Some(buf);
        self.set_bid_state(bid, ProvidedBufBidState::DriverOwnedUnpublished);
    }

    #[inline]
    fn has_unpublished_buf(&self, bid: u16) -> bool {
        self.bufs.get(bid as usize).is_some_and(Option::is_some)
            && self.bid_states.get(bid as usize)
                == Some(&ProvidedBufBidState::DriverOwnedUnpublished)
    }

    #[inline]
    fn queue_vacant(&mut self, bid: u16) {
        let index = bid as usize;
        let is_queued = self.vacant_bids.get(index).unwrap_or(false);
        if !is_queued {
            debug_assert!(self.vacant_bids.set(index).is_ok());
            self.vacant.push(bid);
        }
    }

    #[inline]
    fn clear_vacant(&mut self, bid: u16) {
        debug_assert!(self.vacant_bids.clear(bid as usize).is_ok());
    }

    fn prepare_kernel_return(&mut self, bid: u16, flags: u32) -> Option<u64> {
        let Some(slot) = self.bufs.get(bid as usize) else {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "discarded completion selected an out-of-range bid",
            );
            return None;
        };
        let Some(ProvidedBufBidState::KernelPublished { publish_seq }) =
            self.bid_states.get(bid as usize).copied()
        else {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "discarded completion selected a bid that was not kernel-published",
            );
            return None;
        };
        if slot.is_none() {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "discarded completion selected a bid without a buffer owner",
            );
            return None;
        }
        self.set_bid_state(bid, ProvidedBufBidState::SelectedLease { publish_seq });
        self.mark_selected(bid);
        self.note_consumed();
        self.refresh_ownership_snapshot();
        Some(publish_seq)
    }

    #[inline]
    fn note_bid_anomaly(&mut self, bid: u16, message: &'static str) {
        self.record_bid_anomaly(bid, None, message);
    }

    #[inline]
    fn note_cqe_bid_anomaly(&mut self, bid: u16, flags: u32, message: &'static str) {
        self.record_bid_anomaly(bid, Some(flags), message);
    }

    fn record_bid_anomaly(&mut self, bid: u16, cqe_flags: Option<u32>, message: &'static str) {
        self.bid_anomalies = self.bid_anomalies.saturating_add(1);
        if self.health == ProvidedBufGroupHealth::Healthy {
            self.health = ProvidedBufGroupHealth::Degraded;
        }
        let last_publish_sequence = self
            .last_publish_sequences
            .get(bid as usize)
            .copied()
            .flatten();
        self.last_bid_anomaly = Some(ProvidedBufBidAnomaly {
            bid,
            cqe_flags,
            last_publish_sequence,
        });
        warn!(
            bid,
            cqe_flags = ?cqe_flags,
            last_publish_sequence = ?last_publish_sequence,
            anomalies = self.bid_anomalies,
            message
        );
    }

    #[inline]
    fn quarantine(&mut self) {
        self.health = ProvidedBufGroupHealth::Quarantined;
    }

    /// Stage a buffer that is still owned by this group for the next batch publication. 将仍由 group 持有的 buffer 暂存到下一批发布队列。
    fn stage_driver_owned(&mut self, bid: u16, is_refill: bool) -> bool {
        let can_stage = matches!(
            self.registration_state,
            ProvidedBufRegistrationState::RegisteredButNotPublished
                | ProvidedBufRegistrationState::Registered
        ) && self.health == ProvidedBufGroupHealth::Healthy
            && self.ring.is_registered()
            && self.ring_lifetime_token.is_alive();
        let state = self.bid_state(bid);
        match self.publication.stage(bid, is_refill, can_stage, state) {
            Ok(()) => true,
            Err(message) => {
                self.note_bid_anomaly(bid, message);
                false
            }
        }
    }

    /// Write every staged entry and perform one release tail publication. 写入全部暂存项后，以一次 release 操作发布 tail。
    fn flush_pending_publish(&mut self) -> bool {
        self.flush_pending_publish_with(|ring, items| ring.publish(items))
    }

    fn flush_pending_publish_with<P>(&mut self, publish: P) -> bool
    where
        P: FnMut(&mut RingMappingOwner, &[BufRingItem]) -> io::Result<()>,
    {
        let result = self.publication.flush(
            ProvidedBufPublicationParts {
                ring: &mut self.ring,
                buffers: self.bufs.as_ref(),
                bid_states: &mut self.bid_states,
                last_publish_sequences: &mut self.last_publish_sequences,
                stats: &mut self.stats,
            },
            publish,
        );
        match result {
            Ok(()) => {
                self.refresh_ownership_snapshot();
                debug_assert!(self.bid_ledger_is_consistent());
                true
            }
            Err((bid, message)) => {
                self.note_bid_anomaly(bid, message);
                false
            }
        }
    }

    fn mark_published(&mut self) {
        if self.registration_state == ProvidedBufRegistrationState::RegisteredButNotPublished {
            self.registration_state = ProvidedBufRegistrationState::Registered;
            self.ring.mark_published();
        }
    }

    fn note_consumed(&mut self) {
        self.stats.note_consumed();
    }

    #[inline]
    fn set_bid_state(&mut self, bid: u16, state: ProvidedBufBidState) {
        if let Some(current) = self.bid_states.get_mut(bid as usize) {
            *current = state;
        }
        self.refresh_ownership_snapshot();
    }

    fn refresh_ownership_snapshot(&mut self) {
        let mut selected = 0_u16;
        let mut pending = 0_u16;
        let mut delivery = 0_u16;
        let mut unpublished = 0_u16;
        let mut quarantined = 0_u16;
        for state in &self.bid_states {
            match state {
                ProvidedBufBidState::SelectedLease { .. } => selected = selected.saturating_add(1),
                ProvidedBufBidState::PendingLease { .. } => {
                    selected = selected.saturating_add(1);
                    pending = pending.saturating_add(1);
                }
                ProvidedBufBidState::DeliveryLease { .. } => {
                    selected = selected.saturating_add(1);
                    delivery = delivery.saturating_add(1);
                }
                ProvidedBufBidState::DriverOwnedUnpublished | ProvidedBufBidState::Vacant => {
                    unpublished = unpublished.saturating_add(1)
                }
                ProvidedBufBidState::Quarantined => quarantined = quarantined.saturating_add(1),
                ProvidedBufBidState::KernelPublished { .. } | ProvidedBufBidState::Retired => {}
            }
        }
        self.stats.set_ownership(ProvidedBufOwnershipParts {
            selected,
            pending,
            delivery,
            unpublished,
            quarantined,
        });
    }

    #[inline]
    fn mark_selected(&mut self, bid: u16) {
        let index = bid as usize;
        if self.bid_states.get(index).is_some() && !self.selected_bids.get(index).unwrap_or(false) {
            debug_assert!(self.selected_bids.set(index).is_ok());
            self.selected_bid_count = self.selected_bid_count.saturating_add(1);
        } else {
            self.note_bid_anomaly(bid, "selected a provided bid outside the state ledger");
        }
    }

    #[inline]
    fn clear_selected(&mut self, bid: u16) {
        let index = bid as usize;
        if self.selected_bid_count != 0 && self.selected_bids.get(index).unwrap_or(false) {
            debug_assert!(self.selected_bids.clear(index).is_ok());
            self.selected_bid_count -= 1;
        } else {
            self.note_bid_anomaly(bid, "cleared a provided bid without a selected owner");
        }
    }

    fn bid_ledger_is_consistent(&self) -> bool {
        self.bid_states.iter().enumerate().all(|(bid, state)| {
            let has_buf = self.bufs[bid].is_some();
            match state {
                ProvidedBufBidState::DriverOwnedUnpublished
                | ProvidedBufBidState::KernelPublished { .. } => has_buf,
                // Selected leases own the buffer outside the group. A discarded selected CQE
                // keeps it in the group until the same settlement transaction republishes it.
                // 已选中 lease 在 group 外持有 buffer；丢弃的 selected CQE 会让 buffer 留在 group 内，等待同一结算事务重新发布。
                ProvidedBufBidState::SelectedLease { .. }
                | ProvidedBufBidState::PendingLease { .. }
                | ProvidedBufBidState::DeliveryLease { .. } => true,
                ProvidedBufBidState::Vacant | ProvidedBufBidState::Retired => !has_buf,
                ProvidedBufBidState::Quarantined => true,
            }
        })
    }

    #[inline]
    pub(crate) fn is_usable(&self) -> bool {
        self.health == ProvidedBufGroupHealth::Healthy
            && self.registration_state == ProvidedBufRegistrationState::Registered
            && self.ring.is_registered()
            && self.ring_lifetime_token.is_alive()
    }

    #[inline]
    pub(crate) fn is_kernel_registered(&self) -> bool {
        self.ring.is_registered()
    }

    pub(crate) fn has_selected_bids(&self) -> bool {
        self.selected_bid_count != 0
    }

    fn bid_state(&self, bid: u16) -> Option<ProvidedBufBidState> {
        self.bid_states.get(bid as usize).copied()
    }
}

impl Drop for ProvidedBufGroup {
    fn drop(&mut self) {
        #[cfg(test)]
        // Unit-test groups emulate a registered ring without owning an `IoUring`. Mark the
        // standalone token closed before the resource owners drop so the fixture models the same
        // post-ring teardown ordering as the production driver.
        // 单元测试中的 group 没有持有 `IoUring`；先关闭独立 token，使夹具模拟生产驱动先销毁 ring 再释放资源的顺序。
        self.ring_lifetime_token.close();

        let safe_to_release = !self.ring.is_registered() || !self.ring_lifetime_token.is_alive();
        if safe_to_release {
            // SAFETY: either unregister succeeded or the owning io_uring fd has already gone
            // away, so the kernel cannot dereference these buffer owners anymore.
            // 安全性：注销已成功或所属 io_uring fd 已关闭，内核不再可能解引用这些 buffer owner。
            unsafe { mem::ManuallyDrop::drop(&mut self.bufs) };
        } else {
            tracing::error!(
                state = ?self.registration_state,
                "provided buffer owners may still be referenced; leaking owners as a safety fallback"
            );
        }
    }
}

#[cfg(test)]
#[derive(Debug, Clone)]
struct TestPool;

#[cfg(test)]
impl BufPool for TestPool {
    fn alloc(&self, cap: NonZeroUsize, len: usize) -> Option<FixedBuf> {
        FixedBuf::alloc_heap(cap, len).ok()
    }
}

#[cfg(test)]
impl ProvidedBufGroup {
    pub(crate) fn for_test(entries: u16) -> Self {
        let ring_lifetime_token = RingLifetimeToken::standalone();
        let mut group = ProvidedBufGroup {
            bgid: PROVIDED_BUF_GROUP_ID,
            ring: RingMappingOwner::new_registered_for_test(entries, ring_lifetime_token.clone()),
            ring_lifetime_token,
            registration_state: ProvidedBufRegistrationState::Registered,
            health: ProvidedBufGroupHealth::Healthy,
            bufs: mem::ManuallyDrop::new((0..entries).map(|_| None).collect()),
            bid_states: (0..entries).map(|_| ProvidedBufBidState::Vacant).collect(),
            last_publish_sequences: (0..entries).map(|_| None).collect(),
            vacant: Vec::new(),
            vacant_retry: Vec::new(),
            vacant_bids: BitSet::new(entries as usize),
            publication: ProvidedBufPublication::new(entries),
            selected_bids: BitSet::new(entries as usize),
            selected_bid_count: 0,
            buf_size: NonZeroUsize::new(64).expect("test buffer size is non-zero"),
            pool: AnyBufPool::new(TestPool),
            stats: ProvidedBufStats::default(),
            bid_anomalies: 0,
            last_bid_anomaly: None,
        };
        for bid in 0..entries {
            if let Some(buf) = group.alloc_buf() {
                group.store_driver_owned(bid, buf);
                group.stage_driver_owned(bid, true);
            }
        }
        assert!(
            group.flush_pending_publish(),
            "test publication must succeed"
        );
        group.stats.set_initial_level();
        group
    }
}

#[cfg(test)]
mod tests {
    use veloq_io_uring::{IoUring, types::BufRingItem};

    use super::{
        AnyBufPool, PROVIDED_BUF_GROUP_ID, ProvidedBufConfig, ProvidedBufGroup,
        ProvidedBufInitializationParams, ProvidedBufPublication, RingLifetimeToken,
        RingMappingOwner, TestPool,
    };
    use veloq_std::{
        io,
        num::{NonZeroU16, NonZeroUsize},
    };

    #[test]
    fn failed_unregister_returns_the_group_and_keeps_resources_alive() {
        let group = ProvidedBufGroup::for_test(2);
        let original_stats = group.stats;
        let original_ptr = group.bufs[0]
            .as_ref()
            .expect("test group has an initial buffer")
            .as_ptr();

        let failure = match group.try_unregister_with(|bgid| {
            assert_eq!(bgid, PROVIDED_BUF_GROUP_ID);
            Err(libc::EIO)
        }) {
            Ok(()) => panic!("injected unregister must fail"),
            Err(failure) => failure,
        };
        let (report, group) = failure.into_parts();
        assert_eq!(
            report
                .error_code()
                .and_then(|code| i32::try_from(code).ok()),
            Some(libc::EIO)
        );

        let mut group = group;
        assert_eq!(
            group.registration_state,
            super::ProvidedBufRegistrationState::UnregisterUnknown
        );
        assert_eq!(
            group.ring.state(),
            super::RingMappingOwnerState::UnregisterUnknown
        );
        group.bufs[0]
            .as_mut()
            .expect("failed unregister must retain the buffer")
            .spare_capacity_mut()[0] = 0xA5;

        assert_eq!(group.stats, original_stats);
        assert_eq!(group.bufs[0].as_ref().unwrap().as_ptr(), original_ptr);
        assert_eq!(group.bufs[0].as_ref().unwrap().as_slice()[0], 0xA5);

        match group.try_unregister_with(|_| Ok(())) {
            Ok(()) => {}
            Err(_) => panic!("test group is unregistered and can be dropped"),
        }
    }

    #[test]
    fn allocation_failure_happens_before_registration() {
        let mut ring = match IoUring::new(8) {
            Ok(ring) => ring,
            Err(_) => return,
        };
        let submitter = ring.submitter();
        let config = ProvidedBufConfig {
            entries: NonZeroU16::new(2).expect("test entries are non-zero"),
            buf_size: NonZeroUsize::new(64).expect("test buffer size is non-zero"),
        };

        let result = ProvidedBufPublication::initialize(ProvidedBufInitializationParams {
            submitter: &submitter,
            config,
            pool: AnyBufPool::new(TestPool),
            ring_lifetime_token: RingLifetimeToken::standalone(),
            allocate: |_: &ProvidedBufGroup| None,
            publish: |ring: &mut RingMappingOwner, items: &[BufRingItem]| ring.publish(items),
            unregister: |_| Ok(()),
        });

        assert!(
            result.is_err(),
            "an allocator that always fails must reject the group"
        );
        drop(ring);
    }

    #[test]
    fn publication_failure_retains_registered_group_until_unregister_succeeds() {
        let mut ring = match IoUring::new(8) {
            Ok(ring) => ring,
            Err(_) => return,
        };
        let submitter = ring.submitter();
        let config = ProvidedBufConfig {
            entries: NonZeroU16::new(2).expect("test entries are non-zero"),
            buf_size: NonZeroUsize::new(64).expect("test buffer size is non-zero"),
        };
        let result = ProvidedBufPublication::initialize(ProvidedBufInitializationParams {
            submitter: &submitter,
            config,
            pool: AnyBufPool::new(TestPool),
            ring_lifetime_token: RingLifetimeToken::standalone(),
            allocate: |group: &ProvidedBufGroup| group.alloc_buf(),
            publish: |_: &mut RingMappingOwner, _: &[BufRingItem]| {
                Err(io::Error::from_raw_os_error(libc::EIO))
            },
            unregister: |_| Err(libc::EIO),
        });

        let failure = match result {
            Ok(_) => panic!("injected publication must fail"),
            Err(failure) => failure,
        };
        let (report, group) = failure.into_parts();
        assert_eq!(
            report
                .error_code()
                .and_then(|code| i32::try_from(code).ok()),
            Some(libc::EIO)
        );
        let mut group = group.expect("failed unregister must retain the registered group");
        assert_eq!(
            group.registration_state,
            super::ProvidedBufRegistrationState::UnregisterUnknown
        );
        let original_ptr = group.bufs[0]
            .as_ref()
            .expect("publication failure must retain the buffer owner")
            .as_ptr();
        group.bufs[0]
            .as_mut()
            .expect("publication failure must retain the buffer owner")
            .spare_capacity_mut()[0] = 0xC3;
        assert_eq!(group.bufs[0].as_ref().unwrap().as_ptr(), original_ptr);

        match group.try_unregister(&submitter) {
            Ok(()) => {}
            Err(_) => panic!("the registered group must be unregisterable after retry"),
        }
        drop(ring);
    }

    #[test]
    fn registered_group_can_be_cleaned_after_an_injected_failure() {
        let mut ring = match IoUring::new(8) {
            Ok(ring) => ring,
            Err(_) => return,
        };
        let submitter = ring.submitter();
        let group = ProvidedBufGroup::for_test(2);

        // SAFETY: the group owns a zeroed, page-aligned mapping with two entries and keeps it
        // alive until the real successful unregister below.
        // 安全性：group 持有清零且页对齐的两项映射，并一直保留到下方真实注销成功。
        if unsafe {
            submitter.register_buf_ring_with_flags(group.ring.addr(), 2, PROVIDED_BUF_GROUP_ID, 0)
        }
        .is_err()
        {
            return;
        }

        let failure = match group.try_unregister_with(|_| Err(libc::EIO)) {
            Ok(()) => panic!("injected unregister must fail"),
            Err(failure) => failure,
        };
        let (_, group) = failure.into_parts();
        let mut group = group;
        group.bufs[0]
            .as_mut()
            .expect("registered group retains its buffer")
            .spare_capacity_mut()[0] = 0x5A;

        match group.try_unregister(&submitter) {
            Ok(()) => {}
            Err(_) => panic!("a registered test ring must unregister successfully"),
        }
        drop(ring);
    }

    #[test]
    fn unknown_bid_is_recorded_without_consuming_ring_capacity() {
        let mut group = ProvidedBufGroup::for_test(2);
        let flags = 1 | (7_u32 << 16);

        assert!(group.discard_selected(flags));
        assert_eq!(group.bid_anomalies, 1);
        assert_eq!(group.stats().available(), 2);
        assert_eq!(
            group.health,
            super::ProvidedBufGroupHealth::Quarantined,
            "an unknown bid must stop the provided fast path"
        );
        assert!(group.sqe_info().is_none());
        assert_eq!(
            group.bid_state(0),
            Some(super::ProvidedBufBidState::KernelPublished { publish_seq: 0 })
        );
    }

    #[test]
    fn duplicate_bid_keeps_last_publish_sequence_in_anomaly() {
        let mut group = ProvidedBufGroup::for_test(2);
        let flags = 1;
        let selected = group
            .claim_kernel_selected(0, flags)
            .expect("the first selection must claim the published bid");
        let (publish_seq, selected) = selected;
        drop(selected);
        assert!(group.quarantine_claimed(0, publish_seq));

        assert!(group.discard_selected(flags));
        assert_eq!(
            group.last_bid_anomaly,
            Some(super::ProvidedBufBidAnomaly {
                bid: 0,
                cqe_flags: Some(flags),
                last_publish_sequence: Some(0),
            })
        );
    }

    #[test]
    fn handoff_tracks_kernel_selection_and_replacement() {
        let mut group = ProvidedBufGroup::for_test(2);
        assert_eq!(
            group.bid_state(0),
            Some(super::ProvidedBufBidState::KernelPublished { publish_seq: 0 })
        );

        let flags = 1;
        let buffer = group
            .take_selected_for_test(flags, 7)
            .expect("published bid must produce a buffer");
        assert_eq!(buffer.len(), 7);
        assert!(matches!(
            group.bid_state(0),
            Some(super::ProvidedBufBidState::KernelPublished { publish_seq: 2 })
        ));
        assert_eq!(group.stats().available(), 2);
        assert_eq!(group.bid_anomalies, 0);
    }

    #[test]
    fn selected_lease_defers_refill_until_explicit_settlement() {
        let mut group = ProvidedBufGroup::for_test(2);
        let before = group.stats();
        let mut lease = group
            .claim_selected_lease(1, 7)
            .expect("claim must succeed")
            .expect("the CQE selected a bid");

        assert_eq!(lease.bid(), 0);
        assert_eq!(lease.publish_seq(), 0);
        assert_eq!(lease.len(), 7);
        assert_eq!(group.stats().available(), before.available() - 1);
        assert_eq!(group.stats().refilled(), before.refilled());
        assert_eq!(group.stats().selected_leases(), 1);
        assert_eq!(
            group.bid_state(0),
            Some(super::ProvidedBufBidState::SelectedLease { publish_seq: 0 })
        );

        let unregister = group.try_unregister_with(|_| Ok(()));
        assert!(
            unregister.is_err(),
            "an unsettled lease must block unregister"
        );
        let (_, group) = unregister
            .expect_err("failure owns the group")
            .into_parts();
        let mut group = group;

        group
            .settle_selected_lease(&mut lease, super::ProvidedBufLeaseAction::Republish)
            .expect("republish settlement must succeed");
        assert_eq!(lease.phase(), super::ProvidedBufLeasePhase::Settled);
        assert_eq!(group.stats().available(), before.available());
        assert_eq!(group.stats().refilled(), before.refilled() + 1);
        assert_eq!(group.stats().selected_leases(), 0);
        assert!(matches!(
            group.bid_state(0),
            Some(super::ProvidedBufBidState::KernelPublished { .. })
        ));
        assert_eq!(
            group.settle_selected_lease(&mut lease, super::ProvidedBufLeaseAction::Republish),
            Err(super::ProvidedBufLeaseError::AlreadySettled)
        );

        assert!(group.try_unregister_with(|_| Ok(())).is_ok());
    }

    #[test]
    fn pending_lease_tracks_delivery_and_quarantine_without_republish() {
        let mut group = ProvidedBufGroup::for_test(2);
        let mut lease = group
            .claim_selected_lease(1, 3)
            .expect("claim must succeed")
            .expect("the CQE selected a bid");
        group
            .handoff_selected_lease(&mut lease)
            .expect("handoff must retain the lease");
        assert_eq!(lease.phase(), super::ProvidedBufLeasePhase::Pending);
        assert_eq!(group.stats().pending_udp_leases(), 1);
        assert_eq!(group.stats().refilled(), 2);

        group
            .begin_delivery(&mut lease)
            .expect("delivery transition must retain the lease");
        assert_eq!(group.stats().pending_udp_leases(), 0);
        assert_eq!(group.stats().delivery_leases(), 1);
        group
            .settle_selected_lease(&mut lease, super::ProvidedBufLeaseAction::Quarantine)
            .expect("quarantine settlement must be single-shot");
        assert_eq!(lease.phase(), super::ProvidedBufLeasePhase::Quarantined);
        assert_eq!(group.stats().selected_leases(), 0);
        assert_eq!(group.stats().quarantined(), 1);
        assert_eq!(group.stats().refilled(), 2);
    }

    #[test]
    fn vacancy_membership_is_constant_time_and_refills_as_one_batch() {
        let mut group = ProvidedBufGroup::for_test(4);
        for bid in [0, 1] {
            drop(group.bufs[bid].take());
            group.set_bid_state(bid as u16, super::ProvidedBufBidState::Vacant);
            group.note_consumed();
            group.queue_vacant(bid as u16);
            group.queue_vacant(bid as u16);
        }

        let before = group.stats();
        group.retry_vacant();

        assert!(group.flush_pending_publish());
        assert_eq!(group.stats().available(), 4);
        assert_eq!(group.stats().refilled(), before.refilled() + 2);
        assert_eq!(group.vacant.len(), 0);
        assert!(group.vacant_bids.get(0).is_ok_and(|set| !set));
        assert!(group.vacant_bids.get(1).is_ok_and(|set| !set));
        assert!(matches!(
            group.bid_state(0),
            Some(super::ProvidedBufBidState::KernelPublished { .. })
        ));
        assert!(matches!(
            group.bid_state(1),
            Some(super::ProvidedBufBidState::KernelPublished { .. })
        ));
        assert_eq!(group.bid_anomalies, 0);
    }

    #[test]
    fn repeated_cancel_returns_never_duplicates_a_bid() {
        let mut group = ProvidedBufGroup::for_test(8);
        for round in 0..1024_u32 {
            let bid = round % 8;
            let flags = 1 | (bid << 16);
            assert!(!group.discard_selected(flags));
            assert_eq!(group.stats().available(), 8);
            assert_eq!(group.selected_bid_count, 0);
        }
        assert_eq!(group.stats().returned(), 1024);
        assert_eq!(group.bid_anomalies, 0);
        assert!(group.bid_ledger_is_consistent());
    }
}
