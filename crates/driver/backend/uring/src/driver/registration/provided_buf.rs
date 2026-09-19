//! provided buffer ring（`IORING_REGISTER_PBUF_RING`，Linux 5.19+）。
//!
//! 环里的每个条目是 `(addr, len, bid)`。CQE 选中条目后，buffer 先进入显式
//! [`ProvidedBufLease`]。lease settle 以前，原 bid 不得重新发布；这条规则对 UDP 的
//! bounded pending 尤其重要，因为 datagram 可能暂时没有 packet permit。
//!
//! TCP 的兼容路径仍然可以把 lease 中的 buffer 移交给 record，但也必须在同一个显式
//! settlement transaction 中为原 bid 准备 replacement。UDP 则可以把 lease 保留在
//! pending ledger 中，直到 output buffer 和 permit 都可用。

use core::sync::atomic::{AtomicBool, Ordering as CoreOrdering};

use veloq_std::{
    boxed::Box,
    collections::BitSet,
    io, mem,
    num::NonZeroUsize,
    ptr::{self, NonNull},
    sync::Arc,
    vec::Vec,
};

use diagweave::prelude::*;
use tracing::{debug, warn};
use veloq_buf::{AnyBufPool, BufPool, FixedBuf};
use veloq_io_uring::{
    Submitter, cqueue,
    types::{BufRingEntry, BufRingItem, ProvidedBufRing},
};

use crate::{
    config::{MAX_PROVIDED_BUF_ENTRIES, ProvidedBufConfig},
    driver::env::ProvidedBufSqeInfo,
    error::{UringError, UringResult},
};

/// 本 driver 只注册一组 provided buffer，所以 group id 是常量。
///
/// CQE 只带 bid 不带 bgid，因此「这条完成的 buffer 属于哪一组」是靠约定而不是靠数据回答
/// 的——多开一组就必须另想办法把 bgid 找回来。
pub(crate) const PROVIDED_BUF_GROUP_ID: u16 = 0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvidedBufRegistrationState {
    Unregistered,
    RegisteredButNotPublished,
    Registered,
    UnregisterUnknown,
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvidedBufBidState {
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
pub(crate) enum ProvidedBufLeasePhase {
    Selected,
    Pending,
    Delivery,
    Settled,
    Quarantined,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvidedBufLeaseError {
    GroupMismatch,
    BidMismatch,
    PublishSequenceMismatch,
    InvalidState,
    AlreadySettled,
    NoReplacementBuffer,
    PublicationFailed,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvidedBufLeaseAction {
    Republish,
    Quarantine,
}

/// The exclusive owner of a CQE-selected provided buffer.
///
/// The lease deliberately owns the [`FixedBuf`] instead of retaining only a bid. A bid is not
/// enough to reconstruct a datagram after the kernel has removed it from the ring.
pub(crate) struct ProvidedBufLease {
    group_id: u16,
    bid: u16,
    publish_seq: u64,
    buf: Option<FixedBuf>,
    len: usize,
    phase: ProvidedBufLeasePhase,
    user_buffer_taken: bool,
}

impl ProvidedBufLease {
    pub(crate) fn bid(&self) -> u16 {
        self.bid
    }

    pub(crate) fn publish_seq(&self) -> u64 {
        self.publish_seq
    }

    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn phase(&self) -> ProvidedBufLeasePhase {
        self.phase
    }

    /// Borrow the bytes selected by the CQE without transferring buffer ownership.
    pub(crate) fn as_slice(&self) -> Option<&[u8]> {
        self.buf.as_ref().map(FixedBuf::as_slice)
    }

    /// Move the selected buffer to a TCP record. UDP copy mode must not call this method.
    pub(crate) fn take_buffer_for_delivery(&mut self) -> Option<FixedBuf> {
        let buf = self.buf.take()?;
        self.user_buffer_taken = true;
        Some(buf)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProvidedBufBidAnomaly {
    bid: u16,
    cqe_flags: Option<u32>,
    last_publish_sequence: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ProvidedBufGroupHealth {
    Healthy,
    Degraded,
    Quarantined,
}

#[derive(Clone)]
pub(crate) struct RingLifetimeToken {
    alive: Arc<AtomicBool>,
}

impl RingLifetimeToken {
    pub(crate) fn standalone() -> Self {
        Self {
            alive: Arc::new(AtomicBool::new(true)),
        }
    }

    fn is_alive(&self) -> bool {
        self.alive.load(CoreOrdering::Acquire)
    }

    fn close(&self) {
        self.alive.store(false, CoreOrdering::Release);
    }
}

pub(crate) struct RingLifetimeOwner {
    token: RingLifetimeToken,
}

impl RingLifetimeOwner {
    pub(crate) fn new() -> Self {
        Self {
            token: RingLifetimeToken::standalone(),
        }
    }

    pub(crate) fn token(&self) -> RingLifetimeToken {
        self.token.clone()
    }
}

impl Drop for RingLifetimeOwner {
    fn drop(&mut self) {
        // This field is declared immediately after `IoUring` in the driver. Its drop is the
        // explicit proof that the kernel-side ring fd is gone before a retained mapping owner is
        // dropped by the later `buffer_registry` field.
        self.token.close();
    }
}

/// 一组 provided buffer 的运行期统计。
///
/// [`Self::available`] 是「环里还剩几个 buffer 供内核挑」，但它只在处理 CQE 时才更新——
/// 内核消费与我们收割之间它偏高。作为诊断量足够，别拿它当实时水位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProvidedBufferSnapshot {
    /// 交给用户的 buffer 条数。
    handed_out: u64,
    /// 完成被丢弃、buffer 原样还回环的次数（取消 / orphan / 陈旧 token）。
    returned: u64,
    /// 从池里补进环的次数。
    refilled: u64,
    /// 补充失败的次数。每一次都让环少一个 buffer，直到后续某次补充把它捡回来。
    refill_failed: u64,
    /// 内核报 `-ENOBUFS`（环空了）的完成条数。
    exhausted: u64,
    /// 环里当前可供内核挑选的 buffer 数。
    available: u16,
    /// `available` 的历史最低值——它才是「消费方跟不上」的证据。
    available_low_water: u16,
    /// 已经从 ring claim、但尚未完成 settlement 的 lease 数量。
    selected_leases: u16,
    /// selected lease 中当前由 UDP pending ledger 持有的数量。
    pending_udp_leases: u16,
    /// selected lease 中当前正在 delivery transaction 的数量。
    delivery_leases: u16,
    /// 已经有 owner、但尚未重新发布的 buffer 数量。
    driver_owned_unpublished: u16,
    /// 因 bid/generation 或 publication 异常而隔离的数量。
    quarantined: u16,
}

impl ProvidedBufferSnapshot {
    /// Returns the number of buffers handed to users.
    pub const fn handed_out(self) -> u64 {
        self.handed_out
    }

    /// Returns the number of selected buffers returned to the ring without user delivery.
    pub const fn returned(self) -> u64 {
        self.returned
    }

    /// Returns the number of buffers refilled into the ring.
    pub const fn refilled(self) -> u64 {
        self.refilled
    }

    /// Returns the number of refill attempts that failed.
    pub const fn refill_failed(self) -> u64 {
        self.refill_failed
    }

    /// Returns the number of completions reported with `-ENOBUFS`.
    pub const fn exhausted(self) -> u64 {
        self.exhausted
    }

    /// Returns the current number of buffers available to the kernel.
    pub const fn available(self) -> u16 {
        self.available
    }

    /// Returns the lowest observed number of buffers available to the kernel.
    pub const fn available_low_water(self) -> u16 {
        self.available_low_water
    }

    pub const fn selected_leases(self) -> u16 {
        self.selected_leases
    }

    pub const fn pending_udp_leases(self) -> u16 {
        self.pending_udp_leases
    }

    pub const fn delivery_leases(self) -> u16 {
        self.delivery_leases
    }

    pub const fn driver_owned_unpublished(self) -> u16 {
        self.driver_owned_unpublished
    }

    pub const fn quarantined(self) -> u16 {
        self.quarantined
    }
}

/// 注册给内核的那一段环内存。
///
/// 单独一层是为了让 munmap 挂在 `Drop` 上：未注册的 group 可以在任意初始化错误路径
/// 自动释放；已注册的 group 则由 registry 保留到反注册成功，或等 `IoUring` 先销毁后
/// 再释放。后者是故意维护的生命周期边界，不依赖 `munmap` 偶然没有被内核访问。
struct RingMapping {
    ptr: NonNull<BufRingEntry>,
    bytes: usize,
}

impl RingMapping {
    fn new(entries: u16) -> UringResult<Self> {
        let bytes = match (entries as usize)
            .checked_mul(size_of::<BufRingEntry>())
            .filter(|length| *length != 0)
        {
            Some(bytes) => bytes,
            None => {
                return UringError::InvalidInput
                    .push_ctx("scope", "uring.provided_buf.mmap")
                    .with_ctx("entries", entries)
                    .attach_note("provided buffer ring mapping length overflowed");
            }
        };
        // 内核要求环基址页对齐，mmap 天然满足；MAP_ANONYMOUS 还保证清零，于是 entry 0 的
        // `resv`（也就是环的 tail 字段）从 0 开始，与本地 `tail` 一致。
        let raw = unsafe {
            libc::mmap(
                ptr::null_mut(),
                bytes,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_ANONYMOUS | libc::MAP_PRIVATE,
                -1,
                0,
            )
        };
        if raw == libc::MAP_FAILED {
            return Err(UringError::Registration
                .io_report("uring.provided_buf.mmap", io::Error::last_os_error()));
        }
        // No errno is downgraded here: without this policy the mapping could be inherited by a
        // forked child while the parent still owns the kernel registration.
        if unsafe { libc::madvise(raw, bytes, libc::MADV_DONTFORK) } != 0 {
            let error = io::Error::last_os_error();
            unsafe {
                let _ = libc::munmap(raw, bytes);
            }
            return Err(
                UringError::Registration.io_report("uring.provided_buf.madvise_dontfork", error)
            );
        }
        Ok(Self {
            // SAFETY: `mmap` 只在返回 `MAP_FAILED` 时不给出有效指针，上面刚排除掉。
            ptr: unsafe { NonNull::new_unchecked(raw.cast::<BufRingEntry>()) },
            bytes,
        })
    }

    #[inline]
    fn addr(&self) -> u64 {
        self.ptr.as_ptr() as u64
    }
}

impl Drop for RingMapping {
    fn drop(&mut self) {
        // SAFETY: 这段映射由 `Self::new` 创建，且只在这里释放一次。
        unsafe {
            libc::munmap(self.ptr.as_ptr().cast(), self.bytes);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RingMappingOwnerState {
    Unregistered,
    RegisteredButNotPublished { bgid: u16, entries: u16 },
    Registered { bgid: u16, entries: u16 },
    UnregisterUnknown,
    Released,
}

/// Owns the mmap backing a provided ring and records whether the kernel may still reference it.
///
/// `RingMapping` itself only knows how to unmap memory. This wrapper carries the syscall fact so
/// a normal drop is possible only after successful unregister; the unknown-result fallback is
/// allowed only after [`RingLifetimeOwner`] has proved that the io_uring fd is already gone.
struct RingMappingOwner {
    /// The mapping must be intentionally leaked if the kernel may still reference it. Keeping it
    /// in `ManuallyDrop` makes the release-build behavior independent of `debug_assert!`.
    mapping: mem::ManuallyDrop<RingMapping>,
    ring: ProvidedBufRing,
    state: RingMappingOwnerState,
    lifetime_token: RingLifetimeToken,
}

impl RingMappingOwner {
    fn new(entries: u16, lifetime_token: RingLifetimeToken) -> UringResult<Self> {
        let mapping = RingMapping::new(entries)?;
        let ring = unsafe { ProvidedBufRing::from_raw_parts(mapping.ptr.as_ptr(), entries) }
            .map_err(|error| {
                UringError::InvalidInput.io_report("uring.provided_buf.bind", error)
            })?;
        Ok(Self {
            mapping: mem::ManuallyDrop::new(mapping),
            ring,
            state: RingMappingOwnerState::Unregistered,
            lifetime_token,
        })
    }

    #[cfg(test)]
    fn new_registered_for_test(entries: u16, lifetime_token: RingLifetimeToken) -> Self {
        let mapping = RingMapping::new(entries).expect("test ring mapping must be created");
        Self {
            ring: unsafe {
                ProvidedBufRing::from_raw_parts(mapping.ptr.as_ptr(), entries)
                    .expect("test ring mapping must bind")
            },
            mapping: mem::ManuallyDrop::new(mapping),
            state: RingMappingOwnerState::Registered {
                bgid: PROVIDED_BUF_GROUP_ID,
                entries,
            },
            lifetime_token,
        }
    }

    fn register(&mut self, submitter: &Submitter<'_>, bgid: u16, entries: u16) -> UringResult<()> {
        if self.state != RingMappingOwnerState::Unregistered {
            return Err(UringError::InvalidState
                .report(
                    "uring.provided_buf.register.state",
                    "provided buffer ring was registered more than once",
                )
                .with_ctx("state", "not_unregistered"));
        }
        // SAFETY: the mapping is page-aligned and remains owned by this wrapper for the entire
        // registration syscall. It is kept alive until a successful unregister or ring teardown.
        unsafe { submitter.register_buf_ring_with_flags(self.mapping.addr(), entries, bgid, 0) }
            .map_err(|err| {
                UringError::Registration.io_report("uring.provided_buf.register", err)
            })?;
        self.state = RingMappingOwnerState::RegisteredButNotPublished { bgid, entries };
        Ok(())
    }

    #[cfg(test)]
    #[inline]
    fn addr(&self) -> u64 {
        self.mapping.addr()
    }

    #[inline]
    fn publish(&mut self, items: &[BufRingItem]) -> io::Result<()> {
        if !matches!(
            self.state,
            RingMappingOwnerState::RegisteredButNotPublished { .. }
                | RingMappingOwnerState::Registered { .. }
                | RingMappingOwnerState::UnregisterUnknown
        ) {
            return Err(io::Error::from_raw_os_error(libc::EINVAL));
        }
        self.ring.publish(items)
    }

    #[inline]
    fn is_registered(&self) -> bool {
        matches!(
            self.state,
            RingMappingOwnerState::RegisteredButNotPublished { .. }
                | RingMappingOwnerState::Registered { .. }
                | RingMappingOwnerState::UnregisterUnknown
        )
    }

    #[inline]
    fn mark_published(&mut self) {
        if let RingMappingOwnerState::RegisteredButNotPublished { bgid, entries } = self.state {
            self.state = RingMappingOwnerState::Registered { bgid, entries };
        }
    }

    #[inline]
    fn mark_unregister_unknown(&mut self) {
        if self.is_registered() {
            self.state = RingMappingOwnerState::UnregisterUnknown;
        }
    }

    #[inline]
    fn mark_released(&mut self) {
        if self.is_registered() {
            self.state = RingMappingOwnerState::Released;
        }
    }
}

impl Drop for RingMappingOwner {
    fn drop(&mut self) {
        let safe_to_unmap = match self.state {
            RingMappingOwnerState::Unregistered | RingMappingOwnerState::Released => true,
            RingMappingOwnerState::RegisteredButNotPublished { .. }
            | RingMappingOwnerState::Registered { .. }
            | RingMappingOwnerState::UnregisterUnknown => !self.lifetime_token.is_alive(),
        };
        if safe_to_unmap {
            // SAFETY: the state machine or the ring lifetime token proves the kernel no longer
            // references this mapping.
            unsafe { mem::ManuallyDrop::drop(&mut self.mapping) };
        } else {
            tracing::error!(
                state = ?self.state,
                "provided ring mapping may still be referenced; leaking mapping as a safety fallback"
            );
        }
    }
}

pub(crate) struct ProvidedBufGroup {
    bgid: u16,
    ring: RingMappingOwner,
    /// The token is kept next to the resource ledger so every bid owner can assert that the ring
    /// lifetime still covers its mapping. The driver-owned copy is dropped only after `IoUring`.
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
    last_publish_sequences: Box<[Option<u64>]>,
    /// 补充失败留下的空洞，等后续任一次完成顺手重试。
    vacant: Vec<u16>,
    /// `retry_vacant` 的消费 scratch，避免每次重试都通过 `mem::take` 丢失容量。
    vacant_retry: Vec<u16>,
    vacant_bids: BitSet,
    /// 待批量发布的 bid，membership 由位图保证，避免重复 CQE/取消路径重复入队。
    pending_publish: Vec<u16>,
    pending_publish_bids: BitSet,
    /// 其中哪些待发布 bid 是新分配的 buffer，用于在一次 tail 发布后更新统计。
    pending_refill_bids: BitSet,
    /// Reusable ABI descriptors for one publication batch.
    publish_items: Vec<BufRingItem>,
    /// 处于 selected lease 状态的 bid 数量，避免析构/取消检查扫描整张表。
    selected_bids: BitSet,
    selected_bid_count: usize,
    buf_size: NonZeroUsize,
    pool: AnyBufPool,
    stats: ProvidedBufferSnapshot,
    bid_anomalies: u64,
    last_bid_anomaly: Option<ProvidedBufBidAnomaly>,
    next_publish_sequence: u64,
}

/// Completion-side command port for the provided-buffer owner.
///
/// The port intentionally exposes only buffer settlement commands. It does not expose the group,
/// bid table, scratch storage, or registration state to completion code.
pub(crate) struct ProvidedBufPort<'a> {
    group: &'a mut ProvidedBufGroup,
}

impl<'a> ProvidedBufPort<'a> {
    pub(crate) fn new(group: &'a mut ProvidedBufGroup) -> Self {
        Self { group }
    }

    #[inline]
    pub(crate) fn claim_selected_lease(
        &mut self,
        flags: u32,
        res: i32,
    ) -> Result<Option<ProvidedBufLease>, ProvidedBufLeaseError> {
        self.group.claim_selected_lease(flags, res)
    }

    #[inline]
    pub(crate) fn handoff_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> Result<(), ProvidedBufLeaseError> {
        self.group.handoff_selected_lease(lease)
    }

    #[inline]
    pub(crate) fn begin_delivery(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> Result<(), ProvidedBufLeaseError> {
        self.group.begin_delivery(lease)
    }

    #[inline]
    pub(crate) fn settle_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
        action: ProvidedBufLeaseAction,
    ) -> Result<(), ProvidedBufLeaseError> {
        self.group.settle_selected_lease(lease, action)
    }

    #[inline]
    pub(crate) fn discard_selected(&mut self, flags: u32) -> bool {
        self.group.discard_selected(flags)
    }

    #[inline]
    pub(crate) fn quarantine_claimed(&mut self, bid: u16, publish_seq: u64) -> bool {
        self.group.quarantine_claimed(bid, publish_seq)
    }

    #[inline]
    pub(crate) fn note_exhausted(&mut self) {
        self.group.note_exhausted();
    }
}

/// 反注册失败时的错误和所有权载体。
///
/// `unregister_buf_ring` 返回错误只说明这一次 syscall 没有完成，不能证明内核已经不再
/// 使用 ring。因此失败结果必须把 group 一起交还给调用方，避免 `RingMapping` 和仍发布
/// 给内核的 `FixedBuf` 提前析构。
pub(crate) struct ProvidedBufUnregisterFailure {
    report: Report<UringError>,
    group: ProvidedBufGroup,
}

impl ProvidedBufUnregisterFailure {
    pub(crate) fn into_parts(self) -> (Report<UringError>, ProvidedBufGroup) {
        (self.report, self.group)
    }
}

pub(crate) type ProvidedBufUnregisterResult = Result<(), Box<ProvidedBufUnregisterFailure>>;

/// Initialization failed after the group may have reached kernel-visible registration.
///
/// The optional group is the ownership handoff for a failed unregister. Callers must retain it
/// whenever present; dropping it while the ring lifetime is still alive deliberately leaks the
/// mapping and published buffer owners instead of risking use-after-unmap/use-after-free.
pub(crate) struct ProvidedBufInitializationFailure {
    report: Report<UringError>,
    group: Option<ProvidedBufGroup>,
}

impl ProvidedBufInitializationFailure {
    pub(crate) fn into_parts(self) -> (Report<UringError>, Option<ProvidedBufGroup>) {
        (self.report, self.group)
    }
}

pub(crate) type ProvidedBufInitializationResult =
    Result<ProvidedBufGroup, Box<ProvidedBufInitializationFailure>>;

impl ProvidedBufGroup {
    /// 注册一组 provided buffer 并把它填满。
    ///
    /// 失败一律**降级而不是致命**（调用方据此把 `provided_buffers` 能力留在 `false`）：
    /// 5.6–5.18 的内核根本没有 `IORING_REGISTER_PBUF_RING`，那是仓库声明支持的区间。
    pub(crate) fn new(
        submitter: &Submitter<'_>,
        config: ProvidedBufConfig,
        pool: AnyBufPool,
        ring_lifetime_token: RingLifetimeToken,
    ) -> ProvidedBufInitializationResult {
        Self::new_with_allocator(
            submitter,
            config,
            pool,
            ring_lifetime_token,
            |group| group.alloc_buf(),
            |ring, items| ring.publish(items),
            |bgid| {
                submitter
                    .unregister_buf_ring(bgid)
                    .map_err(|err| err.raw_os_error().unwrap_or(libc::EIO))
            },
        )
    }

    fn new_with_allocator<F, P, U>(
        submitter: &Submitter<'_>,
        config: ProvidedBufConfig,
        pool: AnyBufPool,
        ring_lifetime_token: RingLifetimeToken,
        allocate: F,
        mut publish: P,
        unregister: U,
    ) -> ProvidedBufInitializationResult
    where
        F: Fn(&Self) -> Option<FixedBuf>,
        P: FnMut(&mut RingMappingOwner, &[BufRingItem]) -> io::Result<()>,
        U: FnOnce(u16) -> Result<(), i32>,
    {
        let entries = config.entries.get();
        if !entries.is_power_of_two() || entries > MAX_PROVIDED_BUF_ENTRIES {
            return Err(Box::new(ProvidedBufInitializationFailure {
                report: UringError::InvalidInput
                    .report(
                        "uring.provided_buf.new",
                        "provided buffer ring entries are invalid",
                    )
                    .with_ctx("entries", entries)
                    .with_ctx("max_entries", MAX_PROVIDED_BUF_ENTRIES),
                group: None,
            }));
        }

        let ring = match RingMappingOwner::new(entries, ring_lifetime_token.clone()) {
            Ok(ring) => ring,
            Err(report) => {
                return Err(Box::new(ProvidedBufInitializationFailure {
                    report,
                    group: None,
                }));
            }
        };
        let mut group = Self {
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
            pending_publish: Vec::new(),
            pending_publish_bids: BitSet::new(entries as usize),
            pending_refill_bids: BitSet::new(entries as usize),
            publish_items: Vec::with_capacity(entries as usize),
            selected_bids: BitSet::new(entries as usize),
            selected_bid_count: 0,
            buf_size: config.buf_size,
            pool,
            stats: ProvidedBufferSnapshot::default(),
            bid_anomalies: 0,
            last_bid_anomaly: None,
            next_publish_sequence: 0,
        };

        // 先准备用户态所有权，不写 entry，也不推进 tail。这样注册 syscall 失败时，内核
        // 从未获得这段映射或其中 buffer 的可见引用，group 可以直接析构。
        for bid in 0..entries {
            match allocate(&group) {
                Some(buf) => group.store_driver_owned(bid, buf),
                None => {
                    group.stats.refill_failed = group.stats.refill_failed.saturating_add(1);
                    group.queue_vacant(bid);
                }
            }
        }

        if !(0..entries).any(|bid| group.has_unpublished_buf(bid)) {
            return Err(Box::new(ProvidedBufInitializationFailure {
                report: UringError::Registration
                    .report(
                        "uring.provided_buf.new",
                        "buffer pool could not fill a single provided buffer",
                    )
                    .with_ctx("entries", entries)
                    .with_ctx("buf_size", config.buf_size.get()),
                group: Some(group),
            }));
        }

        if let Err(report) = group
            .ring
            .register(submitter, PROVIDED_BUF_GROUP_ID, entries)
        {
            return Err(Box::new(ProvidedBufInitializationFailure {
                report,
                group: Some(group),
            }));
        }
        group.registration_state = ProvidedBufRegistrationState::RegisteredButNotPublished;

        // 只有注册成功后才发布 entry。保持 entry 的 reserved 字段不变，并保留原有的
        // tail、available 和 refilled 统计语义。
        for bid in 0..entries {
            if group.has_unpublished_buf(bid) {
                group.stage_driver_owned(bid, true);
            }
        }
        if !group.flush_pending_publish_with(&mut publish) {
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
                Ok(()) => Err(Box::new(ProvidedBufInitializationFailure {
                    report,
                    group: None,
                })),
                Err(failure) => {
                    let (unregister_report, group) = (*failure).into_parts();
                    let unregister_report = match publication_errno {
                        Some(errno) => unregister_report
                            .with_ctx("publication_errno", errno)
                            .with_ctx("publication_failed", true),
                        None => unregister_report,
                    };
                    Err(Box::new(ProvidedBufInitializationFailure {
                        report: unregister_report,
                        group: Some(group),
                    }))
                }
            };
        }
        group.mark_published();

        // 起始水位是「填满之后」的那个数，否则低水位线永远停在 0 而不说明任何事。
        group.stats.available_low_water = group.stats.available;

        debug!(
            bgid = group.bgid,
            entries,
            filled = group.stats.available,
            buf_size = config.buf_size.get(),
            "registered provided buffer ring"
        );
        Ok(group)
    }

    #[inline]
    pub(crate) fn sqe_info(&self) -> Option<ProvidedBufSqeInfo> {
        self.is_usable()
            .then(|| ProvidedBufSqeInfo::new(self.bgid, self.buf_size.get() as u32))
    }

    #[inline]
    pub(crate) const fn stats(&self) -> ProvidedBufferSnapshot {
        self.stats
    }

    /// 内核报「环里没 buffer 了」。
    #[inline]
    pub(crate) fn note_exhausted(&mut self) {
        self.stats.exhausted = self.stats.exhausted.saturating_add(1);
    }

    /// Claim the buffer selected by this CQE without publishing a replacement.
    ///
    /// `res` is the kernel-written length. Negative results still carry a selected bid on some
    /// error paths, so their lease length is zero and the lease must still be settled.
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
        Ok(Some(ProvidedBufLease {
            group_id: self.bgid,
            bid,
            publish_seq,
            buf: Some(buf),
            len: filled,
            phase: ProvidedBufLeasePhase::Selected,
            user_buffer_taken: false,
        }))
    }

    /// Claim and hand a selected buffer to the TCP compatibility record path.
    ///
    /// This is intentionally a wrapper around the explicit lease API. UDP must retain the lease
    /// until its copy/delivery transaction has settled.
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
            self.stats.returned = self.stats.returned.saturating_add(1);
            false
        } else {
            self.quarantine();
            true
        }
    }

    /// 尝试反注册。**顺序不能反**：内核在反注册之前仍可能往环里读写。
    ///
    /// 成功才消费 group；失败会把 group 原样放入错误载体，调用方必须继续持有它。
    pub(crate) fn try_unregister(self, submitter: &Submitter<'_>) -> ProvidedBufUnregisterResult {
        self.try_unregister_with(|bgid| {
            submitter
                .unregister_buf_ring(bgid)
                .map_err(|err| err.raw_os_error().unwrap_or(libc::EIO))
        })
    }

    /// 反注册的最小注入点。生产路径通过 [`Self::try_unregister`] 使用 io_uring syscall；
    /// 测试可以传入闭包验证失败时 group 没有被消费，不需要全局可变 syscall hook。
    pub(crate) fn try_unregister_with<F>(mut self, unregister: F) -> ProvidedBufUnregisterResult
    where
        F: FnOnce(u16) -> Result<(), i32>,
    {
        if self.selected_bid_count != 0 {
            return Err(Box::new(ProvidedBufUnregisterFailure {
                report: UringError::InvalidState.report(
                    "uring.provided_buf.unregister.leases",
                    "provided buffer group still has unsettled leases",
                ),
                group: self,
            }));
        }
        if !self.ring.is_registered() {
            return Err(Box::new(ProvidedBufUnregisterFailure {
                report: UringError::InvalidState.report(
                    "uring.provided_buf.unregister.state",
                    "provided buffer ring was not kernel-registered",
                ),
                group: self,
            }));
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
                Err(Box::new(ProvidedBufUnregisterFailure {
                    report: UringError::Registration
                        .io_report(
                            "uring.provided_buf.unregister",
                            io::Error::from_raw_os_error(err),
                        )
                        .with_ctx("bgid", bgid),
                    group,
                }))
            }
        }
    }

    /// Put a selected lease into the backend pending ledger.
    pub(crate) fn handoff_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> Result<(), ProvidedBufLeaseError> {
        self.validate_lease(lease)?;
        if lease.phase != ProvidedBufLeasePhase::Selected {
            return Err(ProvidedBufLeaseError::InvalidState);
        }
        self.set_bid_state(
            lease.bid,
            ProvidedBufBidState::PendingLease {
                publish_seq: lease.publish_seq,
            },
        );
        lease.phase = ProvidedBufLeasePhase::Pending;
        self.refresh_ownership_snapshot();
        Ok(())
    }

    /// Mark a selected or pending lease as being copied to an independent output buffer.
    pub(crate) fn begin_delivery(
        &mut self,
        lease: &mut ProvidedBufLease,
    ) -> Result<(), ProvidedBufLeaseError> {
        self.validate_lease(lease)?;
        if !matches!(
            lease.phase,
            ProvidedBufLeasePhase::Selected | ProvidedBufLeasePhase::Pending
        ) {
            return Err(ProvidedBufLeaseError::InvalidState);
        }
        self.set_bid_state(
            lease.bid,
            ProvidedBufBidState::DeliveryLease {
                publish_seq: lease.publish_seq,
            },
        );
        lease.phase = ProvidedBufLeasePhase::Delivery;
        self.refresh_ownership_snapshot();
        Ok(())
    }

    /// Settle a lease exactly once. Republish may reuse the selected buffer or allocate a
    /// replacement after a TCP delivery took the original buffer.
    pub(crate) fn settle_selected_lease(
        &mut self,
        lease: &mut ProvidedBufLease,
        action: ProvidedBufLeaseAction,
    ) -> Result<(), ProvidedBufLeaseError> {
        self.validate_lease(lease)?;
        if matches!(
            lease.phase,
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
                let Some(buf) = lease.buf.take().or_else(|| self.alloc_buf()) else {
                    self.quarantine_lease(lease);
                    return Err(ProvidedBufLeaseError::NoReplacementBuffer);
                };
                let bid = lease.bid;
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
                    lease.phase = ProvidedBufLeasePhase::Quarantined;
                    self.refresh_ownership_snapshot();
                    return Err(ProvidedBufLeaseError::PublicationFailed);
                }
                self.retry_vacant();
                if !self.flush_pending_publish() {
                    self.quarantine();
                    lease.phase = ProvidedBufLeasePhase::Quarantined;
                    self.refresh_ownership_snapshot();
                    return Err(ProvidedBufLeaseError::PublicationFailed);
                }
                if lease.user_buffer_taken {
                    self.stats.handed_out = self.stats.handed_out.saturating_add(1);
                }
                lease.phase = ProvidedBufLeasePhase::Settled;
                self.refresh_ownership_snapshot();
                Ok(())
            }
        }
    }

    fn validate_lease(&self, lease: &ProvidedBufLease) -> Result<(), ProvidedBufLeaseError> {
        if lease.group_id != self.bgid {
            return Err(ProvidedBufLeaseError::GroupMismatch);
        }
        let Some(state) = self.bid_states.get(lease.bid as usize) else {
            return Err(ProvidedBufLeaseError::BidMismatch);
        };
        let expected = match lease.phase {
            ProvidedBufLeasePhase::Selected => ProvidedBufBidState::SelectedLease {
                publish_seq: lease.publish_seq,
            },
            ProvidedBufLeasePhase::Pending => ProvidedBufBidState::PendingLease {
                publish_seq: lease.publish_seq,
            },
            ProvidedBufLeasePhase::Delivery => ProvidedBufBidState::DeliveryLease {
                publish_seq: lease.publish_seq,
            },
            ProvidedBufLeasePhase::Settled | ProvidedBufLeasePhase::Quarantined => {
                return Err(ProvidedBufLeaseError::AlreadySettled);
            }
        };
        if *state != expected {
            return Err(ProvidedBufLeaseError::PublishSequenceMismatch);
        }
        Ok(())
    }

    fn quarantine_lease(&mut self, lease: &mut ProvidedBufLease) {
        let bid = lease.bid;
        if let Some(slot) = self.bufs.get_mut(bid as usize)
            && slot.is_none()
        {
            *slot = lease.buf.take();
        }
        self.set_bid_state(bid, ProvidedBufBidState::Quarantined);
        self.clear_selected(bid);
        self.health = ProvidedBufGroupHealth::Quarantined;
        lease.phase = ProvidedBufLeasePhase::Quarantined;
        self.refresh_ownership_snapshot();
    }

    fn quarantine_bid_after_failed_publish(&mut self, bid: u16) {
        self.set_bid_state(bid, ProvidedBufBidState::Quarantined);
        self.health = ProvidedBufGroupHealth::Quarantined;
    }

    /// Quarantine a claimed lease when completion cleanup lost the lease owner before settlement.
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

    /// Take the buffer owner out while retaining the publish sequence for lease validation.
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
    fn refill(&mut self, bid: u16) -> bool {
        if !matches!(
            self.bid_states.get(bid as usize),
            Some(ProvidedBufBidState::Vacant)
        ) {
            self.note_bid_anomaly(bid, "refill attempted for a non-vacant provided bid");
            return false;
        }
        let Some(buf) = self.alloc_buf() else {
            self.stats.refill_failed = self.stats.refill_failed.saturating_add(1);
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

    /// Stage a buffer that is still owned by this group for the next batch publication.
    fn stage_driver_owned(&mut self, bid: u16, is_refill: bool) -> bool {
        let can_stage = matches!(
            self.registration_state,
            ProvidedBufRegistrationState::RegisteredButNotPublished
                | ProvidedBufRegistrationState::Registered
        ) && self.health == ProvidedBufGroupHealth::Healthy
            && self.ring.is_registered()
            && self.ring_lifetime_token.is_alive();
        if !can_stage
            || self.bid_states.get(bid as usize)
                != Some(&ProvidedBufBidState::DriverOwnedUnpublished)
        {
            self.note_bid_anomaly(bid, "published a provided bid from an invalid state");
            return false;
        }
        let index = bid as usize;
        if !self.pending_publish_bids.get(index).unwrap_or(false) {
            if self.pending_publish_bids.set(index).is_err() {
                self.note_bid_anomaly(bid, "staged a provided bid outside the publication ledger");
                return false;
            }
            self.pending_publish.push(bid);
        }
        if is_refill {
            debug_assert!(self.pending_refill_bids.set(index).is_ok());
        }
        true
    }

    /// Write every staged entry and perform one release tail publication.
    fn flush_pending_publish(&mut self) -> bool {
        self.flush_pending_publish_with(|ring, items| ring.publish(items))
    }

    fn flush_pending_publish_with<P>(&mut self, mut publish: P) -> bool
    where
        P: FnMut(&mut RingMappingOwner, &[BufRingItem]) -> io::Result<()>,
    {
        if self.pending_publish.is_empty() {
            return true;
        }

        self.publish_items.clear();
        for index in 0..self.pending_publish.len() {
            let bid = self.pending_publish[index];
            let item = {
                let Some(Some(buf)) = self.bufs.get(bid as usize) else {
                    self.note_bid_anomaly(bid, "published a provided bid without a buffer owner");
                    return false;
                };
                let Ok(len) = u32::try_from(buf.capacity()) else {
                    self.note_bid_anomaly(bid, "provided buffer capacity exceeds the ring ABI");
                    return false;
                };
                // SAFETY: `buf` remains owned by this group until the kernel consumes the published
                // entry; its allocation is writable for the complete capacity.
                let Ok(item) = (unsafe { BufRingItem::new(buf.as_ptr() as u64, len, bid) }) else {
                    self.note_bid_anomaly(bid, "provided buffer descriptor is invalid");
                    return false;
                };
                item
            };
            self.publish_items.push(item);
        }

        if publish(&mut self.ring, &self.publish_items).is_err() {
            self.note_bid_anomaly(0, "provided buffer batch publication was rejected");
            return false;
        }

        let mut refilled = 0_u64;
        for publish_index in 0..self.pending_publish.len() {
            let bid = self.pending_publish[publish_index];
            let index = bid as usize;
            let publish_seq = self.next_publish_sequence;
            self.next_publish_sequence = self.next_publish_sequence.saturating_add(1);
            if let Some(last_publish_sequence) = self.last_publish_sequences.get_mut(index) {
                *last_publish_sequence = Some(publish_seq);
            }
            self.set_bid_state(bid, ProvidedBufBidState::KernelPublished { publish_seq });
            if self.pending_refill_bids.get(index).unwrap_or(false) {
                refilled = refilled.saturating_add(1);
            }
            debug_assert!(self.pending_publish_bids.clear(index).is_ok());
            debug_assert!(self.pending_refill_bids.clear(index).is_ok());
        }
        let published = self.pending_publish.len();
        self.pending_publish.clear();
        self.stats.available = self.stats.available.saturating_add(published as u16);
        self.stats.refilled = self.stats.refilled.saturating_add(refilled);
        debug_assert!(self.bid_ledger_is_consistent());
        true
    }

    fn mark_published(&mut self) {
        if self.registration_state == ProvidedBufRegistrationState::RegisteredButNotPublished {
            self.registration_state = ProvidedBufRegistrationState::Registered;
            self.ring.mark_published();
        }
    }

    fn note_consumed(&mut self) {
        self.stats.available = self.stats.available.saturating_sub(1);
        if self.stats.available < self.stats.available_low_water {
            self.stats.available_low_water = self.stats.available;
        }
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
        self.stats.selected_leases = selected;
        self.stats.pending_udp_leases = pending;
        self.stats.delivery_leases = delivery;
        self.stats.driver_owned_unpublished = unpublished;
        self.stats.quarantined = quarantined;
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

    #[cfg(test)]
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
        self.ring_lifetime_token.close();

        let safe_to_release = !self.ring.is_registered() || !self.ring_lifetime_token.is_alive();
        if safe_to_release {
            // SAFETY: either unregister succeeded or the owning io_uring fd has already gone
            // away, so the kernel cannot dereference these buffer owners anymore.
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
pub(crate) fn test_group(entries: u16) -> ProvidedBufGroup {
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
        pending_publish: Vec::new(),
        pending_publish_bids: BitSet::new(entries as usize),
        pending_refill_bids: BitSet::new(entries as usize),
        publish_items: Vec::with_capacity(entries as usize),
        selected_bids: BitSet::new(entries as usize),
        selected_bid_count: 0,
        buf_size: NonZeroUsize::new(64).expect("test buffer size is non-zero"),
        pool: AnyBufPool::new(TestPool),
        stats: ProvidedBufferSnapshot::default(),
        bid_anomalies: 0,
        last_bid_anomaly: None,
        next_publish_sequence: 0,
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
    group.stats.available_low_water = group.stats.available;
    group
}

impl Default for ProvidedBufferSnapshot {
    fn default() -> Self {
        Self {
            handed_out: 0,
            returned: 0,
            refilled: 0,
            refill_failed: 0,
            exhausted: 0,
            available: 0,
            available_low_water: u16::MAX,
            selected_leases: 0,
            pending_udp_leases: 0,
            delivery_leases: 0,
            driver_owned_unpublished: 0,
            quarantined: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use veloq_io_uring::IoUring;

    use super::{
        AnyBufPool, PROVIDED_BUF_GROUP_ID, ProvidedBufConfig, ProvidedBufGroup, TestPool,
        test_group,
    };
    use veloq_std::{
        io,
        num::{NonZeroU16, NonZeroUsize},
    };

    #[test]
    fn failed_unregister_returns_the_group_and_keeps_resources_alive() {
        let group = test_group(2);
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
        assert_eq!(
            failure
                .report
                .error_code()
                .and_then(|code| i32::try_from(code).ok()),
            Some(libc::EIO)
        );

        let mut group = failure.group;
        assert_eq!(
            group.registration_state,
            super::ProvidedBufRegistrationState::UnregisterUnknown
        );
        assert_eq!(
            group.ring.state,
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

        let result = ProvidedBufGroup::new_with_allocator(
            &submitter,
            config,
            AnyBufPool::new(TestPool),
            super::RingLifetimeToken::standalone(),
            |_| None,
            |ring, items| ring.publish(items),
            |_| Ok(()),
        );

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
        let result = ProvidedBufGroup::new_with_allocator(
            &submitter,
            config,
            AnyBufPool::new(TestPool),
            super::RingLifetimeToken::standalone(),
            |group| group.alloc_buf(),
            |_, _| Err(io::Error::from_raw_os_error(libc::EIO)),
            |_| Err(libc::EIO),
        );

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
        let group = test_group(2);

        // SAFETY: the group owns a zeroed, page-aligned mapping with two entries and keeps it
        // alive until the real successful unregister below.
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
        let mut group = failure.group;
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
        let mut group = test_group(2);
        let flags = 1 | (7_u32 << 16);

        assert!(group.discard_selected(flags));
        assert_eq!(group.bid_anomalies, 1);
        assert_eq!(group.stats.available, 2);
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
        let mut group = test_group(2);
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
        let mut group = test_group(2);
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
        assert_eq!(group.stats.available, 2);
        assert_eq!(group.bid_anomalies, 0);
    }

    #[test]
    fn selected_lease_defers_refill_until_explicit_settlement() {
        let mut group = test_group(2);
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
        let mut group = unregister.expect_err("failure owns the group").group;

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
        let mut group = test_group(2);
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
        let mut group = test_group(4);
        for bid in [0, 1] {
            drop(group.bufs[bid].take());
            group.set_bid_state(bid as u16, super::ProvidedBufBidState::Vacant);
            group.note_consumed();
            group.queue_vacant(bid as u16);
            group.queue_vacant(bid as u16);
        }

        let before = group.stats;
        group.retry_vacant();

        assert!(group.flush_pending_publish());
        assert_eq!(group.stats.available, 4);
        assert_eq!(group.stats.refilled, before.refilled + 2);
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
        let mut group = test_group(8);
        for round in 0..1024_u32 {
            let bid = round % 8;
            let flags = 1 | (bid << 16);
            assert!(!group.discard_selected(flags));
            assert_eq!(group.stats.available, 8);
            assert_eq!(group.selected_bid_count, 0);
        }
        assert_eq!(group.stats.returned, 1024);
        assert_eq!(group.bid_anomalies, 0);
        assert!(group.bid_ledger_is_consistent());
    }
}
