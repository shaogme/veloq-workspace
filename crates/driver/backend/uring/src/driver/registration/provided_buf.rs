//! provided buffer ring（`IORING_REGISTER_PBUF_RING`，Linux 5.19+）。
//!
//! 环里的每个条目是 `(addr, len, bid)`。所有权模型是**移交 + 补充**，不是借出 + 归还：
//!
//! - 启动时从 worker 的池里 alloc 一批 [`FixedBuf`]，driver 持有它们，把地址与 bid 写进环；
//! - 完成时按 CQE 的 bid 把对应的 `FixedBuf` 取出来，`set_len` 之后**移交给用户**；
//! - 紧接着从池里再 alloc 一个填回同一个 bid。
//!
//! 用户拿到的就是一个普通 `FixedBuf`，drop 时走现有路径回它自己的池。**没有新类型、没有
//! 跨线程归还通道、也没有「用户不能长期持有」的隐式约束**——被否决的「环保留所有权、交给
//! 用户一个借用视图」方案要求一条跨线程归还通道（`FixedBuf: Send`，用户完全可能在别的
//! worker 上 drop），而它买到的只是省掉一次 order-0 池分配。
//!
//! 收益不在「省掉分配」，而在「**buffer 只在数据到达时才与连接绑定**」：一万个空闲连接不
//! 再各自压着一个 recv buffer。移交 + 补充完整保留了这一点。

use core::sync::atomic::{AtomicBool, AtomicU16, Ordering as CoreOrdering};

use veloq_std::{
    boxed::Box,
    io, mem,
    num::NonZeroUsize,
    ptr::{self, NonNull},
    sync::Arc,
    vec::Vec,
};

use diagweave::prelude::*;
use io_uring::{Submitter, cqueue, types::BufRingEntry};
use tracing::{debug, warn};
use veloq_buf::{AnyBufPool, BufPool, FixedBuf};

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
    Registered,
    UnregisterUnknown,
    Released,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvidedBufBidState {
    DriverOwnedUnpublished,
    KernelPublished { publish_seq: u64 },
    KernelSelected,
    UserOwned,
    Vacant,
    Retired,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProvidedBufBidAnomaly {
    pub(crate) bid: u16,
    pub(crate) cqe_flags: Option<u32>,
    pub(crate) last_publish_sequence: Option<u64>,
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
        // This field is declared immediately after `IoUring` in `UringDriver`. Its drop is the
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
pub struct ProvidedBufStats {
    /// 交给用户的 buffer 条数。
    pub handed_out: u64,
    /// 完成被丢弃、buffer 原样还回环的次数（取消 / orphan / 陈旧 token）。
    pub returned: u64,
    /// 从池里补进环的次数。
    pub refilled: u64,
    /// 补充失败的次数。每一次都让环少一个 buffer，直到后续某次补充把它捡回来。
    pub refill_failed: u64,
    /// 内核报 `-ENOBUFS`（环空了）的完成条数。
    pub exhausted: u64,
    /// 环里当前可供内核挑选的 buffer 数。
    pub available: u16,
    /// `available` 的历史最低值——它才是「消费方跟不上」的证据。
    pub available_low_water: u16,
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
        let bytes = entries as usize * size_of::<BufRingEntry>();
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
    mapping: RingMapping,
    state: RingMappingOwnerState,
    lifetime_token: RingLifetimeToken,
}

impl RingMappingOwner {
    fn new(entries: u16, lifetime_token: RingLifetimeToken) -> UringResult<Self> {
        Ok(Self {
            mapping: RingMapping::new(entries)?,
            state: RingMappingOwnerState::Unregistered,
            lifetime_token,
        })
    }

    #[cfg(test)]
    fn new_registered_for_test(entries: u16, lifetime_token: RingLifetimeToken) -> Self {
        Self {
            mapping: RingMapping::new(entries).expect("test ring mapping must be created"),
            state: RingMappingOwnerState::Registered {
                bgid: PROVIDED_BUF_GROUP_ID,
                entries,
            },
            lifetime_token,
        }
    }

    fn register(&mut self, submitter: &Submitter<'_>, bgid: u16, entries: u16) -> UringResult<()> {
        debug_assert_eq!(self.state, RingMappingOwnerState::Unregistered);
        // SAFETY: the mapping is page-aligned and remains owned by this wrapper for the entire
        // registration syscall. It is kept alive until a successful unregister or ring teardown.
        unsafe { submitter.register_buf_ring_with_flags(self.mapping.addr(), entries, bgid, 0) }
            .map_err(|err| {
                UringError::Registration.io_report("uring.provided_buf.register", err)
            })?;
        self.state = RingMappingOwnerState::Registered { bgid, entries };
        Ok(())
    }

    #[cfg(test)]
    #[inline]
    fn addr(&self) -> u64 {
        self.mapping.addr()
    }

    #[inline]
    fn ptr(&self) -> NonNull<BufRingEntry> {
        debug_assert!(matches!(
            self.state,
            RingMappingOwnerState::Registered { .. } | RingMappingOwnerState::UnregisterUnknown
        ));
        self.mapping.ptr
    }

    #[inline]
    fn is_registered(&self) -> bool {
        matches!(
            self.state,
            RingMappingOwnerState::Registered { .. } | RingMappingOwnerState::UnregisterUnknown
        )
    }

    #[inline]
    fn mark_unregister_unknown(&mut self) {
        debug_assert!(self.is_registered());
        self.state = RingMappingOwnerState::UnregisterUnknown;
    }

    #[inline]
    fn mark_released(&mut self) {
        debug_assert!(self.is_registered());
        self.state = RingMappingOwnerState::Released;
    }
}

impl Drop for RingMappingOwner {
    fn drop(&mut self) {
        let safe_to_unmap = match self.state {
            RingMappingOwnerState::Unregistered | RingMappingOwnerState::Released => true,
            RingMappingOwnerState::Registered { .. } | RingMappingOwnerState::UnregisterUnknown => {
                !self.lifetime_token.is_alive()
            }
        };
        debug_assert!(
            safe_to_unmap,
            "provided ring mapping dropped while io_uring may still reference it"
        );
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
    mask: u16,
    /// 本地 tail，写入环条目后以 release 语义同步到环的 tail 字段。
    tail: u16,
    /// `bufs` 保存实际的 `FixedBuf` owner；`bid_states` 才说明这个 owner 是否已发布、被
    /// kernel 选中、交给用户或暂时空缺。两者必须通过状态方法一起变更。
    bufs: Box<[Option<FixedBuf>]>,
    bid_states: Box<[ProvidedBufBidState]>,
    /// 每个 bid 最近一次成功发布的序号；即使 bid 已经转为 vacant/user-owned，异常记录仍
    /// 能保留它最后一次进入内核可见状态的上下文。
    last_publish_sequences: Box<[Option<u64>]>,
    /// 补充失败留下的空洞，等后续任一次完成顺手重试。
    vacant: Vec<u16>,
    buf_size: NonZeroUsize,
    pool: AnyBufPool,
    stats: ProvidedBufStats,
    bid_anomalies: u64,
    last_bid_anomaly: Option<ProvidedBufBidAnomaly>,
    next_publish_sequence: u64,
}

/// 反注册失败时的错误和所有权载体。
///
/// `unregister_buf_ring` 返回错误只说明这一次 syscall 没有完成，不能证明内核已经不再
/// 使用 ring。因此失败结果必须把 group 一起交还给调用方，避免 `RingMapping` 和仍发布
/// 给内核的 `FixedBuf` 提前析构。
pub(crate) struct ProvidedBufUnregisterFailure {
    pub(crate) report: Report<UringError>,
    pub(crate) group: ProvidedBufGroup,
}

pub(crate) type ProvidedBufUnregisterResult = Result<(), Box<ProvidedBufUnregisterFailure>>;

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
    ) -> UringResult<Self> {
        Self::new_with_allocator(submitter, config, pool, ring_lifetime_token, |group| {
            group.alloc_buf()
        })
    }

    fn new_with_allocator<F>(
        submitter: &Submitter<'_>,
        config: ProvidedBufConfig,
        pool: AnyBufPool,
        ring_lifetime_token: RingLifetimeToken,
        allocate: F,
    ) -> UringResult<Self>
    where
        F: Fn(&Self) -> Option<FixedBuf>,
    {
        let entries = config.entries.get();
        if !entries.is_power_of_two() || entries > MAX_PROVIDED_BUF_ENTRIES {
            return UringError::InvalidInput
                .push_ctx("scope", "uring.provided_buf.new")
                .with_ctx("entries", entries)
                .with_ctx("max_entries", MAX_PROVIDED_BUF_ENTRIES)
                .attach_note(
                    "provided buffer ring entries must be a power of two within the kernel limit",
                );
        }

        let mut group = Self {
            bgid: PROVIDED_BUF_GROUP_ID,
            ring: RingMappingOwner::new(entries, ring_lifetime_token.clone())?,
            ring_lifetime_token,
            registration_state: ProvidedBufRegistrationState::Unregistered,
            health: ProvidedBufGroupHealth::Healthy,
            mask: entries - 1,
            tail: 0,
            bufs: (0..entries).map(|_| None).collect(),
            bid_states: (0..entries).map(|_| ProvidedBufBidState::Vacant).collect(),
            last_publish_sequences: (0..entries).map(|_| None).collect(),
            vacant: Vec::new(),
            buf_size: config.buf_size,
            pool,
            stats: ProvidedBufStats::default(),
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
                    group.vacant.push(bid);
                }
            }
        }

        if !(0..entries).any(|bid| group.has_unpublished_buf(bid)) {
            return UringError::Registration
                .push_ctx("scope", "uring.provided_buf.new")
                .with_ctx("entries", entries)
                .with_ctx("buf_size", config.buf_size.get())
                .attach_note("buffer pool could not fill a single provided buffer");
        }

        group
            .ring
            .register(submitter, PROVIDED_BUF_GROUP_ID, entries)?;
        group.registration_state = ProvidedBufRegistrationState::Registered;

        // 只有注册成功后才发布 entry。保持 entry 的 reserved 字段不变，并保留原有的
        // tail、available 和 refilled 统计语义。
        for bid in 0..entries {
            if group.has_unpublished_buf(bid) {
                group.publish_driver_owned(bid);
                group.stats.refilled = group.stats.refilled.saturating_add(1);
            }
        }

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
    pub(crate) const fn stats(&self) -> ProvidedBufStats {
        self.stats
    }

    /// 内核报「环里没 buffer 了」。
    #[inline]
    pub(crate) fn note_exhausted(&mut self) {
        self.stats.exhausted = self.stats.exhausted.saturating_add(1);
    }

    /// 把这条 CQE 选中的 buffer 取出来交给用户，并立刻补一个回同一个 bid。
    ///
    /// `res` 是 CQE 的结果：非负时它就是内核写进去的字节数。负数（出错）时内核仍可能带回
    /// bid——buffer 是在提交时选中的，失败路径照样要把它还回来——此时长度置 0。
    pub(crate) fn take_selected(&mut self, flags: u32, res: i32) -> Option<FixedBuf> {
        let bid = cqueue::buffer_select(flags)?;
        if !self.is_usable() {
            return None;
        }
        let mut buf = match self.claim_kernel_selected(bid, flags) {
            Some(buf) => buf,
            None => {
                self.quarantine();
                return None;
            }
        };

        let filled = usize::try_from(res).unwrap_or(0).min(buf.capacity());
        buf.set_len(filled);
        self.stats.handed_out = self.stats.handed_out.saturating_add(1);

        self.mark_user_owned(bid);
        self.mark_vacant(bid);
        if !self.refill(bid) {
            self.queue_vacant(bid);
        }
        self.retry_vacant();
        Some(buf)
    }

    /// 这条完成要被丢弃：把它选中的 buffer 原样还回环。
    ///
    /// 与 [`Self::take_selected`] 的差别在于**不碰池**——buffer 从没离开过 `bufs`，被内核
    /// 消费掉的只是那个环条目，重新发布一次就行。漏掉这一步的代价是每次取消泄漏一个 bid。
    pub(crate) fn return_selected(&mut self, flags: u32) -> bool {
        let Some(bid) = cqueue::buffer_select(flags) else {
            return false;
        };
        if !self.is_usable() || !self.prepare_kernel_return(bid, flags) {
            self.quarantine();
            return true;
        }
        self.note_consumed();
        self.set_bid_state(bid, ProvidedBufBidState::DriverOwnedUnpublished);
        self.publish_driver_owned(bid);
        self.stats.returned = self.stats.returned.saturating_add(1);
        self.retry_vacant();
        false
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

    /// 把 `bid` 的 buffer 拿出来，同时记账「内核消费了一个环条目」。
    fn claim_kernel_selected(&mut self, bid: u16, flags: u32) -> Option<FixedBuf> {
        let Some(slot) = self.bufs.get_mut(bid as usize) else {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "kernel selected a provided buffer id out of range",
            );
            return None;
        };
        if !matches!(
            self.bid_states.get(bid as usize),
            Some(ProvidedBufBidState::KernelPublished { .. })
        ) {
            self.note_cqe_bid_anomaly(bid, flags, "kernel selected a bid that was not published");
            return None;
        }
        let Some(buf) = slot.take() else {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "kernel selected a provided buffer we do not hold",
            );
            return None;
        };
        self.set_bid_state(bid, ProvidedBufBidState::KernelSelected);
        self.note_consumed();
        Some(buf)
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
        if !self.publish_driver_owned(bid) {
            self.bufs[bid as usize] = None;
            self.set_bid_state(bid, ProvidedBufBidState::Vacant);
            return false;
        }
        self.stats.refilled = self.stats.refilled.saturating_add(1);
        true
    }

    fn retry_vacant(&mut self) {
        if self.vacant.is_empty() {
            return;
        }
        let mut pending = mem::take(&mut self.vacant);
        pending.retain(|&bid| !self.refill(bid));
        self.vacant = pending;
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
    fn mark_vacant(&mut self, bid: u16) {
        if matches!(
            self.bid_states.get(bid as usize),
            Some(ProvidedBufBidState::UserOwned | ProvidedBufBidState::KernelSelected)
        ) {
            self.set_bid_state(bid, ProvidedBufBidState::Vacant);
        } else {
            self.note_bid_anomaly(bid, "marked a provided bid vacant from an invalid state");
        }
    }

    #[inline]
    fn mark_user_owned(&mut self, bid: u16) {
        if self.bid_states.get(bid as usize) == Some(&ProvidedBufBidState::KernelSelected) {
            self.set_bid_state(bid, ProvidedBufBidState::UserOwned);
        } else {
            self.note_bid_anomaly(bid, "handed out a provided bid that was not selected");
        }
    }

    #[inline]
    fn queue_vacant(&mut self, bid: u16) {
        if !self.vacant.contains(&bid) {
            self.vacant.push(bid);
        }
    }

    fn prepare_kernel_return(&mut self, bid: u16, flags: u32) -> bool {
        let Some(slot) = self.bufs.get(bid as usize) else {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "discarded completion selected an out-of-range bid",
            );
            return false;
        };
        if slot.is_none()
            || !matches!(
                self.bid_states.get(bid as usize),
                Some(ProvidedBufBidState::KernelPublished { .. })
            )
        {
            self.note_cqe_bid_anomaly(
                bid,
                flags,
                "discarded completion selected a bid that was not kernel-published",
            );
            return false;
        }
        self.set_bid_state(bid, ProvidedBufBidState::KernelSelected);
        true
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

    /// Publishes a buffer that is still owned by this group.
    fn publish_driver_owned(&mut self, bid: u16) -> bool {
        debug_assert_eq!(
            self.registration_state,
            ProvidedBufRegistrationState::Registered,
            "provided buffer published before ring registration"
        );
        if !self.is_usable()
            || self.bid_states.get(bid as usize)
                != Some(&ProvidedBufBidState::DriverOwnedUnpublished)
        {
            self.note_bid_anomaly(bid, "published a provided bid from an invalid state");
            return false;
        }
        self.publish(bid)
    }

    /// 把 `bid` 的 buffer 写进环并推进 tail。
    fn publish(&mut self, bid: u16) -> bool {
        let index = (self.tail & self.mask) as usize;
        let (addr, len) = {
            let Some(Some(buf)) = self.bufs.get_mut(bid as usize) else {
                self.note_bid_anomaly(bid, "published a provided bid without a buffer owner");
                return false;
            };
            (buf.as_mut_ptr() as u64, buf.capacity() as u32)
        };
        let publish_seq = self.next_publish_sequence;
        self.next_publish_sequence = self.next_publish_sequence.saturating_add(1);
        if let Some(last_publish_sequence) = self.last_publish_sequences.get_mut(bid as usize) {
            *last_publish_sequence = Some(publish_seq);
        }
        self.set_bid_state(bid, ProvidedBufBidState::KernelPublished { publish_seq });

        // SAFETY: `index <= mask`，而映射里正好有 `mask + 1` 个条目。只写 addr/len/bid，
        // 不碰 `resv`——entry 0 的 `resv` 就是环的 tail 字段（见 `BufRingEntry::tail`）。
        unsafe {
            let entry = &mut *self.ring.ptr().as_ptr().add(index);
            entry.set_addr(addr);
            entry.set_len(len);
            entry.set_bid(bid);
        }

        self.tail = self.tail.wrapping_add(1);
        // SAFETY: `ring.ptr` 指向环的第一个条目，正是 `BufRingEntry::tail` 要求的形参；
        // 该字段 2 字节对齐且在映射范围内。release 保证条目内容先于 tail 对内核可见。
        unsafe {
            let tail_ptr = BufRingEntry::tail(self.ring.ptr().as_ptr()).cast_mut();
            // `veloq_std` 尚未暴露 `AtomicU16::from_ptr`；这里是内核共享映射的原子 FFI 边界。
            AtomicU16::from_ptr(tail_ptr).store(self.tail, CoreOrdering::Release);
        }

        self.stats.available = self.stats.available.saturating_add(1);
        debug_assert!(self.bid_ledger_is_consistent());
        true
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
    }

    fn bid_ledger_is_consistent(&self) -> bool {
        self.bid_states.iter().enumerate().all(|(bid, state)| {
            let has_buf = self.bufs[bid].is_some();
            match state {
                ProvidedBufBidState::DriverOwnedUnpublished
                | ProvidedBufBidState::KernelPublished { .. } => has_buf,
                // `KernelSelected` is a short transition. A return path keeps the owner while a
                // handoff path has already taken it out, so both forms are intentionally allowed
                // only inside the settling method.
                ProvidedBufBidState::KernelSelected => true,
                ProvidedBufBidState::UserOwned
                | ProvidedBufBidState::Vacant
                | ProvidedBufBidState::Retired => !has_buf,
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

    pub(crate) fn has_selected_bids(&self) -> bool {
        self.bid_states
            .contains(&ProvidedBufBidState::KernelSelected)
    }

    #[cfg(test)]
    fn bid_state(&self, bid: u16) -> Option<ProvidedBufBidState> {
        self.bid_states.get(bid as usize).copied()
    }
}

#[cfg(test)]
impl Drop for ProvidedBufGroup {
    fn drop(&mut self) {
        // Unit-test groups emulate a registered ring without owning an `IoUring`. Mark the
        // standalone token closed before `RingMappingOwner` drops so the fixture models the same
        // post-ring teardown ordering as the production driver.
        self.ring_lifetime_token.close();
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
        mask: entries - 1,
        tail: 0,
        bufs: (0..entries).map(|_| None).collect(),
        bid_states: (0..entries).map(|_| ProvidedBufBidState::Vacant).collect(),
        last_publish_sequences: (0..entries).map(|_| None).collect(),
        vacant: Vec::new(),
        buf_size: NonZeroUsize::new(64).expect("test buffer size is non-zero"),
        pool: AnyBufPool::new(TestPool),
        stats: ProvidedBufStats::default(),
        bid_anomalies: 0,
        last_bid_anomaly: None,
        next_publish_sequence: 0,
    };
    for bid in 0..entries {
        if let Some(buf) = group.alloc_buf() {
            group.store_driver_owned(bid, buf);
            group.publish_driver_owned(bid);
        }
    }
    group.stats.available_low_water = entries;
    group.stats.refilled = entries as u64;
    group
}

impl Default for ProvidedBufStats {
    fn default() -> Self {
        Self {
            handed_out: 0,
            returned: 0,
            refilled: 0,
            refill_failed: 0,
            exhausted: 0,
            available: 0,
            available_low_water: u16::MAX,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AnyBufPool, PROVIDED_BUF_GROUP_ID, ProvidedBufConfig, ProvidedBufGroup, TestPool,
        test_group,
    };
    use veloq_std::num::{NonZeroU16, NonZeroUsize};

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
        let entry = unsafe { &mut *group.ring.ptr().as_ptr() };
        entry.set_addr(original_ptr as u64);
        entry.set_len(64);
        entry.set_bid(0);
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
        let ring = match io_uring::IoUring::new(8) {
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
        );

        assert!(
            result.is_err(),
            "an allocator that always fails must reject the group"
        );
        drop(ring);
    }

    #[test]
    fn registered_group_can_be_cleaned_after_an_injected_failure() {
        let ring = match io_uring::IoUring::new(8) {
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
        let entry = unsafe { &mut *group.ring.ptr().as_ptr() };
        entry.set_bid(0);
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

        assert!(group.return_selected(flags));
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
        drop(selected);

        assert!(group.return_selected(flags));
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
            .take_selected(flags, 7)
            .expect("published bid must produce a buffer");
        assert_eq!(buffer.len(), 7);
        assert!(matches!(
            group.bid_state(0),
            Some(super::ProvidedBufBidState::KernelPublished { publish_seq: 2 })
        ));
        assert_eq!(group.stats.available, 2);
        assert_eq!(group.bid_anomalies, 0);
    }
}
