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

use core::sync::atomic::{AtomicU16, Ordering as CoreOrdering};

use veloq_std::{
    boxed::Box,
    io, mem,
    num::NonZeroUsize,
    ptr::{self, NonNull},
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

pub(crate) struct ProvidedBufGroup {
    bgid: u16,
    ring: RingMapping,
    mask: u16,
    /// 本地 tail，写入环条目后以 release 语义同步到环的 tail 字段。
    tail: u16,
    /// 下标就是 bid。`Some` = 这个 buffer 已经发布给内核（所有权在我们手里，内核可能正
    /// 在往里写）；`None` 是一个暂时的空洞：它被交给用户之后补充失败了。
    bufs: Box<[Option<FixedBuf>]>,
    /// 补充失败留下的空洞，等后续任一次完成顺手重试。
    vacant: Vec<u16>,
    buf_size: NonZeroUsize,
    pool: AnyBufPool,
    stats: ProvidedBufStats,
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
    ) -> UringResult<Self> {
        Self::new_with_allocator(submitter, config, pool, |group| group.alloc_buf())
    }

    fn new_with_allocator<F>(
        submitter: &Submitter<'_>,
        config: ProvidedBufConfig,
        pool: AnyBufPool,
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
            ring: RingMapping::new(entries)?,
            mask: entries - 1,
            tail: 0,
            bufs: (0..entries).map(|_| None).collect(),
            vacant: Vec::new(),
            buf_size: config.buf_size,
            pool,
            stats: ProvidedBufStats::default(),
        };

        // 先准备用户态所有权，不写 entry，也不推进 tail。这样注册 syscall 失败时，内核
        // 从未获得这段映射或其中 buffer 的可见引用，group 可以直接析构。
        for bid in 0..entries {
            match allocate(&group) {
                Some(buf) => group.bufs[bid as usize] = Some(buf),
                None => {
                    group.stats.refill_failed = group.stats.refill_failed.saturating_add(1);
                    group.vacant.push(bid);
                }
            }
        }

        if !group.bufs.iter().any(|buf| buf.is_some()) {
            return UringError::Registration
                .push_ctx("scope", "uring.provided_buf.new")
                .with_ctx("entries", entries)
                .with_ctx("buf_size", config.buf_size.get())
                .attach_note("buffer pool could not fill a single provided buffer");
        }

        // SAFETY: group 保有 ring mapping。注册成功后，group 会被 registry 保留到
        // `try_unregister` 成功，或者等字段析构时 `IoUring` 先销毁；注册失败时 group
        // 仍未进入内核可见状态，可以安全析构。
        unsafe {
            submitter.register_buf_ring_with_flags(
                group.ring.addr(),
                entries,
                PROVIDED_BUF_GROUP_ID,
                0,
            )
        }
        .map_err(|err| UringError::Registration.io_report("uring.provided_buf.register", err))?;

        // 只有注册成功后才发布 entry。保持 entry 的 reserved 字段不变，并保留原有的
        // tail、available 和 refilled 统计语义。
        for bid in 0..entries {
            if group.bufs[bid as usize].is_some() {
                group.publish(bid);
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
    pub(crate) fn sqe_info(&self) -> ProvidedBufSqeInfo {
        ProvidedBufSqeInfo::new(self.bgid, self.buf_size.get() as u32)
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
        let mut buf = self.claim(bid, "take_selected")?;

        let filled = usize::try_from(res).unwrap_or(0).min(buf.capacity());
        buf.set_len(filled);
        self.stats.handed_out = self.stats.handed_out.saturating_add(1);

        if !self.refill(bid) {
            self.vacant.push(bid);
        }
        self.retry_vacant();
        Some(buf)
    }

    /// 这条完成要被丢弃：把它选中的 buffer 原样还回环。
    ///
    /// 与 [`Self::take_selected`] 的差别在于**不碰池**——buffer 从没离开过 `bufs`，被内核
    /// 消费掉的只是那个环条目，重新发布一次就行。漏掉这一步的代价是每次取消泄漏一个 bid。
    pub(crate) fn return_selected(&mut self, flags: u32) {
        let Some(bid) = cqueue::buffer_select(flags) else {
            return;
        };
        match self.bufs.get(bid as usize) {
            Some(Some(_)) => {}
            _ => {
                warn!(
                    bid,
                    "discarded completion selected an unknown provided buffer"
                );
                return;
            }
        }
        self.note_consumed();
        self.publish(bid);
        self.stats.returned = self.stats.returned.saturating_add(1);
        self.retry_vacant();
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
    pub(crate) fn try_unregister_with<F>(self, unregister: F) -> ProvidedBufUnregisterResult
    where
        F: FnOnce(u16) -> Result<(), i32>,
    {
        let bgid = self.bgid;
        match unregister(bgid) {
            Ok(()) => Ok(()),
            Err(err) => Err(Box::new(ProvidedBufUnregisterFailure {
                report: UringError::Registration
                    .io_report(
                        "uring.provided_buf.unregister",
                        io::Error::from_raw_os_error(err),
                    )
                    .with_ctx("bgid", bgid),
                group: self,
            })),
        }
    }

    /// 把 `bid` 的 buffer 拿出来，同时记账「内核消费了一个环条目」。
    fn claim(&mut self, bid: u16, scope: &'static str) -> Option<FixedBuf> {
        let Some(slot) = self.bufs.get_mut(bid as usize) else {
            warn!(
                bid,
                scope, "kernel selected a provided buffer id out of range"
            );
            return None;
        };
        let Some(buf) = slot.take() else {
            warn!(
                bid,
                scope, "kernel selected a provided buffer we do not hold"
            );
            return None;
        };
        self.note_consumed();
        Some(buf)
    }

    /// 从池里取一个新 buffer 填进 `bid` 并发布。失败时 `bid` 留空，由 [`Self::retry_vacant`]
    /// 后续捡回来。
    fn refill(&mut self, bid: u16) -> bool {
        let Some(buf) = self.alloc_buf() else {
            self.stats.refill_failed = self.stats.refill_failed.saturating_add(1);
            return false;
        };
        let Some(slot) = self.bufs.get_mut(bid as usize) else {
            return false;
        };
        *slot = Some(buf);
        self.publish(bid);
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

    /// 把 `bid` 的 buffer 写进环并推进 tail。
    fn publish(&mut self, bid: u16) {
        let index = (self.tail & self.mask) as usize;
        let (addr, len) = {
            let Some(Some(buf)) = self.bufs.get_mut(bid as usize) else {
                return;
            };
            (buf.as_mut_ptr() as u64, buf.capacity() as u32)
        };

        // SAFETY: `index <= mask`，而映射里正好有 `mask + 1` 个条目。只写 addr/len/bid，
        // 不碰 `resv`——entry 0 的 `resv` 就是环的 tail 字段（见 `BufRingEntry::tail`）。
        unsafe {
            let entry = &mut *self.ring.ptr.as_ptr().add(index);
            entry.set_addr(addr);
            entry.set_len(len);
            entry.set_bid(bid);
        }

        self.tail = self.tail.wrapping_add(1);
        // SAFETY: `ring.ptr` 指向环的第一个条目，正是 `BufRingEntry::tail` 要求的形参；
        // 该字段 2 字节对齐且在映射范围内。release 保证条目内容先于 tail 对内核可见。
        unsafe {
            let tail_ptr = BufRingEntry::tail(self.ring.ptr.as_ptr()).cast_mut();
            // `veloq_std` 尚未暴露 `AtomicU16::from_ptr`；这里是内核共享映射的原子 FFI 边界。
            AtomicU16::from_ptr(tail_ptr).store(self.tail, CoreOrdering::Release);
        }

        self.stats.available = self.stats.available.saturating_add(1);
    }

    fn note_consumed(&mut self) {
        self.stats.available = self.stats.available.saturating_sub(1);
        if self.stats.available < self.stats.available_low_water {
            self.stats.available_low_water = self.stats.available;
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
    let mut group = ProvidedBufGroup {
        bgid: PROVIDED_BUF_GROUP_ID,
        ring: RingMapping::new(entries).expect("test ring mapping must be created"),
        mask: entries - 1,
        tail: 0,
        bufs: (0..entries).map(|_| None).collect(),
        vacant: Vec::new(),
        buf_size: NonZeroUsize::new(64).expect("test buffer size is non-zero"),
        pool: AnyBufPool::new(TestPool),
        stats: ProvidedBufStats::default(),
    };
    for bid in 0..entries {
        group.bufs[bid as usize] = group.alloc_buf();
    }
    group.stats.available = entries;
    group.stats.available_low_water = entries;
    group.stats.refilled = entries as u64;
    group
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
        let entry = unsafe { &mut *group.ring.ptr.as_ptr() };
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
            |_| None,
        );

        assert!(
            result.is_err(),
            "an allocator that always fails must reject the group"
        );
        drop(submitter);
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
        let entry = unsafe { &mut *group.ring.ptr.as_ptr() };
        entry.set_bid(0);
        group.bufs[0]
            .as_mut()
            .expect("registered group retains its buffer")
            .spare_capacity_mut()[0] = 0x5A;

        match group.try_unregister(&submitter) {
            Ok(()) => {}
            Err(_) => panic!("a registered test ring must unregister successfully"),
        }
        drop(submitter);
        drop(ring);
    }
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
