/// 一组 provided buffer 的运行期统计。
/// Runtime counters for a provided-buffer group.
///
/// [`Self::available`] 是「环里还剩几个 buffer 供内核挑」，但它只在处理 CQE 时才更新——
/// 内核消费与我们收割之间它偏高。作为诊断量足够，别拿它当实时水位。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProvidedBufferSnapshot {
    /// 交给用户的 buffer 条数。
    /// Number of buffers handed to callers.
    handed_out: u64,
    /// 完成被丢弃、buffer 原样还回环的次数（取消 / orphan / 陈旧 token）。
    /// Number of discarded completions whose selected buffer was returned unchanged.
    returned: u64,
    /// 从池里补进环的次数。
    /// Number of buffers refilled from the pool into the ring.
    refilled: u64,
    /// 补充失败的次数。每一次都让环少一个 buffer，直到后续某次补充把它捡回来。
    /// Failed refill count; each failure leaves one ring slot empty until a later refill succeeds.
    refill_failed: u64,
    /// 内核报 `-ENOBUFS`（环空了）的完成条数。
    /// Number of completions reported by the kernel with `-ENOBUFS`.
    exhausted: u64,
    /// 环里当前可供内核挑选的 buffer 数。
    /// Current number of buffers available for kernel selection.
    available: u16,
    /// `available` 的历史最低值——它才是「消费方跟不上」的证据。
    /// Lowest observed `available` count, indicating when consumers fell behind.
    available_low_water: u16,
    /// 已经从 ring claim、但尚未完成 settlement 的 lease 数量。
    /// Number of buffers claimed from the ring whose leases have not settled.
    selected_leases: u16,
    /// selected lease 中当前由 UDP pending ledger 持有的数量。
    /// Number of selected leases currently held by the UDP pending ledger.
    pending_udp_leases: u16,
    /// selected lease 中当前正在 delivery transaction 的数量。
    /// Number of selected leases currently in a delivery transaction.
    delivery_leases: u16,
    /// 已经有 owner、但尚未重新发布的 buffer 数量。
    /// Number of buffers owned by the driver but not yet republished.
    driver_owned_unpublished: u16,
    /// 因 bid/generation 或 publication 异常而隔离的数量。
    /// Number of buffers quarantined after BID, generation, or publication anomalies.
    quarantined: u16,
}

impl ProvidedBufferSnapshot {
    /// Returns the number of buffers handed to users.
    /// 返回已交给调用方的 buffer 数量。
    pub const fn handed_out(self) -> u64 {
        self.handed_out
    }

    /// Returns the number of selected buffers returned to the ring without user delivery.
    /// 返回未交付用户、直接归还 ring 的已选中 buffer 数量。
    pub const fn returned(self) -> u64 {
        self.returned
    }

    /// Returns the number of buffers refilled into the ring.
    /// 返回补充发布到 ring 的 buffer 数量。
    pub const fn refilled(self) -> u64 {
        self.refilled
    }

    /// Returns the number of refill attempts that failed.
    /// 返回补充失败的次数。
    pub const fn refill_failed(self) -> u64 {
        self.refill_failed
    }

    /// Returns the number of completions reported with `-ENOBUFS`.
    /// 返回内核报告 `-ENOBUFS` 的完成数量。
    pub const fn exhausted(self) -> u64 {
        self.exhausted
    }

    /// Returns the current number of buffers available to the kernel.
    /// 返回当前可供内核选择的 buffer 数量。
    pub const fn available(self) -> u16 {
        self.available
    }

    /// Returns the lowest observed number of buffers available to the kernel.
    /// 返回观测到的可用 buffer 最低水位。
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

#[derive(Debug, Clone, Copy)]
pub(super) struct ProvidedBufSnapshotParts {
    pub(super) handed_out: u64,
    pub(super) returned: u64,
    pub(super) refilled: u64,
    pub(super) refill_failed: u64,
    pub(super) exhausted: u64,
    pub(super) available: u16,
    pub(super) available_low_water: u16,
    pub(super) selected_leases: u16,
    pub(super) pending_udp_leases: u16,
    pub(super) delivery_leases: u16,
    pub(super) driver_owned_unpublished: u16,
    pub(super) quarantined: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProvidedBufStats {
    handed_out: u64,
    returned: u64,
    refilled: u64,
    refill_failed: u64,
    exhausted: u64,
    available: u16,
    available_low_water: u16,
    selected_leases: u16,
    pending_udp_leases: u16,
    delivery_leases: u16,
    driver_owned_unpublished: u16,
    quarantined: u16,
}

pub(super) struct ProvidedBufOwnershipParts {
    pub(super) selected: u16,
    pub(super) pending: u16,
    pub(super) delivery: u16,
    pub(super) unpublished: u16,
    pub(super) quarantined: u16,
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
            selected_leases: 0,
            pending_udp_leases: 0,
            delivery_leases: 0,
            driver_owned_unpublished: 0,
            quarantined: 0,
        }
    }
}

impl ProvidedBufStats {
    pub(super) fn snapshot(self) -> ProvidedBufferSnapshot {
        ProvidedBufferSnapshot::from_parts(ProvidedBufSnapshotParts {
            handed_out: self.handed_out,
            returned: self.returned,
            refilled: self.refilled,
            refill_failed: self.refill_failed,
            exhausted: self.exhausted,
            available: self.available,
            available_low_water: self.available_low_water,
            selected_leases: self.selected_leases,
            pending_udp_leases: self.pending_udp_leases,
            delivery_leases: self.delivery_leases,
            driver_owned_unpublished: self.driver_owned_unpublished,
            quarantined: self.quarantined,
        })
    }

    pub(super) fn set_initial_level(&mut self) {
        self.available_low_water = self.available;
    }

    pub(super) fn available(&self) -> u16 {
        self.available
    }
    pub(super) fn note_exhausted(&mut self) {
        self.exhausted = self.exhausted.saturating_add(1);
    }
    pub(super) fn note_returned(&mut self) {
        self.returned = self.returned.saturating_add(1);
    }
    pub(super) fn note_handed_out(&mut self) {
        self.handed_out = self.handed_out.saturating_add(1);
    }
    pub(super) fn note_refill_failed(&mut self) {
        self.refill_failed = self.refill_failed.saturating_add(1);
    }
    pub(super) fn note_published(&mut self, published: u16, refilled: u64) {
        self.available = self.available.saturating_add(published);
        self.refilled = self.refilled.saturating_add(refilled);
    }
    pub(super) fn note_consumed(&mut self) {
        self.available = self.available.saturating_sub(1);
        if self.available < self.available_low_water {
            self.available_low_water = self.available;
        }
    }
    pub(super) fn set_ownership(&mut self, parts: ProvidedBufOwnershipParts) {
        self.selected_leases = parts.selected;
        self.pending_udp_leases = parts.pending;
        self.delivery_leases = parts.delivery;
        self.driver_owned_unpublished = parts.unpublished;
        self.quarantined = parts.quarantined;
    }
}

impl ProvidedBufferSnapshot {
    pub(super) fn from_parts(parts: ProvidedBufSnapshotParts) -> Self {
        Self {
            handed_out: parts.handed_out,
            returned: parts.returned,
            refilled: parts.refilled,
            refill_failed: parts.refill_failed,
            exhausted: parts.exhausted,
            available: parts.available,
            available_low_water: parts.available_low_water,
            selected_leases: parts.selected_leases,
            pending_udp_leases: parts.pending_udp_leases,
            delivery_leases: parts.delivery_leases,
            driver_owned_unpublished: parts.driver_owned_unpublished,
            quarantined: parts.quarantined,
        }
    }
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
