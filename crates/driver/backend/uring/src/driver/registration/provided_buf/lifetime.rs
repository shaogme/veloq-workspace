use core::sync::atomic::{AtomicBool, Ordering as CoreOrdering};

use libc::{madvise, mmap, munmap};
use veloq_io_uring::{
    Submitter,
    types::{BufRingEntry, BufRingItem, ProvidedBufRing},
};
use veloq_std::{
    io, mem,
    ptr::{self, NonNull},
    sync::Arc,
};

use crate::error::{UringError, UringResult};
use diagweave::prelude::*;

/// The driver registers a single provided-buffer group.
/// driver 当前只注册一个 provided-buffer group。
pub(crate) const PROVIDED_BUF_GROUP_ID: u16 = 0;

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

    pub(super) fn is_alive(&self) -> bool {
        self.alive.load(CoreOrdering::Acquire)
    }

    pub(super) fn close(&self) {
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
        // ring_lifetime 位于 IoUring 之后，先确认 ring fd 关闭，再允许释放 mapping。
        // ring_lifetime follows IoUring so the ring fd closes before the mapping can be released.
        // This field is declared immediately after `IoUring` in the driver. Its drop is the
        // explicit proof that the kernel-side ring fd is gone before a retained mapping owner is
        // dropped by the later `buffer_registry` field.
        self.token.close();
    }
}

/// 注册给内核的那一段环内存。
///
/// 单独一层是为了让 munmap 挂在 `Drop` 上：未注册的 group 可以在任意初始化错误路径
/// 自动释放；已注册的 group 则由 registry 保留到反注册成功，或等 `IoUring` 先销毁后
/// 再释放。后者是故意维护的生命周期边界，不依赖 `munmap` 偶然没有被内核访问。
/// This owner binds `munmap` to `Drop`: unregistered mappings release on initialization errors, while registered mappings stay alive until unregister succeeds or the ring is destroyed.
struct RingMapping {
    ptr: NonNull<BufRingEntry>,
    bytes: usize,
}

impl RingMapping {
    fn new(entries: u16) -> UringResult<Self> {
        let bytes = match (entries as usize)
            .checked_mul(mem::size_of::<BufRingEntry>())
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
        // `mmap` provides page alignment and anonymous mappings are zeroed, so entry zero's `resv` tail matches the local tail.
        let raw = unsafe {
            mmap(
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
        // 不忽略此处 errno：若映射可继承，子进程可能在父进程仍持有内核注册时访问该映射。
        if unsafe { madvise(raw, bytes, libc::MADV_DONTFORK) } != 0 {
            let error = io::Error::last_os_error();
            unsafe {
                let _ = munmap(raw, bytes);
            }
            return Err(
                UringError::Registration.io_report("uring.provided_buf.madvise_dontfork", error)
            );
        }
        Ok(Self {
            // SAFETY: `mmap` 只在返回 `MAP_FAILED` 时不给出有效指针，上面刚排除掉。 The failed case was excluded above, so `mmap` returned a valid pointer.
            ptr: unsafe { NonNull::new_unchecked(raw.cast::<BufRingEntry>()) },
            bytes,
        })
    }

    #[inline]
    pub(super) fn addr(&self) -> u64 {
        self.ptr.as_ptr() as u64
    }
}

impl Drop for RingMapping {
    fn drop(&mut self) {
        // SAFETY: 这段映射由 `Self::new` 创建，且只在这里释放一次。 `Self::new` created this mapping, and this drop path releases it exactly once.
        unsafe {
            munmap(self.ptr.as_ptr().cast(), self.bytes);
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum RingMappingOwnerState {
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
/// wrapper 记录系统调用结果；只有注销成功或 lifetime owner 证明 io_uring fd 已关闭后，才能释放映射。
pub(super) struct RingMappingOwner {
    /// The mapping must be intentionally leaked if the kernel may still reference it. Keeping it
    /// in `ManuallyDrop` makes the release-build behavior independent of `debug_assert!`.
    /// 若内核仍可能引用映射，就必须故意泄漏；`ManuallyDrop` 确保 release 构建不依赖 `debug_assert!`。
    mapping: mem::ManuallyDrop<RingMapping>,
    ring: ProvidedBufRing,
    state: RingMappingOwnerState,
    lifetime_token: RingLifetimeToken,
}

impl RingMappingOwner {
    pub(super) fn new(entries: u16, lifetime_token: RingLifetimeToken) -> UringResult<Self> {
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
    pub(super) fn new_registered_for_test(entries: u16, lifetime_token: RingLifetimeToken) -> Self {
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

    pub(super) fn register(
        &mut self,
        submitter: &Submitter<'_>,
        bgid: u16,
        entries: u16,
    ) -> UringResult<()> {
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
        // 安全性：映射页对齐，且 wrapper 在整个注册系统调用期间持有它；仅注销成功或 ring teardown 后才释放。
        unsafe { submitter.register_buf_ring_with_flags(self.mapping.addr(), entries, bgid, 0) }
            .map_err(|err| {
                UringError::Registration.io_report("uring.provided_buf.register", err)
            })?;
        self.state = RingMappingOwnerState::RegisteredButNotPublished { bgid, entries };
        Ok(())
    }

    #[cfg(test)]
    #[inline]
    pub(super) fn addr(&self) -> u64 {
        self.mapping.addr()
    }

    #[inline]
    pub(super) fn publish(&mut self, items: &[BufRingItem]) -> io::Result<()> {
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

    #[cfg(test)]
    pub(super) const fn state(&self) -> RingMappingOwnerState {
        self.state
    }

    #[inline]
    pub(super) fn is_registered(&self) -> bool {
        matches!(
            self.state,
            RingMappingOwnerState::RegisteredButNotPublished { .. }
                | RingMappingOwnerState::Registered { .. }
                | RingMappingOwnerState::UnregisterUnknown
        )
    }

    #[inline]
    pub(super) fn mark_published(&mut self) {
        if let RingMappingOwnerState::RegisteredButNotPublished { bgid, entries } = self.state {
            self.state = RingMappingOwnerState::Registered { bgid, entries };
        }
    }

    #[inline]
    pub(super) fn mark_unregister_unknown(&mut self) {
        if self.is_registered() {
            self.state = RingMappingOwnerState::UnregisterUnknown;
        }
    }

    #[inline]
    pub(super) fn mark_released(&mut self) {
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
            // 安全性：状态机或 ring lifetime token 已证明内核不再引用此映射。
            unsafe { mem::ManuallyDrop::drop(&mut self.mapping) };
        } else {
            tracing::error!(
                state = ?self.state,
                "provided ring mapping may still be referenced; leaking mapping as a safety fallback"
            );
        }
    }
}
