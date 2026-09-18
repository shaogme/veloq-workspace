//! io_uring 能力的生命周期状态。
//!
//! `KernelCapabilities` 记录内核观察值，但它不能表达后端协商结果或运行期降级。
//! `CapabilityState` 将这三个时间语义分开，并把每次运行期禁用的原因保存在同一个 owner
//! 中，避免门面字段和诊断快照各自维护一份“当前能力”。

use veloq_driver_core::driver::{DriverCapabilities, DriverCapability};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityDisableReason {
    pub capability: DriverCapability,
    pub source: &'static str,
    pub errno: Option<i32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CapabilityStateSnapshot {
    pub baseline: DriverCapabilities,
    pub negotiated: DriverCapabilities,
    pub effective: DriverCapabilities,
    pub disabled_reasons: [Option<CapabilityDisableReason>; 3],
}

/// 唯一持有 backend capability 时间语义的状态机。
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct CapabilityState {
    baseline: DriverCapabilities,
    negotiated: DriverCapabilities,
    effective: DriverCapabilities,
    disabled_reasons: [Option<CapabilityDisableReason>; 3],
}

impl CapabilityState {
    pub(crate) const fn new(baseline: DriverCapabilities) -> Self {
        Self {
            baseline,
            negotiated: baseline,
            effective: baseline,
            disabled_reasons: [None; 3],
        }
    }

    #[inline]
    pub(crate) const fn baseline(self) -> DriverCapabilities {
        self.baseline
    }

    #[inline]
    pub(crate) const fn negotiated(self) -> DriverCapabilities {
        self.negotiated
    }

    #[inline]
    pub(crate) const fn effective(self) -> DriverCapabilities {
        self.effective
    }

    /// 提交资源注册结果后设置协商能力。
    ///
    /// 该方法只修改 negotiated/effective，不会覆盖 baseline，也不会重新启用已被内核
    /// 拒绝的能力。
    pub(crate) fn set_negotiated(&mut self, negotiated: DriverCapabilities) {
        self.negotiated = negotiated;
        self.effective.accept_multi &= negotiated.accept_multi;
        self.effective.recv_multi &= negotiated.recv_multi;
        self.effective.provided_buffers &= negotiated.provided_buffers;
    }

    pub(crate) fn enable_negotiated(&mut self, capability: DriverCapability) {
        if self.negotiated.supports(capability) && self.disabled_reason(capability).is_none() {
            self.set_effective(capability, true);
        }
    }

    pub(crate) fn disable(
        &mut self,
        capability: DriverCapability,
        source: &'static str,
        errno: Option<i32>,
    ) {
        self.set_effective(capability, false);
        self.disabled_reasons[capability.capability_index()] = Some(CapabilityDisableReason {
            capability,
            source,
            errno,
        });
    }

    #[inline]
    pub(crate) fn disabled_reason(
        self,
        capability: DriverCapability,
    ) -> Option<CapabilityDisableReason> {
        self.disabled_reasons[capability.capability_index()]
    }

    #[inline]
    pub(crate) const fn snapshot(self) -> CapabilityStateSnapshot {
        CapabilityStateSnapshot {
            baseline: self.baseline,
            negotiated: self.negotiated,
            effective: self.effective,
            disabled_reasons: self.disabled_reasons,
        }
    }

    #[inline]
    fn set_effective(&mut self, capability: DriverCapability, enabled: bool) {
        match capability {
            DriverCapability::AcceptMulti => self.effective.accept_multi = enabled,
            DriverCapability::RecvMulti => self.effective.recv_multi = enabled,
            DriverCapability::ProvidedBuffers => self.effective.provided_buffers = enabled,
        }
    }
}

trait DriverCapabilitiesExt {
    fn supports(self, capability: DriverCapability) -> bool;
}

impl DriverCapabilitiesExt for DriverCapabilities {
    fn supports(self, capability: DriverCapability) -> bool {
        match capability {
            DriverCapability::AcceptMulti => self.accept_multi,
            DriverCapability::RecvMulti => self.recv_multi,
            DriverCapability::ProvidedBuffers => self.provided_buffers,
        }
    }
}

trait CapabilityIndex {
    fn capability_index(self) -> usize;
}

impl CapabilityIndex for DriverCapability {
    #[inline]
    fn capability_index(self) -> usize {
        match self {
            Self::AcceptMulti => 0,
            Self::RecvMulti => 1,
            Self::ProvidedBuffers => 2,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn runtime_rejection_changes_effective_only() {
        let baseline = DriverCapabilities {
            accept_multi: true,
            recv_multi: true,
            provided_buffers: true,
        };
        let mut state = CapabilityState::new(baseline);
        state.set_negotiated(DriverCapabilities {
            provided_buffers: false,
            ..baseline
        });
        state.disable(DriverCapability::RecvMulti, "first_cqe", Some(libc::EINVAL));

        let snapshot = state.snapshot();
        assert_eq!(snapshot.baseline, baseline);
        assert!(!snapshot.negotiated.provided_buffers);
        assert!(snapshot.negotiated.recv_multi);
        assert!(!snapshot.effective.recv_multi);
        assert_eq!(
            snapshot.disabled_reasons[1],
            Some(CapabilityDisableReason {
                capability: DriverCapability::RecvMulti,
                source: "first_cqe",
                errno: Some(libc::EINVAL),
            })
        );
    }

    #[test]
    fn disabled_capability_cannot_be_reenabled_by_later_negotiation() {
        let baseline = DriverCapabilities {
            accept_multi: true,
            recv_multi: false,
            provided_buffers: false,
        };
        let mut state = CapabilityState::new(baseline);
        state.disable(DriverCapability::AcceptMulti, "submit", None);
        state.set_negotiated(baseline);
        state.enable_negotiated(DriverCapability::AcceptMulti);

        assert!(!state.effective().accept_multi);
        assert_eq!(
            state.snapshot().disabled_reasons[0].unwrap().source,
            "submit"
        );
    }
}
