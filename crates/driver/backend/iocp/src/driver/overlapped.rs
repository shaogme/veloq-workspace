use diagweave::prelude::*;
use veloq_driver_core::{OpToken, ReceiveRequest};
use veloq_std::{collections::HashMap, format};

use crate::{
    error::{IocpError, IocpResult},
    op::IocpOpRegistry,
    win32::Overlapped,
};

#[derive(Clone, Copy)]
pub(super) struct ChildOverlappedOwner {
    pub(super) token: OpToken,
    pub(super) request: ReceiveRequest,
}

pub(crate) struct OverlappedRegistry {
    roots: HashMap<usize, usize>,
    children: HashMap<usize, ChildOverlappedOwner>,
}

impl OverlappedRegistry {
    pub(super) fn new(ops: &mut IocpOpRegistry) -> IocpResult<Self> {
        let mut roots = HashMap::default();
        roots.try_reserve(ops.capacity()).map_err(|error| {
            IocpError::Submission
                .to_report()
                .with_ctx("root_overlapped_capacity", ops.capacity())
                .attach_note(format!("failed to reserve IOCP root owner map: {error}"))
        })?;
        for index in 0..ops.capacity() {
            let Some(bundle) = ops.slot_bundle_by_index_mut(index) else {
                return IocpError::InvalidState
                    .with_ctx("slot_index", index)
                    .attach_note("IOCP slot registry is missing a root sidecar");
            };
            let address = bundle
                .storage
                .with_mut(|_, _, sidecar| sidecar.inner() as *const Overlapped as usize);
            roots.insert(address, index);
        }
        Ok(Self {
            roots,
            children: HashMap::default(),
        })
    }

    pub(crate) fn reserve_children(&mut self, additional: usize) -> IocpResult<()> {
        self.children.try_reserve(additional).map_err(|error| {
            IocpError::Submission
                .to_report()
                .with_ctx("child_overlapped_capacity", additional)
                .attach_note(format!("failed to reserve IOCP child owner map: {error}"))
        })
    }

    pub(super) fn root_index(&self, address: usize) -> Option<usize> {
        self.roots.get(&address).copied()
    }

    pub(crate) fn register_child(
        &mut self,
        overlapped: &Overlapped,
        token: OpToken,
        request: ReceiveRequest,
    ) -> bool {
        let address = overlapped as *const Overlapped as usize;
        self.children
            .insert(address, ChildOverlappedOwner { token, request })
            .is_none()
    }

    pub(super) fn take_child(&mut self, address: usize) -> Option<ChildOverlappedOwner> {
        self.children.remove(&address)
    }

    pub(super) fn has_children_for(&self, token: OpToken) -> bool {
        self.child_count_for(token) > 0
    }

    fn child_count_for(&self, token: OpToken) -> usize {
        self.children
            .values()
            .filter(|owner| owner.token == token)
            .count()
    }
}
