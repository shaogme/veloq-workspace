use veloq_buf::BufferRegistrar;

use diagweave::prelude::*;

use crate::config::{BorrowedRawHandle, BufferRegistrationMode};
use crate::error::IocpResult;
use crate::ext::Extensions;
use crate::rio::RioState;

pub(crate) struct IocpRioRuntime<'a> {
    state: RioState,
    registrar: Box<dyn BufferRegistrar + 'a>,
}

impl<'a> IocpRioRuntime<'a> {
    pub(crate) fn new(
        port: BorrowedRawHandle<'_>,
        entries: u32,
        ext: &Extensions,
        registration_mode: BufferRegistrationMode,
        registrar: Box<dyn BufferRegistrar + 'a>,
    ) -> IocpResult<Self> {
        let state = RioState::new(port, entries, ext, registration_mode)
            .with_ctx("entries", entries)
            .with_ctx("port_raw", port.raw().as_handle() as usize)
            .attach_note("failed to initialize RIO state")
            .trans()?;
        Ok(Self { state, registrar })
    }

    pub(crate) fn state(&self) -> &RioState {
        &self.state
    }

    pub(crate) fn state_mut(&mut self) -> &mut RioState {
        &mut self.state
    }

    pub(crate) fn state_and_registrar_mut(
        &mut self,
    ) -> (&mut RioState, &(dyn BufferRegistrar + 'a)) {
        (&mut self.state, self.registrar.as_ref())
    }
}
