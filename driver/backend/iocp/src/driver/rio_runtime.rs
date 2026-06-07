use veloq_buf::BufferRegistrar;

use diagweave::prelude::*;

use crate::config::{BorrowedRawHandle, BufferRegistrationMode};
use crate::error::IocpResult;
use crate::ext::Extensions;
use crate::rio::RioState;

pub(crate) struct IocpRioRuntime<'a> {
    pub(crate) state: RioState,
    pub(crate) registrar: Box<dyn BufferRegistrar + 'a>,
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
}
