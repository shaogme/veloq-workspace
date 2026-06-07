use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use crossbeam_queue::SegQueue;
use veloq_driver_core::driver::{RemoteWaker, SharedCompletionQueue, SharedCompletionTable};

use crate::common::IocpWaker;
use crate::error::IocpError;
use crate::op::IocpUserPayload;

pub(crate) struct CompletionPump {
    pub(crate) port: Arc<crate::win32::IoCompletionPort>,
    pub(crate) is_notified: Arc<AtomicBool>,
    pub(crate) events: SharedCompletionQueue,
    pub(crate) table: SharedCompletionTable<IocpUserPayload, IocpError>,
}

impl CompletionPump {
    pub(crate) fn new(
        port: crate::win32::IoCompletionPort,
        table: SharedCompletionTable<IocpUserPayload, IocpError>,
    ) -> Self {
        Self {
            port: Arc::new(port),
            is_notified: Arc::new(AtomicBool::new(false)),
            events: Arc::new(SegQueue::new()),
            table,
        }
    }

    pub(crate) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        Arc::new(IocpWaker {
            port: self.port.clone(),
            is_notified: self.is_notified.clone(),
        })
    }
}
