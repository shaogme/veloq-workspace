use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossbeam_queue::SegQueue;
use veloq_driver_core::driver::{RemoteWaker, SharedCompletionQueue, SharedCompletionTable};

use crate::common::IocpWaker;
use crate::error::IocpError;
use crate::op::IocpUserPayload;

pub(crate) struct CompletionPump {
    port: Arc<crate::win32::IoCompletionPort>,
    is_notified: Arc<AtomicBool>,
    events: SharedCompletionQueue,
    table: SharedCompletionTable<IocpUserPayload, IocpError>,
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

    pub(crate) fn port(&self) -> &crate::win32::IoCompletionPort {
        self.port.as_ref()
    }

    pub(crate) fn events(&self) -> &SharedCompletionQueue {
        &self.events
    }

    pub(crate) fn completion_queue(&self) -> SharedCompletionQueue {
        self.events.clone()
    }

    pub(crate) fn table(&self) -> &SharedCompletionTable<IocpUserPayload, IocpError> {
        &self.table
    }

    pub(crate) fn completion_table(&self) -> SharedCompletionTable<IocpUserPayload, IocpError> {
        self.table.clone()
    }

    pub(crate) fn clear_notification(&self) {
        self.is_notified.store(false, Ordering::Release);
    }

    pub(crate) fn create_waker(&self) -> Arc<dyn RemoteWaker<IocpError>> {
        Arc::new(IocpWaker {
            port: self.port.clone(),
            is_notified: self.is_notified.clone(),
        })
    }
}
