use crate::slot;
use std::fmt::{Debug, Formatter, Result};

use crate::{DriverCoreError, DriverResult};

use super::super::CompletionPacket;
use super::anomaly::AnomalyOutcome;

pub struct CompletionCleanup {
    action: Box<dyn FnOnce() -> DriverResult<(), DriverCoreError> + Send + 'static>,
}

impl CompletionCleanup {
    pub fn new(
        action: impl FnOnce() -> DriverResult<(), DriverCoreError> + Send + 'static,
    ) -> Self {
        Self {
            action: Box::new(action),
        }
    }

    pub fn run(self) -> DriverResult<(), DriverCoreError> {
        (self.action)()
    }
}

impl Debug for CompletionCleanup {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        f.debug_struct("CompletionCleanup").finish_non_exhaustive()
    }
}

#[derive(Debug, Default)]
pub struct CompletionCleanupGuard {
    cleanup: Option<CompletionCleanup>,
}

impl CompletionCleanupGuard {
    pub fn new(cleanup: CompletionCleanup) -> Self {
        Self {
            cleanup: Some(cleanup),
        }
    }

    pub fn none() -> Self {
        Self::default()
    }

    pub fn is_armed(&self) -> bool {
        self.cleanup.is_some()
    }

    pub fn disarm(&mut self) -> bool {
        self.cleanup.take().is_some()
    }

    pub fn run(&mut self) -> DriverResult<bool, DriverCoreError> {
        let Some(cleanup) = self.cleanup.take() else {
            return Ok(false);
        };
        cleanup.run().map(|()| true)
    }
}

impl Drop for CompletionCleanupGuard {
    fn drop(&mut self) {
        if let Some(cleanup) = self.cleanup.take() {
            let _ = cleanup.run();
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordCompletionOutcome {
    RecordedUser,
    OrphanedDropped,
    Rejected(AnomalyOutcome),
}

impl RecordCompletionOutcome {
    pub fn anomaly_outcome(&self) -> Option<AnomalyOutcome> {
        match self {
            Self::RecordedUser | Self::OrphanedDropped => None,
            Self::Rejected(outcome) => Some(*outcome),
        }
    }
}

pub enum RecordCompletionResult<Spec: slot::SlotSpec> {
    Recorded(RecordCompletionOutcome),
    Rejected {
        outcome: RecordCompletionOutcome,
        packet: Box<CompletionPacket<Spec>>,
    },
}

impl<Spec: slot::SlotSpec> RecordCompletionResult<Spec> {
    pub fn outcome(&self) -> &RecordCompletionOutcome {
        match self {
            Self::Recorded(outcome) => outcome,
            Self::Rejected { outcome, .. } => outcome,
        }
    }

    pub fn into_outcome(self) -> RecordCompletionOutcome {
        match self {
            Self::Recorded(outcome) => outcome,
            Self::Rejected { outcome, .. } => outcome,
        }
    }
}
