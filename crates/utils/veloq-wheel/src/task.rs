use slotmap::{DefaultKey, Key};

/// Unique identifier for timer tasks
///
/// Wraps a slotmap key which includes generation information for safe reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TaskId(DefaultKey);

impl TaskId {
    /// Create TaskId from slotmap key
    #[inline]
    pub(crate) fn from_key(key: DefaultKey) -> Self {
        TaskId(key)
    }

    /// Get the slotmap key
    #[inline]
    pub(crate) fn key(&self) -> DefaultKey {
        self.0
    }

    /// Get the numeric value of the task ID (for debugging/logging)
    #[inline]
    pub fn raw(&self) -> u64 {
        self.0.data().as_ffi()
    }
}
