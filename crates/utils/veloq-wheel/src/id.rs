use slotmap::{DefaultKey, Key};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TimerId(DefaultKey);

impl TimerId {
    pub(crate) fn from_key(key: DefaultKey) -> Self {
        Self(key)
    }

    pub(crate) fn key(self) -> DefaultKey {
        self.0
    }

    pub fn raw(self) -> u64 {
        self.0.data().as_ffi()
    }
}
