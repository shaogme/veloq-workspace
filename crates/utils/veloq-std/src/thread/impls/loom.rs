use core::hash::{Hash, Hasher};

use crate::collections::hash_map::DefaultHasher;

use super::ThreadId;

pub(crate) struct LoomThread;

impl LoomThread {
    pub(crate) fn current_id() -> ThreadId {
        let mut hasher = DefaultHasher::new();
        loom::thread::current().id().hash(&mut hasher);
        ThreadId(hasher.finish())
    }
}
