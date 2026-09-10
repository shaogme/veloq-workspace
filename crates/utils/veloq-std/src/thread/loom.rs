use super::ThreadId;

pub(crate) fn current_id() -> ThreadId {
    use core::hash::{Hash, Hasher};

    let mut hasher = std::hash::DefaultHasher::new();
    loom::thread::current().id().hash(&mut hasher);
    ThreadId(hasher.finish())
}
