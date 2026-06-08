use veloq_runtime::task::{AnyScopeRef, ScopeRef};
use veloq_runtime::utils::storage::{LocalStorage, NonAtomicUsize};

fn assert_send<T: Send>() {}
fn assert_sync<T: Sync>() {}

fn main() {
    assert_send::<LocalStorage>();
    assert_sync::<LocalStorage>();
    assert_send::<NonAtomicUsize>();
    assert_sync::<NonAtomicUsize>();
    assert_send::<ScopeRef<LocalStorage>>();
    assert_sync::<ScopeRef<LocalStorage>>();
    assert_send::<AnyScopeRef>();
    assert_sync::<AnyScopeRef>();
}
