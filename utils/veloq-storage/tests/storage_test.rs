use std::ptr::NonNull;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::task::Waker;
use veloq_storage::*;

unsafe fn dummy_clone(ptr: *const ()) -> std::task::RawWaker {
    std::task::RawWaker::new(ptr, &DUMMY_VTABLE)
}

static DUMMY_VTABLE: std::task::RawWakerVTable =
    std::task::RawWakerVTable::new(dummy_clone, |_| {}, |_| {}, |_| {});

fn create_dummy_waker() -> Waker {
    let raw_waker = std::task::RawWaker::new(std::ptr::null(), &DUMMY_VTABLE);
    unsafe { Waker::from_raw(raw_waker) }
}

#[test]
fn test_strategy_type() {
    assert_eq!(AtomicStorage::strategy_type(), StrategyType::Atomic);
    assert_eq!(LocalStorage::strategy_type(), StrategyType::Local);
}

#[test]
fn test_state_int_atomic() {
    let atomic_int = <AtomicStorage as Storage>::Usize::new(10);
    assert_eq!(atomic_int.load(Ordering::Relaxed), 10);

    atomic_int.store(20, Ordering::Relaxed);
    assert_eq!(atomic_int.load(Ordering::Relaxed), 20);

    assert_eq!(atomic_int.fetch_add(5, Ordering::Relaxed), 20);
    assert_eq!(atomic_int.load(Ordering::Relaxed), 25);

    assert_eq!(atomic_int.fetch_sub(10, Ordering::Relaxed), 25);
    assert_eq!(atomic_int.load(Ordering::Relaxed), 15);

    assert_eq!(atomic_int.fetch_and(7, Ordering::Relaxed), 15); // 15 & 7 = 7
    assert_eq!(atomic_int.load(Ordering::Relaxed), 7);

    assert_eq!(atomic_int.fetch_or(8, Ordering::Relaxed), 7); // 7 | 8 = 15
    assert_eq!(atomic_int.load(Ordering::Relaxed), 15);

    assert_eq!(
        atomic_int.compare_exchange(15, 100, Ordering::Relaxed, Ordering::Relaxed),
        Ok(15)
    );
    assert_eq!(atomic_int.load(Ordering::Relaxed), 100);

    assert_eq!(
        atomic_int.compare_exchange(20, 200, Ordering::Relaxed, Ordering::Relaxed),
        Err(100)
    );
    assert_eq!(atomic_int.load(Ordering::Relaxed), 100);
}

#[test]
fn test_state_int_local() {
    let local_int = <LocalStorage as Storage>::Usize::new(10);
    assert_eq!(local_int.load(Ordering::Relaxed), 10);

    local_int.store(20, Ordering::Relaxed);
    assert_eq!(local_int.load(Ordering::Relaxed), 20);

    assert_eq!(local_int.fetch_add(5, Ordering::Relaxed), 20);
    assert_eq!(local_int.load(Ordering::Relaxed), 25);

    assert_eq!(local_int.fetch_sub(10, Ordering::Relaxed), 25);
    assert_eq!(local_int.load(Ordering::Relaxed), 15);

    assert_eq!(local_int.fetch_and(7, Ordering::Relaxed), 15);
    assert_eq!(local_int.load(Ordering::Relaxed), 7);

    assert_eq!(local_int.fetch_or(8, Ordering::Relaxed), 7);
    assert_eq!(local_int.load(Ordering::Relaxed), 15);

    assert_eq!(
        local_int.compare_exchange(15, 100, Ordering::Relaxed, Ordering::Relaxed),
        Ok(15)
    );
    assert_eq!(local_int.load(Ordering::Relaxed), 100);

    assert_eq!(
        local_int.compare_exchange(20, 200, Ordering::Relaxed, Ordering::Relaxed),
        Err(100)
    );
    assert_eq!(local_int.load(Ordering::Relaxed), 100);
}

#[test]
fn test_state_option_ptr_atomic() {
    let mut val1 = 42;
    let mut val2 = 84;
    let ptr1 = NonNull::new(&mut val1 as *mut i32).unwrap();
    let ptr2 = NonNull::new(&mut val2 as *mut i32).unwrap();

    let opt_ptr = <AtomicStorage as Storage>::OptionPtr::<i32>::new(Some(ptr1));
    assert_eq!(opt_ptr.load(Ordering::Relaxed), Some(ptr1));

    opt_ptr.store(Some(ptr2), Ordering::Relaxed);
    assert_eq!(opt_ptr.load(Ordering::Relaxed), Some(ptr2));

    assert_eq!(opt_ptr.swap(None, Ordering::Relaxed), Some(ptr2));
    assert_eq!(opt_ptr.load(Ordering::Relaxed), None);

    assert_eq!(
        opt_ptr.compare_exchange(None, Some(ptr1), Ordering::Relaxed, Ordering::Relaxed),
        Ok(None)
    );
    assert_eq!(opt_ptr.load(Ordering::Relaxed), Some(ptr1));

    assert_eq!(
        opt_ptr.compare_exchange(None, Some(ptr2), Ordering::Relaxed, Ordering::Relaxed),
        Err(Some(ptr1))
    );
}

#[test]
fn test_state_option_ptr_local() {
    let mut val1 = 42;
    let mut val2 = 84;
    let ptr1 = NonNull::new(&mut val1 as *mut i32).unwrap();
    let ptr2 = NonNull::new(&mut val2 as *mut i32).unwrap();

    let opt_ptr = <LocalStorage as Storage>::OptionPtr::<i32>::new(Some(ptr1));
    assert_eq!(opt_ptr.load(Ordering::Relaxed), Some(ptr1));

    opt_ptr.store(Some(ptr2), Ordering::Relaxed);
    assert_eq!(opt_ptr.load(Ordering::Relaxed), Some(ptr2));

    assert_eq!(opt_ptr.swap(None, Ordering::Relaxed), Some(ptr2));
    assert_eq!(opt_ptr.load(Ordering::Relaxed), None);

    assert_eq!(
        opt_ptr.compare_exchange(None, Some(ptr1), Ordering::Relaxed, Ordering::Relaxed),
        Ok(None)
    );
    assert_eq!(opt_ptr.load(Ordering::Relaxed), Some(ptr1));

    assert_eq!(
        opt_ptr.compare_exchange(None, Some(ptr2), Ordering::Relaxed, Ordering::Relaxed),
        Err(Some(ptr1))
    );
}

#[test]
fn test_state_nonnull_ptr_atomic() {
    let mut val1 = 42;
    let mut val2 = 84;
    let ptr1 = NonNull::new(&mut val1 as *mut i32).unwrap();
    let ptr2 = NonNull::new(&mut val2 as *mut i32).unwrap();

    let nn_ptr = <AtomicStorage as Storage>::NonNullPtr::<i32>::new(ptr1);
    assert_eq!(nn_ptr.load(Ordering::Relaxed), ptr1);

    nn_ptr.store(ptr2, Ordering::Relaxed);
    assert_eq!(nn_ptr.load(Ordering::Relaxed), ptr2);

    assert_eq!(nn_ptr.swap(ptr1, Ordering::Relaxed), ptr2);
    assert_eq!(nn_ptr.load(Ordering::Relaxed), ptr1);

    assert_eq!(
        nn_ptr.compare_exchange(ptr1, ptr2, Ordering::Relaxed, Ordering::Relaxed),
        Ok(ptr1)
    );
    assert_eq!(nn_ptr.load(Ordering::Relaxed), ptr2);

    assert_eq!(
        nn_ptr.compare_exchange(ptr1, ptr2, Ordering::Relaxed, Ordering::Relaxed),
        Err(ptr2)
    );
}

#[test]
fn test_state_nonnull_ptr_local() {
    let mut val1 = 42;
    let mut val2 = 84;
    let ptr1 = NonNull::new(&mut val1 as *mut i32).unwrap();
    let ptr2 = NonNull::new(&mut val2 as *mut i32).unwrap();

    let nn_ptr = <LocalStorage as Storage>::NonNullPtr::<i32>::new(ptr1);
    assert_eq!(nn_ptr.load(Ordering::Relaxed), ptr1);

    nn_ptr.store(ptr2, Ordering::Relaxed);
    assert_eq!(nn_ptr.load(Ordering::Relaxed), ptr2);

    assert_eq!(nn_ptr.swap(ptr1, Ordering::Relaxed), ptr2);
    assert_eq!(nn_ptr.load(Ordering::Relaxed), ptr1);

    assert_eq!(
        nn_ptr.compare_exchange(ptr1, ptr2, Ordering::Relaxed, Ordering::Relaxed),
        Ok(ptr1)
    );
    assert_eq!(nn_ptr.load(Ordering::Relaxed), ptr2);

    assert_eq!(
        nn_ptr.compare_exchange(ptr1, ptr2, Ordering::Relaxed, Ordering::Relaxed),
        Err(ptr2)
    );
}

#[test]
fn test_state_lock_atomic() {
    let lock = <AtomicStorage as Storage>::Lock::<i32>::new(10);
    {
        let mut guard = lock.lock();
        assert_eq!(*guard, 10);
        *guard = 20;
    }
    {
        let guard = lock.lock();
        assert_eq!(*guard, 20);
    }
}

#[test]
fn test_state_lock_local() {
    let lock = <LocalStorage as Storage>::Lock::<i32>::new(10);
    {
        let mut guard = lock.lock();
        assert_eq!(*guard, 10);
        *guard = 20;
    }
    {
        let guard = lock.lock();
        assert_eq!(*guard, 20);
    }
}

#[test]
fn test_state_waker_queue_atomic() {
    let queue = <AtomicStorage as Storage>::WakerQueue::new();
    let waker = create_dummy_waker();

    queue.register(&waker);
    let wakers = queue.take_all();
    assert_eq!(wakers.len(), 1);

    let wakers_empty = queue.take_all();
    assert_eq!(wakers_empty.len(), 0);
}

#[test]
fn test_state_waker_queue_local() {
    let queue = <LocalStorage as Storage>::WakerQueue::new();
    let waker = create_dummy_waker();

    queue.register(&waker);
    let wakers = queue.take_all();
    assert_eq!(wakers.len(), 1);

    let wakers_empty = queue.take_all();
    assert_eq!(wakers_empty.len(), 0);
}

#[test]
fn test_state_option_box_atomic() {
    let opt_box = <AtomicStorage as Storage>::OptionBox::<i32>::new(Some(Box::new(42)));
    assert_eq!(opt_box.take(Ordering::Relaxed), Some(Box::new(42)));
    assert_eq!(opt_box.take(Ordering::Relaxed), None);

    opt_box.store(Some(Box::new(100)), Ordering::Relaxed);
    assert_eq!(
        opt_box.swap(Some(Box::new(200)), Ordering::Relaxed),
        Some(Box::new(100))
    );
    assert_eq!(opt_box.take(Ordering::Relaxed), Some(Box::new(200)));

    assert_eq!(
        opt_box.compare_exchange_none(Box::new(300), Ordering::Relaxed, Ordering::Relaxed),
        Ok(())
    );
    assert_eq!(
        opt_box.compare_exchange_none(Box::new(400), Ordering::Relaxed, Ordering::Relaxed),
        Err(Box::new(400))
    );
}

#[test]
fn test_state_option_box_local() {
    let opt_box = <LocalStorage as Storage>::OptionBox::<i32>::new(Some(Box::new(42)));
    assert_eq!(opt_box.take(Ordering::Relaxed), Some(Box::new(42)));
    assert_eq!(opt_box.take(Ordering::Relaxed), None);

    opt_box.store(Some(Box::new(100)), Ordering::Relaxed);
    assert_eq!(
        opt_box.swap(Some(Box::new(200)), Ordering::Relaxed),
        Some(Box::new(100))
    );
    assert_eq!(opt_box.take(Ordering::Relaxed), Some(Box::new(200)));

    assert_eq!(
        opt_box.compare_exchange_none(Box::new(300), Ordering::Relaxed, Ordering::Relaxed),
        Ok(())
    );
    assert_eq!(
        opt_box.compare_exchange_none(Box::new(400), Ordering::Relaxed, Ordering::Relaxed),
        Err(Box::new(400))
    );
}

#[test]
fn test_state_option_arc_atomic() {
    let opt_arc = <AtomicStorage as Storage>::OptionArc::<i32>::new(Some(Arc::new(42)));
    assert_eq!(opt_arc.load_clone(Ordering::Relaxed), Some(Arc::new(42)));
    assert_eq!(opt_arc.take(Ordering::Relaxed), Some(Arc::new(42)));
    assert_eq!(opt_arc.take(Ordering::Relaxed), None);

    opt_arc.store(Some(Arc::new(100)), Ordering::Relaxed);
    assert_eq!(opt_arc.load_clone(Ordering::Relaxed), Some(Arc::new(100)));

    assert_eq!(
        opt_arc.compare_exchange_none(Arc::new(300), Ordering::Relaxed, Ordering::Relaxed),
        Err(Arc::new(300))
    );
}

#[test]
fn test_state_option_arc_local() {
    let opt_arc = <LocalStorage as Storage>::OptionArc::<i32>::new(Some(Arc::new(42)));
    assert_eq!(opt_arc.load_clone(Ordering::Relaxed), Some(Arc::new(42)));
    assert_eq!(opt_arc.take(Ordering::Relaxed), Some(Arc::new(42)));
    assert_eq!(opt_arc.take(Ordering::Relaxed), None);

    opt_arc.store(Some(Arc::new(100)), Ordering::Relaxed);
    assert_eq!(opt_arc.load_clone(Ordering::Relaxed), Some(Arc::new(100)));

    assert_eq!(
        opt_arc.compare_exchange_none(Arc::new(300), Ordering::Relaxed, Ordering::Relaxed),
        Err(Arc::new(300))
    );
}

#[test]
fn test_static_transfer() {
    let transfer = StaticTransfer::new(vec![1, 2, 3]);
    assert_eq!(transfer.take(0), 1);
    assert_eq!(transfer.take(1), 2);
    assert_eq!(transfer.take(2), 3);
}
