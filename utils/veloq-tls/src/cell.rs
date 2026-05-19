use crate::{RawKey, ResetGuard, TlsError, is_sentinel, sentinel_ptr};
use once_cell::sync::OnceCell;
use std::marker::PhantomData;
use std::ptr;
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};

#[cfg(windows)]
use windows_sys::Win32::System::Threading::{
    FLS_OUT_OF_INDEXES, FlsAlloc, FlsFree, FlsGetValue, FlsSetValue,
};

#[cfg(unix)]
use libc::{pthread_getspecific, pthread_key_create, pthread_key_delete, pthread_setspecific};

/// Base layout for delayed reclamation of nodes.
/// Must be #[repr(C)] to safely cast generic Node<T> to BaseNode.
#[repr(C)]
struct BaseNode {
    next: *mut BaseNode,
    reclaim_fn: unsafe fn(*mut BaseNode),
}

/// Global lock-free orphan queue to defer reclamation of nodes from threads that exit
/// after TlsCell is dropped.
static ORPHAN_HEAD: AtomicPtr<BaseNode> = AtomicPtr::new(ptr::null_mut());

unsafe fn push_to_orphan_queue(node_ptr: *mut BaseNode) {
    unsafe {
        let mut current = ORPHAN_HEAD.load(Ordering::Relaxed);
        loop {
            (*node_ptr).next = current;
            match ORPHAN_HEAD.compare_exchange_weak(
                current,
                node_ptr,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }
}

fn reclaim_orphans() {
    let mut current = ORPHAN_HEAD.swap(ptr::null_mut(), Ordering::Acquire);
    while !current.is_null() {
        let next = unsafe { (*current).next };
        unsafe {
            ((*current).reclaim_fn)(current);
        }
        current = next;
    }
}

/// A node in the lock-free Treiber stack to track thread-allocated TLS memory.
/// Features inline co-allocation of T to save heap allocation overhead.
#[repr(C)]
struct Node<T> {
    /// Pointer to the next node in the lock-free stack.
    next: *mut Node<T>,
    /// Function pointer to reclaim the generic Node<T> memory via BaseNode.
    reclaim_fn: unsafe fn(*mut BaseNode),
    /// Atomic pointer to the actual allocated TLS value T.
    /// Points to the inline `value` field while active, and null if taken or exited.
    ptr: AtomicPtr<T>,
    /// Atomic reference count to coordinate concurrent drop of cell and thread exits.
    /// Initialized to 2 (Treiber stack owns 1 reference, OS TLS owns 1 reference).
    ref_count: AtomicUsize,
    /// Inline user value storage.
    value: std::mem::MaybeUninit<T>,
}

unsafe fn reclaim_node<T>(base_ptr: *mut BaseNode) {
    let node_ptr = base_ptr as *mut Node<T>;
    unsafe {
        let _ = Box::from_raw(node_ptr);
    }
}

/// Helper function to perform high-performance OS TLS node retrieval.
#[inline(always)]
fn get_os_node_ptr<T>(key: RawKey) -> *mut Node<T> {
    #[cfg(windows)]
    unsafe {
        FlsGetValue(key) as *mut Node<T>
    }
    #[cfg(unix)]
    unsafe {
        pthread_getspecific(key) as *mut Node<T>
    }
}

/// A premium, lock-free dynamic tracking TLS Cell that ensures immediate and complete
/// reclamation of all thread-local resources when the cell itself is dropped.
pub struct TlsCell<T, F = fn() -> T> {
    key: OnceCell<RawKey>,
    init: F,
    /// Head of the lock-free Treiber Stack tracking all allocated nodes.
    head: AtomicPtr<Node<T>>,
    cleanup_lock: std::sync::Mutex<()>, // 慢路径专用剪枝锁
    slow_path_count: AtomicUsize,       // 摊销计数器
    _marker: PhantomData<T>,
}

unsafe impl<T: Send, F: Send> Send for TlsCell<T, F> {}
unsafe impl<T: Send, F: Sync> Sync for TlsCell<T, F> {}

#[cfg(unix)]
unsafe extern "C" fn cell_destructor<T>(ptr: *mut libc::c_void) {
    if !ptr.is_null() && !is_sentinel(ptr) {
        let node_ptr = ptr as *mut Node<T>;

        let old_ptr = unsafe { (*node_ptr).ptr.swap(ptr::null_mut(), Ordering::AcqRel) };
        if !old_ptr.is_null() {
            unsafe {
                ptr::drop_in_place(old_ptr);
            }
        }

        if unsafe { (*node_ptr).ref_count.fetch_sub(1, Ordering::AcqRel) } == 1 {
            unsafe {
                let _ = Box::from_raw(node_ptr);
            }
        }
    }
}

#[cfg(windows)]
unsafe extern "system" fn cell_destructor<T>(ptr: *const std::ffi::c_void) {
    if !ptr.is_null() && !is_sentinel(ptr) {
        let node_ptr = ptr as *mut Node<T>;

        let old_ptr = unsafe { (*node_ptr).ptr.swap(ptr::null_mut(), Ordering::AcqRel) };
        if !old_ptr.is_null() {
            unsafe {
                ptr::drop_in_place(old_ptr);
            }
        }

        if unsafe { (*node_ptr).ref_count.fetch_sub(1, Ordering::AcqRel) } == 1 {
            unsafe {
                let _ = Box::from_raw(node_ptr);
            }
        }
    }
}

impl<T, F: Fn() -> T> TlsCell<T, F> {
    /// Creates a new `TlsCell` with an initialization closure.
    pub const fn new(init: F) -> Self {
        Self {
            key: OnceCell::new(),
            init,
            head: AtomicPtr::new(ptr::null_mut()),
            cleanup_lock: std::sync::Mutex::new(()),
            slow_path_count: AtomicUsize::new(0),
            _marker: PhantomData,
        }
    }

    #[inline]
    fn get_key(&self) -> Result<RawKey, TlsError> {
        self.key
            .get_or_try_init(|| {
                #[cfg(windows)]
                {
                    let key = unsafe { FlsAlloc(Some(cell_destructor::<T>)) };
                    if key == FLS_OUT_OF_INDEXES {
                        return Err(TlsError::AllocationFailed);
                    }
                    Ok(key)
                }
                #[cfg(unix)]
                {
                    let mut key = 0;
                    let res = unsafe { pthread_key_create(&mut key, Some(cell_destructor::<T>)) };
                    if res != 0 {
                        return Err(TlsError::AllocationFailed);
                    }
                    Ok(key)
                }
            })
            .copied()
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread.
    ///
    /// If no value has been set, the initialization closure is called.
    ///
    /// # Panics
    ///
    /// Panics if recursive initialization of the TLS variable is detected for the current thread.
    #[inline(always)]
    pub fn with<R>(&self, f: impl FnOnce(&T) -> R) -> R {
        let key = self.get_key().expect("TLS key allocation failed");

        let node_ptr: *mut Node<T> = get_os_node_ptr(key);

        if !node_ptr.is_null() {
            if is_sentinel(node_ptr) {
                panic!("TLS recursive initialization detected!");
            }
            let val_ptr = unsafe { (*node_ptr).ptr.load(Ordering::Acquire) };
            if !val_ptr.is_null() {
                return f(unsafe { &*val_ptr });
            }

            // Reuse the existing node if it was previously taken, saving CAS overhead.
            let val = (self.init)();
            let inline_addr =
                unsafe { &mut (*node_ptr).value as *mut std::mem::MaybeUninit<T> as *mut T };
            unsafe {
                ptr::write(inline_addr, val);
                (*node_ptr).ptr.store(inline_addr, Ordering::Release);
                return f(&*inline_addr);
            }
        }

        self.initialize_slow(key, f)
    }

    /// 慢路径自动剪枝函数
    fn prune_dead_nodes(&self) {
        // 使用 try_lock，绝不让清理工作拖慢并发线程的正常初始化
        if let Ok(_lock) = self.cleanup_lock.try_lock() {
            let mut current = self.head.load(Ordering::Relaxed);

            // 阶段 A: 清理链表头部连续的死节点（需要通过 CAS 竞争修改 head 根指针）
            while !current.is_null() && unsafe { (*current).ref_count.load(Ordering::Acquire) == 1 }
            {
                let next = unsafe { (*current).next };
                if self
                    .head
                    .compare_exchange_weak(current, next, Ordering::Release, Ordering::Relaxed)
                    .is_ok()
                {
                    // 成功从 Treiber 栈顶剥离死节点，将其直接彻底释放
                    unsafe {
                        let _ = Box::from_raw(current);
                    }
                    current = next;
                } else {
                    current = self.head.load(Ordering::Relaxed); // CAS 失败则刷新重试
                }
            }

            // 阶段 B: 清理链表深处的死节点
            let mut prev = current;
            if !prev.is_null() {
                let mut curr_next = unsafe { (*prev).next };
                while !curr_next.is_null() {
                    if unsafe { (*curr_next).ref_count.load(Ordering::Acquire) == 1 } {
                        // 发现深层死节点，标准链表断开操作
                        let next_next = unsafe { (*curr_next).next };
                        unsafe {
                            (*prev).next = next_next;
                        }

                        // 销毁 Node 内存
                        unsafe {
                            let _ = Box::from_raw(curr_next);
                        }
                        curr_next = next_next;
                    } else {
                        prev = curr_next;
                        curr_next = unsafe { (*curr_next).next };
                    }
                }
            }
        }
    }

    #[inline(never)]
    fn initialize_slow<R>(&self, key: RawKey, f: impl FnOnce(&T) -> R) -> R {
        // 摊销克制清理
        if self.slow_path_count.fetch_add(1, Ordering::Relaxed) % 64 == 0 {
            self.prune_dead_nodes();
            reclaim_orphans();
        }

        // Set sentinel to detect recursive initialization
        let sentinel = sentinel_ptr::<Node<T>>();
        #[cfg(windows)]
        unsafe {
            let res = FlsSetValue(key, sentinel as _);
            if res == 0 {
                panic!(
                    "Failed to set TLS sentinel: error code {}",
                    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
                );
            }
        }
        #[cfg(unix)]
        unsafe {
            let res = pthread_setspecific(key, sentinel as _);
            if res != 0 {
                panic!("Failed to set TLS sentinel: error code {}", res);
            }
        }

        // Use ResetGuard to guarantee sentinel cleanup in case of closure panic or set failure
        let guard = ResetGuard::new(key);

        // Initialize using the closure
        let val = (self.init)();

        // Allocate tracking node (Co-allocation)
        let node = Box::new(Node {
            next: ptr::null_mut(),
            reclaim_fn: reclaim_node::<T>,
            ptr: AtomicPtr::new(ptr::null_mut()),
            ref_count: AtomicUsize::new(2),
            value: std::mem::MaybeUninit::new(val),
        });
        let node_ptr = Box::into_raw(node);

        let inline_addr =
            unsafe { &mut (*node_ptr).value as *mut std::mem::MaybeUninit<T> as *mut T };
        unsafe {
            (*node_ptr).ptr.store(inline_addr, Ordering::Release);
        }

        // Push node onto the lock-free Treiber stack via CAS
        let mut current = self.head.load(Ordering::Relaxed);
        loop {
            unsafe {
                (*node_ptr).next = current;
            }
            match self.head.compare_exchange_weak(
                current,
                node_ptr,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }

        #[cfg(windows)]
        unsafe {
            let res = FlsSetValue(key, node_ptr as _);
            if res == 0 {
                (*node_ptr).ptr.store(ptr::null_mut(), Ordering::Release);
                ptr::drop_in_place(inline_addr);
                (*node_ptr).ref_count.store(1, Ordering::Release);
                panic!(
                    "Failed to set TLS value: error code {}",
                    std::io::Error::last_os_error().raw_os_error().unwrap_or(0)
                );
            }
        }
        #[cfg(unix)]
        unsafe {
            let res = pthread_setspecific(key, node_ptr as _);
            if res != 0 {
                (*node_ptr).ptr.store(ptr::null_mut(), Ordering::Release);
                ptr::drop_in_place(inline_addr);
                (*node_ptr).ref_count.store(1, Ordering::Release);
                panic!("Failed to set TLS value: error code {}", res);
            }
        }

        guard.cancel();
        f(unsafe { &*inline_addr })
    }

    /// Executes a closure with a reference to the value stored in TLS for the current thread without initializing it.
    ///
    /// Returns `None` if no value has been set for this thread or if it is currently being initialized.
    #[inline(always)]
    pub fn try_with<R>(&self, f: impl FnOnce(&T) -> R) -> Option<R> {
        let key = self.get_key().ok()?;

        let node_ptr: *mut Node<T> = get_os_node_ptr(key);

        if node_ptr.is_null() || is_sentinel(node_ptr) {
            None
        } else {
            let val_ptr = unsafe { (*node_ptr).ptr.load(Ordering::Acquire) };
            if val_ptr.is_null() {
                None
            } else {
                Some(unsafe { f(&*val_ptr) })
            }
        }
    }

    /// Sets an owned value into TLS for the current thread.
    ///
    /// If there was a previously stored value, it will be dropped.
    ///
    /// # Panics
    ///
    /// Panics if recursive access or modification during replacement is detected.
    #[inline(always)]
    pub fn set_owned(&self, val: impl Into<Box<T>>) -> Result<(), TlsError> {
        let key = self.get_key()?;

        let node_ptr: *mut Node<T> = get_os_node_ptr(key);

        let new_val = *val.into();

        if !node_ptr.is_null() {
            if is_sentinel(node_ptr) {
                panic!("TLS recursive access during modification detected!");
            }

            let inline_addr =
                unsafe { &mut (*node_ptr).value as *mut std::mem::MaybeUninit<T> as *mut T };
            // Direct atomic swap without modifying stack structures.
            let old_ptr = unsafe { (*node_ptr).ptr.swap(ptr::null_mut(), Ordering::AcqRel) };
            if !old_ptr.is_null() {
                unsafe {
                    ptr::drop_in_place(old_ptr);
                }
            }
            // 写入新值
            unsafe {
                ptr::write(inline_addr, new_val);
                (*node_ptr).ptr.store(inline_addr, Ordering::Release);
            }
            Ok(())
        } else {
            // 第一次设置前，也顺便触发一次剪枝和孤儿清理
            if self.slow_path_count.fetch_add(1, Ordering::Relaxed) % 64 == 0 {
                self.prune_dead_nodes();
                reclaim_orphans();
            }

            // First time setting, allocate and push to Treiber stack.
            let node = Box::new(Node {
                next: ptr::null_mut(),
                reclaim_fn: reclaim_node::<T>,
                ptr: AtomicPtr::new(ptr::null_mut()),
                ref_count: AtomicUsize::new(2),
                value: std::mem::MaybeUninit::new(new_val),
            });
            let node_ptr = Box::into_raw(node);

            let inline_addr =
                unsafe { &mut (*node_ptr).value as *mut std::mem::MaybeUninit<T> as *mut T };
            unsafe {
                (*node_ptr).ptr.store(inline_addr, Ordering::Release);
            }

            let mut current = self.head.load(Ordering::Relaxed);
            loop {
                unsafe {
                    (*node_ptr).next = current;
                }
                match self.head.compare_exchange_weak(
                    current,
                    node_ptr,
                    Ordering::Release,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }

            #[cfg(windows)]
            unsafe {
                let res = FlsSetValue(key, node_ptr as _);
                if res == 0 {
                    (*node_ptr).ptr.store(ptr::null_mut(), Ordering::Release);
                    ptr::drop_in_place(inline_addr);
                    (*node_ptr).ref_count.store(1, Ordering::Release);
                    return Err(TlsError::SetFailed(
                        std::io::Error::last_os_error().raw_os_error().unwrap_or(0),
                    ));
                }
            }
            #[cfg(unix)]
            unsafe {
                let res = pthread_setspecific(key, node_ptr as _);
                if res != 0 {
                    (*node_ptr).ptr.store(ptr::null_mut(), Ordering::Release);
                    ptr::drop_in_place(inline_addr);
                    (*node_ptr).ref_count.store(1, Ordering::Release);
                    return Err(TlsError::SetFailed(res as i32));
                }
            }

            Ok(())
        }
    }

    /// Takes the owned value out of the TLS for the current thread, returning it.
    #[inline(always)]
    pub fn take(&self) -> Option<Box<T>> {
        let key = self.get_key().ok()?;

        let node_ptr: *mut Node<T> = get_os_node_ptr(key);

        if node_ptr.is_null() || is_sentinel(node_ptr) {
            None
        } else {
            let old_ptr = unsafe { (*node_ptr).ptr.swap(ptr::null_mut(), Ordering::AcqRel) };
            if old_ptr.is_null() {
                None
            } else {
                let val = unsafe { ptr::read(old_ptr) };
                Some(Box::new(val))
            }
        }
    }
}

impl<T, F> Drop for TlsCell<T, F> {
    fn drop(&mut self) {
        // 1. 先删除 OS Key，防止后续触发新的 native 析构
        if let Some(&key) = self.key.get() {
            #[cfg(windows)]
            unsafe {
                FlsFree(key);
            }
            #[cfg(unix)]
            unsafe {
                pthread_key_delete(key);
            }
        }

        // 2. 安全遍历并清理链表，无需任何全局锁！
        let mut current = self.head.swap(ptr::null_mut(), Ordering::Acquire);
        while !current.is_null() {
            let next = unsafe { (*current).next };

            let val_ptr = unsafe { (*current).ptr.swap(ptr::null_mut(), Ordering::AcqRel) };
            if !val_ptr.is_null() {
                unsafe {
                    ptr::drop_in_place(val_ptr);
                }
            }

            if unsafe { (*current).ref_count.fetch_sub(1, Ordering::AcqRel) } == 1 {
                unsafe {
                    let _ = Box::from_raw(current);
                }
            } else {
                // 对方线程还没退出，我们将其推入全局孤儿队列，延迟释放
                unsafe {
                    push_to_orphan_queue(current as *mut BaseNode);
                }
            }
            current = next;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use AtomicUsize;
    use std::sync::Arc;
    use std::thread;

    struct DropTracker {
        counter: Arc<AtomicUsize>,
    }

    impl Drop for DropTracker {
        fn drop(&mut self) {
            self.counter.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn test_cell_basic_get_init() {
        let cell = TlsCell::new(|| 42);
        cell.with(|v| {
            assert_eq!(*v, 42);
        });
    }

    #[test]
    fn test_cell_thread_isolation() {
        let cell = Arc::new(TlsCell::new(|| 10));
        cell.with(|v| assert_eq!(*v, 10));

        let cell_clone = cell.clone();
        thread::spawn(move || {
            cell_clone.with(|v| assert_eq!(*v, 10));
            cell_clone.set_owned(20).unwrap();
            cell_clone.with(|v| assert_eq!(*v, 20));
        })
        .join()
        .unwrap();

        cell.with(|v| assert_eq!(*v, 10));
    }

    #[test]
    #[should_panic(expected = "TLS recursive initialization detected!")]
    fn test_cell_reentrancy_detection() {
        struct Reentrant;
        static RECURSIVE_CELL: TlsCell<Reentrant> =
            TlsCell::new(|| RECURSIVE_CELL.with(|_| Reentrant));
        RECURSIVE_CELL.with(|_| {});
    }

    #[test]
    fn test_cell_set_owned_and_take() {
        let cell = TlsCell::new(|| "init".to_string());
        cell.with(|v| assert_eq!(v, "init"));

        cell.set_owned("new_val".to_string()).unwrap();
        cell.with(|v| assert_eq!(v, "new_val"));

        let taken = cell.take().unwrap();
        assert_eq!(*taken, "new_val");

        assert!(cell.try_with(|v| v.clone()).is_none());

        // Test recovery / re-initialization after take
        cell.with(|v| assert_eq!(v, "init"));
    }

    #[test]
    fn test_cell_thread_exit_destructor() {
        let counter = Arc::new(AtomicUsize::new(0));
        let cell = Arc::new(TlsCell::new({
            let counter = counter.clone();
            move || DropTracker {
                counter: counter.clone(),
            }
        }));

        let cell_clone = cell.clone();
        let handle = thread::spawn(move || {
            cell_clone.with(|_| {});
        });
        handle.join().unwrap();

        // After thread exits, the native destructor must run immediately and safely reclaim T.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_cell_drop_reclaims_lingering_data() {
        let counter = Arc::new(AtomicUsize::new(0));

        // Scope to drop cell
        {
            let cell = TlsCell::new({
                let counter = counter.clone();
                move || DropTracker {
                    counter: counter.clone(),
                }
            });

            cell.with(|_| {});
            assert_eq!(counter.load(Ordering::SeqCst), 0);
        } // cell dropped here

        // Lingering data must be reclaimed when TlsCell drops.
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn test_cell_drop_reclaims_multiple_lingering_threads() {
        let counter = Arc::new(AtomicUsize::new(0));

        let (tx, rx) = std::sync::mpsc::channel();
        let barrier = Arc::new(std::sync::Barrier::new(4));

        let cell = Arc::new(TlsCell::new({
            let counter = counter.clone();
            move || DropTracker {
                counter: counter.clone(),
            }
        }));

        let mut handles = vec![];
        for _ in 0..3 {
            let cell_clone = cell.clone();
            let tx_clone = tx.clone();
            let barrier_clone = barrier.clone();
            let handle = thread::spawn(move || {
                cell_clone.with(|_| {});
                drop(cell_clone);
                tx_clone.send(()).unwrap();
                barrier_clone.wait(); // Wait here to prevent thread exit
            });
            handles.push(handle);
        }

        // Wait for all threads to initialize TLS
        for _ in 0..3 {
            rx.recv().unwrap();
        }

        assert_eq!(counter.load(Ordering::SeqCst), 0);

        // Reclaim the Arc reference to cell, so that it will be dropped when it goes out of scope.
        // First we drop the main arc.
        let raw_cell = Arc::try_unwrap(cell)
            .ok()
            .expect("Failed to unwrap TlsCell Arc");

        // Drop the cell itself. This must reclaim all TLS data.
        drop(raw_cell);

        // Lingering data for all 3 threads must be immediately reclaimed.
        assert_eq!(counter.load(Ordering::SeqCst), 3);

        // Unblock and join threads
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }

        // Must still be 3 (no double free).
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }
}
