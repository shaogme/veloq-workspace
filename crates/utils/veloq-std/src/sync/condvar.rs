use core::{fmt, ptr::null_mut, sync::atomic::Ordering, time::Duration};

use crate::{
    sync::{
        LockResult,
        atomic::{NativeAtomicPtr, NativeAtomicU32},
        mutex::{NativeMutex, NativeMutexGuard},
        sys::native,
        unpoisoned_mutex::{NativeUnpoisonedMutex, NativeUnpoisonedMutexGuard},
    },
    time::Instant,
};

#[cfg(feature = "loom")]
use crate::sync::{
    atomic::{LoomAtomicPtr, LoomAtomicU32},
    mutex::{LoomMutex, LoomMutexGuard},
    sys::loom::WaitChannel,
    unpoisoned_mutex::{LoomUnpoisonedMutex, LoomUnpoisonedMutexGuard},
};

const WAITING: u32 = 0;
const NOTIFYING: u32 = 1;
const NOTIFIED: u32 = 2;
const TIMED_OUT: u32 = 3;

/// 状态等待结果，用于表示等待是否超时。
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub struct WaitTimeoutResult(bool);

impl WaitTimeoutResult {
    /// 如果等待超时返回 `true`，否则返回 `false`。
    #[inline]
    pub fn timed_out(&self) -> bool {
        self.0
    }
}

macro_rules! impl_condvar {
    (
        $(#[$cvar_meta:meta])*
        condvar: $cvar_name:ident,
        $(#[$unp_meta:meta])*
        unpoisoned_condvar: $unp_cvar_name:ident,
        waiter: $waiter_name:ident,
        queue: $queue_name:ident,
        guard: $guard_name:ident,
        atomic_u32: $atomic_u32_ty:ident,
        atomic_ptr: $atomic_ptr_ty:ident,
        unpoisoned_mutex: $unpoisoned_mutex_ty:ident,
        unpoisoned_guard: $unpoisoned_guard_ty:ident,
        mutex: $mutex_ty:ident,
        mutex_guard: $mutex_guard_ty:ident,
        $(waiter_channel: $channel_field:ident: $channel_ty:ty, init_channel: $init_channel:expr,)?
        wait_once: |$w_self:ident, $exp:ident| $wait_once_expr:expr,
        wait_timeout_sub: |$wt_self:ident, $dur_rem:ident| $wait_timeout_expr:expr,
        finish_notify: |$fn_self:ident| $finish_notify_expr:expr,
        const_new: $is_const:ident
    ) => {
        struct $waiter_name {
            state: $atomic_u32_ty,
            prev: $atomic_ptr_ty<$waiter_name>,
            next: $atomic_ptr_ty<$waiter_name>,
            $($channel_field: $channel_ty,)?
        }

        impl $waiter_name {
            fn new() -> Self {
                Self {
                    state: <$atomic_u32_ty>::new(WAITING),
                    prev: <$atomic_ptr_ty<$waiter_name>>::new(null_mut()),
                    next: <$atomic_ptr_ty<$waiter_name>>::new(null_mut()),
                    $($channel_field: $init_channel,)?
                }
            }

            fn wait(&self, queue: &$queue_name) {
                loop {
                    match self.state.load(Ordering::Acquire) {
                        WAITING => self.wait_once(WAITING),
                        NOTIFYING => self.wait_once(NOTIFYING),
                        NOTIFIED => {
                            // The notifier keeps the node alive until the queue lock is
                            // released after the final state transition.
                            let _guard = queue.lock();
                            return;
                        }
                        TIMED_OUT => panic!("an untimed condvar waiter timed out"),
                        _ => unreachable!("invalid condvar waiter state"),
                    }
                }
            }

            fn wait_once(&self, expected: u32) {
                let $w_self = self;
                let $exp = expected;
                $wait_once_expr;
            }

            fn wait_timeout(&self, queue: &$queue_name, dur: Duration) -> bool {
                let start = Instant::now();

                loop {
                    match self.state.load(Ordering::Acquire) {
                        WAITING => {}
                        NOTIFYING => {
                            self.wait(queue);
                            return false;
                        }
                        NOTIFIED => {
                            let _guard = queue.lock();
                            return false;
                        }
                        TIMED_OUT => unreachable!("timed-out condvar waiter was reused"),
                        _ => unreachable!("invalid condvar waiter state"),
                    }

                    let elapsed = start.elapsed();
                    if elapsed >= dur {
                        return self.cancel(queue);
                    }
                    let remaining = dur - elapsed;

                    let $wt_self = self;
                    let $dur_rem = remaining;
                    let timed_out = $wait_timeout_expr;

                    if timed_out && start.elapsed() >= dur {
                        return self.cancel(queue);
                    }
                }
            }

            fn cancel(&self, queue: &$queue_name) -> bool {
                let guard = queue.lock();
                match self.state.compare_exchange(
                    WAITING,
                    TIMED_OUT,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => {
                        // SAFETY: the node was registered before this call and its links
                        // are protected by the queue lock held by `guard`.
                        unsafe { guard.unlink(self as *const Self as *mut Self) };
                        true
                    }
                    Err(NOTIFYING) => {
                        drop(guard);
                        self.wait(queue);
                        false
                    }
                    Err(NOTIFIED) => false,
                    Err(TIMED_OUT) => unreachable!("condvar waiter was cancelled twice"),
                    Err(_) => unreachable!("invalid condvar waiter state"),
                }
            }

            fn finish_notify(&self) {
                let $fn_self = self;
                $finish_notify_expr;
            }
        }

        struct $queue_name {
            lock: $unpoisoned_mutex_ty<()>,
            head: $atomic_ptr_ty<$waiter_name>,
            tail: $atomic_ptr_ty<$waiter_name>,
        }

        impl $queue_name {
            impl_condvar!(@queue_new $is_const, $unpoisoned_mutex_ty, $atomic_ptr_ty, $waiter_name);

            fn lock(&self) -> $guard_name<'_> {
                $guard_name {
                    queue: self,
                    _lock: self.lock.lock(),
                }
            }
        }

        struct $guard_name<'a> {
            queue: &'a $queue_name,
            _lock: $unpoisoned_guard_ty<'a, ()>,
        }

        impl $guard_name<'_> {
            /// 将节点加入队尾。
            ///
            /// # Safety
            ///
            /// 调用者必须持有队列锁；`waiter` 必须是尚未入队的有效栈上节点，
            /// 且其生命周期覆盖从注册到等待返回的整个过程。
            unsafe fn push_back(&self, waiter: *mut $waiter_name) {
                let tail = self.queue.tail.load(Ordering::Relaxed);
                unsafe {
                    (*waiter).prev.store(tail, Ordering::Relaxed);
                    (*waiter).next.store(null_mut(), Ordering::Relaxed);
                }
                if tail.is_null() {
                    self.queue.head.store(waiter, Ordering::Relaxed);
                } else {
                    unsafe { (*tail).next.store(waiter, Ordering::Relaxed) };
                }
                self.queue.tail.store(waiter, Ordering::Relaxed);
            }

            /// 从队首摘取一个节点。
            ///
            /// # Safety
            ///
            /// 调用者必须持有队列锁。返回的节点仍由其所属等待线程保持存活，
            /// 直到通知者完成最终通知。
            unsafe fn pop_front(&self) -> Option<*mut $waiter_name> {
                let head = self.queue.head.load(Ordering::Relaxed);
                if head.is_null() {
                    return None;
                }

                let next = unsafe { (*head).next.load(Ordering::Relaxed) };
                self.queue.head.store(next, Ordering::Relaxed);
                if next.is_null() {
                    self.queue.tail.store(null_mut(), Ordering::Relaxed);
                } else {
                    unsafe { (*next).prev.store(null_mut(), Ordering::Relaxed) };
                }
                unsafe {
                    (*head).prev.store(null_mut(), Ordering::Relaxed);
                    (*head).next.store(null_mut(), Ordering::Relaxed);
                }
                Some(head)
            }

            /// 从队列中摘除一个仍在等待的节点。
            ///
            /// # Safety
            ///
            /// 调用者必须持有队列锁；`waiter` 必须是当前队列中的节点。
            unsafe fn unlink(&self, waiter: *mut $waiter_name) {
                let prev = unsafe { (*waiter).prev.load(Ordering::Relaxed) };
                let next = unsafe { (*waiter).next.load(Ordering::Relaxed) };

                if prev.is_null() {
                    self.queue.head.store(next, Ordering::Relaxed);
                } else {
                    unsafe { (*prev).next.store(next, Ordering::Relaxed) };
                }
                if next.is_null() {
                    self.queue.tail.store(prev, Ordering::Relaxed);
                } else {
                    unsafe { (*next).prev.store(prev, Ordering::Relaxed) };
                }
                unsafe {
                    (*waiter).prev.store(null_mut(), Ordering::Relaxed);
                    (*waiter).next.store(null_mut(), Ordering::Relaxed);
                }
            }

            /// 摘下整条队列，并把其中每个等待者转为 `NOTIFYING`。
            ///
            /// # Safety
            ///
            /// 调用者必须持有队列锁。返回链上的节点在最终通知完成前都由调用者
            /// 持有，其后继指针必须在当前节点的最终通知临界区内保存。
            unsafe fn detach_all_and_mark_notifying(&self) -> *mut $waiter_name {
                let head = self.queue.head.load(Ordering::Relaxed);
                self.queue.head.store(null_mut(), Ordering::Relaxed);
                self.queue.tail.store(null_mut(), Ordering::Relaxed);

                let mut current = head;
                while !current.is_null() {
                    let previous = unsafe {
                        (*current).state.compare_exchange(
                            WAITING,
                            NOTIFYING,
                            Ordering::AcqRel,
                            Ordering::Acquire,
                        )
                    };
                    debug_assert!(previous.is_ok(), "invalid state in condvar queue");
                    current = unsafe { (*current).next.load(Ordering::Relaxed) };
                }
                head
            }

            /// 在队列锁内完成一个节点的最终通知，并返回其后继。
            ///
            /// # Safety
            ///
            /// 调用者必须持有队列锁；`waiter` 必须来自此前由
            /// `detach_all_and_mark_notifying` 返回的链表。
            unsafe fn finish_detached(&self, waiter: *mut $waiter_name) -> *mut $waiter_name {
                let next = unsafe { (*waiter).next.load(Ordering::Relaxed) };
                unsafe { (*waiter).finish_notify() };
                unsafe {
                    (*waiter).prev.store(null_mut(), Ordering::Relaxed);
                    (*waiter).next.store(null_mut(), Ordering::Relaxed);
                }
                next
            }
        }

        $(#[$cvar_meta])*
        pub struct $cvar_name {
            waiters: $queue_name,
        }

        impl fmt::Debug for $cvar_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($cvar_name)).finish_non_exhaustive()
            }
        }

        impl $cvar_name {
            impl_condvar!(@cvar_new $is_const, $queue_name);

            fn register<'a, T>(
                &self,
                waiter: &mut $waiter_name,
                guard: $mutex_guard_ty<'a, T>,
            ) -> &'a $mutex_ty<T> {
                let mutex = $mutex_guard_ty::mutex(&guard);
                {
                    let queue = self.waiters.lock();
                    // SAFETY: `waiter` remains on this stack frame until notification or
                    // cancellation ownership has been completed.
                    unsafe { queue.push_back(waiter) };
                    // Keep the queue lock while releasing the external mutex. This closes
                    // the registration window for callers that update their predicate.
                    drop(guard);
                }
                mutex
            }

            fn register_unpoisoned<'a, T>(
                &self,
                waiter: &mut $waiter_name,
                guard: $unpoisoned_guard_ty<'a, T>,
            ) -> &'a $unpoisoned_mutex_ty<T> {
                let mutex = $unpoisoned_guard_ty::mutex(&guard);
                {
                    let queue = self.waiters.lock();
                    // SAFETY: `waiter` remains on this stack frame until notification or
                    // cancellation ownership has been completed.
                    unsafe { queue.push_back(waiter) };
                    // Keep the queue lock while releasing the external mutex. This closes
                    // the registration window for callers that update their predicate.
                    drop(guard);
                }
                mutex
            }

            /// 阻塞当前线程，直到此条件变量收到通知。
            pub fn wait<'a, T>(
                &self,
                guard: $mutex_guard_ty<'a, T>,
            ) -> LockResult<$mutex_guard_ty<'a, T>> {
                let mut waiter = $waiter_name::new();
                let mutex = self.register(&mut waiter, guard);
                waiter.wait(&self.waiters);
                mutex.lock()
            }

            /// 阻塞当前线程，直到此条件变量收到通知，或达到指定的超时时间。
            pub fn wait_timeout<'a, T>(
                &self,
                guard: $mutex_guard_ty<'a, T>,
                dur: Duration,
            ) -> (LockResult<$mutex_guard_ty<'a, T>>, WaitTimeoutResult) {
                let mut waiter = $waiter_name::new();
                let mutex = self.register(&mut waiter, guard);
                let timed_out = waiter.wait_timeout(&self.waiters, dur);
                (mutex.lock(), WaitTimeoutResult(timed_out))
            }

            /// 唤醒在此条件变量上等待的其中一个线程。
            pub fn notify_one(&self) {
                let selected = {
                    let queue = self.waiters.lock();
                    loop {
                        // SAFETY: the queue lock protects the intrusive links and keeps
                        // the waiting thread from destroying this node.
                        let Some(waiter) = (unsafe { queue.pop_front() }) else {
                            break None;
                        };
                        let result = unsafe {
                            (*waiter).state.compare_exchange(
                                WAITING,
                                NOTIFYING,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                        };
                        if result.is_ok() {
                            break Some(waiter);
                        }
                        debug_assert_eq!(result, Err(TIMED_OUT));
                    }
                };

                if let Some(waiter) = selected {
                    // Reacquiring the queue lock keeps the queue-lock -> channel-lock order
                    // used by the Loom implementation.
                    let queue = self.waiters.lock();
                    unsafe { (*waiter).finish_notify() };
                    drop(queue);
                }
            }

            /// 唤醒在此条件变量上等待的所有线程。
            pub fn notify_all(&self) {
                let mut current = {
                    let queue = self.waiters.lock();
                    // SAFETY: the queue lock protects the detached chain and every node
                    // remains alive until its notification handoff completes.
                    unsafe { queue.detach_all_and_mark_notifying() }
                };

                while !current.is_null() {
                    let queue = self.waiters.lock();
                    // SAFETY: `current` belongs to the detached chain and the queue lock is
                    // held; `finish_detached` saves its successor before final access.
                    current = unsafe { queue.finish_detached(current) };
                    drop(queue);
                }
            }
        }

        impl Default for $cvar_name {
            #[inline]
            fn default() -> Self {
                Self::new()
            }
        }

        $(#[$unp_meta])*
        pub struct $unp_cvar_name {
            inner: $cvar_name,
        }

        impl fmt::Debug for $unp_cvar_name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_struct(stringify!($unp_cvar_name)).finish_non_exhaustive()
            }
        }

        impl $unp_cvar_name {
            impl_condvar!(@unp_new $is_const, $cvar_name);

            /// Blocks until notified, returning the reacquired unpoisoned guard.
            #[inline]
            pub fn wait<'a, T>(
                &self,
                guard: $unpoisoned_guard_ty<'a, T>,
            ) -> $unpoisoned_guard_ty<'a, T> {
                let mut waiter = $waiter_name::new();
                let mutex = self.inner.register_unpoisoned(&mut waiter, guard);
                waiter.wait(&self.inner.waiters);
                mutex.lock()
            }

            /// Blocks until notified or `dur` expires.
            #[inline]
            pub fn wait_timeout<'a, T>(
                &self,
                guard: $unpoisoned_guard_ty<'a, T>,
                dur: Duration,
            ) -> ($unpoisoned_guard_ty<'a, T>, WaitTimeoutResult) {
                let mut waiter = $waiter_name::new();
                let mutex = self.inner.register_unpoisoned(&mut waiter, guard);
                let timed_out = waiter.wait_timeout(&self.inner.waiters, dur);
                (mutex.lock(), WaitTimeoutResult(timed_out))
            }

            /// Wakes one waiting thread.
            #[inline]
            pub fn notify_one(&self) {
                self.inner.notify_one();
            }

            /// Wakes all waiting threads.
            #[inline]
            pub fn notify_all(&self) {
                self.inner.notify_all();
            }
        }

        impl Default for $unp_cvar_name {
            #[inline]
            fn default() -> Self {
                Self::new()
            }
        }
    };

    (@queue_new const, $mutex:ident, $atomic_ptr:ident, $waiter:ident) => {
        const fn new() -> Self {
            Self {
                lock: $mutex::new(()),
                head: <$atomic_ptr<$waiter>>::new(null_mut()),
                tail: <$atomic_ptr<$waiter>>::new(null_mut()),
            }
        }
    };

    (@queue_new non_const, $mutex:ident, $atomic_ptr:ident, $waiter:ident) => {
        #[track_caller]
        fn new() -> Self {
            Self {
                lock: $mutex::new(()),
                head: <$atomic_ptr<$waiter>>::new(null_mut()),
                tail: <$atomic_ptr<$waiter>>::new(null_mut()),
            }
        }
    };

    (@cvar_new const, $queue:ident) => {
        #[inline]
        pub const fn new() -> Self {
            Self {
                waiters: $queue::new(),
            }
        }
    };

    (@cvar_new non_const, $queue:ident) => {
        #[inline]
        #[track_caller]
        pub fn new() -> Self {
            Self {
                waiters: $queue::new(),
            }
        }
    };

    (@unp_new const, $cvar:ident) => {
        #[inline]
        pub const fn new() -> Self {
            Self {
                inner: $cvar::new(),
            }
        }
    };

    (@unp_new non_const, $cvar:ident) => {
        #[inline]
        #[track_caller]
        pub fn new() -> Self {
            Self {
                inner: $cvar::new(),
            }
        }
    };
}

impl_condvar!(
    /// 原生条件变量，始终可用。
    condvar: NativeCondvar,
    /// 与 [`NativeUnpoisonedMutex`] 配对使用的原生条件变量。
    unpoisoned_condvar: NativeUnpoisonedCondvar,
    waiter: NativeWaiter,
    queue: NativeWaitQueue,
    guard: NativeWaitQueueGuard,
    atomic_u32: NativeAtomicU32,
    atomic_ptr: NativeAtomicPtr,
    unpoisoned_mutex: NativeUnpoisonedMutex,
    unpoisoned_guard: NativeUnpoisonedMutexGuard,
    mutex: NativeMutex,
    mutex_guard: NativeMutexGuard,
    wait_once: |s, expected| native::wait_on_address(&s.state, expected),
    wait_timeout_sub: |s, remaining| native::wait_on_address_timeout(&s.state, WAITING, Some(remaining)),
    finish_notify: |s| {
        let previous = s.state.swap(NOTIFIED, Ordering::AcqRel);
        debug_assert_eq!(previous, NOTIFYING);
        native::wake_by_address(&s.state);
    },
    const_new: const
);

#[cfg(feature = "loom")]
impl_condvar!(
    /// Loom 验证专用的条件变量。
    condvar: LoomCondvar,
    /// 与 [`LoomUnpoisonedMutex`] 配对使用的 Loom 条件变量。
    unpoisoned_condvar: LoomUnpoisonedCondvar,
    waiter: LoomWaiter,
    queue: LoomWaitQueue,
    guard: LoomWaitQueueGuard,
    atomic_u32: LoomAtomicU32,
    atomic_ptr: LoomAtomicPtr,
    unpoisoned_mutex: LoomUnpoisonedMutex,
    unpoisoned_guard: LoomUnpoisonedMutexGuard,
    mutex: LoomMutex,
    mutex_guard: LoomMutexGuard,
    waiter_channel: channel: WaitChannel, init_channel: WaitChannel::new(),
    wait_once: |s, expected| s.channel.wait(&s.state, expected),
    wait_timeout_sub: |s, remaining| !s.channel.wait_timeout(&s.state, WAITING, remaining),
    finish_notify: |s| {
        let previous = s.channel.finish_notify(&s.state, NOTIFIED);
        debug_assert_eq!(previous, NOTIFYING);
    },
    const_new: non_const
);

#[cfg(not(feature = "loom"))]
pub type Condvar = NativeCondvar;

#[cfg(not(feature = "loom"))]
pub type UnpoisonedCondvar = NativeUnpoisonedCondvar;

#[cfg(feature = "loom")]
pub type Condvar = LoomCondvar;

#[cfg(feature = "loom")]
pub type UnpoisonedCondvar = LoomUnpoisonedCondvar;

/// 创建可用于静态初始化的原生条件变量。
#[inline]
pub const fn const_native_condvar() -> NativeCondvar {
    NativeCondvar::new()
}

/// 创建可用于静态初始化的原生非中毒条件变量。
#[inline]
pub const fn const_native_unpoisoned_condvar() -> NativeUnpoisonedCondvar {
    NativeUnpoisonedCondvar::new()
}

#[cfg(not(feature = "loom"))]
/// 创建当前配置下的条件变量。
#[inline]
pub const fn const_condvar() -> Condvar {
    const_native_condvar()
}

#[cfg(not(feature = "loom"))]
/// 创建当前配置下的非中毒条件变量。
#[inline]
pub const fn const_unpoisoned_condvar() -> UnpoisonedCondvar {
    const_native_unpoisoned_condvar()
}
