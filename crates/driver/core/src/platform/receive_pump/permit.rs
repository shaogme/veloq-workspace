use super::config::ReceivePumpError;

use veloq_std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// 非阻塞地通知 owner worker 有 packet permit 已释放。
///
/// 通知器只负责唤醒 owner worker，不得在 packet drop 路径中直接访问 driver、slot 或
/// provided-buffer group。backend 可以据此合并多个通知，并在自己的 worker 上扫描并恢复
/// pending datagram。
pub trait ReceivePermitNotifier: Send + Sync {
    fn notify(&self);

    /// Bind the backend identity used to resume this pump. / 绑定恢复此接收泵所需的后端标识。
    fn bind_resume_token(&self, _token: u64) {}
}

struct PermitState {
    capacity: usize,
    available: AtomicUsize,
    notifier: Option<Arc<dyn ReceivePermitNotifier>>,
}

/// A permit held by one packet or one in-progress delivery.
pub struct ReceivePermit {
    pool: Option<Arc<PermitState>>,
}

impl<F> ReceivePermitNotifier for F
where
    F: Fn() + Send + Sync,
{
    fn notify(&self) {
        self();
    }
}

/// A bounded permit pool shared by the pump and every packet it publishes.
#[derive(Clone)]
pub struct ReceivePermitPool {
    inner: Arc<PermitState>,
}

impl ReceivePermitPool {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self::new_with_notifier(capacity, None)
    }

    pub fn new_with_notifier(
        capacity: NonZeroUsize,
        notifier: Option<Arc<dyn ReceivePermitNotifier>>,
    ) -> Self {
        Self {
            inner: Arc::new(PermitState {
                capacity: capacity.get(),
                available: AtomicUsize::new(capacity.get()),
                notifier,
            }),
        }
    }

    pub(super) fn set_notifier(
        &mut self,
        notifier: Arc<dyn ReceivePermitNotifier>,
    ) -> Result<(), ReceivePumpError> {
        if self.available() != self.capacity() {
            return Err(ReceivePumpError::InvalidTransition);
        }
        let capacity = self.capacity();
        self.inner = Arc::new(PermitState {
            capacity,
            available: AtomicUsize::new(capacity),
            notifier: Some(notifier),
        });
        Ok(())
    }

    pub fn capacity(&self) -> usize {
        self.inner.capacity
    }

    pub fn available(&self) -> usize {
        self.inner.available.load(Ordering::Acquire)
    }

    pub fn in_use(&self) -> usize {
        self.capacity() - self.available()
    }

    pub(super) fn acquire(&self) -> Option<ReceivePermit> {
        let mut available = self.available();
        loop {
            if available == 0 {
                return None;
            }
            match self.inner.available.compare_exchange(
                available,
                available - 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Some(ReceivePermit {
                        pool: Some(self.inner.clone()),
                    });
                }
                Err(observed) => available = observed,
            }
        }
    }

    pub(super) fn bind_resume_token(&self, token: u64) {
        if let Some(notifier) = &self.inner.notifier {
            notifier.bind_resume_token(token);
        }
    }
}

impl ReceivePermit {
    pub(crate) fn detached() -> Self {
        Self { pool: None }
    }

    pub fn release(self) {
        drop(self);
    }
}

impl Drop for ReceivePermit {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            pool.available.fetch_add(1, Ordering::Release);
            if let Some(notifier) = &pool.notifier {
                notifier.notify();
            }
        }
    }
}
