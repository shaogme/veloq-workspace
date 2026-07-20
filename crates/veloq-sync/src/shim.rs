pub mod queue {
    pub trait Queue<T>: Send + Sync {
        fn new(capacity: usize) -> Self;
        fn push(&self, value: T) -> Result<(), T>;
        fn pop(&self) -> Option<T>;
        fn is_full(&self) -> bool;
    }

    #[cfg(not(feature = "loom"))]
    pub use crossbeam_queue::{ArrayQueue, SegQueue};

    #[cfg(not(feature = "loom"))]
    impl<T: Send> Queue<T> for SegQueue<T> {
        fn new(_capacity: usize) -> Self {
            Self::new()
        }

        fn push(&self, value: T) -> Result<(), T> {
            self.push(value);
            Ok(())
        }

        fn pop(&self) -> Option<T> {
            self.pop()
        }

        fn is_full(&self) -> bool {
            false
        }
    }

    #[cfg(not(feature = "loom"))]
    impl<T: Send> Queue<T> for ArrayQueue<T> {
        fn new(capacity: usize) -> Self {
            Self::new(capacity)
        }

        fn push(&self, value: T) -> Result<(), T> {
            self.push(value)
        }

        fn pop(&self) -> Option<T> {
            self.pop()
        }

        fn is_full(&self) -> bool {
            self.is_full()
        }
    }

    #[cfg(feature = "loom")]
    pub use self::loom_queues::{ArrayQueue, SegQueue};

    #[cfg(feature = "loom")]
    mod loom_queues {
        use super::Queue;
        use loom::sync::Mutex;
        use veloq_std::collections::VecDeque;

        pub struct SegQueue<T> {
            inner: Mutex<VecDeque<T>>,
        }

        impl<T> SegQueue<T> {
            pub fn new() -> Self {
                Self {
                    inner: Mutex::new(VecDeque::new()),
                }
            }

            pub fn push(&self, t: T) {
                self.inner.lock().unwrap().push_back(t);
            }

            pub fn pop(&self) -> Option<T> {
                self.inner.lock().unwrap().pop_front()
            }

            pub fn is_empty(&self) -> bool {
                self.inner.lock().unwrap().is_empty()
            }
        }

        impl<T: Send> Queue<T> for SegQueue<T> {
            fn new(_capacity: usize) -> Self {
                Self::new()
            }

            fn push(&self, value: T) -> Result<(), T> {
                self.push(value);
                Ok(())
            }

            fn pop(&self) -> Option<T> {
                self.pop()
            }

            fn is_full(&self) -> bool {
                false
            }
        }

        pub struct ArrayQueue<T> {
            inner: Mutex<VecDeque<T>>,
            cap: usize,
        }

        impl<T> ArrayQueue<T> {
            pub fn new(cap: usize) -> Self {
                Self {
                    inner: Mutex::new(VecDeque::new()),
                    cap,
                }
            }

            pub fn push(&self, t: T) -> Result<(), T> {
                let mut lock = self.inner.lock().unwrap();
                if lock.len() >= self.cap {
                    return Err(t);
                }
                lock.push_back(t);
                Ok(())
            }

            pub fn pop(&self) -> Option<T> {
                self.inner.lock().unwrap().pop_front()
            }

            pub fn is_full(&self) -> bool {
                self.inner.lock().unwrap().len() >= self.cap
            }
        }

        impl<T: Send> Queue<T> for ArrayQueue<T> {
            fn new(capacity: usize) -> Self {
                Self::new(capacity)
            }

            fn push(&self, value: T) -> Result<(), T> {
                self.push(value)
            }

            fn pop(&self) -> Option<T> {
                self.pop()
            }

            fn is_full(&self) -> bool {
                self.is_full()
            }
        }
    }
}
