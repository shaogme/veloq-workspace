#[cfg(not(feature = "loom"))]
mod normal_tests {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    };

    use veloq_std::sync::mpsc;
    use veloq_std::thread;
    use veloq_std::time::Duration;
    use veloq_std::vec;

    #[test]
    fn test_mpsc_basic() {
        let (tx, rx) = mpsc::channel();
        tx.send(1).unwrap();
        tx.send(2).unwrap();
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(rx.recv(), Ok(2));
    }

    #[test]
    fn test_sync_channel_basic_and_try_send() {
        let (tx, rx) = mpsc::sync_channel(1);
        tx.send(1).unwrap();
        assert_eq!(tx.try_send(2), Err(mpsc::TrySendError::Full(2)));
        assert_eq!(rx.recv(), Ok(1));
        assert_eq!(tx.try_send(2), Ok(()));
        assert_eq!(rx.recv(), Ok(2));

        drop(rx);
        assert_eq!(tx.try_send(3), Err(mpsc::TrySendError::Disconnected(3)));
    }

    #[test]
    fn test_sync_channel_send_blocks_until_capacity_is_available() {
        let (tx, rx) = mpsc::sync_channel(1);
        tx.send(1).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            tx.send(2).unwrap();
            done_tx.send(()).unwrap();
        })
        .unwrap();

        started_rx.recv().unwrap();
        assert_eq!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert_eq!(rx.recv(), Ok(1));
        done_rx.recv().unwrap();
        assert_eq!(rx.recv(), Ok(2));
        handle.join().unwrap();
    }

    #[test]
    fn test_sync_channel_zero_bound_is_rendezvous() {
        let (tx, rx) = mpsc::sync_channel(0);
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();

        let handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            tx.send(7).unwrap();
            done_tx.send(()).unwrap();
        })
        .unwrap();

        started_rx.recv().unwrap();
        assert_eq!(done_rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        assert_eq!(rx.recv(), Ok(7));
        done_rx.recv().unwrap();
        handle.join().unwrap();
    }

    #[test]
    fn test_sync_channel_zero_bound_try_send_requires_waiting_receiver() {
        let (tx, rx) = mpsc::sync_channel(0);
        assert_eq!(tx.try_send(1), Err(mpsc::TrySendError::Full(1)));

        let (ready_tx, ready_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            ready_tx.send(()).unwrap();
            assert_eq!(rx.recv(), Ok(2));
        })
        .unwrap();

        ready_rx.recv().unwrap();
        let mut value = 2;
        loop {
            match tx.try_send(value) {
                Ok(()) => break,
                Err(mpsc::TrySendError::Full(next)) => {
                    value = next;
                    let _ = thread::yield_now();
                }
                Err(mpsc::TrySendError::Disconnected(_)) => {
                    panic!("receiver disconnected before try_send succeeded")
                }
            }
        }
        handle.join().unwrap();
    }

    #[test]
    fn test_sync_channel_blocked_sender_returns_value_after_receiver_drop() {
        let (tx, rx) = mpsc::sync_channel(1);
        tx.send(1).unwrap();
        let (started_tx, started_rx) = mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).unwrap();
            tx.send(2)
        })
        .unwrap();

        started_rx.recv().unwrap();
        drop(rx);
        match handle.join().unwrap() {
            Err(mpsc::SendError(value)) => assert_eq!(value, 2),
            Ok(()) => panic!("blocked send unexpectedly succeeded"),
        }
    }

    #[test]
    fn test_sync_channel_receiver_drop_drains_queue_without_holding_lock() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::sync_channel(2);

        for _ in 0..2 {
            tx.send(DropProbe(drops.clone())).unwrap();
        }

        drop(rx);
        assert_eq!(drops.load(Ordering::Relaxed), 2);
        drop(tx);
    }

    #[test]
    fn test_mpsc_threads() {
        let (tx, rx) = mpsc::channel();
        let tx1 = tx.clone();

        let t1 = thread::spawn(move || {
            tx1.send(1).unwrap();
        })
        .unwrap();

        let t2 = thread::spawn(move || {
            tx.send(2).unwrap();
        })
        .unwrap();

        t1.join().unwrap();
        t2.join().unwrap();

        let mut vals = vec![rx.recv().unwrap(), rx.recv().unwrap()];
        vals.sort();
        assert_eq!(vals, vec![1, 2]);
    }

    #[test]
    fn test_mpsc_recv_timeout() {
        let (tx, rx) = mpsc::channel();
        assert!(matches!(
            rx.recv_timeout(Duration::from_millis(10)),
            Err(mpsc::RecvTimeoutError::Timeout)
        ));

        tx.send(42).unwrap();
        assert_eq!(rx.recv_timeout(Duration::from_millis(10)), Ok(42));
    }

    #[test]
    fn test_mpsc_try_recv() {
        let (tx, rx) = mpsc::channel();
        assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Empty));
        tx.send(42).unwrap();
        assert_eq!(rx.try_recv(), Ok(42));
        drop(tx);
        assert_eq!(rx.try_recv(), Err(mpsc::TryRecvError::Disconnected));
    }

    #[test]
    fn test_mpsc_send_after_receiver_drop_returns_value() {
        let (tx, rx) = mpsc::channel();
        drop(rx);

        match tx.send(42) {
            Err(mpsc::SendError(value)) => assert_eq!(value, 42),
            Ok(()) => panic!("sending after receiver close unexpectedly succeeded"),
        }
    }

    #[derive(Debug)]
    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_mpsc_receiver_drop_drains_queue() {
        let drops = Arc::new(AtomicUsize::new(0));
        let (tx, rx) = mpsc::channel();

        for _ in 0..3 {
            tx.send(DropProbe(drops.clone())).unwrap();
        }

        drop(rx);
        assert_eq!(drops.load(Ordering::Relaxed), 3);

        drop(tx);
        assert_eq!(drops.load(Ordering::Relaxed), 3);
    }

    #[derive(Debug)]
    struct ReentrantDrop {
        sender: mpsc::Sender<Option<Box<ReentrantDrop>>>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ReentrantDrop {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
            assert!(self.sender.send(None).is_err());
        }
    }

    #[test]
    fn test_mpsc_receiver_drop_drops_messages_without_holding_lifecycle_lock() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::channel::<Option<Box<ReentrantDrop>>>();
        tx.send(Some(Box::new(ReentrantDrop {
            sender: tx.clone(),
            dropped: dropped.clone(),
        })))
        .unwrap();

        drop(rx);
        assert!(dropped.load(Ordering::Relaxed));
    }

    #[derive(Debug)]
    struct ReentrantSyncDrop {
        sender: mpsc::SyncSender<Option<Box<ReentrantSyncDrop>>>,
        dropped: Arc<AtomicBool>,
    }

    impl Drop for ReentrantSyncDrop {
        fn drop(&mut self) {
            self.dropped.store(true, Ordering::Relaxed);
            assert!(self.sender.try_send(None).is_err());
        }
    }

    #[test]
    fn test_sync_channel_receiver_drop_drops_messages_without_holding_lock() {
        let dropped = Arc::new(AtomicBool::new(false));
        let (tx, rx) = mpsc::sync_channel::<Option<Box<ReentrantSyncDrop>>>(1);
        tx.send(Some(Box::new(ReentrantSyncDrop {
            sender: tx.clone(),
            dropped: dropped.clone(),
        })))
        .unwrap();

        drop(rx);
        assert!(dropped.load(Ordering::Relaxed));
    }

    fn assert_send<T: Send>() {}

    #[test]
    fn test_mpsc_receiver_is_send_but_single_consumer() {
        assert_send::<mpsc::Receiver<usize>>();

        let (tx, rx) = mpsc::channel();
        let handle = thread::spawn(move || rx.recv().unwrap()).unwrap();
        tx.send(7).unwrap();
        assert_eq!(handle.join().unwrap(), 7);
    }
}

#[cfg(feature = "loom")]
mod loom_tests {
    use loom::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
    };
    use veloq_std::sync::mpsc;

    #[test]
    fn test_loom_mpsc() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let (tx, rx) = mpsc::channel();
            let tx1 = tx.clone();

            thread::spawn(move || {
                tx1.send(1).unwrap();
            });

            thread::spawn(move || {
                tx.send(2).unwrap();
            });

            let mut vals = Vec::new();
            if let Ok(v1) = rx.recv() {
                vals.push(v1);
            }
            if let Ok(v2) = rx.recv() {
                vals.push(v2);
            }
            vals.sort();
            assert_eq!(vals, vec![1, 2]);
        });
    }

    struct DropProbe(Arc<AtomicUsize>);

    impl Drop for DropProbe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn test_loom_mpsc_send_close_race() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(5);
        builder.check(|| {
            let drops = Arc::new(AtomicUsize::new(0));
            let observed_drops = drops.clone();
            let (tx, rx) = mpsc::channel();
            let send_tx = tx.clone();

            let send_handle = thread::spawn(move || send_tx.send(DropProbe(drops)));
            let close_handle = thread::spawn(move || drop(rx));

            let result = send_handle.join().unwrap();
            close_handle.join().unwrap();

            if let Err(mpsc::SendError(value)) = result {
                drop(value);
            }
            drop(tx);
            assert_eq!(observed_drops.load(Ordering::Relaxed), 1);
        });
    }

    #[test]
    fn test_loom_mpsc_send_after_close_returns_value() {
        loom::model(|| {
            let (tx, rx) = mpsc::channel();
            drop(rx);

            match tx.send(42) {
                Err(mpsc::SendError(value)) => assert_eq!(value, 42),
                Ok(()) => panic!("sending after receiver close unexpectedly succeeded"),
            }
        });
    }
}
