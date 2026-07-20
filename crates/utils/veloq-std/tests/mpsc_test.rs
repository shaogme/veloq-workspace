#[cfg(not(feature = "loom"))]
mod normal_tests {
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
}

#[cfg(feature = "loom")]
mod loom_tests {
    use loom::thread;
    use veloq_std::sync::mpsc;

    #[test]
    fn test_loom_mpsc() {
        loom::model(|| {
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
}
