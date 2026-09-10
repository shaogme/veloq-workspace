#[cfg(not(feature = "loom"))]
mod normal_tests {
    use std::sync::mpsc::channel;

    use veloq_std::{
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        thread,
    };

    #[test]
    fn barrier_waits_for_all_threads_and_selects_one_leader() {
        const PARTICIPANTS: usize = 4;

        let barrier = Arc::new(Barrier::new(PARTICIPANTS));
        let passed = Arc::new(AtomicUsize::new(0));
        let (ready_tx, ready_rx) = channel();
        let mut handles = Vec::new();

        for _ in 0..PARTICIPANTS - 1 {
            let barrier = barrier.clone();
            let passed = passed.clone();
            let ready_tx = ready_tx.clone();
            handles.push(
                thread::spawn(move || {
                    ready_tx.send(()).unwrap();
                    let result = barrier.wait();
                    passed.fetch_add(1, Ordering::Release);
                    result.is_leader()
                })
                .unwrap(),
            );
        }

        for _ in 0..PARTICIPANTS - 1 {
            ready_rx.recv().unwrap();
        }
        assert_eq!(passed.load(Ordering::Acquire), 0);

        let barrier_for_last = barrier.clone();
        let passed_for_last = passed.clone();
        handles.push(
            thread::spawn(move || {
                let result = barrier_for_last.wait();
                passed_for_last.fetch_add(1, Ordering::Release);
                result.is_leader()
            })
            .unwrap(),
        );

        let leaders = handles
            .into_iter()
            .map(|handle| handle.join().unwrap())
            .filter(|is_leader| *is_leader)
            .count();

        assert_eq!(passed.load(Ordering::Acquire), PARTICIPANTS);
        assert_eq!(leaders, 1);
    }

    #[test]
    fn barrier_is_reusable_across_generations() {
        const PARTICIPANTS: usize = 4;
        const GENERATIONS: usize = 3;

        let barrier = Arc::new(Barrier::new(PARTICIPANTS));
        let passed = Arc::new([
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        ]);
        let leaders = Arc::new([
            AtomicUsize::new(0),
            AtomicUsize::new(0),
            AtomicUsize::new(0),
        ]);
        let mut handles = Vec::new();

        for _ in 0..PARTICIPANTS {
            let barrier = barrier.clone();
            let passed = passed.clone();
            let leaders = leaders.clone();
            handles.push(
                thread::spawn(move || {
                    for generation in 0..GENERATIONS {
                        let result = barrier.wait();
                        passed[generation].fetch_add(1, Ordering::Release);
                        if result.is_leader() {
                            leaders[generation].fetch_add(1, Ordering::Release);
                        }
                    }
                })
                .unwrap(),
            );
        }

        for handle in handles {
            handle.join().unwrap();
        }

        for generation in 0..GENERATIONS {
            assert_eq!(passed[generation].load(Ordering::Acquire), PARTICIPANTS);
            assert_eq!(leaders[generation].load(Ordering::Acquire), 1);
        }
    }

    #[test]
    fn single_thread_barrier_returns_immediately_and_is_reusable() {
        let barrier = Barrier::new(1);

        assert!(barrier.wait().is_leader());
        assert!(barrier.wait().is_leader());
    }

    #[test]
    fn zero_thread_barrier_returns_immediately() {
        let barrier = Barrier::new(0);

        assert!(barrier.wait().is_leader());
        assert!(barrier.wait().is_leader());
    }

    #[test]
    fn barrier_wait_result_and_barrier_are_debuggable() {
        let barrier = Barrier::new(1);
        let result = barrier.wait();

        assert!(format!("{barrier:?}").contains("Barrier"));
        assert!(format!("{result:?}").contains("BarrierWaitResult"));
        assert!(format!("{result:?}").contains("is_leader: true"));
    }

    #[test]
    fn barrier_state_is_not_poisoned_by_a_panicking_participant() {
        let barrier = Arc::new(Barrier::new(2));
        let barrier_for_worker = barrier.clone();
        let worker = thread::spawn(move || {
            barrier_for_worker.wait();
            panic!("participant failed after crossing the barrier");
        })
        .unwrap();

        barrier.wait();
        assert!(worker.join().is_err());

        let barrier_for_next_round = barrier.clone();
        let next_round = thread::spawn(move || barrier_for_next_round.wait()).unwrap();
        barrier.wait();
        next_round.join().unwrap();
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
    use veloq_std::sync::Barrier;

    #[test]
    fn barrier_wait_is_race_free_and_selects_one_leader() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let barrier = Arc::new(Barrier::new(2));
            let leaders = Arc::new(AtomicUsize::new(0));
            let barrier_for_thread = barrier.clone();
            let leaders_for_thread = leaders.clone();

            let handle = thread::spawn(move || {
                if barrier_for_thread.wait().is_leader() {
                    leaders_for_thread.fetch_add(1, Ordering::Release);
                }
            });

            if barrier.wait().is_leader() {
                leaders.fetch_add(1, Ordering::Release);
            }

            handle.join().unwrap();
            assert_eq!(leaders.load(Ordering::Acquire), 1);
        });
    }

    #[test]
    fn barrier_is_reusable_across_generations() {
        let mut builder = loom::model::Builder::new();
        builder.preemption_bound = Some(2);
        builder.check(|| {
            let barrier = Arc::new(Barrier::new(2));
            let leaders = Arc::new(AtomicUsize::new(0));
            let barrier_for_thread = barrier.clone();
            let leaders_for_thread = leaders.clone();

            let handle = thread::spawn(move || {
                for _ in 0..2 {
                    if barrier_for_thread.wait().is_leader() {
                        leaders_for_thread.fetch_add(1, Ordering::Release);
                    }
                }
            });

            for _ in 0..2 {
                if barrier.wait().is_leader() {
                    leaders.fetch_add(1, Ordering::Release);
                }
            }

            handle.join().unwrap();
            assert_eq!(leaders.load(Ordering::Acquire), 2);
        });
    }
}
