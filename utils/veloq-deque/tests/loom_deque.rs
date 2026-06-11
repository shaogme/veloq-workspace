#![cfg(feature = "loom")]

use loom::sync::Arc;
use loom::thread;
use std::num::NonZeroUsize;
use veloq_deque::{Deque, Steal};

#[test]
fn test_spsc() {
    loom::model(|| {
        let q = Arc::new(Deque::new(NonZeroUsize::new(4).unwrap()));
        let q_clone = q.clone();

        let t1 = thread::spawn(move || {
            q_clone.push(1).unwrap();
            q_clone.push(2).unwrap();
        });

        let t2 = thread::spawn(move || {
            let mut stolen = Vec::new();
            while stolen.len() < 2 {
                match q.steal() {
                    Steal::Success(x) => stolen.push(x),
                    Steal::Retry => thread::yield_now(),
                    Steal::Empty => thread::yield_now(),
                }
            }
            assert!(stolen.contains(&1));
            assert!(stolen.contains(&2));
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}

#[test]
fn test_multi_stealers() {
    loom::model(|| {
        let q = Arc::new(Deque::new(NonZeroUsize::new(8).unwrap()));
        q.push(1).unwrap();
        q.push(2).unwrap();
        q.push(3).unwrap();
        q.push(4).unwrap();

        let results = Arc::new(loom::sync::Mutex::new(Vec::new()));

        let q1 = q.clone();
        let results1 = results.clone();
        let t1 = thread::spawn(move || {
            let dest = Deque::new(NonZeroUsize::new(4).unwrap());
            let mut local_results = Vec::new();
            loop {
                match q1.steal_batch(&dest) {
                    Steal::Success(res) => {
                        local_results.push(res.item);
                        for item in res.overflow {
                            local_results.push(item);
                        }
                        break;
                    }
                    Steal::Retry => thread::yield_now(),
                    Steal::Empty => break,
                }
            }
            while let Some(item) = dest.pop() {
                local_results.push(item);
            }
            results1.lock().unwrap().extend(local_results);
        });

        let q2 = q.clone();
        let results2 = results.clone();
        let t2 = thread::spawn(move || {
            let dest = Deque::new(NonZeroUsize::new(4).unwrap());
            let mut local_results = Vec::new();
            loop {
                match q2.steal_batch(&dest) {
                    Steal::Success(res) => {
                        local_results.push(res.item);
                        for item in res.overflow {
                            local_results.push(item);
                        }
                        break;
                    }
                    Steal::Retry => thread::yield_now(),
                    Steal::Empty => break,
                }
            }
            while let Some(item) = dest.pop() {
                local_results.push(item);
            }
            results2.lock().unwrap().extend(local_results);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // Also the main thread pops any remaining from the original q
        let mut main_results = Vec::new();
        while let Some(item) = q.pop() {
            main_results.push(item);
        }
        results.lock().unwrap().extend(main_results);

        let mut final_results = results.lock().unwrap().clone();
        final_results.sort();
        assert_eq!(final_results, vec![1, 2, 3, 4]);
    });
}

#[test]
fn test_wrap_around() {
    loom::model(|| {
        let q = Deque::new(NonZeroUsize::new(4).unwrap());
        // Trigger wrap-around by pushing/popping
        for i in 0..10 {
            q.push(i).unwrap();
            assert_eq!(q.pop(), Some(i));
        }
        // Now push new elements
        q.push(10).unwrap();
        q.push(11).unwrap();

        let dest = Deque::new(NonZeroUsize::new(4).unwrap());
        match q.steal_batch(&dest) {
            Steal::Success(res) => {
                let mut items = vec![res.item];
                items.extend(res.overflow);
                while let Some(item) = dest.pop() {
                    items.push(item);
                }
                while let Some(item) = q.pop() {
                    items.push(item);
                }
                items.sort();
                assert_eq!(items, vec![10, 11]);
            }
            _ => panic!("Expected successful steal"),
        }
    });
}

#[test]
fn test_steal_overflow() {
    loom::model(|| {
        let q = Deque::new(NonZeroUsize::new(8).unwrap());
        for i in 1..=6 {
            q.push(i).unwrap();
        }

        let dest = Deque::new(NonZeroUsize::new(2).unwrap());
        dest.push(100).unwrap();
        dest.push(101).unwrap();

        match q.steal_batch(&dest) {
            Steal::Success(res) => {
                assert_eq!(res.item, 1);
                assert_eq!(res.overflow, vec![2, 3]);

                assert_eq!(dest.pop(), Some(101));
                assert_eq!(dest.pop(), Some(100));

                let mut q_items = Vec::new();
                while let Some(item) = q.pop() {
                    q_items.push(item);
                }
                assert_eq!(q_items, vec![6, 5, 4]);
            }
            _ => panic!("Expected successful steal"),
        }
    });
}

#[test]
fn test_steal_integrity() {
    loom::model(|| {
        let q = Deque::new(NonZeroUsize::new(8).unwrap());
        for i in 1..=5 {
            q.push(i).unwrap();
        }

        let dest = Deque::new(NonZeroUsize::new(4).unwrap());
        match q.steal_batch(&dest) {
            Steal::Success(res) => {
                let mut collected = vec![res.item];
                collected.extend(res.overflow);
                while let Some(item) = dest.pop() {
                    collected.push(item);
                }
                while let Some(item) = q.pop() {
                    collected.push(item);
                }
                collected.sort();
                assert_eq!(collected, vec![1, 2, 3, 4, 5]);
            }
            _ => panic!("Expected successful steal"),
        }
    });
}
