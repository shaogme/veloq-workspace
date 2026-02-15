use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use veloq_runtime::runtime::Runtime;
use veloq_runtime::config::Config;

struct Burn;

impl Future for Burn {
    type Output = ();
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        match veloq_runtime::runtime::coop::poll_proceed(cx) {
            Poll::Ready(_) => Poll::Ready(()),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[test]
fn test_coop_preemption() {
    let config = Config::new().worker_threads(1);

    let runtime = Runtime::builder().config(config).build().unwrap();

    runtime.block_on(async {
        let flag = Arc::new(AtomicBool::new(false));
        let flag_clone = flag.clone();

        let h1 = veloq_runtime::spawn(async move {
            let mut interrupted_at = None;
            for i in 0..1000 {
                Burn.await;
                if flag_clone.load(Ordering::Relaxed) {
                    interrupted_at = Some(i);
                    break;
                }
            }
            interrupted_at
        });

        let flag_clone2 = flag.clone();
        let h2 = veloq_runtime::spawn(async move {
            flag_clone2.store(true, Ordering::Relaxed);
        });

        let res = h1.await;
        h2.await;

        if let Some(i) = res {
            println!("Greedy task interrupted at iteration: {}", i);
            assert!(i >= 127, "Task yielded too early: {}", i);
            assert!(i < 1000, "Task did not yield in time");
        } else {
            panic!("Greedy task was NOT interrupted by polite task. Preemption failed.");
        }
    });
}
