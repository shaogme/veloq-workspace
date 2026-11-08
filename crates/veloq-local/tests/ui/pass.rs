use veloq_local::{Broadcast, Mpmc, Mpsc, Oneshot, Watch};

async fn run() {
    let res = Oneshot::with_borrowed_channel(async |tx, rx| {
        tx.send(42).unwrap();
        rx.await.unwrap()
    })
    .await;
    assert_eq!(res, 42);

    let res = Mpsc::with_borrowed_unbounded(async |tx, rx| {
        tx.send(100).await.unwrap();
        rx.recv().await.unwrap()
    })
    .await;
    assert_eq!(res, 100);

    let res = Mpsc::with_borrowed_bounded(1, async |tx, rx| {
        tx.send(200).await.unwrap();
        rx.recv().await.unwrap()
    })
    .await;
    assert_eq!(res, 200);

    let res = Mpmc::with_borrowed_unbounded(async |tx, rx| {
        tx.send(300).await.unwrap();
        rx.recv().await.unwrap()
    })
    .await;
    assert_eq!(res, 300);

    let res = Mpmc::with_borrowed_bounded(1, async |tx, rx| {
        tx.send(400).await.unwrap();
        rx.recv().await.unwrap()
    })
    .await;
    assert_eq!(res, 400);

    let res = Broadcast::with_borrowed_channel(16, async |tx, mut rx| {
        tx.send(500).unwrap();
        rx.recv().await.unwrap()
    })
    .await;
    assert_eq!(res, 500);

    let res = Watch::with_borrowed_channel(600, async |_tx, rx| {
        *rx.borrow()
    })
    .await;
    assert_eq!(res, 600);
}

fn main() {
    let _ = run();
}
