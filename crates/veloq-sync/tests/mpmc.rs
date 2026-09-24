#![cfg(not(feature = "loom"))]

use std::{future::poll_fn, pin::pin, task::Poll};

use futures_core::Stream;
use tokio::time::{Duration, timeout};
use veloq_sync::mpmc;

async fn next<S>(mut stream: std::pin::Pin<&mut S>) -> Option<S::Item>
where
    S: Stream + ?Sized,
{
    poll_fn(|cx| stream.as_mut().poll_next(cx)).await
}

#[tokio::test]
async fn receives_message_sent_after_pending() {
    let (tx, rx) = mpmc::unbounded();
    let task = tokio::spawn(async move { rx.recv().await });

    tokio::task::yield_now().await;
    tx.send(7).await.unwrap();

    assert_eq!(
        timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Ok(7)
    );
}

#[tokio::test]
async fn receives_message_sent_before_poll() {
    let (tx, rx) = mpmc::unbounded();
    tx.send(11).await.unwrap();
    assert_eq!(rx.recv().await, Ok(11));
}

#[tokio::test]
async fn borrowed_and_owned_streams_share_receive_protocol() {
    mpmc::with_borrowed_unbounded(async |tx, rx| {
        let mut borrowed = pin!(rx.stream());
        let pending = poll_fn(|cx| match borrowed.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Ready(()),
            Poll::Ready(_) => panic!("stream unexpectedly received a value"),
        });
        pending.await;
        tx.send(13).await.unwrap();
        assert_eq!(next(borrowed.as_mut()).await, Some(13));
    })
    .await;

    let (tx, rx) = mpmc::unbounded();
    let mut owned = pin!(rx.stream());
    let pending = poll_fn(|cx| match owned.as_mut().poll_next(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("stream unexpectedly received a value"),
    });
    pending.await;
    tx.send(17).await.unwrap();
    assert_eq!(next(owned.as_mut()).await, Some(17));
}

#[tokio::test]
async fn closed_channel_drains_messages_before_disconnect() {
    let (tx, rx) = mpmc::unbounded();
    tx.send(19).await.unwrap();
    drop(tx);

    assert_eq!(rx.recv().await, Ok(19));
    assert_eq!(rx.recv().await, Err(veloq_sync::TryRecvError::Disconnected));
}
