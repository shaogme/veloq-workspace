//! BorrowedSender and BorrowedReceiver cannot escape with_borrowed_unbounded/bounded closures.

use veloq_sync::mpmc;

async fn test_unbounded_tx_escape() {
    let _escaped = mpmc::with_borrowed_unbounded::<i32, _, _>(async |tx, _rx| tx).await;
}

async fn test_unbounded_rx_escape() {
    let _escaped = mpmc::with_borrowed_unbounded::<i32, _, _>(async |_tx, rx| rx).await;
}

async fn test_bounded_tx_escape() {
    let _escaped = mpmc::with_borrowed_bounded::<i32, _, _>(1, async |tx, _rx| tx).await;
}

async fn test_bounded_rx_escape() {
    let _escaped = mpmc::with_borrowed_bounded::<i32, _, _>(1, async |_tx, rx| rx).await;
}

fn main() {}
