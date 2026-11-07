//! BorrowedSender and BorrowedReceiver cannot escape with_borrowed_channel closure.

use veloq_local::oneshot;

async fn test_tx_escape() {
    let _escaped = oneshot::with_borrowed_channel::<i32, _, _>(async |tx, _rx| tx).await;
}

async fn test_rx_escape() {
    let _escaped = oneshot::with_borrowed_channel::<i32, _, _>(async |_tx, rx| rx).await;
}

fn main() {}
