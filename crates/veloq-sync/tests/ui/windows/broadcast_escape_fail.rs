//! BorrowedSender and BorrowedReceiver cannot escape with_borrowed_channel closure.

use veloq_sync::broadcast;

async fn test_tx_escape() {
    let _escaped = broadcast::with_borrowed_channel::<i32, _, _>(16, async |tx, _rx| tx).await;
}

async fn test_rx_escape() {
    let _escaped = broadcast::with_borrowed_channel::<i32, _, _>(16, async |_tx, rx| rx).await;
}

fn main() {}
