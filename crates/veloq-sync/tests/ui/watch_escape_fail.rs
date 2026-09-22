//! BorrowedSender and BorrowedReceiver cannot escape with_borrowed_channel closure.

use veloq_sync::watch;

async fn test_tx_escape() {
    let _escaped = watch::with_borrowed_channel(42, async |tx, _rx| tx).await;
}

async fn test_rx_escape() {
    let _escaped = watch::with_borrowed_channel(42, async |_tx, rx| rx).await;
}

fn main() {}
