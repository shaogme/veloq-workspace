#![no_std]

mod broadcast;
mod common;
mod condvar;
mod mpmc;
mod mpsc;
mod mutex;
mod notify;
mod oneshot;
mod rwlock;
mod semaphore;
mod set_once;
mod spsc;
mod wait_queue;
mod watch;

pub use broadcast::{
    BorrowedReceiver as BroadcastBorrowedReceiver, BorrowedSender as BroadcastBorrowedSender,
    Broadcast, Receiver as BroadcastReceiver, RecvError as BroadcastRecvError,
    SendError as BroadcastSendError, Sender as BroadcastSender,
    TryRecvError as BroadcastTryRecvError,
};
pub use common::{ChannelCapacity, SendError, TryRecvError};
pub use condvar::{Condvar, Wait};
pub use mpmc::{
    BorrowedChannelStream as MpmcBorrowedChannelStream, BorrowedReceiver as MpmcBorrowedReceiver,
    BorrowedSender as MpmcBorrowedSender, ChannelStream as MpmcChannelStream, Mpmc,
    Receiver as MpmcReceiver, Sender as MpmcSender,
};
pub use mpsc::{
    BorrowedChannelStream as MpscBorrowedChannelStream, BorrowedReceiver as MpscBorrowedReceiver,
    BorrowedSender as MpscBorrowedSender, ChannelStream as MpscChannelStream, Mpsc,
    Receiver as MpscReceiver, Sender as MpscSender,
};
pub use mutex::{Mutex, MutexGuard};
pub use notify::{Notified, Notify};
pub use oneshot::{
    BorrowedReceiver as OneshotBorrowedReceiver, BorrowedSender as OneshotBorrowedSender, Oneshot,
    Receiver as OneshotReceiver, RecvError as OneshotRecvError, Sender as OneshotSender,
};
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use semaphore::{
    AcquireError, AddPermitsError, CapacityError, Semaphore, SemaphoreAcquireFuture,
    SemaphorePermit, TryAcquireError,
};
pub use set_once::{SetOnce, SetOnceError, SetOnceWait};
pub use spsc::{
    ChannelStream as SpscChannelStream, OwnedChannelStream as OwnedSpscChannelStream,
    OwnedReceiver as OwnedSpscReceiver, OwnedSender as OwnedSpscSender, Receiver as SpscReceiver,
    RecvFuture as SpscRecvFuture, SendFuture as SpscSendFuture, Sender as SpscSender, Spsc,
};
pub use watch::{
    BorrowedReceiver as WatchBorrowedReceiver, BorrowedSender as WatchBorrowedSender,
    Receiver as WatchReceiver, RecvError as WatchRecvError, Ref as WatchRef,
    SendError as WatchSendError, Sender as WatchSender, Watch,
};

pub(crate) mod waker {
    pub(crate) use crate::common::{WaiterAdapter, WaiterNode};
}
