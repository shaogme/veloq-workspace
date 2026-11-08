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
mod wait_queue;
mod watch;

pub use broadcast::{
    BorrowedReceiver as BroadcastBorrowedReceiver, BorrowedSender as BroadcastBorrowedSender,
    Broadcast, Receiver as BroadcastReceiver, RecvError as BroadcastRecvError,
    Sender as BroadcastSender, TryRecvError as BroadcastTryRecvError,
};
pub use common::{
    RecvError, RecvError as WatchRecvError, SendError, SendError as BroadcastSendError,
    SendError as WatchSendError, TryRecvError, TrySendError,
};
pub use condvar::{Condvar, Wait};
pub use mpmc::{
    ArrayQueue as MpmcArrayQueue, BorrowedBoundedReceiver as MpmcBorrowedBoundedReceiver,
    BorrowedBoundedSender as MpmcBorrowedBoundedSender,
    BorrowedGenericReceiver as MpmcBorrowedGenericReceiver,
    BorrowedGenericSender as MpmcBorrowedGenericSender, BorrowedReceiver as MpmcBorrowedReceiver,
    BorrowedReceiverStream as MpmcBorrowedReceiverStream, BorrowedSender as MpmcBorrowedSender,
    Bounded as MpmcBoundedFlavor, BoundedBorrowedReceiver as MpmcBoundedBorrowedReceiver,
    BoundedBorrowedSender as MpmcBoundedBorrowedSender, BoundedReceiver as MpmcBoundedReceiver,
    BoundedSender as MpmcBoundedSender, ChannelFlavor as MpmcChannelFlavor,
    GenericBorrowedReceiver as MpmcGenericBorrowedReceiver,
    GenericBorrowedSender as MpmcGenericBorrowedSender, GenericReceiver as MpmcGenericReceiver,
    GenericSender as MpmcGenericSender, Mpmc, Queue as MpmcQueue, Receiver as MpmcReceiver,
    ReceiverStream as MpmcReceiverStream, SegQueue as MpmcSegQueue, Sender as MpmcSender,
    Unbounded as MpmcUnboundedFlavor,
};
pub use mpsc::{
    BorrowedBoundedReceiver as MpscBorrowedBoundedReceiver,
    BorrowedBoundedSender as MpscBorrowedBoundedSender,
    BorrowedGenericReceiver as MpscBorrowedGenericReceiver,
    BorrowedGenericSender as MpscBorrowedGenericSender, BorrowedReceiver as MpscBorrowedReceiver,
    BorrowedSender as MpscBorrowedSender, BoundedBorrowedReceiver as MpscBoundedBorrowedReceiver,
    BoundedBorrowedSender as MpscBoundedBorrowedSender, BoundedReceiver as MpscBoundedReceiver,
    BoundedSender as MpscBoundedSender, BoundedStrategy as MpscBoundedStrategy,
    ChannelStrategy as MpscChannelStrategy, GenericBorrowedReceiver as MpscGenericBorrowedReceiver,
    GenericBorrowedSender as MpscGenericBorrowedSender, GenericReceiver as MpscGenericReceiver,
    GenericSender as MpscGenericSender, Mpsc, Receiver as MpscReceiver, Sender as MpscSender,
    UnboundedStrategy as MpscUnboundedStrategy,
};
pub use mutex::{Mutex, MutexGuard};
pub use notify::{Notified, Notify};
pub use oneshot::{
    BorrowedReceiver as OneshotBorrowedReceiver, BorrowedSender as OneshotBorrowedSender, Oneshot,
    Receiver as OneshotReceiver, RecvError as OneshotRecvError, Sender as OneshotSender,
    TryRecvError as OneshotTryRecvError,
};
pub use rwlock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
pub use semaphore::{
    AcquireError, AddPermitsError, CapacityError, Semaphore, SemaphoreAcquireFuture,
    SemaphorePermit, TryAcquireError,
};
pub use set_once::{SetOnce, SetOnceError, SetOnceWait};
pub use watch::{
    BorrowedReceiver as WatchBorrowedReceiver, BorrowedSender as WatchBorrowedSender,
    Receiver as WatchReceiver, Ref as WatchRef, Sender as WatchSender, Watch,
};
