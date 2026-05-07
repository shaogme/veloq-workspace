use std::cell::RefCell;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use veloq_atomic_waker::AtomicWaker;
use veloq_driver::driver::{Driver, PlatformDriver};
use veloq_driver::op::{
    Accept, DetachedOp, Op, Recv, Send as OpSend, SendTo, UdpConnect, UdpRecv, UdpRecvStream,
    UdpSend,
};

use crate::runtime::context as runtime_context;

type DriverDetachedOp<T> =
    DetachedOp<T, <PlatformDriver as Driver>::Op, <PlatformDriver as Driver>::Completion>;

pub(crate) struct RouteCell<T> {
    value: Mutex<Option<T>>,
    waker: AtomicWaker,
}

impl<T> RouteCell<T> {
    pub(crate) fn new() -> Arc<Self> {
        Arc::new(Self {
            value: Mutex::new(None),
            waker: AtomicWaker::new(),
        })
    }

    pub(crate) fn set(&self, value: T) {
        let mut slot = self.value.lock().expect("socket route slot poisoned");
        debug_assert!(slot.is_none(), "socket route slot already populated");
        *slot = Some(value);
        self.waker.wake();
    }

    pub(crate) fn take(&self) -> Option<T> {
        self.value
            .lock()
            .expect("socket route slot poisoned")
            .take()
    }

    pub(crate) fn register(&self, waker: &Waker) {
        self.waker.register(waker);
    }
}

pub(crate) struct RoutedDetachedOp<T>
where
    T: veloq_driver::op::IntoPlatformOp<
            <PlatformDriver as Driver>::Op,
            DriverCompletion = <PlatformDriver as Driver>::Completion,
        > + Send
        + 'static,
{
    slot: Arc<RouteCell<DriverDetachedOp<T>>>,
    inner: Option<DriverDetachedOp<T>>,
}

impl<T> RoutedDetachedOp<T>
where
    T: veloq_driver::op::IntoPlatformOp<
            <PlatformDriver as Driver>::Op,
            DriverCompletion = <PlatformDriver as Driver>::Completion,
        > + Send
        + 'static,
{
    fn new(slot: Arc<RouteCell<DriverDetachedOp<T>>>) -> Self {
        Self { slot, inner: None }
    }
}

impl<T> Future for RoutedDetachedOp<T>
where
    T: veloq_driver::op::IntoPlatformOp<
            <PlatformDriver as Driver>::Op,
            DriverCompletion = <PlatformDriver as Driver>::Completion,
        > + Send
        + 'static,
{
    type Output = veloq_driver::op::OpResult<T, T::Completion>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = unsafe { self.get_unchecked_mut() };

        if this.inner.is_none() {
            if let Some(op) = this.slot.take() {
                this.inner = Some(op);
            } else {
                this.slot.register(cx.waker());
                if let Some(op) = this.slot.take() {
                    this.inner = Some(op);
                } else {
                    return Poll::Pending;
                }
            }
        }

        let inner = this.inner.as_mut().expect("route future missing inner op");
        unsafe { Pin::new_unchecked(inner) }.poll(cx)
    }
}

pub(crate) enum SocketRouteCommand {
    TcpAccept {
        op: Accept,
        slot: Arc<RouteCell<DriverDetachedOp<Accept>>>,
    },
    TcpSend {
        op: OpSend,
        slot: Arc<RouteCell<DriverDetachedOp<OpSend>>>,
    },
    TcpRecv {
        op: Recv,
        slot: Arc<RouteCell<DriverDetachedOp<Recv>>>,
    },
    UdpSendTo {
        op: SendTo,
        slot: Arc<RouteCell<DriverDetachedOp<SendTo>>>,
    },
    UdpRecvStream {
        op: UdpRecvStream,
        slot: Arc<RouteCell<DriverDetachedOp<UdpRecvStream>>>,
    },
    UdpConnect {
        op: UdpConnect,
        slot: Arc<RouteCell<DriverDetachedOp<UdpConnect>>>,
    },
    UdpSend {
        op: UdpSend,
        slot: Arc<RouteCell<DriverDetachedOp<UdpSend>>>,
    },
    UdpRecv {
        op: UdpRecv,
        slot: Arc<RouteCell<DriverDetachedOp<UdpRecv>>>,
    },
}

impl SocketRouteCommand {
    fn execute(self) {
        let Some(ctx) = runtime_context::try_current() else {
            return;
        };

        let driver_rc = ctx.driver();
        let mut driver = driver_rc.borrow_mut();

        match self {
            Self::TcpAccept { op, slot } => {
                slot.set(Op::new(op).submit_detached(&mut *driver));
            }
            Self::TcpSend { op, slot } => {
                slot.set(Op::new(op).submit_detached(&mut *driver));
            }
            Self::TcpRecv { op, slot } => {
                slot.set(Op::new(op).submit_detached(&mut *driver));
            }
            Self::UdpSendTo { op, slot } => {
                slot.set(Op::new(op).submit_detached(&mut *driver));
            }
            Self::UdpRecvStream { op, slot } => {
                slot.set(Op::new(op).submit_detached(&mut *driver));
            }
            Self::UdpConnect { op, slot } => {
                slot.set(Op::new(op).submit_detached(&mut *driver));
            }
            Self::UdpSend { op, slot } => {
                slot.set(Op::new(op).submit_detached(&mut *driver));
            }
            Self::UdpRecv { op, slot } => {
                slot.set(Op::new(op).submit_detached(&mut *driver));
            }
        }
    }
}

#[derive(Clone)]
pub(crate) struct SocketRouteDispatcher {
    senders: Arc<Vec<mpsc::Sender<SocketRouteCommand>>>,
}

impl SocketRouteDispatcher {
    pub(crate) fn new(senders: Vec<mpsc::Sender<SocketRouteCommand>>) -> Self {
        Self {
            senders: Arc::new(senders),
        }
    }

    pub(crate) fn dispatch(&self, worker_id: usize, command: SocketRouteCommand) -> bool {
        let Some(sender) = self.senders.get(worker_id) else {
            return false;
        };
        if sender.send(command).is_err() {
            return false;
        }
        if runtime_context::try_current().is_some() {
            veloq_runtime::runtime::wake_worker(worker_id);
        }
        true
    }
}

struct WorkerSocketRouteState {
    receiver: mpsc::Receiver<SocketRouteCommand>,
}

thread_local! {
    static SOCKET_ROUTE_STATE: RefCell<Option<WorkerSocketRouteState>> = const { RefCell::new(None) };
}

pub(crate) fn init_worker_socket_route_state(receiver: mpsc::Receiver<SocketRouteCommand>) {
    SOCKET_ROUTE_STATE.with(|state| {
        *state.borrow_mut() = Some(WorkerSocketRouteState { receiver });
    });
}

pub(crate) fn drain_pending_socket_route_commands() {
    SOCKET_ROUTE_STATE.with(|state_cell| {
        let mut state_opt = state_cell.borrow_mut();
        let Some(state) = state_opt.as_mut() else {
            return;
        };

        let mut pending = Vec::new();
        while let Ok(command) = state.receiver.try_recv() {
            pending.push(command);
        }

        for command in pending {
            command.execute();
        }
    });
}

fn route_command<T>(
    worker_id: usize,
    slot: Arc<RouteCell<DriverDetachedOp<T>>>,
    command: SocketRouteCommand,
) -> io::Result<RoutedDetachedOp<T>>
where
    T: veloq_driver::op::IntoPlatformOp<
            <PlatformDriver as Driver>::Op,
            DriverCompletion = <PlatformDriver as Driver>::Completion,
        > + Send
        + 'static,
{
    let Some(ctx) = runtime_context::try_current() else {
        return Err(io::Error::other("runtime context not set"));
    };

    if !ctx.socket_route_dispatcher().dispatch(worker_id, command) {
        return Err(io::Error::other("failed to dispatch socket route command"));
    }

    Ok(RoutedDetachedOp::new(slot))
}

pub(crate) fn route_tcp_accept(
    worker_id: usize,
    op: Accept,
) -> io::Result<RoutedDetachedOp<Accept>> {
    let slot = RouteCell::new();
    route_command(
        worker_id,
        slot.clone(),
        SocketRouteCommand::TcpAccept { op, slot },
    )
}

pub(crate) fn route_tcp_send(worker_id: usize, op: OpSend) -> io::Result<RoutedDetachedOp<OpSend>> {
    let slot = RouteCell::new();
    route_command(
        worker_id,
        slot.clone(),
        SocketRouteCommand::TcpSend { op, slot },
    )
}

pub(crate) fn route_tcp_recv(worker_id: usize, op: Recv) -> io::Result<RoutedDetachedOp<Recv>> {
    let slot = RouteCell::new();
    route_command(
        worker_id,
        slot.clone(),
        SocketRouteCommand::TcpRecv { op, slot },
    )
}

pub(crate) fn route_udp_send_to(
    worker_id: usize,
    op: SendTo,
) -> io::Result<RoutedDetachedOp<SendTo>> {
    let slot = RouteCell::new();
    route_command(
        worker_id,
        slot.clone(),
        SocketRouteCommand::UdpSendTo { op, slot },
    )
}

pub(crate) fn route_udp_recv_stream(
    worker_id: usize,
    op: UdpRecvStream,
) -> io::Result<RoutedDetachedOp<UdpRecvStream>> {
    let slot = RouteCell::new();
    route_command(
        worker_id,
        slot.clone(),
        SocketRouteCommand::UdpRecvStream { op, slot },
    )
}

pub(crate) fn route_udp_connect(
    worker_id: usize,
    op: UdpConnect,
) -> io::Result<RoutedDetachedOp<UdpConnect>> {
    let slot = RouteCell::new();
    route_command(
        worker_id,
        slot.clone(),
        SocketRouteCommand::UdpConnect { op, slot },
    )
}

pub(crate) fn route_udp_send(
    worker_id: usize,
    op: UdpSend,
) -> io::Result<RoutedDetachedOp<UdpSend>> {
    let slot = RouteCell::new();
    route_command(
        worker_id,
        slot.clone(),
        SocketRouteCommand::UdpSend { op, slot },
    )
}

pub(crate) fn route_udp_recv(
    worker_id: usize,
    op: UdpRecv,
) -> io::Result<RoutedDetachedOp<UdpRecv>> {
    let slot = RouteCell::new();
    route_command(
        worker_id,
        slot.clone(),
        SocketRouteCommand::UdpRecv { op, slot },
    )
}
