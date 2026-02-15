# Veloq Driver 异步 I/O 驱动架构文档

本文档详细阐述了 `veloq-driver` 核心 I/O 驱动层的设计哲学、架构结构及实现细节。该层旨在屏蔽操作系统底层的异步 I/O 差异（Linux io_uring 与 Windows IOCP），为上层运行时提供统一的高性能 Proactor 接口。

## 1. 概要 (Overview)

`veloq-driver` 是运行时与操作系统内核交互的独立 Crate。与 Rust 生态中常见的 Reactor 模型（如 Tokio 基于 mio/epoll）不同，Veloq 采用了纯 **Proactor** 模型。

- **Reactor**: 关注“就绪”事件（Readiness）。当 socket 可读时通知应用，应用再调用 `read`。
- **Proactor**: 关注“完成”事件（Completion）。应用直接提交 `read` 操作，内核将数据写入缓冲区后通知应用完成。

这种设计是为了原生适配现代高性能 I/O 接口（io_uring 和 IOCP 均为 Proactor 性质），避免中间层的模拟开销，并最大化利用内核的零拷贝和批量处理能力。

统一的核心抽象是 `Driver` Trait，它定义了操作提交、轮询、取消和资源管理的行为。

## 2. 理念和思路 (Philosophy and Design)

### 2.1 统一的 Proactor 抽象
尽管 io_uring（基于环形队列）和 IOCP（基于完成端口队列）在 API 形式上差异巨大，但在逻辑上它们都遵循：
`提交请求 (Submit) -> 等待 (Wait) -> 处理完成 (Process Completion)`

Veloq 抽象出了这一公共流程：
1.  **Reserve**: 在用户态分配一个 Slot (User Data)，用于关联上下文。
2.  **Submit**: 将操作描述符提交给内核，并传入 Slot Index 作为 User Data。
3.  **Poll**: 上层 `Future` 在 `poll` 时检查共享 Slot 的状态。
4.  **Complete**: 驱动收到内核完成通知，通过 User Data 找到 Slot，填入结果并唤醒 Waker。

### 2.2 零分配与共享槽位 (Zero-Allocation & Shared Slots)
异步 I/O 的一个核心挑战是**资源的生命周期管理**和**跨线程通知**。
- **旧设计问题**: 传统 `DetachedOp` 往往依赖 `oneshot` 通道或 `Arc<State> + Box<Completer>` 来实现跨线程结果返回，导致每次 I/O 都有额外的堆分配。
- **新设计方案**: **Shared Slot Table**。
    - 驱动维护一个 `Arc<SlotTable<Op>>`，其中包含预分配的、地址固定的 `Slot` 数组。
    - **零分配提交**: `DetachedOp` Future 直接持有 `Arc<SlotTable>` 和索引。提交时，I/O 资源（如 `Op`）直接移动到 Slot 中，无需额外分配。
    - **无锁状态机**: Slot 内部维护原子状态机 (`EMPTY` -> `SUBMITTED` -> `COMPLETED`)。Future 直接轮询 Slot 状态，驱动完成时直接更新 Slot 并唤醒 Waker，无需中间通道。

### 2.3 零开销类型擦除 (Zero-Cost Type Erasure)
驱动需要支持多种 I/O 操作（Read, Write, Connect, Accept, Close 等）。
- **传统做法**: 使用巨大的 `Enum` 包裹所有可能的操作。缺点是内存浪费（结构体大小取决于最大的那个变体）。
- **Veloq 做法**: 使用 **Union + VTable** (`PlatformOp` Trait 和具体的 `Op` 实现)。
    - **Payload**: 使用 `union` 存储不同操作的数据载荷。
    - **VTable**: 每个操作携带一个静态虚函数表（VTable），包含构建提交项、处理完成回调、销毁逻辑等指针。
    - 这种类似 C++ 虚函数的机制是在编译期生成的，避免了运行时的动态内存分配（Heap Allocation），同时保持了数据结构的紧凑。

### 2.4 Mesh 网络协同与远程注入
驱动不仅处理本地 I/O，还通过 **Injector** 机制深度集成了跨线程调度能力。
- **Injector**: 每个驱动实例暴露一个线程安全的注入器 (`Injector<D>`)。
- **Closure Injection**: 允许其他线程将闭包 (`Box<dyn FnOnce(&mut Driver) + Send>`) 发送到驱动线程执行。这构成了 `RemoteOp` 的基础——远程线程如果不持有 Driver 的资源的线程所有权，可以通过注入闭包，“委托” Driver 线程提交操作。
- **Inner Handle / Notify**: 暴露底层的 `RawFd` 或 `Handle` 并提供唤醒机制，用于实现高效的事件通知。

## 3. 模块内结构 (Internal Structure)

```
veloq-driver/src/
├── driver.rs           // Driver 模块定义与接口 (Trait, PlatformOp)
└── driver/             // Driver 具体实现与组件
    ├── op_registry.rs  // 动静分离的操作注册表
    ├── slot.rs         // 核心 Slot 定义与状态机
    ├── iocp.rs         // Windows 平台实现入口
    ├── iocp/           // Windows 子模块目录
    │   ├── blocking.rs
    │   ├── op.rs
    │   └── ...
    ├── uring.rs        // Linux 平台实现入口
    └── uring/          // Linux 子模块目录
        ├── submit.rs
        └── ...
```

- **`driver.rs`**: 定义了驱动必须实现的接口规范。
- **`slot.rs`**: 定义了核心的 `Slot<Op>` 结构。它是 `CachePadded` 的，包含原子状态、Waker、Result 和 UnsafeCell 包裹的 Op 资源。
- **`op_registry.rs`**: 管理驱动的本地状态 (`local`) 和共享状态 (`shared`)。
    - `shared`: `Arc<SlotTable>`，供 Future 和 Driver 共享访问。
    - `local`: Driver 线程私有的状态（如 Timer ID、Backlog 链表节点），无锁开销。

## 4. 代码详细分析 (Detailed Analysis)

### 4.1 Driver Trait (`driver.rs`)
```rust
pub trait Driver: 'static {
    type Op: PlatformOp;

    // 核心生命周期
    fn reserve_op(&mut self) -> io::Result<(usize, u32)>; // 返回 (index, generation)
    fn slot_table(&self) -> Arc<SlotTable<Self::Op>>;
    fn submit(&mut self, user_data: usize, op: Self::Op) -> Result<Poll<()>, ...>;
    fn poll_op(&mut self, user_data: usize, cx: &mut Context) -> Poll<...>;

    // 驱动循环
    fn wait(&mut self) -> io::Result<()>;
    fn process_completions(&mut self);

    // 资源管理
    fn register_files(...) -> ...;
    fn unregister_files(...) -> ...;
    fn register_chunk(&mut self, id: u16, ptr: *const u8, len: usize) -> io::Result<()>;
    fn submit_background(&mut self, op: Self::Op) -> ...;
}
```

### 4.2 Slot & SlotTable (`slot.rs`)
- **Slot**: 
  - `state`: `AtomicU8` (EMPTY, SUBMITTED, COMPLETED)。
  - `generation`: `AtomicU32`，防止 ABA 问题（Slot 被回收复用后，旧 Future 仍尝试访问）。
  - `op`: `UnsafeCell<Option<Op>>`。在 `SUBMITTED` 状态下由 Driver 访问（提交给内核），在 `COMPLETED` 状态下由 Future 取走（获取所有权）。
  - `waker`: `AtomicWaker`，用于唤醒等待的 Future。
  - `overlapped` (Windows): 嵌入的 IOCP 重叠结构，利用指针反推技术定位 Slot。

### 4.3 OpRegistry (`op_registry.rs`)
它是连接 `Driver` 和 `Future` 的桥梁，采用了**动静分离**设计。
- **Shared (SlotTable)**: 存储重量级的 `Op` 资源和同步原语。`DetachedOp` 持有它的引用。
- **Local**: 存储驱动内部的轻量级状态（如 `lifecycle`, `timer_id`）。
- **流程**:
    1.  **Submit**: Driver 分配索引，将 `Op` 放入 Shared Slot，初始化 Local 状态。
    2.  **Poll**: Future 检查 Shared Slot 的 `state` 和 `generation`。
    3.  **Complete**: Driver 收到内核事件，更新 Shared Slot `result` 和 `state`，唤醒 Waker。
    4.  **Take**: Future 被唤醒，从 Shared Slot `take()` 走 Result 和 Op。

## 5. 存在的问题和 TODO (Issues and TODOs)

1.  **Backlog 策略差异**:
    - **Linux**: io_uring 的 SQ 也是环形队列，会满。`UringDriver` 必须在用户态实现一个 Backlog 链表来暂存无法提交的操作。
    - **Windows**: IOCP 本身没有“提交队列满”的概念（它是直接调 API），但为了防止内存无限增长，我们人为限制了 `SlotTable` 的大小。
    - **TODO**: 统一 Backpressure（背压）策略，当驱动过载时，向上层返回明确的错误或挂起信号，而不是无限缓冲。

2.  **Buffer Registration 抽象泄漏 (已解决)**:
    - 引入了**逻辑区域映射 (Logical Region Mapping)** 层。
    - `BufferPool` 将内存暴露为带索引的 Region。
    - 驱动层（IOCP/Uring）分别将这些索引映射为 RIO Buffer ID 或 io_uring fixed indices。
    - 结果：`FixedBuf` 只需携带一个通用的 `region_index`，实现了跨平台的 O(1) 提交，完全屏蔽了底层差异。

3.  **同步文件 I/O**:
    - 在 io_uring 上文件 I/O 是真异步的。在 IOCP 上，部分文件操作（特别是打开/关闭）仍通过线程池模拟。这种差异导致性能特性的不一致。
    - **TODO**: 在 Linux 上对于不支持 io_uring 的动作也应有统一的线程池回退机制（目前可能有隐式阻塞）。

## 6. 未来的方向 (Future Directions)

1.  **支持更多后端**:
    - 虽然目前专注高性能 Proactor，但为了兼容性（如 macOS），未来可能需要引入 `kqueue` 后端。但这需要适配层模拟 Proactor 行为（类似 Tokio 的做法，但在 Driver 层内部封装）。

2.  **Direct I/O 与 Zerocopy**:
    - 进一步深挖 `IORING_OP_SEND_ZC` 和 Windows RIO。
    - 目标是实现网络栈的零拷贝发送和接收，这对于高吞吐场景（如 100Gbps 网络）是必须的。

3.  **内核旁路 (Kernel Bypass) 集成**:
    - 随着 io_uring 支持 `IORING_OP_URING_CMD`，未来可以直接对接 NVMe 驱动或用户态网络栈（如 AF_XDP），进一步绕过内核协议栈开销。
