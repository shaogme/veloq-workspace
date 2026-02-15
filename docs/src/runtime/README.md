# Veloq Runtime 核心架构文档

本文档主要介绍 `veloq-runtime` 核心运行时层 (`src/runtime`) 的架构设计、核心组件及实现原理。

## 1. 概要 (Overview)

Veloq Runtime 是一个基于 **Thread-per-Core** 模型的高性能异步运行时，同时集成了 **Work-Stealing** 机制以实现更好的计算负载均衡。它旨在充分利用现代多核硬件的特性，通过减少跨核通信、锁竞争和缓存抖动来最大化 I/O 和计算吞吐量。

## 2. 理念和思路 (Philosophy and Design)

### 2.1 Thread-per-Core 与 混合任务模型
我们坚信在现代高并发场景下，**数据局部性 (Data Locality)** 是性能的关键。为此，Veloq 实现了双任务系统：
- **Pinned Task (Task)**: 绑定在特定核心，拥有独立的 I/O 驱动和缓冲区池。用于 I/O 密集型操作，保持零锁。
- **Stealable Task (Runnable)**: 实现了 `Send`，可以在核心间窃取和迁移。用于计算密集型操作，平衡负载。

### 2.2 Handle-based Distribution (基于句柄的分发)
传统的运行时通常使用全局的 `Mutex<Queue>` 或 `SegQueue` 来进行跨线程任务调度的高负载下争用。
Veloq 使用 **Executor Handles**：
- 每个 Worker 维护一个 `ExecutorShared` 状态，包含一个专用的 **MPSC (Multi-Producer Single-Consumer)** 通道 (`pinned`) 用于接收定向任务。
- 当 Worker A 需要将任务发给 Worker B 时 (`spawn_to`)，它持有 B 的 `ExecutorHandle`，直接将任务发送到 B 的通道。
- 这种方式简化了拓扑结构，同时通过 `ExecutorRegistry` 保持了灵活性。

### 2.3 显式上下文 (Explicit Context)
为了避免隐式的全局状态（如 TLS 中的隐藏变量），Veloq 提供了 `RuntimeContext`：
- 显式通过上下文访问 `Spawner` (用于负载均衡生成任务) 和 `Driver` (I/O)。
- `spawn`: 创建一个可窃取的任务 (`Runnable`)，优先本地执行，支持被窃取。
- `spawn_local`: 创建一个绑定任务 (`Task`)，仅限本地执行。
- `spawn_to`: 将任务直接发送到指定 Worker 的 `pinned` 通道。

## 3. 模块内结构 (Internal Structure)

代码位于 `src/runtime/`：

```
src/runtime/
├── runtime.rs    // Runtime 主入口，包含 Runtime 结构体、RuntimeBuilder 及 Worker 线程启动逻辑
├── blocking.rs   // 全局阻塞线程池，处理 BlockingTask
├── context.rs    // 线程局部上下文 (TLS)，提供 spawn 接口和资源访问 (RuntimeContext)
├── task.rs       // Pinned Task 定义，手动实现的 RawWakerVTable
    └── harness.rs // Stealable Task (Runnable) 定义
├── join.rs       // JoinHandle 实现，管理任务结果的异步等待
├── executor.rs   // LocalExecutor 定义，Worker 线程主循环
└── executor/     // 执行器内部实现细节
    └── spawner.rs // 任务生成器、注册表 (Registry) 和负载均衡逻辑
```

## 4. 代码详细分析 (Detailed Analysis)

### 4.1 Runtime & Initialization (`runtime.rs`)
`Runtime` 结构体持有所有 Worker 线程的句柄 (`JoinHandle`) 和全局注册表 (`ExecutorRegistry`)。
在 `RuntimeBuilder::build()` 过程中，会进行如下关键初始化：
- **Shared States**: 预分配所有 Worker 的共享状态 (`ExecutorShared`)，包含注入队列 (`Injector`)、Pinned 通道 (`mpsc`) 和负载计数器。
- **Dynamic Memory Listener**: 连接 `PoolTopology` 的扩容监听器。当缓冲池动态分配新 Chunk 时，自动通知全局注册表，触发所有 Worker 的 Driver 进行内存注册。
- **Thread Spawning**: 启动 Worker 线程，每个线程运行一个 `LocalExecutor`，并绑定 Buffer Pool。

### 4.2 Context (`context.rs`)
`RuntimeContext` 是运行时与任务之间的桥梁。它包含：
- **Driver**: 指向底层 `PlatformDriver` 的弱引用。
- **ExecutorHandle**: 当前执行器的句柄，包含共享状态 (Shared State)。
- **Spawner**: 全局生成器，用于 `spawn` 和 `spawn_to`。

### 4.3 Task System (`task.rs` & `harness.rs`)
Veloq 的任务系统经过了深度优化，分为两类：

1.  **Stealable Task (Runnable)** (`harness.rs`):
    *   专为 Work-Stealing 设计。
    *   包含 `[Header][Scheduler][Future]` 布局。
    *   支持原子状态机 (`IDLE`, `RUNNING`, `NOTIFIED`) 和跨线程调度 (`Schedule` trait)。

2.  **Pinned Task (Task)** (`task.rs`):
    *   专为本地执行设计。
    *   手动内存布局 `[Header][Future]`，无 Scheduler 指针，更节省内存。
    *   使用 `VecDeque` 进行调度，极低开销。

### 4.4 JoinHandle (`join.rs`)
实现了任务结果的异步获取。
- **无锁状态机**: 使用 `AtomicU8` 维护状态 (`IDLE` -> `WAITING` -> `READY`)，结合 `UnsafeCell` 存储结果。
- **Local vs Send**:
  - `LocalJoinHandle`: 单线程内使用，基于 `Rc<RefCell<...>>`，零原子开销。
  - `JoinHandle`: 跨线程使用，基于 `Arc<JoinState>` 和原子操作，支持 `Send`。

## 5. 存在的问题和 TODO (Issues and TODOs)

1.  **Task Debugging**:
    目前的 Task 结构体非常精简，缺乏调试信息。
    *TODO*: 在 Debug 模式下注入追踪信息。

2.  **Local Task 饿死**:
    虽然有 Budget 机制，但在极端混合场景下（大量远程注入任务 + 本地任务），调度策略可能仍需微调。

## 6. 未来的方向 (Future Directions)

1.  **结构化并发 (Structured Concurrency)**:
    实现类似 `TaskScope` 的机制。

2.  **协作式抢占 (Cooperative Preemption)**:
    目前依赖用户代码中的 `.await` 点进行调度。如果用户写了死循环，Worker 会卡死。未来可考虑结合编译器插件或计时器信号进行强制让出检测。
