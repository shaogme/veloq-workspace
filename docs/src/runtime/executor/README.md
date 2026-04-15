# Veloq Executor 与调度系统

本文档详细阐述 `src/runtime/executor` 模块的内部工作机制。这是 Veloq 运行时的“引擎”，负责任务的调度、负载均衡和执行循环。

## 1. 概要 (Overview)

`Executor` 模块实现了 **Work-Stealing** 和 **P2C (Power of Two Choices)** 相结合的混合调度算法。
它由两部分组成：
1.  **LocalExecutor** (`executor.rs`): 运行在每个 Worker 线程上的主循环，负责驱动 I/O、处理队列和任务窃取。
2.  **Runtime & Spawning** (`runtime.rs` / `spawner.rs`): 负责线程的生命周期管理和任务分发策略。

为了实现这一目标，执行器支持三种类型的任务执行：
1.  **Pinned Tasks (绑定任务)**: 必须在特定线程运行的任务（如 `spawn_local`）。由 `task.rs` 定义的 `Task`。
2.  **Stealable Tasks (可窃取任务)**: 实现了 `Send`，可以在任意线程运行的任务（通过 `spawn` / `spawn_eager` / `Runtime::spawn` 创建）。由 `runtime/task/harness.rs` 定义的 `Runnable`。
3.  **Blocking Tasks (阻塞任务)**: 长时间运行的 CPU 密集型任务或同步 I/O 任务。由 `runtime/blocking.rs` 的全局线程池处理。

## 2. 理念和思路 (Philosophy and Design)

### 2.1 混合调度策略 (Hybrid Scheduling)
Veloq 结合了发送端和接收端的负载均衡：
- **发送端 (Direct Push)**: 绑定任务 (`spawn_local`/`spawn_to`) 直接通过 MPSC 通道发送到目标 Worker。
- **本地优先 (Local Injection)**: 当 Worker 内部调用 `spawn` / `spawn_eager` 时，`Send` 任务会直接进入当前 Worker 的 `Stealable` 队列，优先由本地消费，但也允许被窃取。
- **远程注入缓冲 (Remote Injection Buffer)**: 跨线程创建或远程唤醒 `Runnable` 时，任务先进入目标 Worker 的 `future_injector`，再由目标线程转存到本地 `Stealable` 队列。
- **接收端 (Work-Stealing)**: 当 Worker 空闲时，主动去“窃取”其他 Worker 的 `Stealable` 队列中的任务。

### 2.2 优先级倒置 (Priority Inversion for Latency)
在 `LocalExecutor` 的主循环中，显式定义了轮询顺序：
1.  **Local Stealable**: 优先执行当前线程刚刚生成的 `Send` 任务。这利用了 CPU 缓存热度。
2.  **Local Pinned Queue**: 处理要求被绑定在当前 Worker 执行的任务 (`spawn_local`)。
3.  **Injectors**: 处理外部注入的任务（远程唤醒的绑定任务、全局注入的任务）。
4.  **Work Stealing**: 最后尝试去偷任务。

### 2.3 动态注册 (Dynamic Registry)
`ExecutorRegistry` 支持通过 `Arc` 指针共享 Worker 列表，允许 `Spawner` 在运行时动态选择目标 Worker。

## 3. 模块内结构 (Internal Structure)

- `runtime.rs`:
    - `Runtime`: 运行时顶层入口。
    - `RuntimeBuilder`: 负责初始化所有 Worker 和共享状态，并启动线程。

- `executor.rs`:
    - `LocalExecutor`: Worker 线程的本地执行器。
    - `block_on`: 创建一个临时的 `LocalExecutor` 在当前线程运行 Future。

- `context.rs`:
    - `RuntimeContext`: 提供给用户的 TLS 上下文，包含 `spawn`, `spawn_eager`, `spawn_local`, `spawn_to` 等接口。

- `executor/spawner.rs`:
    - `Spawner`: 封装任务分发逻辑。
    - `ExecutorRegistry`: 全局注册表。
    - `ExecutorShared`: 跨线程共享的原子状态（负载计数、注入队列、Pinned 通道）。

- `task/harness.rs`:
    - `Runnable`: **Stealable Task** 的运行时句柄。封装了 `Future`、状态机 (`AtomicUsize`) 和调度器 (`Schedule` Trait)。

## 4. 代码详细分析 (Detailed Analysis)

### 4.1 主循环 (`LocalExecutor::run`)
核心是一个带有 **Budget** (预算) 的循环：
```rust
loop {
    // 0. Check Memory Updates
    // 检查并注册新扩展的内存块 (Pull Model)
    self.check_for_memory_updates();

    let mut executed = 0;
    while executed < BUDGET {
        
        // 1. Local Stealable (Send Tasks - LIFO)
        if let Some(task) = self.stealable.pop() { ... }

        // 2. Local Pinned Queue
        // 处理本地任务
        if let Some(task) = self.queue.pop() { ... }
        
        // 3. Injectors (Pinned/Remote)
        // try_poll_injector 内部会依次检查:
        // - pinned queue (绑定任务)
        // - remote receiver (唤醒的绑定任务)
        if self.try_poll_injector() { ... }
        // future_injector 只负责远程注入的 Runnable，下沉到本地 Stealable 后再执行
        if self.try_poll_future_injector() { ... }
        
        // 4. Stealing
        // 从其他 Worker 偷任务
        if self.try_steal(executed) { ... }
    }
    
    // 5. IO Wait & Park
    self.park_and_wait(&main_woken);
}
```
`BUDGET` 机制（默认 64）防止计算密集型任务饿死 I/O 事件的轮询。

### 4.2 停车与唤醒 (`park_and_wait`)
这是一个精细的状态机：
1.  **Set PARKING**: 标记状态为 PARKING。
2.  **Double Check**: 再次检查队列。
3.  **Commit PARKED**: 状态设为 PARKED。
4.  **Driver.wait()**: 调用底层的 `epoll_wait` / `GetQueuedCompletionStatus`。

当其他线程通过 `pinned.send()` 发送任务时，会主动唤醒目标 Executor。

### 4.3 任务生成 (`context.rs` & `spawner.rs`)
- **`spawn` / `spawn_eager`**: 创建一个 `Runnable` (Harness)。若在 Worker 内部调用，优先推入当前 Worker 的 `Stealable` 队列；若跨线程创建或远程唤醒，则先进入目标 Worker 的 `future_injector`，再由目标线程转存到本地 `Stealable` 队列。
- **`spawn_to` / `Spawner` Pinned Spawn**: 明确目标 Worker，将任务通过 MPSC 通道发送到目标的 `pinned` 队列。
- **`spawn_local`**: 创建 `SpawnedTask` 并推入本地 `VecDeque`，永不离开线程。

### 4.4 注册表实现 (Registry Implementation)
目前 `ExecutorRegistry` 使用静态初始化的 `Arc<Vec<ExecutorHandle>>`。动态扩缩容（及之前的 `smr-swap` 方案）已简化为启动时配置，以减少运行时开销。

## 5. 存在的问题和 TODO (Issues and TODOs)

1.  **负载倾斜后的 Ping-Pong**:
    P2C 可能导致任务抖动。
    *TODO*: 实现指数退避 (Exponential Backoff) 的 Stealing 策略。

2.  **NUMA 感知**:
    *TODO*: 实现分层 Stealing。

3.  **Worker ID 回收**:
    ID 目前单调递增。

## 6. 未来的方向 (Future Directions)

1.  **时间片轮转**: 防止单个任务霸占 Budget。
2.  **自适应 Budget**: 动态调整 Budget。
