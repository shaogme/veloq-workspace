---
trigger: model_decision
description: 当前项目架构
---

# AI_GUIDELINE.md

此文件为 AI 在处理本仓库代码时提供指导。

## 架构 (Architecture)

本项目包含三个核心 Crate：`veloq-wheel`、`veloq-queue` 和 `veloq-runtime`。

### `veloq-wheel`
高性能分层时间轮 (Hierarchical Timing Wheel)。
- **组件**:
  - `Wheel`: 核心结构，管理分层 (L0, L1) 的任务。
  - `SlotMap`: 存储任务 (`WheelEntry`)，使用稳定的 `TaskId` 作为键，实现 O(1) 访问。
  - `WheelEntry`: 任务节点，形成单向链表（针对惰性取消进行了优化）。
- **关键机制**:
  - **惰性取消 (Lazy Cancellation)**: `cancel()` 仅将任务标记为已移除（将 `item` 设为 `None`）。任务在 `advance()` 推进到相应槽位时才会被物理移除。这避免了昂贵的链表解绑操作。
  - **级联 (Cascading)**: 随着时间推进，高层级 (L1) 的任务会被移动到 L0 或过期。

### `veloq-runtime`
高性能异步 I/O 运行时。
- **核心组件**:
  - **Runtime Core (`src/runtime/`)**:
    - **Runtime (`runtime.rs`)**: 运行时入口，定义 `Runtime` 结构体和组装逻辑。
    - **Mesh (`mesh.rs`)**: 无锁 SPSC 环形缓冲区，用于 Worker 间通信。使用 `#[repr(align(128))]` 防止伪共享。
    - **Executor (`executor.rs`)**:
      - `LocalExecutor`: 线程局部执行器。启动时会自动检测并注册当前线程绑定的 `BufPool`。
      - **调度**: 实现工作窃取 (Work Stealing) 和 P2C (Power of Two Choices)。
      - **策略**: Mesh 消息优先，使用 `BUDGET` 防止 I/O 饿死。
    - **Infrastructure**:
      - `context.rs`: 线程局部上下文管理。负责维护 `RuntimeContext` 及线程绑定的 `BufPool`（通过 `bind_pool`）。
      - `task.rs`: 基于 `Rc` 的任务封装，手动实现 `RawWakerVTable`。
  - **Driver (`src/io/driver.rs`)**:
    - 平台特定 I/O 的抽象层（Linux 上使用 io_uring，Windows 上使用 IOCP）。
    - **Windows IOCP (`src/io/driver/iocp/`)**:
      - `IocpDriver` (`iocp.rs`): 核心驱动，管理完成端口 (IOCP)、时间轮和线程池。
      - `submit.rs`: 处理 I/O 操作的提交，支持原生 IOCP 操作（如 `ReadFile`, `WSASendTo`）和阻塞任务的分流。
      - `blocking.rs`: 线程池实现，用于处理阻塞文件操作（`Open`, `Close`, `Fsync` 等），通过 `PostQueuedCompletionStatus` 通知完成。
      - `op.rs`: 定义 `IocpOp` 和 VTable，使用 `OVERLAPPED` 结构与内核交互。
      - `ext.rs`: 加载 Winsock 扩展函数指针（如 `ConnectEx`, `AcceptEx`）。
    - **Linux io_uring (`src/io/driver/uring/`)**:
      - `UringDriver` (`uring.rs`): 核心驱动，管理 `io_uring` 实例 (`IoUring`) 和操作注册表。
      - `submit.rs`: 实现了各操作的提交逻辑 (`make_sqe_*`) 和完成回调 (`on_complete_*`)。使用宏 (`impl_lifecycle!`) 简化了代码。
      - `op.rs`: 定义 `UringOp` 和 VTable (`OpVTable`)。使用 `union UringOpPayload` 存储不同操作的负载，并通过 `ManuallyDrop` 管理生命周期，实现了类型擦除和动态分发。
    - **StableSlab (`src/io/driver/stable_slab.rs`)**: 提供地址稳定的内存分配，用于存储 I/O 操作对象，确保异步回调安全。
    - **OpRegistry (`src/io/driver/op_registry.rs`)**: 管理飞行中的 I/O 操作。
    - 关键操作: `submit_op`, `poll_op`, `process_completions`.
  - **Buffers (`src/io/buffer.rs`)**:
    - `BufPool`: 内存池 Trait。
    - `FixedBuf`: 具有稳定地址的缓冲区，用于异步 I/O（io_uring 所需）。
    - `BuddyPool`/`HybridPool`: 具体的分配器实现。**注意**：`BufPool` 现在必须通过 `context::bind_pool` 绑定到线程，否则会导致 IO 错误或 Panic。

## 代码结构 (Code Structure)
- `veloq-wheel/`: 核心时间轮库。
- `veloq-runtime/`: 异步运行时。
  - `src/io/`: I/O 驱动和缓冲区管理。
  - `src/runtime/`: 任务执行、调度逻辑及 Mesh 网络。