# Linux io_uring 驱动文档

本文档详细介绍了 `veloq-runtime` 中基于 Linux io_uring 的异步驱动实现。

## 1. 概要 (Overview)

`veloq-runtime` 的 Linux 驱动层位于 `src/io/driver/uring/` 目录下。它实现了 `Driver` trait，利用 Linux 内核最新的 `io_uring` 接口提供高性能的异步 I/O 能力。该驱动采用 **Proactor** 模式，通过共享内存的提交队列 (SQ) 和完成队列 (CQ) 与内核进行零拷贝交互，避免了传统系统调用（syscall）的频繁上下文切换开销。

## 2. 理念和思路 (Philosophy and Design)

### 2.1 高性能配置
我们在初始化 `io_uring` 时启用了一系列高级特性（依赖较新的内核版本），以榨取极致性能：
- **`setup_coop_taskrun`**: 减少核间中断 (IPI)。
- **`setup_single_issuer`**: 针对单线程提交优化，进一步减少锁开销 (Kernel 6.0+)。
- **`setup_defer_taskrun`**: 推迟内核任务执行直到 `io_uring_enter`，优化批处理能力 (Kernel 6.1+)。
- **`sqpoll` (可选)**: 如果启用 polling 模式，内核线程会轮询 SQ，实现真正的零系统调用提交。

### 2.2 类型擦除与动态分发 (Type Erasure)
为了支持多种 I/O 操作（Read, Write, Connect, Accept 等）而不引入枚举 (Enum) 的巨大内存开销，本驱动采用了与 IOCP 驱动类似的类型擦除技术 (`src/io/driver/uring/op.rs`)。
- **Union Payload**: 使用 `union UringOpPayload` 存储各种操作的具体参数结构。这确保了 `UringOp` 的大小仅等于最大负载的大小，而不是所有变体之和。
- **VTable**: 每个操作类型通过宏 `define_uring_ops!` 自动生成对应的 `OpVTable`。包含 `make_sqe` (构建提交项), `on_complete` (处理完成), `drop` (资源清理) 等静态函数指针。
- **生命周期管理**: 使用 `ManuallyDrop` 手动管理 Union 中字段的生命周期，确保在操作完成或取消时正确释放资源（如 `CString`, `Vec` 等）。

### 2.3 提交积压处理 (Backlog Handling)
`io_uring` 的提交队列 (SQ) 大小是固定的。当 SQ 满时，驱动必须暂存无法立即提交的操作。
- 我们在 `UringOpState` 中维护了一个**侵入式单向链表** (`backlog_head`, `backlog_tail`, `next`)。
- 当 `push_entry` 失败（SQ 满）时，操作被加入 backlog 链表。
- 每次 `submit` 或 `wait` 后，驱动会尝试 `flush_backlog`，将暂存的操作重新推入 SQ。

### 2.4 唤醒机制 (Waker)
由于 `driver.wait()` 通常会阻塞在 `io_uring_enter` 系统调用上，我们需要一种机制从其他线程唤醒它（例如当新的任务通过 Mesh 通道发送过来时）。
- 驱动使用 `eventfd` 创建一个特殊的唤醒文件描述符。
- 注册一个 `Poll` 或 `Read` 操作 (`Wakeup`) 到 `io_uring` 监听该 fd。
- `RemoteWaker` 的实现只是简单地向该 `eventfd` 写入 8 字节，从而触发 `io_uring` 完成事件，唤醒驱动主循环。

### 2.5 内核兼容性与降级策略 (Kernel Compatibility)
`veloq-runtime` 优先使用较新内核的特性以获得最佳性能，但也提供了针对较旧内核的回退支持。

#### 功能降级矩阵 (Degradation Matrix)
驱动初始化时会尝试启用所有高级特性 (`SingleIssuer`, `DeferTaskRun`, `CoopTaskRun`)。如果内核不支持（返回 `EINVAL`），将自动回退到基础模式。

| 内核版本 (Kernel) | 模式 (Mode) | 特性支持 (Features) | 性能影响 (Performance) |
| :--- | :--- | :--- | :--- |
| **≥ 6.1** | **高性能 (High Perf)** | `SingleIssuer` + `DeferTaskRun` + `CoopTaskRun` | 最佳。最小化系统调用开销和 IPI。 |
| **5.6 - 6.0** | **基础 (Basic)** | 标准 `io_uring` | 良好。无 `DeferTaskRun` 批处理优化，可能有少量多余 IPI。 |
| **< 5.6** | **不支持 (Unsupported)** | - | 无法运行。缺少 `IORING_OP_RECV` / `IORING_OP_SEND` 等核心指令支持。 |

*注：虽然 5.10+ (LTS) 是推荐的生产环境最低版本，但在 5.6+ 上理论上可运行基础功能。*

## 3. 模块内结构 (Internal Structure)

```
src/io/driver/uring/
├── mod.rs          // 驱动入口 (UringDriver)，主循环，Backlog 管理
├── op.rs           // UringOp 定义，VTable 定义，Union Payload 宏
├── submit.rs       // 各个 Op 的具体实现 (make_sqe_*, on_complete_*)
└── tests/          // (如果存在) 单元测试
```

外部依赖：
- `../op_registry.rs`: `OpRegistry` 负责存储所有飞行中的操作 (`UringOp`) 及其状态 (`UringOpState`)。
- `../stable_slab.rs`: 提供地址稳定的内存分配，确保 `user_data` 索引在操作生命周期内有效。

## 4. 代码详细分析 (Detailed Analysis)

### 4.1 UringDriver (`uring.rs`)
核心结构体 `UringDriver` 维护了：
- `ring`: `io_uring::IoUring` 实例。
- `ops`: `OpRegistry<UringOp, UringOpState>`，所有 In-Flight 操作的仓库。
- `backlog_head/tail`: 积压队列指针。
- `pending_cancellations`: 等待取消的操作队列。

**生命周期**:
1.  **提交 (submit)**: 用户调用 `submit`，驱动分配 Slot，保存资源，调用 `vtable.make_sqe` 构建 SQE (Submission Queue Entry)，尝试推入 Ring。若 Ring 满，加入 Backlog。
2.  **等待 (wait)**: 调用 `ring.submit_and_wait(1)`。
3.  **处理 (process_completions)**:
    - 遍历 CQE (Completion Queue Entry)。
    - 根据 `user_data` 找到对应的 `OpEntry`。
    - 调用 `vtable.on_complete` 处理结果（转换错误码，解析地址等）。
    - 移除 Op，唤醒对应的 `Waker`。

### 4.2 操作定义 (`op.rs`)
`UringOp` 是驱动中流转的核心数据结构：
```rust
#[repr(C)]
pub struct UringOp {
    pub vtable: &'static OpVTable, // 8 bytes
    pub payload: UringOpPayload,   // union
}
```
宏 `define_uring_ops!` 极大地简化了新操作的添加。开发者只需定义 Payload 结构和几个静态方法，宏会自动生成 `IntoPlatformOp` 实现和 `Union`定义。

### 4.3 静态分发 (`submit.rs`)
该文件包含所有具体操作的逻辑。
- **make_sqe_***: 将高层 Op 转换为 `io_uring::squeue::Entry`。处理 `IoFd::Fixed` (注册文件) 和 `IoFd::Raw` 的区别。
- **on_complete_***: 处理内核返回的 `i32` 结果。例如 `Accept` 操作在此处将内核写入的 `sockaddr` 字节解析为 Rust 的 `SocketAddr`。
- **drop_***: 安全地释放 Union 中的资源。

### 4.4 缓冲区管理
驱动支持 `FixedBuf`，对应 `io_uring` 的 `IOSQE_FIXED_FILE` 和预注册缓冲区 (`IORING_REGISTER_BUFFERS`)。
- 在 `make_sqe` 中，如果检测到 `buf_index != NO_REGISTRATION_INDEX`，会自动使用 `opcode::ReadFixed` / `WriteFixed` 等变体，实现零拷贝和内核侧缓冲区复用。

## 5. 存在的问题和 TODO (Issues and TODOs)

1.  **内核版本依赖**:
    - 当前使用了许多较新的 io_uring 特性（如 `Single Issuer`, `Defer Taskrun`）。在旧内核（< 5.10）上虽然有回退逻辑，但性能和功能可能受限。
    - 降级策略与兼容性矩阵详见 [2.5 内核兼容性与降级策略](#25-内核兼容性与降级策略-kernel-compatibility)。

2.  **Backlog 性能**:
    - 目前 Backlog 是一个单向链表。如果 Ring 长期处于满载状态，大量的 Backlog 插入/弹出可能导致 CPU 开销增加。
    - **TODO**: 考虑在极端压力下引入背压 (Backpressure) 机制，暂时拒绝新任务。

3.  **取消机制的可靠性**:
    - `AsyncCancel` 发出后，原操作可能正好完成。需要仔细处理 `ECANCELED` 和正常完成的竞态条件，确保资源不被双重释放或泄露。

4.  **Send/Recv Msg**:
    - `SendTo`/`RecvFrom` 目前使用了 `SendMsg`/`RecvMsg`。对于单纯的 UDP 发送，可能可以直接优化为 `Send`/`Recv` 配合地址连接，或者使用 `IORING_OP_SEND_ZC` (Zero Copy)。

## 6. 未来的方向 (Future Directions)

1.  **Zero Copy (IORING_OP_SEND_ZC)**:
    - 随着内核支持的完善，引入零拷贝发送将显著提升大包吞吐量。

2.  **io_uring_cmd**:
    - 支持 `IORING_OP_URING_CMD`，为 NVMe Passthrough 或其他内核子系统提供直接通道，绕过文件系统层。

3.  **多重 Shot (Multishot)**:
    - 利用 `IORING_RECV_MULTISHOT` (Provide Buffers)，允许一个系统调用接收多个数据包，极大减少网络密集型应用的 syscall 数量。

4.  **Ring Sharing**:
    - 探索在多个 Worker 线程间安全共享 Ring 的可能性（虽然目前的设计是每线程一个 Ring 以避免锁），或者利用 `IORING_SETUP_ATTACH_WQ` 共享内核侧工作队列。
