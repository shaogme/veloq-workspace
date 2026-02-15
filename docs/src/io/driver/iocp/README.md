# Windows IOCP 驱动文档

本文档详细介绍了 `veloq-driver` 中基于 Windows I/O Completion Ports (IOCP) 的异步驱动实现。

## 1. 概要 (Overview)

`veloq-driver` 的 Windows 驱动层实现了 `Driver` trait，为上层运行时提供异步 I/O 支持。该驱动采用了 **Proactor** 模式，利用 Windows 内核提供的 IOCP 机制来处理网络和文件 I/O 事件。

一个显著的特点是它实现了 **混合 I/O 模型**：对于标准句柄使用传统的 IOCP 模型，而对于支持 Registered I/O (RIO) 的网络操作，则自动尝试升级到更高效的 RIO 路径，以降低内核开销。

## 2. 理念和思路 (Concepts)

### 2.1 Proactor 模型
IOCP 是原生的 Proactor 模型。应用程序“投递”一个 I/O 请求（如 `ReadFile` 或 `RIOReceive`），内核在操作完成后通知应用程序。在此期间，驱动必须保证传递给内核的数据结构（如 `OVERLAPPED` 或 `RIO_BUF`）地址固定且有效。

### 2.2 内存稳定性与 Slot Embedding
由于异步操作通过指针引用 `OVERLAPPED`，我们需要确保其在整个生命周期内地址固定。
- **SlotTable**: 驱动使用 `SlotTable` 存储飞行中（In-Flight）的操作。`Slot` 数组预先分配并固定在内存中。
- **Overlapped Embedding**: `Slot<Op>` 结构体中直接嵌入了 Windows 的 `OVERLAPPED` 结构。
    ```rust
    pub struct Slot<Op> {
        // ...
        #[cfg(windows)]
        pub overlapped: UnsafeCell<OverlappedEntry>, // 包含 OVERLAPPED 和额外元数据
    }
    ```
- **指针反推 (Pointer Back-tracing)**: 当 IOCP 完成时，我们得到的是指向 `OVERLAPPED` 的指针。由于 `OVERLAPPED` 嵌入在 `Slot` 中，且 `Slot` 大小固定，我们可以通过指针算术（`container_of` 逻辑）反推算出不仅是 `Slot` 对象的地址，还能算出该 `Slot` 在 Table 中的 **索引 (Index)**。这也允许我们不再依赖 `completion_key` 来传递上下文。

### 2.3 类型擦除与 VTable (Type Erasure)
为了避免使用巨大的 `Enum` 定义所有 I/O 操作，驱动使用了自定义的类型擦除技术 (`src/driver/iocp/op.rs`)。
- **Union Payload**: 所有具体的 Op 负载（如 `ReadFixed`, `Connect`, `SendToPayload`）存储在一个 `union` 中。
- **VTable Logic**: 配合 `define_iocp_ops!` 宏，自动为每个 Op 生成 `OpVTable`（包含 `submit`, `on_complete`, `drop`, `get_fd` 等函数指针）。
- 这使得 `IocpOp` 结构体保持紧凑，同时支持动态分发，无需堆分配。

### 2.4 阻塞操作分流 (Blocking Offload)
文件系统的某些元数据操作（如 `Open`, `Close`, `Fsync`）在 Windows 上往往是同步阻塞的。为了不阻塞 Reactor 线程，这些操作被自动分流到内部的 `ThreadPool` (`src/driver/iocp/blocking.rs`)。
- 线程池任务完成后，手动调用 `PostQueuedCompletionStatus` 向 IOCP 端口发送完成通知，使其在主循环中表现为普通的异步事件。

### 2.5 Registered I/O (RIO) 集成
为了追求极致的网络性能，驱动集成了 Windows RIO 扩展，并实现了多项优化：

- **缓冲区注册**: 实现了 `Driver::register_chunk` 接口。驱动内部维护 `chunk_registry`，将 `veloq-buf` 的 `ChunkID` 映射到 Windows RIO 的 `RIO_BUFFERID`。这允许内存块的动态与增量注册。
- **逻辑区域映射 (Logical Region Mapping)**: Veloq 引入了创新的缓冲区管理设计。Windows 驱动将逻辑区域索引直接映射到 RIO Buffer ID，实现了 O(1) 的缓冲区解析。
- **O(1) 请求队列 (RQ) 查找**: 对于注册文件 (`IoFd::Fixed`)，驱动在 `RioState` 中维护了一个直接映射表 `registered_rio_rqs`。这消除了哈希表查找的开销，使得获取 Request Queue 的操作也是 O(1) 的。
- **Slab 页注册**: 为了支持 `SendTo`/`RecvFrom` 等操作中的地址结构体（`SOCKADDR`），驱动会自动将 `SlotTable` 占用的内存页通过 `register_buffer` 注册到 RIO。这允许 `Slot` 内的元数据结构也通过 RIO 零拷贝地传递。
- **自动降级**: 在提交操作时 (`submit.rs`)，驱动会自动检测是否满足 RIO 条件（Buffer 已注册、RQ 获取成功）。如果满足则走 RIO 路径；否则无缝回退到普通 IOCP。

## 3. 模块内结构 (Internal Structure)

```
src/driver/iocp/
├── op.rs       // IocpOp 定义, VTable, Union Payload, 宏定义
├── submit.rs   // 核心提交逻辑，包含各 Op 的 submit_* 函数及 RIO 升级检查
├── blocking.rs // 阻塞任务线程池
├── ext.rs      // Winsock 扩展加载 (ConnectEx, AcceptEx, RIO Table)
├── inner.rs    // 内部实现细节 (State, IocpDriver 实现)
├── rio.rs      // RIO 状态管理 (RioState), Buffer 注册, RQ 管理
└── tests.rs    // 单元测试
```

外部交互：
- `iocp.rs`: 驱动入口，包含 `IocpDriver` 结构体定义。
- `../slot.rs`: 定义 `Slot` 结构体及 `OVERLAPPED` 的嵌入方式。

## 4. 代码详细分析 (Detailed Analysis)

### 4.1 IocpDriver (`iocp.rs` & `inner.rs`)
`IocpDriver` 是整个驱动的核心，它负责协调 IOCP 和 RIO：
- **资源管理**:
    - `port`: IOCP 句柄。
    - `rio_state`: `Option<RioState>`，封装了所有 RIO 相关资源。
    - `ops`: `OpRegistry`，其中的 `shared` 部分持有 `SlotTable`。
- **文件注册**: `register_files` 不仅更新 IOCP 的文件表，还会通知 `RioState` 调整其内部映射表，确保存储 RQ 的 vector 足够大。
- **主循环 (`get_completion`)**:
    1. 计算定时器超时。
    2. 调用 `GetQueuedCompletionStatus` 等待事件。
    3. 如果收到 `RIO_EVENT_KEY`，则调用 `rio_state.process_completions` 处理 RIO 完成事件。
    4. 如果是普通 IOCP 事件，通过指针反推（Pointer Back-tracing）从 `OVERLAPPED` 指针计算出 `Slot` 索引，然后处理完成逻辑。

### 4.2 RIO 状态管理 (`rio.rs`)
`RioState` 集中管理 RIO 资源：
- **RQ 管理**: 
    - `rio_rqs`: `HashMap<HANDLE, RIO_RQ>` 用于原始句柄。
    - `registered_rio_rqs`: `Vec<Option<RIO_RQ>>` 用于注册文件，提供 O(1) 访问。
- **Buffer 管理**: 维护 `chunk_registry: Vec<RIO_BUFFERID>` 以及 `slab_rio_pages`（用于内部地址缓冲区的 RIO 注册）。
- **完成处理**: `process_completions` 使用 `RIODequeueCompletion` 批量获取完成结果，并更新对应 `Slot` 的状态。

### 4.3 智能提交 (`submit.rs`)
该模块定义了所有操作的提交逻辑。宏 `submit_io_op!` 和 `impl_lifecycle!` 简化了代码。
- **Recv/Send**:
    检查 `ctx.rio` 是否存在。如果存在，尝试调用 `rio.try_submit_recv/send`。这些函数会尝试进行 O(1) 的 Buffer 和 RQ 查找。
- **UDP 支持 (`SendTo/RecvFrom`)**:
    驱动实现了 `try_submit_send_to` 和 `try_submit_recv_from`。利用 `RIOSendEx` 和 `RIOReceiveEx`，以及预先注册的 Slab 页面（用于存放地址信息），实现了全路径的 RIO UDP 支持。

### 4.4 操作定义 (`op.rs`)
通过 `define_iocp_ops!` 宏定义了如 `Accept`, `SendTo`, `Wakeup` 等操作。
- **SubmitContext**: 包含 `rio: Option<&'a mut RioState>`，使得提交逻辑可以访问 RIO 状态。
- **复杂 Payload**: 如 `SendToPayload` 包含了 `WSABUF` 和地址存储。对于 RIO 路径，这些结构体在 `Slot` 中的位置已被注册，可直接作为 `RIO_BUF` 使用。

## 5. 存在的问题和 TODO

1.  **SyncFileRange 语义**:
    - Windows 缺乏对应 Linux `sync_file_range` 的细粒度刷新 API。目前实现为全文件 Flush，性能可能低于预期。

2.  **线程池策略**:
    - `blocking.rs` 中的线程池虽然支持动态伸缩，但缺乏基于负载的精细化调度，海量阻塞文件操作可能导致线程数抖动。

## 6. 未来的方向 (Future Directions)

1.  **完成通知批处理**:
    - 目前 `GetQueuedCompletionStatus` 是一次处理一个。可以引入 `GetQueuedCompletionStatusEx` 批量获取，减少系统调用。注：RIO 的 `process_completions` 已经实现了批量出队 (MAX 128)。

2.  **错误码标准化**:
    - 进一步完善 WSA Error 到 `std::io::Error` 的映射，提供更跨平台一致的错误语义。
