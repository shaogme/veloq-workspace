# Veloq

🚀 一个基于 **Thread-per-Core** 模型的高性能 Rust 异步运行时。
---

# Linux Kernel Requirements (内核版本要求)

Veloq Runtime 基于 `io_uring` 构建高性能异步运行时。由于使用了较新的 io_uring 特性，对 Linux 内核版本有一定要求。

## 1. 核心操作支持 (Core Operations)

下表列出了运行时中使用的主要 io_uring 操作及其最低内核版本要求：

| 操作 (Operation) | 底层 Opcode | 最低内核版本 | 说明 |
| :--- | :--- | :--- | :--- |
| **基础 I/O** | `READ_FIXED`, `WRITE_FIXED` | **5.1** | 固定缓冲区读写 |
| **文件同步** | `FSYNC` | **5.1** | |
| **文件范围同步** | `SYNC_FILE_RANGE` | **5.2** | |
| **文件管理** | `OPENAT`, `CLOSE`, `FALLOCATE` | **5.6** | 文件打开/关闭/预分配 |
| **网络连接** | `CONNECT`, `ACCEPT` | **5.5** | 建立连接 |
| **网络收发** | `SEND`, `RECV` | **5.6** | 高效的单次收发 |
| **网络数据报** | `SENDMSG`, `RECVMSG` | **5.1** | 用于 `SendTo` / `RecvFrom` |
| **超时控制** | `TIMEOUT` | **5.4** | 用于定时器 |
| **取消操作** | `ASYNC_CANCEL` | **5.5** | 异步取消 |

## 2. 高级特性与 Mesh 通信 (Advanced Features)

运行时采用了特定的优化标志和跨线程通信机制 (Mesh Communication)。

| 特性 (Feature) | 机制/标志 | 最低内核版本 | 必须性 |
| :--- | :--- | :--- | :--- |
| **协作式调度** | `IORING_SETUP_COOP_TASKRUN` | **5.19** | 可选 (性能优化) |
| **单提交者模式** | `IORING_SETUP_SINGLE_ISSUER` | **6.0** | 可选 (性能优化) |
| **延迟任务运行** | `IORING_SETUP_DEFER_TASKRUN` | **6.1** | 可选 (性能优化) |

## 3. 版本建议 (Recommendation)

*   **最低运行版本 (Minimum)**: **Linux 5.6**
    *   需要支持 `IORING_OP_SEND` / `IORING_OP_RECV` 等基础网络操作。

*   **推荐生产版本 (Recommended)**: **Linux 6.1+**
    *   为了获得最佳的吞吐量和最低的延迟，建议使用 6.1 或更高版本，以启用 `DEFER_TASKRUN` 等关键优化。

> **注意**: 在不支持可选优化标志的旧内核上，运行时会自动回退到兼容模式，但性能会有所下降。
