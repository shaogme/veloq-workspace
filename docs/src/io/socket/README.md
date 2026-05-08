# socket 模块文档

本文档详细介绍了 `veloq-runtime` 中的 `io::socket` 及其相关操作模块 (`op.rs`)。该模块旨在提供跨平台的网络和文件 I/O 原语抽象，屏蔽 Windows (IOCP) 和 Linux (io_uring) 之间的底层差异。

## 1. 概要 (Overview)

`src/io/socket.rs` 和 `src/io/op.rs` 构成了运行时与用户态 API 之间的操作桥梁。它们的主要职责是：

1.  **统一 API**: 提供统一的结构体（如 `Connect`, `Accept`, `Recv`）来描述 I/O 操作，无论底层使用何种驱动。
2.  **生命周期管理**: 管理异步操作从定义、提交到完成的全生命周期 (`OpLifecycle`)。
3.  **平台适配**: 处理不同操作系统在 socket 创建、地址解析和缓冲区管理上的细微差异（例如 Windows 的 `AcceptEx` 需要预先创建 Socket）。

## 2. 理念和思路 (Philosophy and Design)

### 2.1 统一的操作封装 (The `Op<T>` Wrapper)
所有的异步操作都被封装在 `Op<T>` 结构中，这是一个 Rust `Future`。
*   **State Machine**: `Op` 内部维护了一个简单的状态机 (`Defined` -> `Submitted` -> `Completed`)。
*   **Ownership**: 当操作处于 `Submitted` 状态时，`Op` 将资源的所有权转移给驱动（Driver）。只有在操作完成或被驱动退回时，所有权才会返回给用户。这是为了满足 Proactor 模式下“缓冲区必须保持有效”的要求。

### 2.2 预分配策略 (Pre-allocation Strategy)
Windows IOCP 的某些 API（特别是 `AcceptEx`）要求调用者提供“输出参数”所需的资源。例如，`AcceptEx` 不会像 `accept` 系统调用那样返回一个新的 Socket fd，而是要求用户先创建一个 Socket 传进去，内核将新连接绑定到这个 Socket 上。

为了抹平这种差异，引入了 `OpLifecycle` trait：
*   **`pre_alloc`**: 在构造 Op 之前执行。
    *   *Windows*: 预先调用 `socket()` 创建句柄。
    *   *Linux*: 空操作 (No-op)。
*   **`into_output`**: 操作完成后，将结果和预分配的资源组合成最终的返回值（如 `TcpStream`）。

### 2.3 地址无关性
使用 `SockAddrStorage` (基于 `libc::sockaddr_storage` 或 Windows `SOCKADDR_STORAGE`) 来存储地址。这使得上层代码可以统一处理 IPv4、IPv6 甚至 Unix Domain Socket，而无需到处写 `match` 语句。

## 3. 模块内结构 (Internal Structure)

```
src/io/
├── socket.rs          // 平台特定 Socket 实现的重导出 (Facade)
├── op.rs              // 核心操作定义 (Read, Write, Connect, Accept...) 和 Op Future 实现
├── socket/
│   ├── unix.rs        // #[cfg(unix)] 实现
│   └── windows.rs     // #[cfg(windows)] 实现
```

*   **`socket.rs`**: 这是一个 Facade，根据编译目标重导出 `unix` 或 `windows` 子模块中的 `Socket` 类型及辅助函数。
*   **`op.rs`**: 定义了所有 I/O 操作的数据结构（Payload）。这些结构体是跨平台的（Condition-less），但其实现细节（如 `pre_alloc`）通过 `cfg` 宏进行区分。

## 4. 代码详细分析 (Detailed Analysis)

### 4.1 `Op<T>` Future (`op.rs`)
```rust
pub struct Op<T: IntoPlatformOp<PlatformDriver>> {
    state: State,
    data: Option<T>,
    user_data: usize,
    driver: Weak<RefCell<PlatformDriver>>,
}
```
*   `poll` 方法实现了提交逻辑：
    1.  如果状态是 `Defined`，尝试向 `driver` 提交操作 (`driver.submit`)。
    2.  如果提交失败（这通常不应该发生，除非 Ring 满了且无 Backpress），立即返回错误。
    3.  如果提交成功，状态变为 `Submitted`，随后轮询 `driver.poll_op` 等待完成。
    4.  完成时，调用 `T::from_platform_op` 将驱动返回的底层 Op 还原为高级 Op 结构。

### 4.2 关键操作分析

#### `Accept`
*   **结构**: 包含 `fd` (监听 socket) 和 `accept_socket` (仅 Windows)。
*   **差异处理**:
    *   在 Windows 上，`AcceptEx` 极其高效，但需要预先创建 socket 并消耗一个重叠结构。`OpLifecycle` 完美封装了这一复杂性。
    *   在完成时 (`into_output`)，Linux 版本直接将 syscall 返回的 fd 包装；Windows 版本则返回预创建的 `accept_socket`，并解析内核填充的地址 buffer。

#### `Connect`
*   **结构**: `fd`, `addr` (Raw bytes), `addr_len`.
*   **机制**: 封装了 `connect` (Linux) 和 `ConnectEx` (Windows)。

#### `ReadFixed` / `WriteFixed`
*   **结构**: `fd`, `buf` (FixedBuf), `offset`.
*   **特点**: 强制使用 `FixedBuf`，确保了缓冲区在异步过程中的地址稳定性，是实现 Zero-Copy 的基础。

### 4.3 `IoFd` 枚举
```rust
pub enum IoFd {
    Raw(RawHandle),
    Fixed(u32),
}
```
区分了普通文件描述符和 io_uring 的注册文件描述符 (Fixed File)。后者可以避免内核在每次 I/O 时查找文件表的开销，是高性能的关键。

## 5. 存在的问题和 TODO (Issues and TODOs)

1.  **VTable 样板代码**:
    *   目前 `IoFd` 到平台特定 Op 的转换可能涉及较多的样板代码。
    *   **TODO**: 利用宏进一步简化 `IntoPlatformOp` 的实现。

2.  **Socket 选项配置**:
    *   目前的 `socket.rs` 仅暴露了最基础的创建功能。设置 `TCP_NODELAY`, `SO_RCVBUF` 等选项通常需要通过原始句柄进行。
    *   **TODO**: 提供更丰富的、类型安全的 Socket 选项配置接口。

3.  **Buffer 传递**:
    *   `Op` 拿走了 `FixedBuf` 的所有权。如果操作失败，用户需要能够方便地拿回 Buffer。目前虽然支持，但 API 易用性有待提升。

## 6. 未来的方向 (Future Directions)

1.  **Zero-Copy Networking**:
    *   结合 `io_uring` 的 `IORING_OP_SEND_ZC`，进一步优化 `Send` 操作，彻底消除用户态到内核态的数据拷贝。

2.  **AF_XDP 集成**:
    *   探索通过 `socket.rs` 暴露 AF_XDP 接口，实现内核旁路的高性能包处理。

3.  **Completer-Based API**:
    *   目前是 Future-Based。考虑是否提供基于回调或 Completer 的底层 API，以供极致性能场景使用（减少 Waker 唤醒开销）。
