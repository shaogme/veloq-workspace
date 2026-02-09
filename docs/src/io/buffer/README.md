# buffer 模块文档 (veloq-buf)

本文档详细介绍了 `veloq-buf` crate。该模块负责高性能异步 I/O 的内存管理，特别针对 io_uring 和 IOCP 的需求进行了优化，提供了一套能够保持地址稳定、支持类型擦除且对零拷贝友好的内存池抽象。

**注意：** 原有的 `io::buffer` 模块已独立为 `veloq-buf` crate，以提供更好的复用性和隔离性。

## 1. 概要 (Overview)

`veloq-buf` 不仅仅是一个内存分配器，它是连接**用户态内存**与**内核 I/O** 的桥梁。其核心设计目标包括：

*   **地址稳定 (Address Stability)**: 异步 I/O 提交期间，缓冲区物理地址不可变。
*   **注册优化 (Registration Friendly)**: 为了支持 io_uring 的 `IORING_REGISTER_BUFFERS` 或 Windows RIO，底层内存必须易于提取并以大块形式注册。
*   **灵活的池拓扑 (Flexible Pool Topology)**: 通过 `PoolTopology` trait，支持多种内存管理策略（如全局共享池、线程独立池或混合模式）。
*   **类型擦除**: 通过 `AnyBufPool` 和手动 VTable，使得上层应用无需关心底层的具体分配策略（Buddy 还是 Hybrid）。

核心组件结构：
*   **`FixedBuf`**: 面向用户的最终句柄，拥有底层内存块的所有权，通过 VTable 进行释放。
*   **`BufPool` Trait**: 面向用户的顶层接口，提供 `alloc` 方法返回 `FixedBuf`。
*   **`PoolTopology` Trait**: 定义运行时内存池的初始化和构建逻辑，支持自定义拓扑。
*   **`UniformBlock`**: 默认的拓扑实现，采用 **N*2 Block** 策略（全局分块）。
*   **`GlobalBlockPool`**: `UniformBlock` 使用的全局内存管理器，维护所有线程的内存块 (Block)。
*   **`BlockBasedPool`**: `UniformBlock` 拓扑下的线程本地 Pool 实现。
*   **`RawAllocator` Trait**: 底层分配算法接口，由 `BuddyAllocator` 和 `HybridAllocator` 实现。

## 2. 理念和思路 (Philosophy and Design)

### 2.1 灵活的池拓扑 (Pool Topology)
为了适应不同的工作负载和硬件环境，`veloq-buf` 引入了 `PoolTopology` 抽象：
*   **初始化隔离**: 拓扑决定了全局状态 (`State`) 如何初始化。
*   **构建灵活性**: 拓扑控制每个 Worker 线程如何构建其本地的 `BufPool` 实例，以及如何向驱动注册内存。
*   **默认实现 (UniformBlock)**: 提供了基于 **N*2 Block** 的标准实现，适用于大多数通用场景。

### 2.2 全局分块架构 (N*2 Block Strategy - UniformBlock)
`UniformBlock` 拓扑采用全局分块策略来解决多线程环境下的内存分配竞争和碎片问题：
*   **预分配大页**: 启动时向 OS 申请 Huge Pages (2MB/page)，减少 TLB Miss。
*   **N*2 Blocks**: 如果系统有 N 个工作线程，则创建 2N 个 `Block`。每个线程被分配：
    *   **Primary Block (主块)**: 优先使用的内存块。
    *   **Backup Block (备块)**: 主块耗尽时使用的备用块。
*   **4级分配优先级**:
    1.  **Own Primary**: 尝试锁定并从自己的主块分配（阻塞）。
    2.  **Own Backup**: 尝试从自己的备块分配（阻塞）。
    3.  **Others' Backup**: 尝试从其他线程的备块分配（非阻塞 Work Stealing）。
    4.  **Others' Primary**: 尝试从其他线程的主块分配（非阻塞，最后兜底）。

### 2.3 内存稳定性与生命周期
在 Proactor 模式中，内核直接操作用户内存。`FixedBuf` 句柄拥有底层的内存块，并且不支持原地扩容。其生命周期通过 Rust 所有权系统管理，确保在 I/O 完成前内存有效。

### 2.4 Direct I/O 对齐
所有 Pool 实现均基于 `AlignedMemory`，强制执行 4KB (Page Size) 对齐。这确保了生成的缓冲区天然满足 O_DIRECT / FILE_FLAG_NO_BUFFERING 的严格要求。

### 2.5 核心与注册分离
内存分配逻辑与 I/O 驱动的注册逻辑通过 `PoolTopology` 分离：
*   **RawAllocator**: 只管内存怎么切分（Buddy 算法或 Hybrid Slab 算法）。
*   **Topology**: 负责将分配好的 RawAllocator 组装成 Pool，并调用 `BufferRegistrar` 进行注册。
*   **BufferRegistrar**: 驱动层提供的接口，负责将内存区域注册给内核。

## 3. 模块内结构 (Internal Structure)

```
veloq-buf/src/
├── lib.rs                 // 模块导出与基础宏定义 (nz!)
├── buffer.rs              // 核心定义：FixedBuf, BufPool, PoolTopology, UniformBlock
├── block.rs               // Block 定义：Mutex 保护的分配单元与 RemoteFree 队列
├── global.rs              // GlobalBlockPool: 全局 N*2 Block 管理与分配策略
├── os.rs                  // OS 特定实现：Huge Page 分配
└── buffer/
    ├── buddy.rs           // BuddyAllocator: 伙伴系统实现 (RawAllocator)
    └── hybrid.rs          // HybridAllocator: 混合 Slab 实现 (RawAllocator)
```

## 4. 代码详细分析 (Detailed Analysis)

### 4.1 PoolTopology 抽象 (`buffer.rs`)
`PoolTopology` 是运行时构建内存池的核心入口：
```rust
pub trait PoolTopology: Clone + Send + Sync + 'static {
    type State: Clone + Send + Sync + 'static;

    // 初始化全局状态（如分配一大块连续物理内存）
    fn init(&self, worker_count: usize) -> std::io::Result<Self::State>;

    // 为特定 Worker 构建 BufPool 并注册内存
    fn build(
        &self,
        state: &Self::State,
        worker_idx: usize,
        registrar: Box<dyn BufferRegistrar>,
    ) -> AnyBufPool;
}
```

### 4.2 Block 与并发控制 (`block.rs`)
`Block` 是 `UniformBlock` 拓扑中内存管理的最小并发单元。
*   **互斥锁保护**: 内部使用 `parking_lot::Mutex` 保护分配器状态。
*   **Remote Free Queue (远程释放队列)**:
    为了减少跨线程释放时的锁竞争，`Block` 维护了一个 `remote_frees` 队列。
    *   **Fast Path**: 如果能立即获取主锁，直接归还内存。
    *   **Slow Path**: 如果主锁被占用（例如正在分配），则将待释放内存推入 `remote_frees` 队列（使用独立的锁，竞争极小）。
    *   **Lazy Reclaim**: 下次分配时，持有主锁的线程会顺便回收 `remote_frees` 中的内存。

### 4.3 核心抽象 (`buffer.rs`)

**`FixedBuf`**:
用户持有的最终句柄，类似于标准库的 `Box<[u8]>` 但带有自定义释放逻辑。
```rust
pub struct FixedBuf {
    ptr: NonNull<u8>,
    cap: NonZeroUsize,
    global_index: Option<GlobalIndex>, // 注册后的 Buffer Index (io_uring use)
    pool_data: NonNull<()>,            // 指向 Pool 状态的指针 (Type Erased)
    vtable: &'static PoolVTable,       // 虚函数表
    context: usize,                    // 分配上下文 (如 Block Index 或 Slab Index)
    ...
}
```

**`BlockBasedPool`**:
这是 `UniformBlock` 拓扑下用户使用的具体类型，对应单个线程。
1.  **Global Static 架构**: 直接持有 `&'static GlobalBlockPool`。
2.  `alloc()`: 调用 `GlobalBlockPool::alloc`，按照 4 级优先级策略尝试获取内存。
3.  生成 `FixedBuf` 时，将 `Block Index` 编码进 `context` 的高 32 位，确保释放时能路由回正确的 `Block`。

### 4.4 HybridAllocator (`buffer/hybrid.rs`)
`HybridAllocator` 专为固定大小的网络包设计，采用 **Unified Arena (统一竞技场)** 布局。
*   **统一内存 layout**: 预先计算所有规格 Slab (4K, 8K, 16K, 32K, 64K) 所需的总内存。
*   **分配策略**:
    *   **Small Alloc**: 根据大小直接映射到对应的 Slab (O(1))。
    *   **Fallback**: 超过 64KB 的请求通过 Global Allocator (系统堆) 分配（此时无法享受零拷贝注册）。
*   **BitSet Check**: 使用 `veloq_bitset` 进行 Double-Free 检测。

### 4.5 BuddyAllocator (`buffer/buddy.rs`)
`BuddyAllocator` 采用了两层架构来平衡性能与碎片率：
1.  **L0 Layer: Slab Cache**: 针对常用大小 (Order 0-5, 即 4KB-128KB) 维护栈式缓存 (`Vec<ptr>`)，实现 O(1) 分配。
2.  **L1 Layer: Raw Buddy System**: 经典的二进制伙伴系统，管理剩余内存，支持动态分裂与合并 (Coalescing)。

## 5. 存在的问题和 TODO (Issues and TODOs)

1.  **静态配置限制**:
    *   目前的 Block 大小和数量在启动时固定。虽然支持 `ThreadMemoryMultiplier` 配置，但运行时无法动态扩容 `GlobalBlockPool`。
    *   **TODO**: 通过实现新的 `PoolTopology` 来探索动态扩容机制。

2.  **跨线程释放开销**:
    *   虽然引入了 `RemoteFree` 队列优化，但跨线程释放仍涉及原子操作和锁。
    *   在极高并发且 Cross-Thread 流量巨大的场景下，可能需要进一步优化（如无锁队列）。

3.  **内存利用率**:
    *   `HybridAllocator` 的 Slab 比例是静态固定的。如果工作负载与预设比例严重不符（例如全是 64K 包），会导致其他 Slab 内存浪费。
    *   **TODO**: 实现自适应的 Slab 大小调整或更灵活的分配器。

4.  **BitSet 依赖**:
    *   `HybridPool` 依赖 `veloq_bitset` 进行 Double-Free 检测，性能开销需持续关注。
