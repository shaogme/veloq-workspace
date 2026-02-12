# buffer 模块文档 (veloq-buf)

本文档详细介绍了 `veloq-buf` crate。该模块负责高性能异步 I/O 的内存管理，特别针对 io_uring 和 IOCP 的需求进行了优化，提供了一套能够保持地址稳定、支持类型擦除且对零拷贝友好的内存池抽象。

**注意：** 原有的 `io::buffer` 模块已独立为 `veloq-buf` crate，以提供更好的复用性和隔离性。

## 1. 概要 (Overview)

`veloq-buf` 不仅仅是一个内存分配器，它是连接**用户态内存**与**内核 I/O** 的桥梁。其核心设计目标包括：

*   **地址稳定 (Address Stability)**: 异步 I/O 提交期间，缓冲区物理地址不可变。
*   **注册优化 (Registration Friendly)**: 为了支持 io_uring 的 `IORING_REGISTER_BUFFERS` 或 Windows RIO，底层内存必须易于提取并以大块形式注册。
*   **灵活的池拓扑 (Flexible Pool Topology)**: 通过 `PoolTopology` trait，支持多种内存管理策略（如全局共享池、独立池等）。
*   **类型擦除**: 通过 `AnyBufPool` 和手动 VTable，使得上层应用无需关心底层的具体分配策略。

核心组件结构：

*   **`FixedBuf`**: 面向用户的最终句柄，拥有底层内存块的所有权，通过 VTable 进行释放。内部内嵌了 64位的 context 用于路由释放逻辑。
*   **`BufPool` Trait**: 面向用户的顶层接口，提供 `alloc` 方法返回 `FixedBuf`。
*   **`PoolTopology` Trait**: 定义运行时内存池的初始化和构建逻辑。
*   **`UniformSlot`**: 标准的拓扑实现，采用 **Sharded Global Pool + Superblock Cache** 策略。
*   **`GlobalSlotPool`**: 全局内存管理器，管理整个系统的物理内存（Huge Pages），并将其切分为多个分片 (Shards) 以减少锁竞争。
*   **`SlotBasedPool`**: 线程本地的 Pool 句柄，指向全局的 `GlobalSlotPool`。
*   **`BuddyAllocator`**: 底层分配算法，管理分片内的内存，支持 Order 0 (4KB) 到 Order 18 (1GB) 的分配。
*   **`SuperblockState`**: 针对 4KB 小对象的快速分配缓存，使用原子操作管理 64 个 Slot。

## 2. 理念和思路 (Philosophy and Design)

### 2.1 灵活的池拓扑 (Pool Topology)
为了适应不同的工作负载和硬件环境，`veloq-buf` 引入了 `PoolTopology` 抽象：
*   **初始化 (`init`)**: 负责申请全局物理内存（如 1GB Huge Pages），并建立全局管理结构（如 `GlobalSlotPool`）。
*   **构建 (`build`)**: 为每个 Worker 线程构建 `BufPool` 实例，并负责将内存区域注册到对应的驱动（如 io_uring）。
*   **默认实现 (`UniformSlot`)**: 提供了基于分片 Buddy System 的标准实现，适用于通用高性能场景。

### 2.2 全局分块与分片架构 (Sharded Global Pool Strategy)
`UniformSlot` 拓扑摒弃了旧的 N*2 Block 策略，采用了更具扩展性的 **Sharded Global Slot** 架构：

*   **全局内存**: 启动时申请一大块连续内存 (MemoryChunk)，优先使用 Huge Pages。
*   **分片 (Sharding)**: 将全局内存切分为 $K$ 个分片 (`Shard`)，$K$ 通常为 CPU 核心数的倍数（最小 16）。每个分片拥有独立的 `BuddyAllocator` 和互斥锁。
*   **Superblock 缓存 (L0 Cache)**:
    *   针对最常用的 4KB 分配 (Order 0)，每个线程维护一个本地的 **Superblock** 引用 (TLS Cache)。
    *   一个 Superblock 包含 64 个 Slot (256KB, Order 6)。
    *   线程在 Superblock 内分配只需原子操作 (`AtomicU64`)，**无需全局锁**，实现极高的热路径性能。
*   **负载均衡与窃取**:
    *   当本地 Shard 分配失败或争用严重时，分配器会根据线程 ID 哈希计算“步长 (Stride)”，尝试遍历其他 Shard 进行内存窃取 (Work Stealing)。
    *   这保证了在个别 Shard 耗尽时，系统仍能利用整体剩余内存。

### 2.3 内存稳定性与生命周期
`FixedBuf` 句柄通过 VTable 和 Context (u64) 唯一确定了其归属的内存位置。无论句柄在线程间如何传递，其指向的物理地址恒定不变，满足 Proactor 模式下内核直接写入的需求。

### 2.4 Direct I/O 对齐
核心单元 **Slot** 大小固定为 4KB。所有分配均基于 4KB 对齐，天然满足 `O_DIRECT` / `FILE_FLAG_NO_BUFFERING` 以及各种 DMA 操作的严格对齐要求。

### 2.5 核心与注册分离
*   **BuddyAllocator**: 纯粹的内存算法，只管分配 Slot Index。
*   **BufferRegistrar**: 驱动层接口，负责将 `GlobalSlotPool` 的大块内存注册给内核。
*   **GlobalIndex**: 注册后的索引被编码在 `FixedBuf` 的 context 中，提交 I/O 时可直接获取。

## 3. 模块内结构 (Internal Structure)

```
veloq-buf/src/
├── lib.rs                 // 模块导出与基础宏定义 (nz!, MIN_THREAD_MEMORY)
├── buffer.rs              // 核心定义：FixedBuf, BufPool, PoolTopology, UniformSlot, SlotBasedPool
├── global.rs              // GlobalSlotPool: 全局内存管理、分片逻辑、Work Stealing
├── slot.rs                // Slot 定义 (4KB) 与 SlotIndex
├── os.rs                  // OS 特定实现：Huge Page 分配
└── buffer/
    ├── buddy.rs           // BuddyAllocator: 伙伴系统实现 (0~1GB)
    └── superblock.rs      // SuperblockState: 4KB 对象的原子位图分配器
```

## 4. 代码详细分析 (Detailed Analysis)

### 4.1 PoolTopology 抽象 (`buffer.rs`)
`PoolTopology` 的职责进一步明确为“初始化全局状态”和“为 Worker 建池并注册内存”：
```rust
pub trait PoolTopology: Clone + Send + Sync + 'static {
    type State: Clone + Send + Sync + 'static;
    fn init(&self, worker_count: usize) -> std::io::Result<Self::State>;
    fn build(
        &self,
        state: &Self::State,
        worker_idx: usize,
        registrar: Box<dyn BufferRegistrar>,
    ) -> AnyBufPool;
}
```
在 `UniformSlot` 实现中，`State` 即为 `Arc<GlobalSlotPool>`。

### 4.2 Superblock 与原子分配 (`buffer/superblock.rs`)
`Superblock` 是 4KB 分配的加速层。它管理一个 Order 6 (256KB) 的内存块。
*   **状态管理**:
    *   `free_mask` (AtomicU64): 位图标记 64 个 Slot 的占用情况。
    *   `is_active` (AtomicBool): 标记该 Superblock 是否正被某个线程作为“活跃缓存”持有。
*   **分配 (Alloc)**: 使用 `compare_exchange_weak` 在 `free_mask` 上寻找并置零一位，实现无锁分配。
*   **释放 (Free)**: 使用 `fetch_or` 归还位。如果释放后 Superblock 变为空且非活跃，则将其归还给底层 Buddy System。

### 4.3 核心对象 (`buffer.rs`)

**`FixedBuf`**:
context 字段 (u64) 被深度利用，紧凑存储了元数据：
```text
Layout: [GlobalIndex 16b] [Order 8b] [SlotIndex 40b]
```
*   `GlobalIndex`: 对应 io_uring 注册的 buffer index。
*   `Order`: 分配时的阶数 (Buddy Order)。
*   `SlotIndex`: 全局唯一的 Slot 索引，用于计算物理偏移。

**`SlotBasedPool`**:
用户侧的 Pool 实现。
1.  **Tiny Alloc (<=4KB)**: 通过 TLS Cache 获取当前活跃的 `Superblock`，尝试原子分配。
2.  **Large Alloc (>4KB) / Miss**: 穿透到 `GlobalSlotPool::alloc_slots`，走全局 Sharded Buddy 流程。

### 4.4 Sharded BuddyAllocator (`global.rs` & `buffer/buddy.rs`)
`GlobalSlotPool` 内部维护了一组 `BuddyAllocator`。
*   **Sharding**: 内存被均分。线程通过 `hash(thread_id)` 决定首选 Shard。
*   **Buddy System**:
    *   维护 `MAX_ORDER` (18) 个双向链表 (`LinkedList`)。
    *   使用 `BitSet` 追踪分配状态，防止 Double-Free。
    *   支持合并 (Coalescing) 与分裂 (Splitting)。
*   **Work Stealing**: 首选 Shard 锁争用时，通过线性同余法生成的步长 (Stride) 遍历其他 Shard，确保高并发下的吞吐量。

## 5. 存在的问题和 TODO (Issues and TODOs)

1.  **静态内存总量**:
    *   目前 `GlobalSlotPool` 需要在启动时指定总内存大小 (`AllocConfig`)，运行时无法动态增加物理内存（受限于 Huge Page 和注册机制）。
    *   **TODO**: 探索支持动态注册新 Region 的混合拓扑。

2.  **碎片化**:
    *   虽然 Buddy System 能合并内存，但在长期运行且分配模式复杂的情况下，仍可能产生碎片导致大块内存申请失败。

3.  **大对象回退**:
    *   超过 1GB (Order 18) 的分配目前不支持（代码中限制）。虽然对于网络 I/O 及其罕见，但通过 `GlobalAlloc` 回退的机制尚未明确集成在 `SlotBasedPool` 中。

4.  **NUMA 感知**:
    *   当前的分片策略基于线程 ID 哈希，未感知物理 NUMA 节点。在多路服务器上可能导致跨 NUMA 访问。
    *   **TODO**: 根据线程绑核情况将 Shard 绑定到特定的 NUMA 内存节点。
