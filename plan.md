# veloq-buf 动态扩展与延迟注册优化方案

## 1. 背景与问题分析

当前 `veloq` 运行时采用 `Thread-per-Core` 模型，内存管理依赖 `veloq-buf` 的 `GlobalSlotPool`。目前的实现存在以下瓶颈：

1.  **静态内存分配**：运行时启动时必须一次性申请所有内存（`MemoryChunk`），无法根据负载动态扩容。
2.  **注册同步阻塞**：`io_uring` 的 `FIXED_BUFFER` 机制要求所有缓冲区在使用前必须注册到内核。如果在运行时动态添加内存块，需要所有线程同步注册（Stop-the-World），这严重破坏了异步运行时的性能。
3.  **延迟注册导致的 IO 失败**：如果在 Thread A 分配了新内存并传递给 Thread B，而 Thread B 尚未完成该内存块的内核注册，直接使用 `READ_FIXED/WRITE_FIXED` 会导致 `-EFAULT` 或其他错误。

## 2. 总体架构设计

本方案旨在引入**动态内存池**、**稀疏注册（Sparse Registration）**、**增量更新（Incremental Update）**以及**自动降级（Automatic Fallback）**机制，彻底解决上述瓶颈。

### 核心策略

1.  **Dynamic Chunks**: `GlobalSlotPool` 不再持有单一 `MemoryChunk`，而是持有 `RwLock<Vec<Arc<MemoryChunk>>>`，支持动态追加。
2.  **Sparse Registry**: 利用 Linux 5.19+ 的特性，预先注册一个较大的、包含空洞的 Buffer Table（例如 1024 个槽位），后续扩容时无需注销重建，只需填补空洞。
3.  **Lazy & Fallback**: 驱动层维护 `ChunkID -> KernelIndex` 的映射。当 IO 请求指向未映射的 Chunk 时，自动降级为普通 IO（Non-Fixed），直到注册消息被处理。
4.  **Async Broadcast**: 扩容操作通过内部命令队列异步广播给所有 Worker。

---

## 3. 模块详细设计

### 3.1 veloq-buf：动态内存管理

**目标**：支持运行时动态申请新的内存块，并分配全局唯一的 Chunk ID。

*   **数据结构改造**：
    *   新增 `ChunkRegistry` 结构，内部维护 `Vec<Arc<MemoryChunk>>`。
    *   每个 `MemoryChunk` 分配一个单调递增的 `u16 ChunkID`。
    *   `GlobalSlotPool` 的分配逻辑修改：当当前 Chunk 耗尽时，申请新 Chunk，获得新 ID，并追加到 Registry。

*   **FixedBuf 元数据调整**：
    *   目前的 `context` 字段包含 `[GlobalIndex 16b] ...`。
    *   **修改语义**：将 `GlobalIndex` 字段重定义为 `ChunkID`。这确保了 Buffer 的标识在所有线程中是统一且持久的，不依赖于本地驱动的注册状态。

### 3.2 veloq-driver：稀疏注册与降级策略

**目标**：使 Driver 能够处理“尚未注册”的 Buffer，并支持无锁/无停顿的增量注册。

*   **初始化阶段（Sparse Init）**：
    *   在 `io_uring_register_buffers` 时，不再只注册当前的 chunks。
    *   **策略**：注册一个固定大小的数组（如 `MAX_CHUNKS = 1024`）。
    *   初始时，只有已有的 Chunk 填入对应的 `iovec`，其余槽位设为 `NULL` (Zeroed iovec)。内核会自动识别并跳过空槽位。

*   **增量更新（Incremental Update）**：
    *   引入 `register_buffer_update(chunk_id, ptr, len)` 接口。
    *   使用 `IORING_REGISTER_BUFFERS_UPDATE` opcode。
    *   该操作非常轻量，不需要暂停 Ring，也不需要注销旧表。

*   **IO 提交时的自动降级（Critical）**：
    *   Driver 内部维护一个轻量级的位图或数组 `local_registry: [Option<u16>; MAX_CHUNKS]`，记录 `ChunkID` 到 `FixedIndex` 的映射（通常是一一对应的）。
    *   **在 `submit_op` 阶段**：
        1.  解析 `FixedBuf` 的 `ChunkID`。
        2.  检查 `local_registry[ChunkID]` 是否有效。
        3.  **Case A (已注册)**: 使用 `IORING_OP_READ_FIXED`，传入对应的 `buf_index`。
        4.  **Case B (未注册)**:
            *   **降级**：通过 `FixedBuf::as_ptr()` 获取原始指针。
            *   **替换 Opcode**：将操作转换为普通的 `IORING_OP_READ`。
            *   **性能影响**：仅损失一次系统调用的 Zero-Copy 特性，但保证了功能的正确性和非阻塞。

### 3.3 veloq-runtime：异步广播机制

**目标**：协调所有 Worker 线程最终完成新内存块的注册，使系统恢复到最佳性能状态。

*   **触发扩容**：
    *   当某个 Worker (Thread A) 触发 `GlobalSlotPool` 扩容时：
        1.  执行系统调用申请内存。
        2.  更新全局 `ChunkRegistry`。
        3.  生成一条 `SystemCommand::RegisterMemory(ChunkID, Ptr, Len)`。

*   **广播流程**：
    *   Thread A 利用 `ExecutorRegistry` 遍历所有其他 Worker。
    *   向每个 Worker 的 `remote_queue` 发送上述系统命令。

*   **命令处理**：
    *   Worker (Thread B) 在 `poll` 循环中收到 `RegisterMemory` 命令。
    *   调用 `driver.register_buffer_update(...)` 完成内核态注册。
    *   更新本地 `local_registry` 状态。
    *   **结果**：后续该 Chunk 的 IO 将自动切换回 `FIXED` 模式。

---

## 4. 实施步骤

### 第一阶段：基础设施重构 (veloq-buf)
1.  定义 `Chunk` 结构体，包含 `id`, `ptr`, `len`。
2.  重构 `GlobalSlotPool`，使其支持 `Vec<Chunk>` 的存储和查找。
3.  修改 `FixedBuf` 的 `context` 编码逻辑，确保携带 `ChunkID`。

### 第二阶段：驱动层增强 (veloq-driver)
1.  修改 `UringDriver::new`，实现 Sparse Buffer Registration（预留 1024 槽位）。
2.  实现 `register_memory_update` 方法，封装 `io_uring_register_buffers_update`。
3.  **核心逻辑**：修改 `UringDriver::submit`，在解析 Buffer 时增加降级判断逻辑。

### 第三阶段：运行时集成 (veloq-runtime)
1.  定义新的系统任务类型 `RegisterMemoryTask`。
2.  在 `GlobalSlotPool` 扩容路径上注入回调钩子（Hook），允许 Runtime 捕获扩容事件。
3.  实现广播逻辑，确保所有 Worker 最终一致。

## 5. 预期效果

*   **无感扩容**：内存不足时自动申请新页，业务线程无感知。
*   **零停顿**：不再需要 Stop-the-world 暂停所有线程进行注册。
*   **高可用**：即使注册消息在网络/队列中延迟，IO 操作也会自动降级，绝不会抛出错误。
