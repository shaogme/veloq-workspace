# veloq-intrusive-linklist

`veloq-intrusive-linklist` 是一个高性能、类型安全的 Rust **侵入式链表 (Intrusive Linked List)** 库。

与标准库中的 `Vec` 或 `LinkedList` 不同，侵入式链表将“链接（Link）”本身嵌入到数据结构中。这意味着节点本身负责存储其在链表中的连接状态，从而避免了额外的内存分配，并允许在已知节点引用的情况下以 O(1) 的时间复杂度将节点从链表中移除。

本项目支持**非并发单线程**与**多线程并发**两种节点链接场景，并对它们进行了底层的通用抽象，为构建高性能并发原语（如任务调度器、等待队列等）提供基础数据结构支持。

## ✨ 核心特性

- **零内存分配 (Zero Allocation)**：节点链接字段嵌入在结构体中，所有链表操作仅涉及指针操作，无需任何堆内存分配。
- **高性能 (High Performance)**：极大减少了缓存未命中（Cache Miss）和内存碎片。
- **双版本支持**：
  - **非并发版 (`Link`)**：适用于单线程或外层有锁保护的单线程链表操作（例如 `LinkedList`）。
  - **并发版 (`ConcurrentLink`)**：链接状态 `linked` 采用原子变量 (`AtomicBool`) 管理，专为多线程无锁或精细粒度同步场景设计（例如 `ConcurrentLinkedList`）。
- **游标支持 (Cursor API)**：提供 `Cursor`/`ConcurrentCursor` 和 `CursorMut`/`ConcurrentCursorMut`，支持双向遍历、获取 Pin/非 Pin 引用以及原地移除操作。
- **安全性检查 (Safety Checks)**：
  - 运行时检查防止节点重复插入或在未移除时被 Drop（触发 Panic 保护）。
  - 使用 `Pin` 保证节点在链表中的内存地址稳定性。
- **宏辅助 (Macro Support)**：提供 `intrusive_adapter!` 与 `concurrent_intrusive_adapter!` 宏，一键为普通节点与并发节点生成适配器，处理指针偏移计算。

## 🚀 快速开始

### 1. 使用非并发版链表 (`Link` + `LinkedList`)

首先，你需要在结构体中包含一个 `Link` 字段，并使用 `intrusive_adapter!` 宏定义适配器：

```rust
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};
use std::boxed::Box;
use std::pin::Pin;

pub struct MyNode {
    pub data: i32,
    // 侵入式链接字段
    link: Link,
}

// 自动生成适配器实现，建立 MyNode 与 link 字段的映射关系
intrusive_adapter!(pub MyAdapter = MyNode { link: Link });

fn main() {
    // 创建一个使用 MyAdapter 的链表
    let mut list = LinkedList::new(MyAdapter);

    // 节点通常需要固定在内存中 (Pinned)，因为链表指针依赖节点的稳定地址
    let mut node1 = Box::pin(MyNode { data: 10, link: Link::new() });
    let mut node2 = Box::pin(MyNode { data: 20, link: Link::new() });

    unsafe {
        // 将节点加入链表尾部
        // SAFETY: 必须保证节点在链表中时有效且不发生移动
        list.push_back(node1.as_mut());
        list.push_back(node2.as_mut());
    }

    assert_eq!(list.len(), 2);

    // 弹出头部节点
    if let Some(popped_node) = list.pop_front() {
        assert_eq!(popped_node.data, 10);
    }

    assert_eq!(list.len(), 1);
}
```

### 2. 使用并发版链表 (`ConcurrentLink` + `ConcurrentLinkedList`)

当需要在并发或多线程场景下共享链接时，使用 `ConcurrentLink` 和 `concurrent_intrusive_adapter!` 宏：

```rust
use veloq_intrusive_linklist::{ConcurrentLink, ConcurrentLinkedList, concurrent_intrusive_adapter};
use std::boxed::Box;
use std::pin::Pin;

pub struct ConcurrentNode {
    pub value: String,
    // 并发侵入式链接字段，链接状态 linked 内部使用 AtomicBool
    link: ConcurrentLink,
}

concurrent_intrusive_adapter!(pub ConcurrentAdapter = ConcurrentNode { link: ConcurrentLink });

fn main() {
    let mut list = ConcurrentLinkedList::new(ConcurrentAdapter);

    let mut node1 = Box::pin(ConcurrentNode {
        value: "Hello".to_string(),
        link: ConcurrentLink::new(),
    });

    unsafe {
        list.push_back(node1.as_mut());
    }

    assert_eq!(list.len(), 1);
    assert!(node1.link.is_linked());
}
```

## 📖 详细功能

### Cursor (游标操作)

游标允许在遍历链表的同时对链表节点进行查询或删除。以非并发版的 `CursorMut` 为例：

```rust
let mut cursor = list.front_mut();

while let Some(node) = cursor.get() {
    if node.data == 20 {
        // 找到目标节点，将其从链表中移除
        // 此操作是 O(1) 的，并且不会销毁节点本身
        let removed_node = cursor.remove();
        println!("Removed: {}", removed_node.unwrap().data);
        // remove 后游标会自动指向被删除节点的后继节点
    } else {
        cursor.move_next();
    }
}
```

- 可以通过 `cursor_mut_from_ptr` 根据节点的裸指针在 $O(1)$ 时间内快速创建游标并执行删除操作。

### Scoped Push (作用域自动移除守卫)

为了实现类似于 RAII 的资源释放，本项目提供了 `push_back_scoped` 方法。它会将节点推入链表并返回一个 Guard。

当 Guard 离开作用域时，会自动将节点从链表中安全移除：

```rust
{
    let mut node = Box::pin(MyNode { data: 99, link: Link::new() });
    
    // 返回一个 RemoveOnDrop / ConcurrentRemoveOnDrop Guard
    let _guard = unsafe { list.push_back_scoped(node.as_mut()) };
    
    assert_eq!(list.len(), 1);
    assert!(node.link.is_linked());
    
    // 当 _guard 离开作用域时，即使后续有提前 return，node 也会自动被安全 unlink 移除
}
assert!(list.is_empty());
```

## ⚠️ 安全性说明 (Safety)

由于侵入式链表不直接拥有节点的所有权（它通过节点中的 `link` 建立指针关联），它的实现和使用都极度依赖 `unsafe`：

1.  **内存固定 (Pinning)**：传递给链表的节点必须是用 `Pin<&mut T>`（如通过 `Box::pin` 或栈上 pin）包裹的引用。这是因为节点一旦被链接，其余节点的 `next/prev` 指针将直接指向它，任何内存移动都会导致悬垂指针。
2.  **生命周期保护 (Lifetime & Drop Panic)**：用户必须保证节点在链表中的生命周期内有效。如果节点在尚未从链表中移除时被 Drop，`Link` / `ConcurrentLink` 的 `Drop` 实现将抛出 **Panic** 以强行中断程序，防止产生悬垂指针从而导致内存安全漏洞。
3.  **线程安全性**：如果链表本身需要在多线程之间共享和修改，需要外层进行加锁等同步操作。虽然 `ConcurrentLink` 在标记节点是否被链接时使用了原子变量，但其双向指针操作依然是非线程安全的，应配合外部同步器使用。