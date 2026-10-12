# Coding Guidelines and Instructions for Agents

When making modifications to this repository, please adhere strictly to the following requirements. Failure to run and pass these checks will result in a Continuous Integration (CI) failure.

**重要提示 (IMPORTANT):** 与用户沟通、提供解释及编写代码注释时，必须始终使用**简体中文**。

---

## 1. 核心原则 (Core Principles)

### 1.1 交互与沟通语言

所有交互、解释与答复必须使用**简体中文**。

### 1.2 事实先于修改（读取验证原则）

严禁凭空推断代码逻辑、依赖关系或文件内容。在进行任何代码修改、方案设计或问题解答之前，**必须先完整读取相关代码与上下文文件**。

### 1.3 缺陷主动报告责任

在阅读或分析代码过程中，若发现潜在逻辑漏洞、安全隐患、性能瓶颈或边界处理不当，必须主动指出并报告，不得隐瞒或忽略。

### 1.4 注释规范

* **语言要求**：代码注释必须使用中英双语。
* **内容原则**：
* 禁止无意义的废话注释（即“代码已经表达清楚的事情无需用中文重复一遍”）。
* 仅用于解释复杂的业务意图、边界条件或算法关键设计。
* **严禁将注释当作工作日志、技术方案草稿、重构备忘录或 PR 记录**。

```rust
// Bad: 无意义描述，或夹带方案/重构历史说明
// 定义一个计数器变量
let mut count = 0;
// 【重构方案第2步】：因旧逻辑会导致内存泄漏，临时保留该方案，后续引入 Arc 替换

// Good: 准确阐述业务意图与设计权衡
// 采用指数退避重试机制，防止上游服务雪崩
let delay = base_delay * 2u32.pow(attempts);

```

### 1.5 跨平台执行统一入口

所有 Linux/Windows 目标的 `test / clippy / check` **一律交给 `xtest-runner`**，并通过 `.cargo/config.toml` 别名调用，禁止自行拼接跨平台脚本命令。

当前统一入口如下：

```toml
xtest-linux
xtest-windows
xclippy-linux
xclippy-windows
xcheck-linux
xcheck-windows
```

#### 平台路由强制规则 (Platform Routing Rules)

1. **Windows 主机 -> Linux 目标**：
   - Linux 相关命令一律在 Docker 内执行。
   - 不允许在 Windows 主机直接原生运行 Linux 目标的编译/检查/测试。

2. **Linux 主机 -> Windows 目标**：
   - Windows 相关命令一律通过 **QEMU Windows 虚拟机** 执行（当前使用 QEMU 而非 `cross`）。
   - 不允许在 Linux 主机直接用 `cargo` 原生跑 Windows 目标。
   - 由 `xtest-runner` 统一管理 QEMU 虚拟机的生命周期、SSH 等待、工作区同步及在虚拟机内部的原生执行。


---

## 2. 格式与代码风格 (Code Style)

### 2.1 模块文件组织（Strict Rust 2018+）

**严禁使用遗留的 `mod.rs` 命名规范**，必须严格遵循 Rust 2018 Edition 及更高版本的模块目录结构：

* 模块 `foo` 的入口定义必须位于 `foo.rs`。
* 若 `foo` 拥有子模块，创建同名目录 `foo/`，子模块文件置于 `foo/` 内。
* 父模块逻辑保留在 `foo.rs`，严禁使用 `foo/mod.rs`。

```text
# Bad (Rust 2015 遗留风格)
src/
├── main.rs
└── foo/
    ├── mod.rs
    └── sub.rs

# Good (Rust 2018+ 规范风格)
src/
├── main.rs
├── foo.rs
└── foo/
    └── sub.rs

```

### 2.2 禁止绕过 Lint（零容忍放宽属性）

严禁添加全局或局部的放宽属性（如 `#[allow(...)]`、`#![allow(...)]`）来规避 Clippy 或编译器警告。**必须从代码设计层面根治问题**。

```rust
// Bad: 通过 allow 绕过警告
#[allow(clippy::too_many_arguments)]
fn process_order(id: u64, user: String, item: String, qty: u32, price: f64, addr: String) {}

// Good: 重构数据结构解决参数过多问题
struct OrderParams {
    id: u64,
    user: String,
    item: String,
    qty: u32,
    price: f64,
    addr: String,
}

fn process_order(params: OrderParams) {}

```

### 2.3 顶级/同层级定义空行隔离

同一层级的作用域内，结构体（`struct`）、枚举（`enum`）、函数（`fn`）、特征（`trait`）、常量（`const`）之间，**无论相邻类型是否相同，必须使用单个空行显式分隔**。

```rust
// Bad: 顶层定义紧凑无空行
struct User {
    id: u64,
}
struct Role {
    name: String,
}
fn get_user() -> User {
    User { id: 1 }
}

// Good: 同层级定义之间以单空行分隔
struct User {
    id: u64,
}

struct Role {
    name: String,
}

fn get_user() -> User {
    User { id: 1 }
}

```

---

## 3. 架构与模块约束 (Module & Architecture Constraints)

### 3.1 子模块可见性与依赖流向

* **子模块声明私有化**：父模块声明子模块时，必须声明为私有（`mod child;`），**严禁声明为 `pub mod child;` 或 `pub(crate) mod child;**`。
* **单向依赖控制**：
* 允许同级子模块之间相互引用（`use crate::parent::sibling;`）。
* **严禁子模块反向依赖父模块的私有实现**：禁止在子模块内使用 `super::*` 或绝对路径读取父模块的内部未公开实现；必要的数据与状态应通过参数向下传递。

```rust
// ===== 父模块 (parent.rs) =====

// Bad: 公开暴露子模块
pub mod child;

// Good: 子模块必须完全私有
mod child;

```

```rust
// ===== 子模块 (parent/child.rs) =====

// Bad: 反向引用父模块的私有上下文
use super::ParentContext;

// Good: 仅依赖同级模块或公共抽象，状态通过入参传入
use crate::sibling::SiblingService;

```

### 3.2 子模块代码行数限制

在运行 `cargo fmt` 格式化后，单个子模块源文件的**有效代码行数**必须满足：**70 行 ≤ 有效行数 ≤ 800 行**。

* **统计规则**：剔除纯空行、单行/多行注释、以及测试代码块（即 `#[cfg(test)]` 标记的模块或代码）。
* **处理要求**：低于 70 行应考虑与同类小模块合并；超过 800 行必须按单一职责拆分子模块。

### 3.3 模块实现（Impl）作用域隔离

模块内部仅允许为**当前文件所定义的结构体或枚举**编写 `impl` 块。

* 严禁跨文件/跨模块为父模块、同级模块或外部 crate 的类型提供固有实现（Inherent impl）。
* **语言内置类型与标准库类型例外**：为 Rust 语言内置类型（如 `bool`、整数、浮点数和数组）或标准库类型（包括 `std`、`core`、`alloc` 中的类型，如 `String`、`Option<T>`、`Vec<T>`、`BTreeMap<K, V>`、`HashMap<K, V>`）实现 Trait 时，不受“类型定义必须位于当前文件”的限制。
* 该例外仅适用于符合 Rust 孤儿规则的 **Trait impl**，不允许为内置类型或标准库类型添加固有实现，也不扩展到仓库自定义类型或第三方 crate 类型；其他结构、可见性和导入规范仍须遵守。

```rust
// ===== 当前模块 (service.rs) =====

// Bad: 跨模块为外部/父级类型实现方法
impl super::ParentState {
    fn update(&mut self) {}
}

impl crate::other::ExternalType {
    fn handle(&self) {}
}

// Good: 仅为当前模块内声明的类型编写实现
struct LocalService;

impl LocalService {
    fn update(&mut self) {}
}

```

### 3.4 字段私有化与函数可见性

* **结构体字段私有化**：
* **纯数据结构（DTO）**：仅当结构体**没有任何 `impl` 实现**（包括固有实现与 Trait 实现）时，其成员字段才允许设为 `pub` 或 `pub(crate)`。
* **业务逻辑结构**：凡是存在 `impl` 实现的结构体，**所有成员变量必须保持私有（private）**，外部访问必须通过方法暴露。

* **独立函数可见性**：子模块内部严禁定义非私有的独立函数（禁止 `pub fn helper()`）。如需对外暴露能力，应将其定义为结构体的关联函数（Associated Functions）或方法。

```rust
// ===== 子模块内部 (child.rs) =====

// Bad: 包含 impl 的结构体对外暴露了公有成员变量
pub struct Worker {
    pub task_id: u64,
}

impl Worker {
    pub fn new(task_id: u64) -> Self {
        Self { task_id }
    }
}

// Bad: 子模块内暴露公开的独立函数
pub fn calculate_hash(data: &[u8]) -> u64 {
    // ...
}

// Good: 无任何 impl 的纯 DTO 允许暴露字段
pub struct DataDto {
    pub id: u64,
    pub name: String,
}

// Good: 包含业务行为的结构体字段严格私有，通过方法暴露
pub struct Worker {
    task_id: u64,
}

impl Worker {
    pub fn new(task_id: u64) -> Self {
        Self { task_id }
    }

    pub fn task_id(&self) -> u64 {
        self.task_id
    }
}

```

---

## 4. 类型与接口质量约束 (Type & Interface Quality)

### 4.1 参数与元组复杂度限制

* **函数与方法参数上限**：独立函数（`fn`）与关联方法（`method`）的入参总数**严禁超过 5 个**。超过 5 个必须重构为配置对象或参数结构体（Params / Options）。
* **闭包参数上限**：闭包参数**严禁超过 5 个**。
* **元组（Tuple）容量上限**：严禁定义超过 2 个成员的元组。包含 3 个或以上元素的组合必须封装为具有明确语义的具名结构体。

```rust
// Bad: 函数参数超过 5 个
fn query_logs(app: &str, level: u8, start: u64, end: u64, limit: usize, offset: usize) {}

// Good: 超过 5 个参数收敛为结构体
struct LogFilter<'a> {
    app: &'a str,
    level: u8,
    start: u64,
    end: u64,
    limit: usize,
    offset: usize,
}

fn query_logs(filter: LogFilter) {}

// Bad: 元组超过 2 个成员
type Point3D = (f64, f64, f64);

// Good: 多于 2 个成员使用具名结构体
struct Point3D {
    x: f64,
    y: f64,
    z: f64,
}

// Good: 允许最多 2 个元素的简单元组（如键值对）
type KeyValue = (String, Vec<u8>);

```

---

## 5. 引用与导入规范 (Import & Use Directives)

### 5.1 禁止行内全限定命名空间调用

严禁在业务代码中直接使用绝对路径调用类型或函数。必须先在文件顶部使用 `use` 显式导入后再调用。

```rust
// Bad: 行内使用全限定路径
let file = std::fs::File::open("config.json")?;
let duration = std::time::Duration::from_secs(5);

// Good: 先导入后使用
use std::{fs::File, time::Duration};

let file = File::open("config.json")?;
let duration = Duration::from_secs(5);

```

### 5.2 合并同前缀导入

相同根路径或前缀的导入项，必须使用花括号合并为单条 `use` 语句，禁止重复书写相同前缀。

```rust
// Bad: 重复前缀
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;

// Good: 前缀合并
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

```

### 5.3 `use` 声明集中置顶

当前文件或作用域内的所有 `use` 语句必须**统一集中放置在文件顶部区域**。严禁将 `use` 语句穿插在常量定义、结构体、函数或业务实现逻辑之间。

```rust
// Bad: use 语句散落穿插在代码中
use std::collections::HashMap;

const DEFAULT_TIMEOUT: u64 = 30;

use std::sync::Arc;

pub struct Client;

```

```rust
// Good: 所有 use 语句严格置顶，且与后续代码块以单空行分隔
use std::{
    collections::HashMap,
    sync::Arc,
    time::Duration,
};

const DEFAULT_TIMEOUT: u64 = 30;

pub struct Client {
    timeout: u64,
}

```

### 5.4 条件编译 `cfg` 导入隔离

* **禁止括号内嵌套 `cfg**`：严禁在 `use { ... }` 内部嵌套使用 `#[cfg(...)]`。
* **按编译条件分组**：相同 `#[cfg(...)]` 条件的 `use` 语句归为一组；不同编译条件的分组之间、以及条件组与无条件导入组之间，必须以**单空行显式分隔**。

```rust
// Bad: 大括号内部嵌套 cfg 属性
use std::{
    collections::HashMap,
    #[cfg(unix)]
    os::unix::fs::PermissionsExt,
};

// Good: 拆解为外层独立 cfg 语句，并按条件隔离分块
use std::collections::HashMap;

#[cfg(target_os = "linux")]
use std::os::unix::fs::{MetadataExt as UnixMetadataExt, PermissionsExt};

#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt as WinMetadataExt;

```

---

> **提交准则**：在提交代码（Commit/PR）之前，必须在本地完整运行测试与检查（包括 `cargo fmt --check`、`cargo clippy -- -D warnings` 与 `cargo test`）。如任一检查未通过，必须修复后方可提交。
