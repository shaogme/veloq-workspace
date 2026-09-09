# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

Veloq 是一个基于 **完成式（completion-based）I/O** 与 **Thread-per-Core** 模型的 Rust 异步运行时，后端为 Linux `io_uring` 与 Windows IOCP/RIO。Rust Edition 2024，workspace `resolver = "3"`。

`AGENTS.md` 与 `.agent/rules/rust-principles.md` 是本仓库的强制规则源，本文件是其摘要 + 架构补充；两者冲突时以 `AGENTS.md` 为准。

## 沟通与代码风格（强制）

- **始终使用简体中文回复与解释。**
- **严禁 `mod.rs`**：模块 `foo` 写在 `foo.rs`，子模块放 `foo/` 目录下。全仓库无一例外。
- **禁止猜测**：修改/回答前必须先读相关代码；阅读中发现的 bug、UB、性能问题要主动报告。
- 文件修改工具必须使用**绝对路径**。
- 充分利用 Edition 2024 的异步闭包与 `AsyncFn*` trait，不要手动 `Box<dyn Future>`。
- 禁止在代码里写超过 15 字符的全限定路径（尤其 `crate::` 开头），先 `use` 再调用。
- 相同前缀的 `use` 必须合并成嵌套形式；相同 `#[cfg(...)]` 条件的 `use` 分组并用空行隔开；**禁止在 `use {...}` 的花括号内使用 `#[cfg]`**。
- `-D warnings` 全局生效（`[workspace.lints.rust] warnings = "deny"`），另外 `clippy::cognitive_complexity` 为 deny。

## 命令

跨平台 `test / clippy / check` **一律走 `xtest-runner`**（`.cargo/config.toml` 中的别名），禁止自行拼跨平台脚本：

```bash
cargo xfmt              # cargo fmt --all
cargo xcheck-linux      # / xcheck-windows
cargo xclippy-linux     # / xclippy-windows  (--all-targets -D warnings)
cargo xtest-linux       # / xtest-windows    (nextest，连续跑 20 轮)
```

提交前必须依次全绿：`xfmt` → `xclippy-{linux,windows}` → `xcheck-{linux,windows}` → `xtest-{linux,windows}`。

`xtest-runner` 的平台路由（自动判定，无需手工干预）：

- **Windows 主机 → Linux 目标**：通过 `docker compose run --rm standalone` 在容器内重新调用自身；不允许在 Windows 上原生跑 Linux 目标。
- **Linux 主机 → Windows 目标**：通过 QEMU Windows 虚拟机执行（使用 QEMU 而非 `cross`），由 `xtest-runner` 自动拉起 QEMU、等待 SSH 就绪、同步工作区并在虚拟机内部原生调用自身（依赖 `qemu-system-x86_64`、`ssh`/`sshpass` 及 `image/win2025-core-rust-gnu.qcow2` 镜像；存在 `/dev/kvm` 时自动开启 KVM 硬件加速）。
- 原生目标：`cargo nextest run --workspace --exclude <对端后端 crate> --test-threads 1 --run-ignored all`，先 `--no-run` 预构建并预热 trybuild。

常用参数：`--task {test,clippy,check}`、`--target {linux,windows}`、`-n/--count <次数>`（test 默认 20，用 `-n 1` 快速验证）、`--features`、`--quiet`。例：

```bash
cargo run -p xtest-runner -- --task test --target windows -n 1
```

### 单测 / 局部验证

`xtest-*` 是全量入口，迭代时可在**当前原生平台**直跑（nextest 全局超时 20s，见 `.config/nextest.toml`）：

```bash
cargo nextest run -p veloq --test fs                       # 单个测试目标
cargo nextest run -p veloq --test fs test_file_integrity    # 单个测试
cargo test -p veloq-runtime --test compile_tests            # trybuild UI 测试（tests/ui/）
```

Linux 上跑 io_uring 相关测试需要放开 memlock：`sudo prlimit --pid $$ --memlock=unlimited:unlimited`。

### Loom 并发模型检查

`veloq-sync` / `veloq-waker` / `veloq-std` / `veloq-driver-core` 的无锁数据结构有 Loom 测试，**不在 `xtest-*` 里**，需单独跑（CI 仅在 PR 上跑）：

```bash
cargo test --release -p veloq-sync --tests --features loom
```

### Bench / 示例

```bash
cargo bench -p veloq --bench fs_benchmark
cargo run -p disk-bench --release -- --help
```

Linux 相关工作也可用 `docker-compose up -d --build dev`（源码挂载到 `/root/workspace`，SSH `root@localhost:2222`，密码 `root`），详见 `README_DEV.md`。

## 架构

分层从下往上，**依赖是单向的**：

```
veloq-std / veloq-storage / veloq-tls / veloq-pod / veloq-hash / veloq-intrusive-linklist / veloq-waker / veloq-wheel
        ↓
veloq-buf (FixedBuf + 注册式内存池)   veloq-driver-core (Slot/Op/Completion 抽象)
        ↓                                   ↓
                        veloq-driver-{uring,iocp} → veloq-driver-native (cfg 选平台)
        ↓
veloq-runtime (与驱动无关的 thread-per-core 调度器 + 结构化并发)
        ↓
veloq (面向用户的门面：fs / net / time / io / runtime)
```

### veloq-runtime：驱动无关的调度核心

`veloq-runtime` **不认识任何 I/O 后端**。它对每个 worker 的额外状态泛型化为 `RuntimeShared<T>`，并通过三个函数指针钩子把 I/O 语义外置：

- `worker_factory: fn(worker_id, &RuntimeShared<T>) -> T` —— 每个 worker 线程构造自己的 `T`（存入 `extra_tls`）。
- `idle_hook`（`IdleDecision` / `IdleWaitStrategy`）—— 无任务时先让上层 poll 完成队列。
- `park_hook` —— 真正阻塞等待时交给上层（驱动）挂起线程。

`veloq` crate 正是用这套钩子把驱动接进来：`WorkerState { driver, buf_pool, registrar_state, .. }` 作为 `T`，`poll_current_driver` / `park_current_driver` 作为 idle/park 钩子（见 `crates/veloq/src/runtime.rs` 与 `runtime/context.rs`）。**新增运行时能力时优先考虑能否用钩子表达，而不是让 `veloq-runtime` 反向依赖驱动。**

### 结构化并发（scope）

任务生命周期由 scope 树管理，不是 detached spawn：入口 `Runtime::builder(topology).worker_count(..).scope(async |ctx| ..)` 或 `.block_on(..)`，内部用 `scope!(ctx, ..)` / `scope_local!(ctx, ..)` 派生子作用域；`scope.wait_all()` 保证子任务在作用域结束前完成，panic 与取消沿 scope 树传播（`crates/veloq-runtime/src/scope.rs`、`scope/{completion,guard,join,router}.rs`）。`select!` 默认公平轮询（TLS `FastRand` 随机起点），`biased;` 切换为声明序——完成式 I/O 下 biased 可减少无谓的提交/资源分配。

### Send / Local 双形态 + Storage 策略泛型

跨线程与线程内两套实现不是复制粘贴，而是对 `veloq-storage` 的 `Storage` trait 做泛型：`AtomicStorage`（原子，`ThreadSafeStorage`）与 `LocalStorage`（`Cell` 系，`LocalOnlyStorage`）提供同名的 `Usize / OptionPtr / Lock / OptionBox / OptionArc` 关联类型。于是 `GenericTaskHeader<S>` 特化出 `TaskHeader` / `LocalTaskHeader`，`GenericScopeCompletion` 特化出 send/local 版本。写新的运行时内部结构时沿用这个模式，而不是新开一条并行实现。

对应地，通道也分两套：`veloq-sync`（跨线程，`no_std` + Loom 可验证）与 `veloq-local`（线程内 `Rc`/`RefCell`，零原子）。

### 驱动层：Slot / Op / Completion

`veloq-driver-core` 定义平台中立抽象，后端只填关联类型：

- `SlotSpec`：把 `Op / UserPayload / PlatformData / Sidecar / Error / Completion / CompletionDiagnostics` 一次性绑定；`SlotTable` 管理 slot 的类型状态（`Reserved` → `InFlightWaiting` / `InFlightOrphaned` → `Completed`）。
- `IntoPlatformOp<Spec>`：用户操作 → 内核操作 + payload 的擦除/还原，`Op` future 负责提交与完成取值。
- `IoFd` 是**注册描述符索引 + generation**，不是裸 fd —— 完成式模型下句柄通过 registry 注册后使用。
- 完成异常走轻量 `CompletionAnomalyKind`（热路径不搬运完整 `CompletionAnomaly`），仅在有 token/raw 上下文的边界处物化。
- `PlatformOp::completion_cleanup` / `orphan_cleanup` 处理取消后内核仍会写回的悬挂完成（**这是完成式模型最容易出错的地方**：取消不等于结束，buffer 必须活到内核真正完成）。

`veloq-driver-native` 用 `#[cfg]` 把 `UringDriver` / `IocpDriver` 统一别名为 `PlatformDriver`。**平台后端 crate 保持目标平台原生形态，不要为跨平台编译加 wrapper/stub 入口文件**；`xtest-runner` 靠 `--exclude` 排除对端后端。

### 缓冲区所有权

完成式 I/O 要求 buffer 在内核操作期间保持有效且地址稳定，因此 API 是**所有权转移式**的：`AsyncBufRead::read(&self, buf: FixedBuf) -> Result<(usize, FixedBuf), _>`。`FixedBuf` 自带池上下文（`PoolKind + context`），不用侵入式前置 header，以满足 Direct I/O 的 512B/4096B 对齐要求。内存池按 worker 由 `PoolTopology`（如 `UniformSlot::new(ThreadMemoryMultiplier(nz!(4)))`）构建，新 chunk 通过 `RegistrarDispatcher` 广播给各 worker 驱动去注册（`BufferRegistrar`）。

### no_std 与 Loom

`#![no_std]` 的 crate：`veloq-buf` / `veloq-storage` / `veloq-tls` / `veloq-pod` / `veloq-hash` / `veloq-blocking` / `veloq-intrusive-linklist`（`veloq-std` 自身是 `cfg_attr(not(feature = "std"), no_std)`）。这些 crate 加上 `veloq-sync` / `veloq-driver-core` / `veloq-runtime`（它们带 `loom` / `std` feature）统一从 **`veloq-std`** 取 `sync/atomic/thread/time/cell/collections` 等设施——`veloq-std` 在 `loom` feature 下切换到 loom 的原语，这是 Loom 测试能覆盖真实代码的前提。**在这些 crate 里不要直接 `use std::...`，也不要绕过 `veloq-std` 直接 `use core::sync::atomic`**，否则 Loom 检查会失效。`loom` / `std` feature 需要沿依赖链逐层透传（见各 `Cargo.toml` 的 feature 列表）。

### 错误处理

统一使用 `diagweave`：`set!` / `union!` 宏声明错误枚举，`Report<E>` 承载上下文（`.with_ctx(k, v)`、`.trans()`、`.compact()`）。驱动层通过 `DriverError::from_core_report` 把 `DriverCoreError` 提升为后端错误类型。运行时初始化失败走 `Result`（如 `Runtime::block_on -> Result<R>`）而不是 panic。

## 测试布局

- `crates/veloq/tests/`：端到端集成测试（`fs.rs` / `tcp.rs` / `udp.rs` / `time.rs` / `buffer_test.rs` / `runtime_context.rs`，以及 `sync.rs`+`sync/`、`local.rs`+`local/` 的通道测试）。典型写法是包一个 `run_with_runtime(async |ctx| ..)` helper。
- `crates/veloq-runtime/tests/`：scope/任务/panic/取消/`select!` 行为，`compile_tests.rs` + `tests/ui/` 是 trybuild 编译期断言。
- `crates/veloq-sync/tests/`、`crates/driver/core/tests/loom_completion.rs`、`crates/utils/veloq-std/tests/`：`loom_*.rs` 只在 `--features loom` 下有意义。
- 平台内核要求见 `README.md`（最低 Linux 5.6，推荐 6.1+ 以启用 `DEFER_TASKRUN` 等优化；旧内核自动回退）。

## 提交信息

`<type>(<scope>): <subject>`，type ∈ `feat|fix|refactor|perf|style|test|chore`，scope 小写（`sync` / `runtime` / `scope` / `completion` / `driver` / `driver-core` / `tls` / `hash` …），subject 用祈使句现在时、结尾不加句号。`refactor` / `perf` / `feat` **必须**写正文，按「背景动机 → 核心设计与改动要点 → API/破坏性变动 → 测试与后端配套更新」组织。完整示例见 `AGENTS.md`。
