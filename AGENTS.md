# Coding Guidelines and Instructions for Agents

When making modifications to this repository, please adhere to the following strict requirements. Failure to run and pass these checks will result in a Continuous Integration (CI) failure.

**IMPORTANT:** Always use Simplified Chinese (简体中文) when communicating and providing explanations.

## 核心原则 (Core Principles)

1. **回复语言**：始终使用**中文**回复。
2. **代码风格**：
   - **严禁使用 `mod.rs`**。必须遵守 Rust 2018 Edition 及更新版本的目录结构标准。
   - 模块 `foo` 应定义在 `foo.rs` 中；若有子模块，创建 `foo/` 目录，但父模块代码仍保留在 `foo.rs`，而非 `foo/mod.rs`。
3. **禁止猜测**：严禁猜测代码逻辑或文件内容；修改或回答前必须先读取相关代码。
4. **主动报告**：阅读代码时应主动报告潜在错误、安全漏洞、性能问题。
5. **绝对路径**：使用文件修改工具时（如 `write_to_file`、`replace_file_content`），**必须**使用**绝对路径**。
6. **Rust Edition 2024**：充分利用 Rust 2024 新特性，特别是异步闭包和 `AsyncFnOnce` / `AsyncFnMut` / `AsyncFn`，避免手动装箱 `Future`。

## alloy-check 使用规范 (Alloy Check Usage)

- **Windows 运行方法**：`alloy-check` 在 Windows 下运行的唯一正确方式是：
  ```powershell
  $out = (alloy-check | Out-String); if($out.Length -gt 10000){$out.Substring(0,10000) + "...[Truncated]"}else{$out}
  ```
- **文档说明**：`alloy-check` 不需要任何额外文档，以运行结果为准。
- **搜索限制**：严禁在当前目录搜索 `alloy` 或 `alloy-check` 关键字。
- **禁止操作**：
  - 严禁运行 `Get-Command alloy-check`。
  - 严禁直接运行 `alloy-check`。
  - 未明确要求时，严禁运行任何包含 `alloy-check` 的命令。

## 跨平台执行统一入口 (Unified Cross-Platform Entrypoint)

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

## 平台路由强制规则 (Platform Routing Rules)

1. **Windows 主机 -> Linux 目标**：
   - Linux 相关命令一律在 Docker 内执行。
   - 不允许在 Windows 主机直接原生运行 Linux 目标的编译/检查/测试。

2. **Linux 主机 -> Windows 目标**：
   - Windows 相关命令一律通过 `cross` 执行。
   - 不允许在 Linux 主机直接用 `cargo` 原生跑 Windows 目标。
   - `cross` 执行必须带上：
     ```bash
     CROSS_SKIP_AUTO_UPDATE=1
     ```

## 环境设置 (Environment Setup)

在 Linux 上执行 Windows 目标任务前，需准备：

1. 安装 `cross`：
   ```bash
   cargo install cross
   ```

2. 添加 Windows GNU 目标工具链：
   ```bash
   rustup target add x86_64-pc-windows-gnu
   ```

若遇到跨设备链接错误（`os error 18`），使用：

```bash
RUSTUP_HOME=$HOME/.rustup TMPDIR=$HOME/.rustup/tmp rustup target add x86_64-pc-windows-gnu
```

`cross` 示例：

```bash
CROSS_SKIP_AUTO_UPDATE=1 cross test --target x86_64-pc-windows-gnu
```

## 提交前检查 (Pre-commit Checks)

在最终确定任何变更前，必须依次执行并通过：

1. 格式化：
   ```bash
   cargo xfmt
   ```

2. Linux 目标：
   ```bash
   cargo xclippy-linux
   cargo xcheck-linux
   cargo xtest-linux
   ```

3. Windows 目标：
   ```bash
   cargo xclippy-windows
   cargo xcheck-windows
   cargo xtest-windows
   ```

所有命令必须零错误、零告警（按 `-D warnings` 生效）。

## CI 命令风格 (CI Command Style)

- 禁止在 CI 中使用批处理循环/脚本进行重试或编排。
- 统一使用 `.cargo/config.toml` 中的 `x*` 别名入口。
- 平台差异应收敛在 Rust/Cargo 配置与 `xtest-runner` 内，不要散落在 shell 脚本中。
- 不要为跨平台编译引入包装/存根入口文件；平台后端 crate 保持其目标平台原生形态。

如任一检查未通过，必须先修复再提交。
