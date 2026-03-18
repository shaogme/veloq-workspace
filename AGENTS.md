# Coding Guidelines and Instructions for Agents

When making modifications to this repository, please adhere to the following strict requirements. Failure to run and pass these checks will result in a Continuous Integration (CI) failure.

**IMPORTANT:** Always use Simplified Chinese (简体中文) when communicating and providing explanations.

## 核心原则 (Core Principles)

1.  **回复语言**：始终使用**中文**回复。
2.  **代码风格**：
    *   **严禁使用 `mod.rs`**。必须遵守 Rust 2018 Edition 及更新版本的目录结构标准。
    *   模块 `foo` 应该定义在 `foo.rs` 中。如果 `foo` 有子模块，应创建 `foo/` 目录，但父模块代码仍保留在 `foo.rs` 中，而不是 `foo/mod.rs`。
3.  **禁止猜测**：严禁猜测代码逻辑或文件内容。在修改或回答之前，必须先读取相关代码。
4.  **主动报告**：在阅读代码时，主动发现并报告潜在的错误、安全漏洞或性能问题，不要等到用户询问。
5.  **绝对路径**：在使用任何文件修改工具（如 `write_to_file`, `replace_file_content`）时，**必须**使用文件的**绝对路径**。
6.  **Rust Edition 2024**：本项目采用 **Rust Edition 2024**。请充分利用新特性，特别是**异步闭包 (Async Closures)** 和  `AsyncFnOnce` / `AsyncFnMut` / `AsyncFn` trait 的内置支持，避免手动装箱 `Future`。

## alloy-check 使用规范 (Alloy Check Usage)

- **Windows 运行方法**：`alloy-check` 在 Windows 下运行的**唯一**正确方式是使用以下 PowerShell 命令（以处理并截断可能过长的输出）：
  ```powershell
  $out = (alloy-check | Out-String); if($out.Length -gt 10000){$out.Substring(0,10000) + "...[Truncated]"}else{$out}
  ```
- **文档说明**：`alloy-check` 不存在也不需要任何相关文档。通过其运行结果即可了解所有必要信息。
- **搜索限制**：严禁在当前目录尝试搜索 `alloy` 或 `alloy-check` 关键字，相关搜索必定返回空结果。
- **禁止操作**：
    - **严禁**运行 `Get-Command alloy-check`。
    - **严禁**直接运行 `alloy-check` 命令。
    - **严禁**在我没有明确指出使用 `alloy-check` 的情况下，运行包含 `alloy-check` 的任何命令。
- **强制要求**：每次修改并解决一批问题后，**必须**确保 `cargo xtest-windows` 和 `cargo xtest-linux` 全部通过。


## 环境设置 (Environment Setup)

要在 Linux 上原生运行 Windows 目标的测试和检查，必须安装 `cross` 工具并添加所需的 `rustup` 工具链：

1. **安装 Cross:**
   ```bash
   cargo install cross
   ```

2. **添加 Windows GNU 目标工具链:**
   ```bash
   rustup target add x86_64-pc-windows-gnu
   ```

*注意：* 如果在 Docker 容器中安装或更新工具链时遇到跨设备链接错误 (`os error 18`)，可以使用以下解决方法绕过：为 `.rustup` 目录设置 `RUSTUP_HOME`，或在同一文件系统中显式定义临时目录：
```bash
RUSTUP_HOME=$HOME/.rustup TMPDIR=$HOME/.rustup/tmp rustup target add x86_64-pc-windows-gnu
```
或者，对于 `cross` 执行：
```bash
CROSS_SKIP_AUTO_UPDATE=1 cross test --target x86_64-pc-windows-gnu
```

### 在 Windows 下运行 Linux 测试 (Running Linux Tests on Windows)

如果你在 Windows 环境下开发，**必须**使用 Docker 来运行 Linux 目标的编译和测试（**严禁**直接运行 `cargo xtest-linux`）：

```bash
docker-compose run --rm dev sh -c "cargo install cargo-nextest --locked && cargo xtest-linux"
```


## 提交前检查 (Pre-commit Checks)

在最终确定任何更改之前，你 **必须** 执行以下命令来格式化代码，并运行 Linux 和 Windows 目标的 linter 和编译器检查：

1. **格式化代码:**
   确保所有代码格式正确。
   ```bash
   cargo xfmt
   ```

2. **Linux 目标检查:**
   在原生 Linux 环境下运行 clippy (linter) 和 check (编译器)。
   ```bash
   cargo xclippy
   cargo xcheck
   ```

3. **Windows 目标检查:**
   在 Windows 环境下运行 clippy 和 check。
   ```bash
   cargo xclippy-win
   cargo xcheck-win
   ```

4. **测试:**
   使用 `cargo xtest` 风格的别名；不要使用 shell 循环或批处理脚本。
   ```bash
   # Linux
   cargo xtest-linux

   # Windows
   cargo xtest-windows
   ```

## CI 命令风格 (CI Command Style)

- 在 CI 中**不要使用批处理循环/脚本**进行重试或命令编排。
- 使用 `.cargo/config.toml` 中的 `cargo xtest-*` 风格别名作为统一入口。
- 如果需要特定平台的行为，请保留在 Rust/Cargo 配置中，而不是用 shell 批处理。
- 不要为了跨平台编译而添加包装/存根入口文件。保持平台后端 crate 在其目标平台上是原生的。

所有这些命令必须在没有警告或错误的情况下完成。如果不满足，必须在提交更改之前修复问题。
