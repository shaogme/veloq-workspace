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
   - Windows 相关命令一律通过 **QEMU Windows 虚拟机** 执行（当前使用 QEMU 而非 `cross`）。
   - 不允许在 Linux 主机直接用 `cargo` 原生跑 Windows 目标。
   - 由 `xtest-runner` 统一管理 QEMU 虚拟机的生命周期、SSH 等待、工作区同步及在虚拟机内部的原生执行。

## 环境设置 (Environment Setup)

在 Linux 上执行 Windows 目标任务前，需准备：

1. 安装 QEMU（`qemu-system-x86_64`）。
2. 安装 `ssh` 与 `sshpass`（推荐通过 devbox 管理，`xtest-runner` 会自动探测并加载 `.devbox/nix/profile/default/bin`）。
3. 准备 Windows 虚拟机镜像：默认路径 `image/win2025-core-rust-gnu.qcow2`（可通过 `image/win2025-core-rust-gnu.json` 配置路径与认证信息）。
4. 硬件虚拟化（推荐）：若 `/dev/kvm` 存在，`xtest-runner` 会自动启用 KVM 硬件加速（`-enable-kvm -cpu host`）；否则自动回退至 TCG 软件仿真。

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


## CI 命令风格 (CI Command Style)

- 禁止在 CI 中使用批处理循环/脚本进行重试或编排。
- 统一使用 `.cargo/config.toml` 中的 `x*` 别名入口。
- 平台差异应收敛在 Rust/Cargo 配置与 `xtest-runner` 内，不要散落在 shell 脚本中。
- 不要为跨平台编译引入包装/存根入口文件；平台后端 crate 保持其目标平台原生形态。


所有命令必须零错误、零告警（按 `-D warnings` 生效）。

## 代码质量要求

- **质量与测试**: 注重代码质量、可测试性和测试覆盖。
- **编码规范**:
    - **禁止长路径**: 禁止在代码中使用全限定命名空间（尤其是以 `crate::` 开头的路径）超过 15 个字符。必须通过 `use` 语句导入后再调用。
    - **合并相同前缀的use语句**: 当有多个`use`语句具有相同前缀时，应合并为一条`use`语句，例如：
    ```rust
    //Bad
    use crate::nix::build;
    use crate::nix::store;
    use crate::nix::path;
    use crate::nix::refpath;
    //Good
    use crate::nix::{
        build,
        store,
        path,
        refpath,
    };
    ```
    - **cfg属性分组与换行隔离**: 所有 `#[cfg(...)]` 内的条件相同的use语句必须放在一起，并且与其他不同条件或不带 `cfg` 的use语句显式使用空行分隔。
    - **禁止在use嵌套导入内使用cfg**: 禁止在 `use {...}` 的 `{...}` 内部使用 `#[cfg(...)]`。

如任一检查未通过，必须先修复再提交。

## 通用 Git 提交信息规范 (Conventional Commits Specification)


### 1. 提交信息结构

每次提交信息由 **标题 (Header)**、**正文 (Body)** 和 **页脚 (Footer)** 组成。各部分之间必须用 **一个空行** 隔开。复杂的改动、Bug 修复、重构、破坏性变更或需要关联 Issue 的提交必须包含完整的正文或页脚。

```text
<type>(<scope>): <subject>

[optional body]

[optional footer(s)]
```

---

### 2. 标题格式 (Header)

标题控制在 **50~72 字符内**，单行展示，保持精练。

#### 2.1 `<type>`（类型，必填）
描述本次变更的性质，**必须全部小写**：
* `feat`: 新增功能 (Feature)
* `fix`: 修复 Bug
* `refactor`: 代码重构（既不修复 Bug 也不添加新功能）
* `perf`: 性能优化 (Performance)
* `style`: 代码格式调整（不影响运行逻辑，如缩进、空格、缺失的分号等）
* `test`: 新增或修改测试代码
* `docs`: 文档变更 (Documentation)
* `build`: 构建系统或外部依赖变更（如 npm, Cargo, Maven, Dockerfile 等）
* `ci`: CI/CD 配置文件与脚本变更（如 GitHub Actions, GitLab CI）
* `chore`: 其它辅助工具变动或例行维护（不修改 src 或 test 目录）
* `revert`: 撤销之前的提交

#### 2.2 `<scope>`（范围，可选）
表示变更影响的模块、组件或业务区域，**必须小写**（如 `auth`, `api`, `ui`, `db`, `config`, `deps`）。若涉及全局可省略或填 `global`。

#### 2.3 `!`（破坏性变更标识，可选）
若包含破坏性变更（BREAKING CHANGE），可在 `<type>` 或 `<scope>` 后添加 `!`，如 `feat(api)!: remove deprecated endpoints`。

#### 2.4 `<subject>`（主题，必填）
简短描述变更核心内容，遵守以下规定：
* **语态与时态**：必须使用**英文现在时祈使句**（如 `add` 而非 `added`，`fix` 而非 `fixed`）。
* **大小写与标点**：首字母小写，结尾**不加句号**或其他标点符号。

---

### 3. 正文格式与深度指南 (Body)

正文是解决“**正文内容过少且描述不全**”问题的核心所在。标题回答“修改了什么”，而正文必须回答“**为什么修改、具体怎么修改、有何影响及如何验证**”。

#### 3.1 正文 4 维写作框架 (Body Standard Framework)

对于非微小改动，正文建议按以下四个维度展开，确保描述充实、逻辑清晰：

1. **背景与动机 (Motivation & Context)**：
   * 详细说明为什么需要本次修改。
   * 描述改动之前的旧逻辑/现象、触发问题的场景或业务需求背景。如果是 Bug，说明根因 (Root Cause)。
2. **核心变更与技术细节 (Technical Implementation)**：
   * 使用无序列表 (`-`) 分点阐述具体的代码实现变动。
   * 涉及核心算法、架构调整、数据模型变动或 API 签名变更时，给予详细解释。
3. **副作用与影响评估 (Side Effects & Risks)**：
   * 改动是否对现有性能、并发、内存或数据库产生影响？
   * 是否引入了新的配置项、环境变量或依赖包？
   * 是否存在潜在的向后兼容风险？
4. **验证与测试覆盖 (Verification & Testing)**：
   * 说明如何验证本次变更的正确性（如添加的单元测试、集成测试或压测结果）。
   * 性能优化类变更需附带优化前后的指标对比数据（如 QPS、延迟、内存占用等）。

#### 3.2 写作规范与自查清单

* **排版格式**：正文每行建议控制在 **72 字符以内**，段落之间空一行，合理使用无序列表提升可读性。
* **语言要求**：提交信息（包含 Header, Body, Footer）**必须统一使用英文**。
* **反模式自查 (Anti-Patterns to Avoid)**：
  * ❌ *错误写法*：正文只重复标题或用一句话概括（例如："Fix user bug."）。
  * ❌ *错误写法*：未交代改动动机，直接粘贴代码 Diff。
  * ✅ *正确做法*：清晰说明问题根由 -> 技术解法 -> 影响点 -> 测试手段。

---

### 4. 页脚格式 (Footer)

页脚通常用于标识 **破坏性变更 (Breaking Changes)** 或关联 **Task / Issue / PR**。

#### 4.1 破坏性变更 (Breaking Changes)
如果包含不兼容改动，页脚**必须**以 `BREAKING CHANGE:` 开头，后跟：
1. 破坏性变动的具体内容说明。
2. **迁移指南 (Migration Guide)**：清晰指导使用者如何修改原有代码以适配新版本。

#### 4.2 Issue 关联与状态变更
使用标准关联词连接 Issue/Task，格式为 `<Token> #<Issue_ID>`：
* 关闭 Issue：`Closes #123`, `Fixes #456`, `Resolves #789`
* 引用/关联 Issue：`Refs #101`, `See also #202`

> **注意**：若不知道或不确定 Issue 关联与状态变更，请不要填写该部分。

---

### 5. 典型场景提交示例 (English Examples)

#### 场景 1：新增复杂功能 (`feat`)

```text
feat(auth): support OAuth2 login with PKCE flow

Motivation & Context:
Users previously relied strictly on standard username/password authentication,
which lacks support for single sign-on (SSO) and third-party login providers.
This change introduces OAuth2 login with Proof Key for Code Exchange (PKCE) to
enhance login security for native and single-page applications.

Key Technical Details:
- Implement `OAuth2PKCEProvider` to manage code verifiers and challenges.
- Add `/api/v2/auth/oauth2/authorize` and `/api/v2/auth/oauth2/callback` endpoints.
- Store short-lived session tokens in HTTP-only, Secure cookies.
- Integrate token auto-refresh logic in the middleware interceptor.

Side Effects & Risks:
- Requires setting environment variables `OAUTH2_CLIENT_ID` and `OAUTH2_CLIENT_SECRET`.
- Database schema updated to include `external_auth_providers` table.

Verification:
- Added unit tests for PKCE challenge generation and validation in `pkce_test.go`.
- Added integration tests covering complete login flow with mock identity provider.

Closes #412
```

#### 场景 2：深度 Bug 修复与根因分析 (`fix`)

```text
fix(db): resolve connection pool exhaustion under high concurrency

Motivation & Context:
Under heavy traffic spikes (>5,000 req/sec), service instances experienced thread
starvation and client timeouts with error "DB connection pool exhausted".
Root cause analysis revealed that failed transaction read operations did not
properly release connections back to the pool due to unhandled promise rejections.

Key Changes:
- Wrap connection acquisition in a `try...finally` block to guarantee connection
  release regardless of query success or failure.
- Decrease connection idle timeout from 30s to 10s to clean up stale connections faster.
- Implement exponential backoff retry mechanism when acquiring pool connections.

Verification:
- Simulated 10,000 concurrent requests in load testing environment.
- Connection leaks were fully eliminated and connection acquisition latency dropped
  from 2,500ms (p99) to 12ms (p99).

Fixes #879
Refs #850
```

#### 场景 3：重构与破坏性变更 (`refactor` + `BREAKING CHANGE`)

```text
refactor(storage): convert synchronous FileStore API to async Promises

Motivation & Context:
The legacy `FileStore` implementation executed synchronous file I/O operations directly
on the main event loop thread. This blocked CPU execution during large file operations
and severely degraded overall application responsiveness.

Technical Changes:
- Migrate all internal `fs.readFileSync` and `fs.writeFileSync` calls to `fs.promises`.
- Update `FileStore.read()`, `FileStore.write()`, and `FileStore.delete()` methods
  to return Promises.
- Remove deprecated synchronous helper functions.

BREAKING CHANGE: All `FileStore` public instance methods are now asynchronous and return Promises.

Migration Guide:
- Update calls from `fileStore.read(path)` to `await fileStore.read(path)` or `.then()`.
- Wrap calls in `async` functions or handle Promise rejections explicitly.

Closes #1052
```

#### 场景 4：性能优化与数据对比 (`perf`)

```text
perf(search): optimize fuzzy search algorithm using trie data structure

Motivation & Context:
The existing search implementation performed linear scanning (`O(N)`) across all user
records on every keystroke. Response latency exceeded 450ms when search space grew
beyond 100,000 records.

Key Changes:
- Replace array scanning with an in-memory Trie (prefix tree) index.
- Pre-build and cache the search index asynchronously upon service startup.
- Add debouncing logic to prevent unnecessary index lookups.

Verification & Metrics:
- Benchmark tests run against 500,000 dataset records:
  - Search latency: reduced from 480ms to 4.2ms (99.1% reduction).
  - Memory consumption: slight increase by ~18MB to maintain Trie nodes in memory.

Resolves #633
```

#### 场景 5：依赖升级与构建变动 (`build` / `chore`)

```text
build(deps): upgrade Webpack to Vite 5 and update build scripts

Motivation & Context:
Webpack 4 build times have grown excessively long (over 4 minutes for cold builds),
slowing down developer workflows and CI deployment pipelines. Modernizing to Vite
leverages native ES modules for near-instantaneous dev server startup.

Key Technical Changes:
- Replace `webpack.config.js` with `vite.config.ts`.
- Update `package.json` scripts (`dev`, `build`, `preview`).
- Configure SVG inline loading plugin to maintain compatibility with legacy icons.
- Update CI pipeline caching keys to match `package-lock.json`.

Verification:
- Development server startup time reduced from 42s to 320ms.
- Production bundle size reduced by 14% due to improved tree-shaking.
- Verified all page routes and asset loading across major browsers.

Refs #1205
```