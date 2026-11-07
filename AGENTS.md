# Coding Guidelines and Instructions for Agents

When making modifications to this repository, please adhere strictly to the following requirements. Failure to run and pass these checks will result in a Continuous Integration (CI) failure.

---

## 1. Core Principles

### 1.1 Facts Before Edits (Read-Before-Modify Principle)

Inferring code logic, dependency chains, or file contents without verification is strictly prohibited. Before performing any code changes, architectural design, or issue troubleshooting, **you must thoroughly inspect all relevant source code and context files first**.

### 1.2 Duty to Proactively Report Defects

If you encounter latent logical flaws, security vulnerabilities, performance bottlenecks, or unhandled edge cases while reviewing or analyzing code, you must report them explicitly. Never conceal or ignore them.

### 1.3 Comment Standards

* **Content Principles**:
* Prohibit redundant or trivial comments (i.e., do not state what the code already clearly expresses).
* Use comments exclusively to explain complex business intent, edge-case handling, or critical algorithmic decisions.
* **Never use comments as work logs, technical draft spaces, refactoring memos, or PR change records**.

```rust
// Bad: Redundant description, or mixed with refactoring/history notes
// Define a counter variable
let mut count = 0;
// [Refactoring Step 2]: Temporarily retained because the old logic caused memory leaks; replace with Arc later.

// Good: Clearly explains business rationale and engineering trade-offs
// Use exponential backoff retry to prevent upstream service cascading failures
let delay = base_delay * 2u32.pow(attempts);

```

### 1.4 Unified Entry Point for Cross-Platform Execution

All `test / clippy / check` commands targeting Linux/Windows **must be delegated exclusively to `xtest-runner**` and invoked via `.cargo/config.toml` aliases. Manually composing custom cross-platform script commands is strictly prohibited.

The current unified entry points are as follows:

```toml
xtest-linux
xtest-windows
xclippy-linux
xclippy-windows
xcheck-linux
xcheck-windows

```

#### Platform Routing Rules (Mandatory)

1. **Windows Host -> Linux Target**:
    * All Linux-related commands must be executed inside Docker.
    * Compiling, checking, or testing for Linux targets directly and natively on a Windows host is not permitted.

2. **Linux Host -> Windows Target**:
    * All Windows-related commands must be executed via a **QEMU Windows Virtual Machine** (currently using QEMU rather than `cross`).
    * Running Windows targets directly and natively with `cargo` on a Linux host is not permitted.
    * `xtest-runner` centrally manages the QEMU VM lifecycle, SSH availability checks, workspace synchronization, and native execution inside the virtual machine.

---

## 2. Code Style

### 2.1 Module File Organization (Strict Rust 2018+)

**Legacy `mod.rs` conventions are strictly forbidden.** All module layouts must adhere to the Rust 2018 Edition hierarchy:

* The entry definition for module `foo` must reside at `foo.rs`.
* If `foo` defines child modules, create a matching directory named `foo/`, and place submodule files inside `foo/`.
* The parent module's implementation logic remains in `foo.rs`; never use `foo/mod.rs`.

```text
# Bad (Legacy Rust 2015 layout)
src/
├── main.rs
└── foo/
    ├── mod.rs
    └── sub.rs

# Good (Idiomatic Rust 2018+ layout)
src/
├── main.rs
├── foo.rs
└── foo/
    └── sub.rs

```

### 2.2 No Lint Bypassing (Zero-Tolerance Policy on Permissive Attributes)

Never introduce global or scoped bypass attributes (such as `#[allow(...)]` or `#![allow(...)]`) to silence Clippy or rustc compiler warnings. **Fix the root cause through proper code design.**

```rust
// Bad: Suppressing warnings with allow
#[allow(clippy::too_many_arguments)]
fn process_order(id: u64, user: String, item: String, qty: u32, price: f64, addr: String) {}

// Good: Refactoring data structures to eliminate excessive arguments
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

### 2.3 Blank Line Isolation for Top-Level and Sibling Definitions

Within the same immediate scope, `item_spacing` compares adjacent definitions of these kinds: `struct`, `enum`, `union`, type alias, trait, constant, `static`, function, and `impl` block. Every checked pair must have exactly one blank line, including an `impl` block next to any other checked kind and two adjacent `impl` blocks, except for the repeated same-kind definitions described below.

**Exceptions:** Two consecutive constants, two consecutive `static` items, or two consecutive type aliases of the same AST kind may have zero or one blank line between them. `const fn` declarations are functions, not constants, so they require exactly one blank line next to every checked kind, including another function. All other checked pairs require exactly one blank line. These rules apply in both directions; for example, both `struct` followed by `impl` and `impl` followed by `struct` require one blank line. Items outside the listed kinds remain structural boundaries: comparisons do not skip over them to reach a later checked item. Pairs whose `cfg` conditions are provably mutually exclusive are not compared.

```rust
const FIRST: u8 = 1;
const SECOND: u8 = 2;

static FIRST_STATIC: u8 = 1;
static SECOND_STATIC: u8 = 2;

type FirstAlias = u8;
type SecondAlias = u8;

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

## 3. Module & Architecture Constraints

### 3.1 Submodule Visibility and Dependency Direction

* **Submodules Are Private by Default**: When a parent module registers a submodule, it must default to private visibility (`mod child;`). `pub` or `pub(crate)` is permitted only if the submodule consists exclusively of module declarations, private/`pub`/`pub(crate)` `use` imports, `macro_rules!` definitions, type aliases, structs, enums, traits, unions, constants, or static variables. Submodules containing functions, `impl` blocks, `extern crate`, scoped imports like `pub(super)` / `pub(in path)`, macro invocations, or unrecognized items do not qualify for this exception.
* **Unidirectional Dependency Control**:
* Sibling submodules may import one another (`use crate::parent::sibling;`).
* **Submodules must never depend backward on private implementations of parent modules**: Do not use `super::*` or absolute crate paths inside a submodule to read unexposed internal details from the parent module. Supply all required context and state explicitly downward via function arguments.

```rust
// ===== Parent Module (parent.rs) =====

// Bad: Exposing a submodule containing executable implementations
pub mod child;

// Good: Default private submodule
mod child;

// Good: A submodule containing purely declarative definitions may be public
pub mod declarations {
    pub struct Record;
}

```

```rust
// ===== Submodule (parent/child.rs) =====

// Bad: Reverse reference to private parent context
use super::ParentContext;

// Good: Depend only on siblings or public abstractions; pass state via arguments
use crate::sibling::SiblingService;

```

### 3.2 Submodule Line Count Bounds

After applying `cargo fmt`, any individual submodule source file must contain at most **800 effective lines of code (ELOC)**. A file without any direct out-of-line child module must also contain at least **70 ELOC**.

* **Measurement Rules**: Blank lines, single/multi-line comment blocks, and test scopes (annotated via `#[cfg(test)]`) are excluded from this count.
* **Remediation**: Files below 70 ELOC with no direct out-of-line child modules should be merged into related modules; files exceeding 800 ELOC must be split into dedicated submodules following the Single Responsibility Principle.

### 3.3 Module Implementation (`impl`) Scope Isolation

An `impl` block inside a module may only target **structs or enums defined within that immediate file**.

* Never write inherent implementations (`impl`) across files or modules for types defined in parent modules, sibling modules, or external crates.
* **Primitive and Standard Library Type Exceptions**: Implementing traits on Rust primitive types (e.g., `bool`, numeric types, floats, arrays) or standard library types from `std`, `core`, and `alloc` (e.g., `String`, `Option<T>`, `Vec<T>`, `BTreeMap<K, V>`, `HashMap<K, V>`) is exempt from the requirement that the target type be declared in the current file.
* This exception strictly applies to **Trait implementations** conforming to Rust's orphan rules. It does not allow inherent implementations on primitive/standard library types, nor does it extend to project-defined types or external crate types located in other modules. Visibility, structural integrity, and import conventions must still be maintained.

```rust
// ===== Current Module (service.rs) =====

// Bad: Implementing methods on external or parent types across module boundaries
impl super::ParentState {
    fn update(&mut self) {}
}

impl crate::other::ExternalType {
    fn handle(&self) {}
}

// Good: Only write implementations for types declared in the current module
struct LocalService;

impl LocalService {
    fn update(&mut self) {}
}

```

### 3.4 Field Encapsulation and Function Visibility

* **Struct Field Encapsulation**:
* **Pure Data Transfer Objects (DTOs)**: Member fields may only be marked `pub` or `pub(crate)` if the struct contains **no `impl` blocks of any kind** (neither inherent nor trait implementations).
* **Domain / Logic Structures**: Any struct carrying an `impl` block must keep **all member fields strictly private**. Expose state externally exclusively through accessor methods.

* **Standalone Function Visibility**: Independent functions in submodules must not be public (no `pub fn helper()`). Expose functional capabilities via associated functions or methods attached to a struct instead.

```rust
// ===== Inside Submodule (child.rs) =====

// Bad: Struct with an impl block exposes public fields
pub struct Worker {
    pub task_id: u64,
}

impl Worker {
    pub fn new(task_id: u64) -> Self {
        Self { task_id }
    }
}

// Bad: Standalone public function exposed from a submodule
pub fn calculate_hash(data: &[u8]) -> u64 {
    // ...
}

// Good: Pure DTO without any impl blocks exposes fields directly
pub struct DataDto {
    pub id: u64,
    pub name: String,
}

// Good: Struct with domain logic keeps fields private and exposes accessor methods
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

### 3.5 Declaration Before Use

Within a single module, **every type declaration (including `struct`, `enum`, `type` aliases, `trait`, `const`, etc.) must appear prior to where it is consumed**. Instantiating, calling, or referencing a type before its declaration statement is strictly forbidden. The file must be structured so that definitions sit on top, with consumers and logic placed below.

```rust
// Bad: Referenced in a function/variable before being declared
fn process_payload(payload: RequestPayload) {
    let _timeout = DEFAULT_TIMEOUT_MS;
    // ...
}

const DEFAULT_TIMEOUT_MS: u64 = 5000;

struct RequestPayload {
    data: String,
}

// Good: All types and constants are declared before being used
const DEFAULT_TIMEOUT_MS: u64 = 5000;

struct RequestPayload {
    data: String,
}

fn process_payload(payload: RequestPayload) {
    let _timeout = DEFAULT_TIMEOUT_MS;
    // ...
}

```

### 3.6 External Module Paths Must Not Traverse Parent Directories

In external module declarations (i.e., `mod name;` ending with a semicolon), the `#[path]` string literal, once decoded per Rust string literal rules, must not contain a `..` parent-directory component. Both `/` and `\` are treated as path separators. Names such as `foo..rs` or `.../child.rs` are not considered parent-directory components. Even if a path resolves inside the base directory after canonicalization, the explicit presence of a `..` component is strictly prohibited.

This constraint also applies to nested `path` attributes inside `cfg_attr`, regardless of whether the target configuration, target platform, or feature flag is active. It does not apply to inline modules (defined with braces: `mod name { ... }`).

```rust
// Bad: The path contains a parent-directory component
#[path = "../shared.rs"]
mod shared;

// Good: The path resolves strictly within the current directory
#[path = "shared.rs"]
mod shared;

```

### 3.7 Avoid Unnecessary Crate Visibility

Give an item or associated item `pub(crate)` only if it is referenced from outside its declaring module and that module's descendants, or if another crate-visible API signature that must remain available refers to it. If its references are confined to its declaring module tree, use private visibility instead.

This rule applies only to named declarations whose visibility is exactly `pub(crate)`: module declarations (`mod`); `extern crate` names and individual `use` aliases; structs, enums, unions, type aliases, traits, functions, constants, and statics; foreign functions and statics; and associated functions, type aliases, constants, or statics in traits and `impl` blocks. It does not report `pub`, `pub(super)`, or other `pub(in path)` visibility, struct fields, enum variants, or glob imports themselves.

```rust
// Bad: Both items are used only within their private module.
mod parser {
    pub(crate) struct State;
    pub(crate) fn reset() {}

    fn parse() {
        let _ = State;
        reset();
    }
}

// Good: A sibling module uses State, so it needs crate visibility.
mod parser {
    pub(crate) struct State;
}

mod driver {
    use crate::parser::State;

    fn run(_: State) {}
}
```

When a crate-visible API declaration must remain accessible, types and other items referenced by its signature may also need `pub(crate)`. Do not reduce visibility based on an incomplete view: unresolved syntax or macro invocations can hide references, so retain visibility when usage cannot be determined reliably.

### 3.8 Explicit Path-Restricted Visibility Is Forbidden

Never use explicit `pub(in path)` visibility on items, `use` imports, foreign items, fields, or associated declarations. Choose private visibility, `pub`, `pub(crate)`, `pub(super)`, or `pub(self)` according to the intended access boundary. If these forms cannot express the required boundary, restructure the module or API.

```rust
// Bad: Explicit path-restricted visibility
mod service {
    pub(in crate::service) struct Request;

    pub(in crate::service) use crate::model::Record;

    pub struct Response {
        pub(in crate::service) status: u16,
    }
}

// Good: Use supported visibility forms
mod service {
    pub(crate) struct Request;

    pub(super) use crate::model::Record;

    pub struct Response {
        pub(self) status: u16,
    }
}
```

---

## 4. Type & Interface Quality

### 4.1 Parameter and Tuple Complexity Limits

* **Function & Method Parameter Ceiling**: Free functions (`fn`) and associated methods (`method`) **must not take more than 5 parameters**. When parameter counts exceed 5, refactor them into a structured parameter object or configuration struct (`Params` / `Options`).
* **Closure Parameter Ceiling**: Closures **must not take more than 5 parameters**.
* **Tuple Capacity Ceiling**: Tuples with more than 2 elements are forbidden. Combinations of 3 or more elements must be packaged into a named struct with distinct, semantic field names.

```rust
// Bad: Function exceeds 5 parameters
fn query_logs(app: &str, level: u8, start: u64, end: u64, limit: usize, offset: usize) {}

// Good: Parameters consolidated into a dedicated struct
struct LogFilter<'a> {
    app: &'a str,
    level: u8,
    start: u64,
    end: u64,
    limit: usize,
    offset: usize,
}

fn query_logs(filter: LogFilter) {}

// Bad: Tuple with more than 2 members
type Point3D = (f64, f64, f64);

// Good: Use a named struct for multi-element data
struct Point3D {
    x: f64,
    y: f64,
    z: f64,
}

// Good: Simple 2-element tuples (e.g., key-value pairs) are permitted
type KeyValue = (String, Vec<u8>);

```

---

## 5. Import & Use Directives

### 5.1 Prohibit Inline Fully-Qualified Namespace Calls

Direct invocations using inline fully-qualified paths in business logic are strictly forbidden. Always import the target symbol explicitly with a `use` statement at the top of the file before invoking it.

```rust
// Bad: Inline fully-qualified path call
let file = std::fs::File::open("config.json")?;
let duration = std::time::Duration::from_secs(5);

// Good: Import first, then consume
use std::{fs::File, time::Duration};

let file = File::open("config.json")?;
let duration = Duration::from_secs(5);

```

### 5.2 Merge Imports with Shared Prefixes

Imports sharing the same root path or prefix must be unified into a single grouped `use` statement using nested braces. Do not repeat identical prefixes across multiple statements.

```rust
// Bad: Repeated prefixes
use std::fs::File;
use std::fs::OpenOptions;
use std::path::Path;
use std::path::PathBuf;

// Good: Consolidated prefixes
use std::{
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
};

```

### 5.3 Consolidate `use` Statements at the Top

All `use` declarations in a file or scope must be **grouped cleanly at the top of the file**. Never interleave `use` statements between constants, structs, functions, or implementation code.

```rust
// Bad: use statements scattered throughout the file
use std::collections::HashMap;

const DEFAULT_TIMEOUT: u64 = 30;

use std::sync::Arc;

pub struct Client;

```

```rust
// Good: All use statements placed at the top, isolated by an empty line
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

### 5.4 Conditional Compilation (`cfg`) Import Isolation

* **No Inlined `cfg` Inside Braces**: Never nest `#[cfg(...)]` attributes inside a braced `use { ... }` group.
* **Group by Compilation Predicates**: Place `use` statements sharing the same `#[cfg(...)]` condition into distinct groups. Separate conditional blocks from one another and from unconditional imports with **exactly one blank line**.

```rust
// Bad: Inlined cfg inside curly braces
use std::{
    collections::HashMap,
    #[cfg(unix)]
    os::unix::fs::PermissionsExt,
};

// Good: Split into top-level statements grouped by target cfg
use std::collections::HashMap;

#[cfg(target_os = "linux")]
use std::os::unix::fs::{MetadataExt as UnixMetadataExt, PermissionsExt};

#[cfg(target_os = "windows")]
use std::os::windows::fs::MetadataExt as WinMetadataExt;

```

### 5.5 Module Scope Declaration Order

Top-level declarations in a module scope must adhere strictly to this sequence: `extern crate` statements, external module declarations (`mod name;`), `use` import blocks, followed by the remaining items. Each group may contain zero or more declarations. Outer attributes like `#[cfg(...)]` or `#[path = "..."]` remain attached to their respective declaration. Inline modules (`mod name { ... }`) are categorized as general items, and nested `use` statements within blocks follow standard block-level import rules.

```rust
extern crate legacy_dep;
extern crate another_dep as alias;

mod alpha;
mod beta;

use crate::alpha::Alpha;
use alias::Thing;

fn run() {}
```

```rust
mod alpha;
extern crate legacy_dep;

use crate::alpha::Alpha;
fn run() {}
use crate::beta::Beta;
```

---

> **Submission Guidelines**: Prior to committing code or opening a pull request, run all verification suites locally (including `cargo fmt --check`, `cargo clippy -- -D warnings`, and `cargo test`). If any check fails, resolve the issues completely before pushing.
