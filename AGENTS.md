# Coding Guidelines and Instructions for Agents

When making modifications to this repository, please adhere to the following strict requirements. Failure to run and pass these checks will result in a Continuous Integration (CI) failure.

**IMPORTANT:** Always use Simplified Chinese (简体中文) when communicating and providing explanations.

## Environment Setup

To run tests and checks for the Windows target natively on Linux, you must install the `cross` tool and add the required `rustup` toolchain:

1. **Install Cross:**
   ```bash
   cargo install cross
   ```

2. **Add Windows GNU Target Toolchain:**
   ```bash
   rustup target add x86_64-pc-windows-gnu
   ```

*Note:* If you encounter a cross-device link error (`os error 18`) during the toolchain installation or update in a Docker container, you can bypass it using the following workaround for your `.rustup` directory or by explicitly defining the temp directory on the same filesystem:
```bash
RUSTUP_HOME=$HOME/.rustup TMPDIR=$HOME/.rustup/tmp rustup target add x86_64-pc-windows-gnu
```
Alternatively, for `cross` executions:
```bash
CROSS_SKIP_AUTO_UPDATE=1 cross test --target x86_64-pc-windows-gnu
```

## Pre-commit Checks

Before finalizing any changes, you **MUST** execute the following commands to format the code and run the linter and compiler checks for both Linux and Windows targets:

1. **Format Code:**
   Ensure all code is correctly formatted.
   ```bash
   cargo xfmt
   ```

2. **Linux Target Checks:**
   Run clippy (linter) and check (compiler) for the native Linux environment.
   ```bash
   cargo xclippy
   cargo xcheck
   ```

3. **Windows Target Checks:**
   Run clippy and check for the Windows environment.
   ```bash
   cargo xclippy-win
   cargo xcheck-win
   ```

4. **Tests:**
   Use cargo xtest style aliases; do not use shell loops or batch scripts.
   ```bash
   # Linux
   cargo xtest-linux

   # Windows
   cargo xtest-windows
   ```

## CI Command Style

- **Do not use batch loops/scripts** in CI for retries or command orchestration.
- Use `cargo xtest-*` style aliases from `.cargo/config.toml` as the unified entrypoint.
- If platform-specific behavior is needed, keep it in Rust/Cargo configuration, not shell batching.
- Do not add wrapper/stub entry files just for cross-platform compilation. Keep platform backend crates native to their target.

All of these commands must complete without warnings or errors. If they do not, you must fix the issues before submitting your changes.
