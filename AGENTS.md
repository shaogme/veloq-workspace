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
   cargo fmt --all
   ```

2. **Linux Target Checks:**
   Run clippy (linter) and check (compiler) for the native Linux environment.
   ```bash
   cargo clippy --all-targets -- -D warnings
   cargo check
   ```

3. **Windows Target Checks:**
   Run clippy and check for the Windows environment.
   ```bash
   cargo clippy --all-targets --target x86_64-pc-windows-gnu -- -D warnings
   cargo check --target x86_64-pc-windows-gnu
   ```

All of these commands must complete without warnings or errors. If they do not, you must fix the issues before submitting your changes.
