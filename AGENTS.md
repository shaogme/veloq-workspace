# Coding Guidelines and Instructions for Agents

When making modifications to this repository, please adhere to the following strict requirements. Failure to run and pass these checks will result in a Continuous Integration (CI) failure.

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
