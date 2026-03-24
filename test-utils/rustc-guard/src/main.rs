use std::env;
use std::path::Path;
use std::process::{Command, ExitCode};

const XRUNNER_TOKEN_ENV: &str = "VELOQ_XRUNNER_GUARD";
const GUARD_LAUNCHER_TOKEN_ENV: &str = "VELOQ_GUARD_FROM_LAUNCHER";

fn main() -> ExitCode {
    let mut args = env::args_os();
    let _wrapper_path = args.next();
    let Some(first_arg) = args.next() else {
        eprintln!("[rustc-guard] 缺少参数");
        return ExitCode::FAILURE;
    };

    if first_arg == "--warmup" {
        return ExitCode::SUCCESS;
    }

    let real_rustc = first_arg;
    let rustc_args = args.collect::<Vec<_>>();

    if !from_guard_launcher() {
        eprintln!(
            "[rustc-guard] 禁止直接执行 rustc-guard 可执行文件。请通过 cargo xcheck-*/xclippy-*/xtest-* 触发。"
        );
        return ExitCode::FAILURE;
    }

    if is_forbidden_direct_invocation(&real_rustc, &rustc_args) && !from_xtest_runner() {
        eprintln!(
            "[rustc-guard] 检测到直接调用 cargo check/clippy/test，被策略拒绝。请使用 cargo xcheck-*/xclippy-*/xtest-*。"
        );
        return ExitCode::FAILURE;
    }

    let status = match Command::new(real_rustc).args(rustc_args).status() {
        Ok(status) => status,
        Err(error) => {
            eprintln!("[rustc-guard] 调用 rustc 失败: {error}");
            return ExitCode::FAILURE;
        }
    };

    if status.success() {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn from_xtest_runner() -> bool {
    matches!(env::var(XRUNNER_TOKEN_ENV).ok().as_deref(), Some("1"))
}

fn from_guard_launcher() -> bool {
    matches!(
        env::var(GUARD_LAUNCHER_TOKEN_ENV).ok().as_deref(),
        Some("1")
    )
}

fn is_forbidden_direct_invocation(
    real_rustc: &std::ffi::OsString,
    rustc_args: &[std::ffi::OsString],
) -> bool {
    if is_clippy_driver(real_rustc) {
        return true;
    }

    if has_flag(rustc_args, "--test") {
        return true;
    }

    emit_without_link(rustc_args)
}

fn is_clippy_driver(real_rustc: &std::ffi::OsString) -> bool {
    Path::new(real_rustc)
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| name.to_ascii_lowercase().contains("clippy-driver"))
        .unwrap_or(false)
}

fn has_flag(args: &[std::ffi::OsString], flag: &str) -> bool {
    args.iter().any(|arg| arg == flag)
}

fn emit_without_link(args: &[std::ffi::OsString]) -> bool {
    args.iter().any(|arg| {
        let arg = arg.to_string_lossy();
        if !arg.starts_with("--emit=") {
            return false;
        }
        let emits = arg.trim_start_matches("--emit=");
        !emits.split(',').any(|item| item == "link")
    })
}
