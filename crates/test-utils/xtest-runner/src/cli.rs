use clap::{Parser, ValueEnum};
use diagweave::Error;
use std::io::Error as IoError;

#[derive(Debug, Error)]
pub(crate) enum RunnerError {
    #[display("{0}")]
    Io(#[from] IoError),
    #[display("{0}")]
    Cli(String),
    #[display("未检测到 docker-compose（或 docker compose），无法在 Windows 上执行 Linux 相关命令")]
    DockerComposeNotFound,
    #[display("无法解析 workspace 根目录")]
    WorkspaceRootResolutionFailed,
    #[display("未检测到 qemu-system-x86_64，无法在 Linux 上执行 Windows 镜像")]
    QemuNotFound,
    #[display("未检测到 qemu-img，无法创建 qcow2 差分镜像")]
    QemuImgNotFound,
    #[display("未检测到 ssh 或 sshpass")]
    SshDependencyNotFound,
    #[display("未找到 Windows 镜像: {0}")]
    WindowsImageNotFound(String),
    #[display("创建差分镜像失败（退出码: {code:?}）")]
    CreateOverlayFailed { code: Option<i32> },
    #[display("在 Windows 虚拟机中执行 cargo fetch 失败（退出码: {code:?}）")]
    CargoFetchFailed { code: Option<i32> },
    #[display("清理 Windows 虚拟机工作区失败（退出码: {code:?}）")]
    VmCleanWorkspaceFailed { code: Option<i32> },
    #[display("等待 Windows 虚拟机关机超时")]
    VmShutdownTimeout,
    #[display("等待 Windows 虚拟机 SSH 就绪超时")]
    VmSshTimeout,
    #[display("同步代码到 Windows 虚拟机失败（退出码: {code:?}）")]
    VmSyncFailed { code: Option<i32> },
    #[display("{step} 失败（退出码: {code:?}）")]
    PrebuildFailed { step: String, code: Option<i32> },
    #[display("{task}-{target} 第 {round}/{total} 次执行失败（退出码: {code:?}）")]
    RoundFailed {
        task: &'static str,
        target: &'static str,
        round: usize,
        total: usize,
        code: Option<i32>,
    },
    #[display("命令执行失败（退出码: {code:?}）")]
    CommandFailed { code: Option<i32> },
}

impl RunnerError {
    pub(super) fn exit_code(&self) -> Option<i32> {
        match self {
            Self::CreateOverlayFailed { code }
            | Self::CargoFetchFailed { code }
            | Self::VmCleanWorkspaceFailed { code }
            | Self::VmSyncFailed { code }
            | Self::PrebuildFailed { code, .. }
            | Self::RoundFailed { code, .. }
            | Self::CommandFailed { code } => *code,
            _ => None,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub(crate) enum Target {
    Linux,
    Windows,
}

impl Target {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Windows => "windows",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub(crate) enum LinuxTarget {
    #[value(name = "x86_64-unknown-linux-gnu")]
    X86_64UnknownLinuxGnu,
    #[value(name = "i686-unknown-linux-gnu")]
    I686UnknownLinuxGnu,
    #[value(name = "riscv32gc-unknown-linux-gnu")]
    Riscv32gcUnknownLinuxGnu,
    #[value(name = "riscv32gc-unknown-linux-musl")]
    Riscv32gcUnknownLinuxMusl,
    #[value(name = "aarch64-unknown-linux-gnu")]
    Aarch64UnknownLinuxGnu,
    #[value(name = "x86_64-unknown-linux-musl")]
    X86_64UnknownLinuxMusl,
    #[value(name = "aarch64-linux-android")]
    Aarch64LinuxAndroid,
}

impl LinuxTarget {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::X86_64UnknownLinuxGnu => "x86_64-unknown-linux-gnu",
            Self::I686UnknownLinuxGnu => "i686-unknown-linux-gnu",
            Self::Riscv32gcUnknownLinuxGnu => "riscv32gc-unknown-linux-gnu",
            Self::Riscv32gcUnknownLinuxMusl => "riscv32gc-unknown-linux-musl",
            Self::Aarch64UnknownLinuxGnu => "aarch64-unknown-linux-gnu",
            Self::X86_64UnknownLinuxMusl => "x86_64-unknown-linux-musl",
            Self::Aarch64LinuxAndroid => "aarch64-linux-android",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub(crate) enum Task {
    Test,
    Clippy,
    Check,
}

impl Task {
    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Test => "xtest",
            Self::Clippy => "xclippy",
            Self::Check => "xcheck",
        }
    }

    fn default_count(self) -> usize {
        match self {
            Self::Test => 20,
            Self::Clippy | Self::Check => 1,
        }
    }
}

#[derive(Debug, Parser)]
#[command(
    name = "xtest-runner",
    about = "统一执行跨平台 test/clippy/check 命令",
    version,
    disable_help_subcommand = true
)]
pub(super) struct Cli {
    #[arg(long, value_enum, help = "目标平台")]
    target: Option<Target>,
    #[arg(value_enum, hide = true)]
    target_positional: Option<Target>,
    #[arg(long, value_enum, default_value_t = Task::Test, help = "执行任务类型")]
    task: Task,
    #[arg(long, short = 'n', value_parser = parse_count, help = "执行次数（默认: test=20, clippy/check=1）")]
    count: Option<usize>,
    #[arg(long, help = "静默模式，仅在失败时输出日志")]
    quiet: bool,
    #[arg(long, help = "启用 features")]
    features: Option<String>,
    #[arg(long, help = "禁用 package 的默认 features")]
    no_default_features: bool,
    #[arg(long, short = 'p', help = "仅执行指定 package")]
    package: Option<String>,
    #[arg(long, help = "包含所有目标（lib, bin, test, example, bench）")]
    all_targets: bool,
    #[arg(long, help = "仅执行指定 nextest 过滤表达式")]
    filter: Option<String>,
    #[arg(long, value_enum, help = "Linux 编译目标（仅支持 check/clippy）")]
    linux_target: Option<LinuxTarget>,
}

#[derive(Debug)]
pub(crate) struct Config {
    target: Target,
    task: Task,
    count: usize,
    quiet: bool,
    features: Option<String>,
    no_default_features: bool,
    all_targets: bool,
    package: Option<String>,
    filter: Option<String>,
    linux_target: Option<LinuxTarget>,
}

impl Config {
    pub(crate) fn target(&self) -> Target {
        self.target
    }

    pub(crate) fn task(&self) -> Task {
        self.task
    }

    pub(crate) fn count(&self) -> usize {
        self.count
    }

    pub(crate) fn quiet(&self) -> bool {
        self.quiet
    }

    pub(crate) fn features(&self) -> Option<&str> {
        self.features.as_deref()
    }

    pub(crate) fn no_default_features(&self) -> bool {
        self.no_default_features
    }

    pub(crate) fn all_targets(&self) -> bool {
        self.all_targets
    }

    pub(crate) fn package(&self) -> Option<&str> {
        self.package.as_deref()
    }

    pub(crate) fn filter(&self) -> Option<&str> {
        self.filter.as_deref()
    }

    pub(crate) fn linux_target(&self) -> Option<LinuxTarget> {
        self.linux_target
    }
}

impl TryFrom<Cli> for Config {
    type Error = RunnerError;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let target = match (cli.target, cli.target_positional) {
            (Some(target), None) | (None, Some(target)) => target,
            (Some(flag), Some(positional)) if flag == positional => flag,
            (Some(_), Some(_)) => {
                return Err(RunnerError::Cli(
                    "--target 与位置参数冲突，请仅保留一种写法".to_string(),
                ));
            }
            (None, None) => {
                return Err(RunnerError::Cli(
                    "缺少目标平台，请使用 --target <linux|windows>".to_string(),
                ));
            }
        };

        let count = cli.count.unwrap_or_else(|| cli.task.default_count());

        if cli.linux_target.is_some() && target != Target::Linux {
            return Err(RunnerError::Cli(
                "--linux-target 只能与 --target linux 一起使用".to_string(),
            ));
        }
        if cli.linux_target.is_some() && cli.task == Task::Test {
            return Err(RunnerError::Cli(
                "Linux 非主机目标当前仅支持 compile-only 的 check/clippy".to_string(),
            ));
        }

        Ok(Self {
            target,
            task: cli.task,
            count,
            quiet: cli.quiet,
            features: cli.features,
            no_default_features: cli.no_default_features,
            all_targets: cli.all_targets,
            package: cli.package,
            filter: cli.filter,
            linux_target: cli.linux_target,
        })
    }
}

fn parse_count(input: &str) -> Result<usize, RunnerError> {
    let count = input
        .parse::<usize>()
        .map_err(|_| RunnerError::Cli(format!("无效的次数: {input}")))?;

    if count == 0 {
        return Err(RunnerError::Cli("--count 必须大于 0".to_string()));
    }

    Ok(count)
}

#[cfg(test)]
#[test]
fn test_cli_package_short_alias() {
    let cli = Cli::try_parse_from(["xtest-runner", "--target", "linux", "-p", "veloq-sync"])
        .expect("parse should succeed");
    assert_eq!(cli.package.as_deref(), Some("veloq-sync"));
}

#[cfg(test)]
#[test]
fn test_cli_all_targets() {
    let cli = Cli::try_parse_from(["xtest-runner", "--target", "linux", "--all-targets"])
        .expect("parse should succeed");
    assert!(cli.all_targets);
    let config = Config::try_from(cli).expect("config conversion should succeed");
    assert!(config.all_targets());
}
