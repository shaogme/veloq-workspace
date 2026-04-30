use clap::{Parser, ValueEnum};
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output};

const WINDOWS_TARGET: &str = "x86_64-pc-windows-gnu";
const XRUNNER_TOKEN_ENV: &str = "VELOQ_XRUNNER_GUARD";
const LINUX_GUARD_WRAPPER: &str = "test-utils/rustc-guard-launcher.sh";

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum Target {
    Linux,
    Windows,
}

impl Target {
    fn name(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Windows => "windows",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
enum Task {
    Test,
    Clippy,
    Check,
}

impl Task {
    fn name(self) -> &'static str {
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
struct Cli {
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
}

#[derive(Debug)]
struct Config {
    target: Target,
    task: Task,
    count: usize,
    quiet: bool,
    features: Option<String>,
}

impl TryFrom<Cli> for Config {
    type Error = String;

    fn try_from(cli: Cli) -> Result<Self, Self::Error> {
        let target = match (cli.target, cli.target_positional) {
            (Some(target), None) | (None, Some(target)) => target,
            (Some(flag), Some(positional)) if flag == positional => flag,
            (Some(_), Some(_)) => {
                return Err("--target 与位置参数冲突，请仅保留一种写法".to_string());
            }
            (None, None) => return Err("缺少目标平台，请使用 --target <linux|windows>".to_string()),
        };

        let count = cli.count.unwrap_or_else(|| cli.task.default_count());

        Ok(Self {
            target,
            task: cli.task,
            count,
            quiet: cli.quiet,
            features: cli.features,
        })
    }
}

#[derive(Clone, Debug)]
struct CommandSpec {
    program: String,
    args: Vec<String>,
    envs: Vec<(String, String)>,
}

impl CommandSpec {
    fn new(program: impl Into<String>, args: impl Into<Vec<String>>) -> Self {
        Self {
            program: program.into(),
            args: args.into(),
            envs: vec![(XRUNNER_TOKEN_ENV.to_string(), "1".to_string())],
        }
    }

    fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.envs.push((key.into(), value.into()));
        self
    }

    fn display(&self) -> String {
        let env_part = self
            .envs
            .iter()
            .map(|(k, v)| format!("{k}={v}"))
            .collect::<Vec<_>>()
            .join(" ");

        let command_part = if self.args.is_empty() {
            self.program.clone()
        } else {
            format!("{} {}", self.program, self.args.join(" "))
        };

        if env_part.is_empty() {
            command_part
        } else {
            format!("{env_part} {command_part}")
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum DockerComposeVariant {
    Standalone,
    Plugin,
}

#[derive(Clone, Copy, Debug)]
enum RunMode {
    Native,
    WindowsOnLinux,
    LinuxOnWindows(DockerComposeVariant),
}

#[derive(Debug)]
struct Runner {
    config: Config,
    workspace_root: PathBuf,
    mode: RunMode,
}

impl Runner {
    fn new(config: Config) -> io::Result<Self> {
        let workspace_root = workspace_root()?;
        let mode = determine_mode(config.target, &workspace_root)?;

        let runner = Self {
            config,
            workspace_root,
            mode,
        };
        runner.prepare_environment()?;
        Ok(runner)
    }

    fn run(&self) -> io::Result<()> {
        let command = self.round_command();

        if !matches!(self.mode, RunMode::Native) {
            // 在非原生环境下，我们直接运行一次 xtest-runner，由内部的 xtest-runner 负责循环
            let status = command_status(&command, &self.workspace_root)?;
            if status.success() {
                return Ok(());
            } else {
                // 代理子命令已经输出了详细报告（含 count 信息），外层直接对应 code 退出即可
                std::process::exit(status.code().unwrap_or(1));
            }
        }

        for round in 1..=self.config.count {
            self.run_round(&command, round)?;
        }

        println!(
            "{}-{} 连续执行 {} 次全部成功",
            self.config.task.name(),
            self.config.target.name(),
            self.config.count
        );

        Ok(())
    }

    fn run_round(&self, command: &CommandSpec, round: usize) -> io::Result<()> {
        if !self.config.quiet {
            let status = command_status(command, &self.workspace_root)?;
            if status.success() {
                return Ok(());
            }

            return Err(io::Error::other(format!(
                "{}-{} 第 {round}/{} 次执行失败（退出码: {:?}）",
                self.config.task.name(),
                self.config.target.name(),
                self.config.count,
                status.code()
            )));
        }

        let output = command_output(command, &self.workspace_root)?;
        if output.status.success() {
            return Ok(());
        }

        eprintln!(
            "{}-{} 第 {round}/{} 次执行失败（退出码: {:?}）",
            self.config.task.name(),
            self.config.target.name(),
            self.config.count,
            output.status.code()
        );
        print_output(&output);
        Err(io::Error::other("命令执行失败"))
    }

    fn prepare_environment(&self) -> io::Result<()> {
        if matches!(self.mode, RunMode::WindowsOnLinux) {
            if !command_works("cross", &["--version"], &self.workspace_root) {
                self.run_setup(
                    "安装 cross",
                    CommandSpec::new("cargo", vec!["install".into(), "cross".into()]),
                )?;
            }

            if !has_rust_target(WINDOWS_TARGET, &self.workspace_root)? {
                self.run_setup(
                    "安装 x86_64-pc-windows-gnu 工具链",
                    CommandSpec::new(
                        "rustup",
                        vec!["target".into(), "add".into(), WINDOWS_TARGET.into()],
                    ),
                )?;
            }
        }

        Ok(())
    }

    fn run_setup(&self, step: &str, command: CommandSpec) -> io::Result<()> {
        if !self.config.quiet {
            eprintln!("[xtest-runner] {step}: {}", command.display());
            let status = command_status(&command, &self.workspace_root)?;
            if status.success() {
                return Ok(());
            }

            return Err(io::Error::other(format!(
                "{step} 失败（退出码: {:?}）",
                status.code()
            )));
        }

        let output = command_output(&command, &self.workspace_root)?;
        if output.status.success() {
            return Ok(());
        }

        eprintln!("{step} 失败（退出码: {:?}）", output.status.code());
        print_output(&output);
        Err(io::Error::other(format!("{step} 失败")))
    }

    fn round_command(&self) -> CommandSpec {
        match (self.mode, self.config.target) {
            (RunMode::LinuxOnWindows(variant), Target::Linux) => {
                let program = match variant {
                    DockerComposeVariant::Standalone => "docker-compose",
                    DockerComposeVariant::Plugin => "docker",
                };
                let mut args = if matches!(variant, DockerComposeVariant::Plugin) {
                    vec!["compose".into()]
                } else {
                    vec![]
                };
                args.extend(vec![
                    "run".into(),
                    "--rm".into(),
                    "-e".into(),
                    "CARGO_TARGET_DIR=target/guard-linux".into(),
                    "-e".into(),
                    format!("CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER={LINUX_GUARD_WRAPPER}"),
                    "dev".into(),
                    "cargo".into(),
                    "run".into(),
                    "-q".into(),
                    "-p".into(),
                    "xtest-runner".into(),
                    "--".into(),
                ]);
                args.extend(std::env::args().skip(1));
                CommandSpec::new(program, args)
            }
            (RunMode::WindowsOnLinux, Target::Windows) => {
                let mut args = vec![
                    "run".into(),
                    "--target".into(),
                    WINDOWS_TARGET.into(),
                    "-p".into(),
                    "xtest-runner".into(),
                    "--".into(),
                ];
                args.extend(std::env::args().skip(1));
                CommandSpec::new("cross", args)
                    .with_env("CROSS_SKIP_AUTO_UPDATE", "1")
                    .with_env("CARGO_BUILD_RUSTC_WORKSPACE_WRAPPER", LINUX_GUARD_WRAPPER)
            }
            (_, Target::Linux) => {
                linux_native_command(self.config.task, self.config.features.as_deref())
            }
            (_, Target::Windows) => {
                windows_native_command(self.config.task, self.config.features.as_deref())
            }
        }
    }
}

fn linux_native_command(task: Task, features: Option<&str>) -> CommandSpec {
    let mut args = match task {
        Task::Test => vec!["nextest".into(), "run".into()],
        Task::Clippy => vec!["clippy".into()],
        Task::Check => vec!["check".into()],
    };

    if let Some(f) = features {
        args.push("--features".into());
        args.push(f.into());
    }

    match task {
        Task::Test => {
            args.extend(vec![
                "--workspace".into(),
                "--exclude".into(),
                "veloq-driver-iocp".into(),
                "--test-threads".into(),
                "1".into(),
                "--run-ignored".into(),
                "all".into(),
            ]);
        }
        Task::Clippy => {
            args.extend(vec![
                "--all-targets".into(),
                "--".into(),
                "-D".into(),
                "warnings".into(),
            ]);
        }
        Task::Check => {}
    }

    CommandSpec::new("cargo", args)
}

fn windows_native_command(task: Task, features: Option<&str>) -> CommandSpec {
    let mut args = match task {
        Task::Test => vec!["nextest".into(), "run".into()],
        Task::Clippy => vec!["clippy".into()],
        Task::Check => vec!["check".into()],
    };

    if let Some(f) = features {
        args.push("--features".into());
        args.push(f.into());
    }

    match task {
        Task::Test => {
            args.extend(vec![
                "--workspace".into(),
                "--exclude".into(),
                "veloq-driver-uring".into(),
                "--test-threads".into(),
                "1".into(),
                "--run-ignored".into(),
                "all".into(),
            ]);
        }
        Task::Clippy => {
            args.extend(vec![
                "--all-targets".into(),
                "--target".into(),
                WINDOWS_TARGET.into(),
                "--".into(),
                "-D".into(),
                "warnings".into(),
            ]);
        }
        Task::Check => {
            args.extend(vec!["--target".into(), WINDOWS_TARGET.into()]);
        }
    }

    CommandSpec::new("cargo", args)
}

fn parse_count(input: &str) -> Result<usize, String> {
    let count = input
        .parse::<usize>()
        .map_err(|_| format!("无效的次数: {input}"))?;

    if count == 0 {
        return Err("--count 必须大于 0".to_string());
    }

    Ok(count)
}

fn determine_mode(target: Target, workspace_root: &Path) -> io::Result<RunMode> {
    if cfg!(target_os = "windows") && target == Target::Linux {
        let compose_variant = docker_compose_variant(workspace_root).ok_or_else(|| {
            io::Error::other(
                "未检测到 docker-compose（或 docker compose），无法在 Windows 上执行 Linux 相关命令",
            )
        })?;

        return Ok(RunMode::LinuxOnWindows(compose_variant));
    }

    if cfg!(target_os = "linux") && target == Target::Windows {
        return Ok(RunMode::WindowsOnLinux);
    }

    Ok(RunMode::Native)
}

fn has_rust_target(target: &str, workspace_root: &Path) -> io::Result<bool> {
    let output = command_output(
        &CommandSpec::new(
            "rustup",
            vec!["target".into(), "list".into(), "--installed".into()],
        ),
        workspace_root,
    )?;

    if !output.status.success() {
        eprintln!(
            "检查 rustup target 失败（退出码: {:?}）",
            output.status.code()
        );
        print_output(&output);
        return Err(io::Error::other("无法读取已安装 target 列表"));
    }

    let installed = String::from_utf8_lossy(&output.stdout);
    Ok(installed.lines().any(|line| line.trim() == target))
}

fn command_works(program: &str, args: &[&str], workspace_root: &Path) -> bool {
    Command::new(program)
        .args(args)
        .current_dir(workspace_root)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn docker_compose_variant(workspace_root: &Path) -> Option<DockerComposeVariant> {
    if command_works("docker-compose", &["version"], workspace_root) {
        return Some(DockerComposeVariant::Standalone);
    }

    if command_works("docker", &["compose", "version"], workspace_root) {
        return Some(DockerComposeVariant::Plugin);
    }

    None
}

fn print_output(output: &Output) {
    if !output.stdout.is_empty() {
        eprintln!("----- stdout -----");
        eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    }

    if !output.stderr.is_empty() {
        eprintln!("----- stderr -----");
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    }
}

fn workspace_root() -> io::Result<PathBuf> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or_else(|| io::Error::other("无法解析 workspace 根目录"))
}

fn command_status(
    command: &CommandSpec,
    workspace_root: &Path,
) -> io::Result<std::process::ExitStatus> {
    let mut process = Command::new(&command.program);
    process
        .args(&command.args)
        .envs(command.envs.iter().map(|(k, v)| (k, v)))
        .current_dir(workspace_root);
    process.status()
}

fn command_output(command: &CommandSpec, workspace_root: &Path) -> io::Result<Output> {
    let mut process = Command::new(&command.program);
    process
        .args(&command.args)
        .envs(command.envs.iter().map(|(k, v)| (k, v)))
        .current_dir(workspace_root);
    process.output()
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let config = match Config::try_from(cli) {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    match Runner::new(config).and_then(|runner| runner.run()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
