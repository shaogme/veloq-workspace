use clap::{Parser, ValueEnum};
use diagweave::prelude::*;
use diagweave::union;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Output};

union! {
    pub enum RunnerError =
        std::io::Error as Io |
        {
            #[display("{0}")]
            Cli(String),

            #[display("未检测到 docker-compose（或 docker compose），无法在 Windows 上执行 Linux 相关命令")]
            DockerComposeNotFound,

            #[display("无法解析 workspace 根目录")]
            WorkspaceRootResolutionFailed,

            #[display("无法读取已安装 target 列表")]
            FailedToReadTargetList,

            #[display("检查 rustup target 失败")]
            FailedToCheckRustupTarget,

            #[display("{step} 失败（退出码: {code:?}）")]
            SetupFailed {
                step: String,
                code: Option<i32>,
            },

            #[display("{step} 失败（退出码: {code:?}）")]
            PrebuildFailed {
                step: String,
                code: Option<i32>,
            },

            #[display("{task}-{target} 第 {round}/{total} 次执行失败（退出码: {code:?}）")]
            RoundFailed {
                task: &'static str,
                target: &'static str,
                round: usize,
                total: usize,
                code: Option<i32>,
            },

            #[display("命令执行失败")]
            CommandFailed,
        }
}

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
            envs: Vec::new(),
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
    windows_target: Option<String>,
}

impl Runner {
    fn new(config: Config) -> Result<Self, Report<RunnerError>> {
        let workspace_root = workspace_root()?;
        let mode = determine_mode(config.target, &workspace_root).with_ctx(
            "workspace_root",
            workspace_root.to_string_lossy().to_string(),
        )?;

        let windows_target = if cfg!(target_os = "windows") || config.target != Target::Windows {
            None
        } else {
            // 在非 Windows 环境下（如 Linux），探测已安装的 Windows target
            let output = command_output(
                &CommandSpec::new(
                    "rustup",
                    vec!["target".into(), "list".into(), "--installed".into()],
                ),
                &workspace_root,
            );

            match output {
                Ok(output) if output.status.success() => {
                    let installed = String::from_utf8_lossy(&output.stdout);
                    let mut targets: Vec<_> =
                        installed.lines().map(|l| l.trim().to_string()).collect();

                    targets.sort_by_key(|t| {
                        if t == "x86_64-pc-windows-msvc" {
                            0
                        } else if t == "x86_64-pc-windows-gnu" {
                            1
                        } else if t.contains("-windows-") {
                            2
                        } else {
                            3
                        }
                    });

                    targets
                        .into_iter()
                        .find(|t| t.contains("-windows-"))
                        .or_else(|| Some("x86_64-pc-windows-gnu".to_string()))
                }
                _ => Some("x86_64-pc-windows-gnu".to_string()),
            }
        };

        let runner = Self {
            config,
            workspace_root,
            mode,
            windows_target,
        };
        runner.prepare_environment()?;
        Ok(runner)
    }

    fn run(&self) -> Result<(), Report<RunnerError>> {
        if !matches!(self.mode, RunMode::Native) {
            // 在非原生环境下，我们直接运行一次 xtest-runner，由内部的 xtest-runner 负责循环
            let command = self.round_command();
            let status = command_status(&command, &self.workspace_root)
                .with_ctx("command", command.display())?;
            if status.success() {
                return Ok(());
            } else {
                // 代理子命令已经输出了详细报告（含 count 信息），外层直接对应 code 退出即可
                std::process::exit(status.code().unwrap_or(1));
            }
        }

        self.prebuild_tests()?;

        let command = self.round_command();
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

    fn prebuild_tests(&self) -> Result<(), Report<RunnerError>> {
        if self.config.task != Task::Test {
            return Ok(());
        }

        let steps = [
            ("预构建 nextest 测试二进制", self.nextest_prebuild_command()),
            ("预热 trybuild 编译测试", trybuild_warmup_command()),
        ];

        for (step, command) in steps {
            self.run_prebuild_step(step, command)?;
        }

        Ok(())
    }

    fn nextest_prebuild_command(&self) -> CommandSpec {
        let mut command = match self.config.target {
            Target::Linux => linux_native_command(Task::Test, self.config.features.as_deref()),
            Target::Windows => windows_native_command(
                Task::Test,
                self.config.features.as_deref(),
                self.windows_target.as_deref(),
            ),
        };
        command.args.push("--no-run".into());
        command
    }

    fn run_prebuild_step(
        &self,
        step: &str,
        command: CommandSpec,
    ) -> Result<(), Report<RunnerError>> {
        if !self.config.quiet {
            eprintln!("[xtest-runner] {step}: {}", command.display());
            let status = command_status(&command, &self.workspace_root)
                .with_ctx("step", step.to_string())?;
            if status.success() {
                return Ok(());
            }

            return RunnerError::PrebuildFailed {
                step: step.to_string(),
                code: status.code(),
            }
            .with_ctx("command", command.display());
        }

        let output =
            command_output(&command, &self.workspace_root).with_ctx("step", step.to_string())?;
        if output.status.success() {
            return Ok(());
        }

        eprintln!("{step} 失败（退出码: {:?}）", output.status.code());
        print_output(&output);
        RunnerError::PrebuildFailed {
            step: step.to_string(),
            code: output.status.code(),
        }
        .with_ctx("command", command.display())
    }

    fn run_round(&self, command: &CommandSpec, round: usize) -> Result<(), Report<RunnerError>> {
        if !self.config.quiet {
            let status = command_status(command, &self.workspace_root)
                .with_ctx("round", round.to_string())?;
            if status.success() {
                return Ok(());
            }

            return RunnerError::RoundFailed {
                task: self.config.task.name(),
                target: self.config.target.name(),
                round,
                total: self.config.count,
                code: status.code(),
            }
            .with_ctx("command", command.display());
        }

        let output =
            command_output(command, &self.workspace_root).with_ctx("round", round.to_string())?;
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
        Err(RunnerError::CommandFailed.trans())
    }

    fn prepare_environment(&self) -> Result<(), Report<RunnerError>> {
        if matches!(self.mode, RunMode::WindowsOnLinux) {
            if !command_works("cross", &["--version"], &self.workspace_root) {
                self.run_setup(
                    "安装 cross",
                    CommandSpec::new("cargo", vec!["install".into(), "cross".into()]),
                )?;
            }

            if let Some(target) = &self.windows_target
                && !has_rust_target(target, &self.workspace_root)
                    .with_ctx("target", target.clone())?
            {
                self.run_setup(
                    &format!("安装 {} 工具链", target),
                    CommandSpec::new(
                        "rustup",
                        vec!["target".into(), "add".into(), target.clone()],
                    ),
                )?;
            }
        }

        Ok(())
    }

    fn run_setup(&self, step: &str, command: CommandSpec) -> Result<(), Report<RunnerError>> {
        if !self.config.quiet {
            eprintln!("[xtest-runner] {step}: {}", command.display());
            let status = command_status(&command, &self.workspace_root)
                .with_ctx("step", step.to_string())?;
            if status.success() {
                return Ok(());
            }

            return RunnerError::SetupFailed {
                step: step.to_string(),
                code: status.code(),
            }
            .with_ctx("command", command.display());
        }

        let output =
            command_output(&command, &self.workspace_root).with_ctx("step", step.to_string())?;
        if output.status.success() {
            return Ok(());
        }

        eprintln!("{step} 失败（退出码: {:?}）", output.status.code());
        print_output(&output);
        RunnerError::SetupFailed {
            step: step.to_string(),
            code: output.status.code(),
        }
        .with_ctx("command", command.display())
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
                let target = self
                    .windows_target
                    .as_deref()
                    .unwrap_or("x86_64-pc-windows-gnu");
                let mut args = vec![
                    "run".into(),
                    "--target".into(),
                    target.into(),
                    "-p".into(),
                    "xtest-runner".into(),
                    "--".into(),
                ];
                args.extend(std::env::args().skip(1));
                CommandSpec::new("cross", args).with_env("CROSS_SKIP_AUTO_UPDATE", "1")
            }
            (_, Target::Linux) => {
                linux_native_command(self.config.task, self.config.features.as_deref())
            }
            (_, Target::Windows) => windows_native_command(
                self.config.task,
                self.config.features.as_deref(),
                self.windows_target.as_deref(),
            ),
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

fn windows_native_command(task: Task, features: Option<&str>, target: Option<&str>) -> CommandSpec {
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
            args.extend(vec!["--all-targets".into()]);
            if let Some(t) = target {
                args.push("--target".into());
                args.push(t.into());
            }
            args.extend(vec!["--".into(), "-D".into(), "warnings".into()]);
        }
        Task::Check => {
            if let Some(t) = target {
                args.push("--target".into());
                args.push(t.into());
            }
        }
    }

    CommandSpec::new("cargo", args)
}

fn trybuild_warmup_command() -> CommandSpec {
    CommandSpec::new(
        "cargo",
        vec![
            "test".into(),
            "-p".into(),
            "veloq-runtime".into(),
            "--test".into(),
            "compile_tests".into(),
            "compile_tests".into(),
            "--".into(),
            "--exact".into(),
        ],
    )
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

fn determine_mode(target: Target, workspace_root: &Path) -> Result<RunMode, RunnerError> {
    if cfg!(target_os = "windows") && target == Target::Linux {
        let compose_variant =
            docker_compose_variant(workspace_root).ok_or(RunnerError::DockerComposeNotFound)?;

        return Ok(RunMode::LinuxOnWindows(compose_variant));
    }

    if cfg!(target_os = "linux") && target == Target::Windows {
        return Ok(RunMode::WindowsOnLinux);
    }

    Ok(RunMode::Native)
}

fn has_rust_target(target: &str, workspace_root: &Path) -> Result<bool, RunnerError> {
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
        return Err(RunnerError::FailedToCheckRustupTarget);
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

fn workspace_root() -> Result<PathBuf, RunnerError> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or(RunnerError::WorkspaceRootResolutionFailed)
}

fn command_status(
    command: &CommandSpec,
    workspace_root: &Path,
) -> Result<std::process::ExitStatus, RunnerError> {
    if !workspace_root.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Workspace root does not exist: {:?}", workspace_root),
        )
        .into());
    }
    let mut process = Command::new(&command.program);
    process
        .args(&command.args)
        .envs(command.envs.iter().map(|(k, v)| (k, v)))
        .current_dir(workspace_root);
    Ok(process.status()?)
}

fn command_output(command: &CommandSpec, workspace_root: &Path) -> Result<Output, RunnerError> {
    if !workspace_root.exists() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("Workspace root does not exist: {:?}", workspace_root),
        )
        .into());
    }
    let mut process = Command::new(&command.program);
    process
        .args(&command.args)
        .envs(command.envs.iter().map(|(k, v)| (k, v)))
        .current_dir(workspace_root);
    Ok(process.output()?)
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run_app(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(report) => {
            eprintln!("{}", report.pretty());
            ExitCode::FAILURE
        }
    }
}

fn run_app(cli: Cli) -> Result<(), Report<RunnerError>> {
    let config = Config::try_from(cli)?;
    let runner = Runner::new(config)?;
    runner.run()?;
    Ok(())
}
