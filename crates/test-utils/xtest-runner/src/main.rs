use clap::{Parser, ValueEnum};
use diagweave::prelude::*;
use diagweave::union;
use std::process::ExitCode;

mod runner;

use runner::Runner;

union! {
    pub(crate) enum RunnerError =
        std::io::Error as Io |
        {
            #[display("{0}")]
            Cli(String),

            #[display("未检测到 docker-compose（或 docker compose），无法在 Windows 上执行 Linux 相关命令")]
            DockerComposeNotFound,

            #[display("无法解析 workspace 根目录")]
            WorkspaceRootResolutionFailed,

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

    pub(crate) fn default_count(self) -> usize {
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
pub(crate) struct Config {
    pub(crate) target: Target,
    pub(crate) task: Task,
    pub(crate) count: usize,
    pub(crate) quiet: bool,
    pub(crate) features: Option<String>,
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

fn parse_count(input: &str) -> Result<usize, RunnerError> {
    let count = input
        .parse::<usize>()
        .map_err(|_| RunnerError::Cli(format!("无效的次数: {input}")))?;

    if count == 0 {
        return Err(RunnerError::Cli("--count 必须大于 0".to_string()));
    }

    Ok(count)
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match run_app(cli) {
        Ok(()) => ExitCode::SUCCESS,
        Err(report) => {
            eprintln!("{}", report.compact());
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
