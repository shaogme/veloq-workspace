use std::env;
use std::ffi::OsString;
use std::io;
use std::process::{Command, ExitCode};

#[derive(Clone, Copy)]
enum Target {
    Linux,
    Windows,
}

impl Target {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "linux" => Some(Self::Linux),
            "windows" => Some(Self::Windows),
            _ => None,
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Linux => "linux",
            Self::Windows => "windows",
        }
    }

    fn cargo_args(self) -> &'static [&'static str] {
        match self {
            Self::Linux => &[
                "nextest",
                "run",
                "--workspace",
                "--exclude",
                "veloq-driver-iocp",
                "--test-threads",
                "1",
                "--run-ignored",
                "all",
            ],
            Self::Windows => &[
                "nextest",
                "run",
                "--workspace",
                "--exclude",
                "veloq-driver-uring",
                "--test-threads",
                "1",
                "--run-ignored",
                "all",
            ],
        }
    }
}

struct Config {
    target: Target,
    count: usize,
    quiet: bool,
}

impl Config {
    fn parse() -> Result<Self, String> {
        let mut args = env::args_os().skip(1);
        let mut target = None;
        let mut count = 20usize;
        let mut quiet = false;

        while let Some(arg) = args.next() {
            let arg_str = arg
                .to_str()
                .ok_or_else(|| "参数中包含无效 UTF-8 字符".to_string())?;

            match arg_str {
                "--target" => {
                    let value = next_arg(&mut args, "--target")?;
                    target = Some(Target::parse(&value).ok_or_else(|| {
                        format!("不支持的 --target 值: {value}（仅支持 linux/windows）")
                    })?);
                }
                "--count" | "-n" => {
                    let value = next_arg(&mut args, arg_str)?;
                    count = value
                        .parse::<usize>()
                        .map_err(|_| format!("无效的次数: {value}"))?;
                    if count == 0 {
                        return Err("--count 必须大于 0".to_string());
                    }
                }
                "--quiet" => quiet = true,
                "linux" | "windows" if target.is_none() => {
                    target = Target::parse(arg_str);
                }
                "--help" | "-h" => return Err(help_message()),
                _ => return Err(format!("未知参数: {arg_str}\n{}", help_message())),
            }
        }

        let target = target.ok_or_else(help_message)?;

        Ok(Self {
            target,
            count,
            quiet,
        })
    }
}

fn next_arg(args: &mut impl Iterator<Item = OsString>, flag: &str) -> Result<String, String> {
    let value = args
        .next()
        .ok_or_else(|| format!("{flag} 缺少参数值"))?
        .into_string()
        .map_err(|_| format!("{flag} 参数值包含无效 UTF-8 字符"))?;

    Ok(value)
}

fn help_message() -> String {
    "用法: xtest-runner --target <linux|windows> [--count <次数>] [--quiet]\n示例: xtest-runner --target linux --count 20 --quiet".to_string()
}

fn main() -> ExitCode {
    let config = match Config::parse() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    match run_loop(config) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) if error.kind() == io::ErrorKind::Other => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("执行器异常: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run_loop(config: Config) -> io::Result<()> {
    let args = config.target.cargo_args();

    for round in 1..=config.count {
        if !config.quiet {
            let status = Command::new("cargo").args(args).status()?;
            if !status.success() {
                eprintln!(
                    "xtest-{} 第 {round}/{} 次执行失败（退出码: {:?}）",
                    config.target.name(),
                    config.count,
                    status.code()
                );
                return Err(io::Error::other("测试命令失败"));
            }
            continue;
        }

        let output = Command::new("cargo").args(args).output()?;

        if !output.status.success() {
            eprintln!(
                "xtest-{} 第 {round}/{} 次执行失败（退出码: {:?}）",
                config.target.name(),
                config.count,
                output.status.code()
            );

            if !output.stdout.is_empty() {
                eprintln!("----- stdout -----");
                eprintln!("{}", String::from_utf8_lossy(&output.stdout));
            }

            if !output.stderr.is_empty() {
                eprintln!("----- stderr -----");
                eprintln!("{}", String::from_utf8_lossy(&output.stderr));
            }

            return Err(io::Error::other("测试命令失败"));
        }
    }

    println!(
        "xtest-{} 连续执行 {} 次全部成功",
        config.target.name(),
        config.count
    );

    Ok(())
}
