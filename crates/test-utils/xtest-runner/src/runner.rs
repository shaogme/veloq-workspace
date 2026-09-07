use crate::{Config, RunnerError, Target, Task};
use diagweave::prelude::*;
use std::path::{Path, PathBuf};

mod cmd;
mod qemu;

pub use cmd::{CommandSpec, DockerComposeVariant};
use cmd::{
    command_output, command_status, command_works, docker_compose_variant, print_output,
    workspace_root,
};
use qemu::{QemuInstance, WindowsVmConfig, ensure_devbox_path};

#[derive(Clone, Copy, Debug)]
pub enum RunMode {
    Native,
    WindowsOnLinux,
    LinuxOnWindows(DockerComposeVariant),
}

#[derive(Debug)]
pub struct Runner {
    config: Config,
    workspace_root: PathBuf,
    mode: RunMode,
    windows_target: Option<String>,
}

impl Runner {
    pub fn new(config: Config) -> Result<Self, Report<RunnerError>> {
        let workspace_root = workspace_root()?;
        let mode = determine_mode(config.target, &workspace_root).with_ctx(
            "workspace_root",
            workspace_root.to_string_lossy().to_string(),
        )?;

        let windows_target = if !cfg!(target_os = "windows") || config.target != Target::Windows {
            None
        } else {
            // 在 Windows 原生环境下，探测已安装的 Windows target
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

    pub fn run(&self) -> Result<(), Report<RunnerError>> {
        if matches!(self.mode, RunMode::WindowsOnLinux) {
            return self.run_windows_on_linux();
        }

        if matches!(self.mode, RunMode::LinuxOnWindows(_)) {
            let command = self.round_command();
            let status = command_status(&command, &self.workspace_root)
                .with_ctx("command", command.display())?;

            if status.success() {
                return Ok(());
            } else {
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
            ensure_devbox_path(&self.workspace_root);

            if !command_works("qemu-system-x86_64", &["--version"], &self.workspace_root) {
                return Err(RunnerError::QemuNotFound.trans());
            }

            if !command_works("ssh", &["-V"], &self.workspace_root)
                || !command_works("sshpass", &["-V"], &self.workspace_root)
            {
                return Err(RunnerError::SshDependencyNotFound.trans());
            }

            WindowsVmConfig::from_workspace(&self.workspace_root)?;
        }

        Ok(())
    }

    fn run_windows_on_linux(&self) -> Result<(), Report<RunnerError>> {
        let vm_config = WindowsVmConfig::from_workspace(&self.workspace_root)?;

        if !self.config.quiet {
            eprintln!("[xtest-runner] 正在启动 QEMU Windows 虚拟机...");
        }
        let vm = QemuInstance::start(vm_config)?;

        if !self.config.quiet {
            eprintln!("[xtest-runner] 等待 Windows 虚拟机 SSH 就绪...");
        }
        vm.wait_for_ssh()?;

        if !self.config.quiet {
            eprintln!("[xtest-runner] 正在同步工作区代码至 Windows 虚拟机...");
        }
        vm.sync_workspace(&self.workspace_root)?;

        if !self.config.quiet {
            eprintln!("[xtest-runner] 在 Windows 虚拟机中执行任务...");
        }
        let forward_args = std::env::args().skip(1).collect::<Vec<_>>();
        let status = vm.run_in_vm(&forward_args)?;

        if status.success() {
            Ok(())
        } else {
            std::process::exit(status.code().unwrap_or(1));
        }
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
                    "--build".into(),
                    "--rm".into(),
                    "standalone".into(),
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
                unreachable!("WindowsOnLinux 由 QEMU 直接管理执行")
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
