mod cmd;
mod qemu;

use crate::cli::{Config, RunnerError, Target, Task};
use cmd::{CommandSpec, DockerComposeVariant};
use diagweave::{DiagnosticResult, Report, Transform};
use qemu::{QemuInstance, WindowsVmConfig};
use std::{
    env,
    path::{Path, PathBuf},
};

#[derive(Clone, Copy, Debug)]
enum RunMode {
    Native,
    WindowsOnLinux,
    LinuxOnWindows(DockerComposeVariant),
}

#[derive(Clone)]
struct NativeCommandOptions<'a> {
    task: Task,
    features: Option<&'a str>,
    no_default_features: bool,
    all_targets: bool,
    package: Option<&'a str>,
    filter: Option<&'a str>,
    target: Option<String>,
}

#[derive(Debug)]
pub(crate) struct Runner {
    config: Config,
    workspace_root: PathBuf,
    mode: RunMode,
    windows_target: Option<String>,
}

impl Runner {
    pub(crate) fn new(config: Config) -> Result<Self, Report<RunnerError>> {
        let workspace_root = CommandSpec::workspace_root()?;
        let mode = determine_mode(config.target(), &workspace_root).with_ctx(
            "workspace_root",
            workspace_root.to_string_lossy().to_string(),
        )?;

        let windows_target = if !cfg!(target_os = "windows") || config.target() != Target::Windows {
            None
        } else {
            // 在 Windows 原生环境下，探测已安装的 Windows target
            let output = CommandSpec::command_output(
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

    pub(crate) fn run(&self) -> Result<(), Report<RunnerError>> {
        if matches!(self.mode, RunMode::WindowsOnLinux) {
            return self.run_windows_on_linux();
        }

        if matches!(self.mode, RunMode::LinuxOnWindows(_)) {
            let command = self.round_command();
            let status = command
                .status(&self.workspace_root)
                .with_ctx("command", command.display())?;

            if status.success() {
                return Ok(());
            } else {
                let code = status.code();
                let mut report: Report<RunnerError> = RunnerError::CommandFailed { code }.trans();
                if let Some(c) = code {
                    report = report.set_error_code(c);
                }
                return Err(report.with_ctx("command", command.display()));
            }
        }

        self.prebuild_tests()?;

        let command = self.round_command();
        for round in 1..=self.config.count() {
            self.run_round(&command, round)?;
        }

        println!(
            "{}-{} 连续执行 {} 次全部成功",
            self.config.task().name(),
            self.config.target().name(),
            self.config.count()
        );

        Ok(())
    }

    fn prebuild_tests(&self) -> Result<(), Report<RunnerError>> {
        if self.config.task() != Task::Test {
            return Ok(());
        }

        let mut steps = vec![("预构建 nextest 测试二进制", self.nextest_prebuild_command())];

        if package_matches(self.config.package(), "veloq-runtime") {
            steps.push((
                "预热 trybuild 编译测试 (veloq-runtime)",
                veloq_runtime_trybuild_warmup_command(),
            ));
        }

        if package_matches(self.config.package(), "veloq-std") {
            steps.push((
                "预热 trybuild 编译测试 (veloq-std)",
                veloq_std_trybuild_warmup_command(),
            ));
        }

        if package_matches(self.config.package(), "veloq-sync") {
            steps.push((
                "预热 trybuild 编译测试 (veloq-sync)",
                veloq_sync_trybuild_warmup_command(),
            ));
        }

        if package_matches(self.config.package(), "veloq-local") {
            steps.push((
                "预热 trybuild 编译测试 (veloq-local)",
                veloq_local_trybuild_warmup_command(),
            ));
        }

        for (step, command) in steps {
            self.run_prebuild_step(step, command)?;
        }

        Ok(())
    }

    fn nextest_prebuild_command(&self) -> CommandSpec {
        let options = self.native_command_options(
            self.config
                .linux_target()
                .map(|target| target.name().to_string()),
        );
        let command = match self.config.target() {
            Target::Linux => linux_native_command(options),
            Target::Windows => windows_native_command(NativeCommandOptions {
                target: self.windows_target.clone(),
                ..options
            }),
        };
        command.with_arg("--no-run")
    }

    fn run_prebuild_step(
        &self,
        step: &str,
        command: CommandSpec,
    ) -> Result<(), Report<RunnerError>> {
        if !self.config.quiet() {
            eprintln!("[xtest-runner] {step}: {}", command.display());
            let status = command
                .status(&self.workspace_root)
                .with_ctx("step", step.to_string())?;
            if status.success() {
                return Ok(());
            }

            let code = status.code();
            let mut report: Report<RunnerError> = RunnerError::PrebuildFailed {
                step: step.to_string(),
                code,
            }
            .trans();
            if let Some(c) = code {
                report = report.set_error_code(c);
            }
            return Err(report.with_ctx("command", command.display()));
        }

        let output = CommandSpec::command_output(&command, &self.workspace_root)
            .with_ctx("step", step.to_string())?;
        if output.status.success() {
            return Ok(());
        }

        eprintln!("{step} 失败（退出码: {:?}）", output.status.code());
        CommandSpec::print_output(&output);
        let code = output.status.code();
        let mut report: Report<RunnerError> = RunnerError::PrebuildFailed {
            step: step.to_string(),
            code,
        }
        .trans();
        if let Some(c) = code {
            report = report.set_error_code(c);
        }
        Err(report.with_ctx("command", command.display()))
    }

    fn run_round(&self, command: &CommandSpec, round: usize) -> Result<(), Report<RunnerError>> {
        if !self.config.quiet() {
            let status = command
                .status(&self.workspace_root)
                .with_ctx("round", round.to_string())?;
            if status.success() {
                return Ok(());
            }

            let code = status.code();
            let mut report: Report<RunnerError> = RunnerError::RoundFailed {
                task: self.config.task().name(),
                target: self.config.target().name(),
                round,
                total: self.config.count(),
                code,
            }
            .trans();
            if let Some(c) = code {
                report = report.set_error_code(c);
            }
            return Err(report.with_ctx("command", command.display()));
        }

        let output = CommandSpec::command_output(command, &self.workspace_root)
            .with_ctx("round", round.to_string())?;
        if output.status.success() {
            return Ok(());
        }

        eprintln!(
            "{}-{} 第 {round}/{} 次执行失败（退出码: {:?}）",
            self.config.task().name(),
            self.config.target().name(),
            self.config.count(),
            output.status.code()
        );
        CommandSpec::print_output(&output);
        let code = output.status.code();
        let mut report: Report<RunnerError> = RunnerError::CommandFailed { code }.trans();
        if let Some(c) = code {
            report = report.set_error_code(c);
        }
        Err(report)
    }

    fn prepare_environment(&self) -> Result<(), Report<RunnerError>> {
        if matches!(self.mode, RunMode::WindowsOnLinux) {
            if !CommandSpec::command_works(
                "qemu-system-x86_64",
                &["--version"],
                &self.workspace_root,
            ) {
                return Err(RunnerError::QemuNotFound.trans());
            }

            if !CommandSpec::command_works("qemu-img", &["--version"], &self.workspace_root) {
                return Err(RunnerError::QemuImgNotFound.trans());
            }

            if !CommandSpec::command_works("ssh", &["-V"], &self.workspace_root)
                || !CommandSpec::command_works("sshpass", &["-V"], &self.workspace_root)
            {
                return Err(RunnerError::SshDependencyNotFound.trans());
            }

            WindowsVmConfig::from_workspace(&self.workspace_root)?;
        }

        Ok(())
    }

    fn run_windows_on_linux(&self) -> Result<(), Report<RunnerError>> {
        let vm_config = WindowsVmConfig::from_workspace(&self.workspace_root)?;
        vm_config.ensure_overlay_image(&self.workspace_root, self.config.quiet())?;

        if !self.config.quiet() {
            eprintln!("[xtest-runner] 正在启动 QEMU Windows 虚拟机 (snapshot 模式)...");
        }
        let vm = QemuInstance::start(vm_config, true)?;

        if !self.config.quiet() {
            eprintln!("[xtest-runner] 等待 Windows 虚拟机 SSH 就绪...");
        }
        vm.wait_for_ssh()?;

        if !self.config.quiet() {
            eprintln!("[xtest-runner] 正在同步工作区代码至 Windows 虚拟机...");
        }
        vm.sync_workspace(&self.workspace_root)?;

        if !self.config.quiet() {
            eprintln!("[xtest-runner] 在 Windows 虚拟机中执行任务...");
        }
        let forward_args = env::args().skip(1).collect::<Vec<_>>();
        let output = vm.run_in_vm(&forward_args, self.config.quiet())?;

        if output.status.success() {
            if self.config.quiet() {
                print!("{}", String::from_utf8_lossy(&output.stdout));
            }
            Ok(())
        } else {
            if self.config.quiet() {
                CommandSpec::print_output(&output);
            }
            let code = output.status.code();
            let mut report: Report<RunnerError> = RunnerError::CommandFailed { code }.trans();
            if let Some(c) = code {
                report = report.set_error_code(c);
            }
            Err(report)
        }
    }

    fn round_command(&self) -> CommandSpec {
        match (self.mode, self.config.target()) {
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
                args.extend(env::args().skip(1));
                CommandSpec::new(program, args)
            }
            (RunMode::WindowsOnLinux, Target::Windows) => {
                unreachable!("WindowsOnLinux 由 QEMU 直接管理执行")
            }
            (_, Target::Linux) => linux_native_command(
                self.native_command_options(
                    self.config
                        .linux_target()
                        .map(|target| target.name().to_string()),
                ),
            ),
            (_, Target::Windows) => {
                windows_native_command(self.native_command_options(self.windows_target.clone()))
            }
        }
    }

    fn native_command_options(&self, target: Option<String>) -> NativeCommandOptions<'_> {
        NativeCommandOptions {
            task: self.config.task(),
            features: self.config.features(),
            no_default_features: self.config.no_default_features(),
            all_targets: self.config.all_targets(),
            package: self.config.package(),
            filter: self.config.filter(),
            target,
        }
    }
}

fn linux_native_command(options: NativeCommandOptions<'_>) -> CommandSpec {
    let mut args = match options.task {
        Task::Test => vec!["nextest".into(), "run".into()],
        Task::Clippy => vec!["clippy".into()],
        Task::Check => vec!["check".into()],
    };

    if options.no_default_features {
        args.push("--no-default-features".into());
    }

    if let Some(f) = options.features {
        args.push("--features".into());
        args.push(f.into());
    }

    if options.all_targets {
        args.push("--all-targets".into());
    }

    append_workspace_packages(&mut args, options.package);

    match options.task {
        Task::Test => {
            args.extend(vec!["--run-ignored".into(), "all".into()]);
            if let Some(filter) = options.filter {
                args.push("-E".into());
                args.push(filter.into());
            }
        }
        Task::Clippy => {
            if let Some(target) = options.target {
                args.push("--target".into());
                args.push(target);
                if !options.all_targets {
                    args.push("--lib".into());
                }
            }
            args.extend(vec!["--".into(), "-D".into(), "warnings".into()]);
        }
        Task::Check => {
            if let Some(target) = options.target {
                args.push("--target".into());
                args.push(target);
            }
        }
    }

    CommandSpec::new("cargo", args)
}

fn append_packages(args: &mut Vec<String>, packages: Option<&str>) {
    if let Some(packages) = packages {
        for package in packages.split(',').filter(|package| !package.is_empty()) {
            args.push("--package".into());
            args.push(package.into());
        }
    }
}

fn append_workspace_packages(args: &mut Vec<String>, package: Option<&str>) {
    append_packages(args, package);
    if package.is_some() {
        return;
    }

    args.push("--workspace".into());
}

fn windows_native_command(options: NativeCommandOptions<'_>) -> CommandSpec {
    let mut args = match options.task {
        Task::Test => vec!["nextest".into(), "run".into()],
        Task::Clippy => vec!["clippy".into()],
        Task::Check => vec!["check".into()],
    };

    if options.no_default_features {
        args.push("--no-default-features".into());
    }

    if let Some(f) = options.features {
        args.push("--features".into());
        args.push(f.into());
    }

    if options.all_targets {
        args.push("--all-targets".into());
    }

    append_workspace_packages(&mut args, options.package);

    match options.task {
        Task::Test => {
            args.extend(vec!["--run-ignored".into(), "all".into()]);
            if let Some(filter) = options.filter {
                args.push("-E".into());
                args.push(filter.into());
            }
        }
        Task::Clippy => {
            if let Some(target) = options.target {
                args.push("--target".into());
                args.push(target);
            }
            args.extend(vec!["--".into(), "-D".into(), "warnings".into()]);
        }
        Task::Check => {
            if let Some(target) = options.target {
                args.push("--target".into());
                args.push(target);
            }
        }
    }

    CommandSpec::new("cargo", args)
}

fn veloq_runtime_trybuild_warmup_command() -> CommandSpec {
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

fn veloq_std_trybuild_warmup_command() -> CommandSpec {
    CommandSpec::new(
        "cargo",
        vec![
            "test".into(),
            "-p".into(),
            "veloq-std".into(),
            "--test".into(),
            "compile_tests".into(),
            "receiver_is_not_sync".into(),
            "--".into(),
            "--exact".into(),
        ],
    )
}

fn veloq_sync_trybuild_warmup_command() -> CommandSpec {
    CommandSpec::new(
        "cargo",
        vec![
            "test".into(),
            "-p".into(),
            "veloq-sync".into(),
            "--test".into(),
            "compile_tests".into(),
            "compile_tests".into(),
            "--".into(),
            "--exact".into(),
        ],
    )
}

fn veloq_local_trybuild_warmup_command() -> CommandSpec {
    CommandSpec::new(
        "cargo",
        vec![
            "test".into(),
            "-p".into(),
            "veloq-local".into(),
            "--test".into(),
            "compile_tests".into(),
            "compile_tests".into(),
            "--".into(),
            "--exact".into(),
        ],
    )
}

fn package_matches(package: Option<&str>, target: &str) -> bool {
    match package {
        Some(packages) => packages.split(',').any(|p| p.trim() == target),
        None => true,
    }
}

fn determine_mode(target: Target, workspace_root: &Path) -> Result<RunMode, RunnerError> {
    if cfg!(target_os = "windows") && target == Target::Linux {
        let compose_variant = CommandSpec::docker_compose_variant(workspace_root)
            .ok_or(RunnerError::DockerComposeNotFound)?;

        return Ok(RunMode::LinuxOnWindows(compose_variant));
    }

    if cfg!(target_os = "linux") && target == Target::Windows {
        return Ok(RunMode::WindowsOnLinux);
    }

    Ok(RunMode::Native)
}

#[cfg(test)]
#[test]
fn explicit_packages_do_not_change_workspace_scope() {
    let command = windows_native_command(NativeCommandOptions {
        task: Task::Check,
        features: None,
        no_default_features: false,
        all_targets: false,
        package: Some("veloq-driver-iocp"),
        filter: None,
        target: None,
    });

    assert!(command.args().contains(&"--package".into()));
    assert!(!command.args().contains(&"--workspace".into()));
    assert!(!command.args().contains(&"--exclude".into()));
}

#[cfg(test)]
#[test]
fn all_targets_flag_is_propagated() {
    for task in [Task::Test, Task::Clippy, Task::Check] {
        let options = NativeCommandOptions {
            task,
            features: None,
            no_default_features: false,
            all_targets: true,
            package: None,
            filter: None,
            target: None,
        };
        let linux = linux_native_command(options.clone());
        assert!(linux.args().contains(&"--all-targets".into()));

        let windows = windows_native_command(options);
        assert!(windows.args().contains(&"--all-targets".into()));
    }
}

#[cfg(test)]
#[test]
fn trybuild_warmup_commands_configuration() {
    let runtime_warmup = veloq_runtime_trybuild_warmup_command();
    assert_eq!(runtime_warmup.program(), "cargo");
    assert!(runtime_warmup.args().contains(&"veloq-runtime".into()));
    assert!(runtime_warmup.args().contains(&"compile_tests".into()));

    let std_warmup = veloq_std_trybuild_warmup_command();
    assert_eq!(std_warmup.program(), "cargo");
    assert!(std_warmup.args().contains(&"veloq-std".into()));
    assert!(std_warmup.args().contains(&"receiver_is_not_sync".into()));

    let sync_warmup = veloq_sync_trybuild_warmup_command();
    assert_eq!(sync_warmup.program(), "cargo");
    assert!(sync_warmup.args().contains(&"veloq-sync".into()));
    assert!(sync_warmup.args().contains(&"compile_tests".into()));

    let local_warmup = veloq_local_trybuild_warmup_command();
    assert_eq!(local_warmup.program(), "cargo");
    assert!(local_warmup.args().contains(&"veloq-local".into()));
    assert!(local_warmup.args().contains(&"compile_tests".into()));
}

#[cfg(test)]
#[test]
fn package_matches_filter() {
    assert!(package_matches(None, "veloq-std"));
    assert!(package_matches(None, "veloq-runtime"));
    assert!(package_matches(Some("veloq-std"), "veloq-std"));
    assert!(!package_matches(Some("veloq-std"), "veloq-runtime"));
    assert!(package_matches(
        Some("veloq-runtime,veloq-std"),
        "veloq-std"
    ));
}
