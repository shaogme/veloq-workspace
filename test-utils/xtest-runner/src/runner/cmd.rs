use crate::RunnerError;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

#[derive(Clone, Copy, Debug)]
pub enum DockerComposeVariant {
    Standalone,
    Plugin,
}

#[derive(Clone, Debug)]
pub struct CommandSpec {
    pub program: String,
    pub args: Vec<String>,
    pub envs: Vec<(String, String)>,
}

impl CommandSpec {
    pub fn new(program: impl Into<String>, args: impl Into<Vec<String>>) -> Self {
        Self {
            program: program.into(),
            args: args.into(),
            envs: Vec::new(),
        }
    }

    pub fn with_env(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.envs.push((key.into(), value.into()));
        self
    }

    pub fn display(&self) -> String {
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

pub fn has_rust_target(target: &str, workspace_root: &Path) -> Result<bool, RunnerError> {
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

pub fn command_works(program: &str, args: &[&str], workspace_root: &Path) -> bool {
    Command::new(program)
        .args(args)
        .current_dir(workspace_root)
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

pub fn docker_compose_variant(workspace_root: &Path) -> Option<DockerComposeVariant> {
    if command_works("docker-compose", &["version"], workspace_root) {
        return Some(DockerComposeVariant::Standalone);
    }

    if command_works("docker", &["compose", "version"], workspace_root) {
        return Some(DockerComposeVariant::Plugin);
    }

    None
}

pub fn print_output(output: &Output) {
    if !output.stdout.is_empty() {
        eprintln!("----- stdout -----");
        eprintln!("{}", String::from_utf8_lossy(&output.stdout));
    }

    if !output.stderr.is_empty() {
        eprintln!("----- stderr -----");
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
    }
}

pub fn workspace_root() -> Result<PathBuf, RunnerError> {
    let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or(RunnerError::WorkspaceRootResolutionFailed)
}

pub fn command_status(
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

pub fn command_output(command: &CommandSpec, workspace_root: &Path) -> Result<Output, RunnerError> {
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
