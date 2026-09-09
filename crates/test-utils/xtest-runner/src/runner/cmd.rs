use crate::RunnerError;
use std::{
    fs,
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Output},
};

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
    let mut current = manifest_dir;
    while let Some(parent) = current.parent() {
        let cargo_toml = parent.join("Cargo.toml");
        if cargo_toml.exists()
            && let Ok(content) = fs::read_to_string(&cargo_toml)
            && content.contains("[workspace]")
        {
            return Ok(parent.to_path_buf());
        }
        current = parent;
    }

    manifest_dir
        .parent()
        .and_then(Path::parent)
        .and_then(Path::parent)
        .map(Path::to_path_buf)
        .ok_or(RunnerError::WorkspaceRootResolutionFailed)
}

pub fn command_status(
    command: &CommandSpec,
    workspace_root: &Path,
) -> Result<ExitStatus, RunnerError> {
    if !workspace_root.exists() {
        return Err(Error::new(
            ErrorKind::NotFound,
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
        return Err(Error::new(
            ErrorKind::NotFound,
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

pub fn create_overlay_image(base_image: &Path, overlay_image: &Path) -> Result<(), RunnerError> {
    let mut cmd = Command::new("qemu-img");
    cmd.args(["create", "-f", "qcow2", "-b"]);
    cmd.arg(base_image);
    cmd.args(["-F", "qcow2"]);
    cmd.arg(overlay_image);

    let output = cmd.output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(RunnerError::CreateOverlayFailed {
            code: output.status.code(),
        })
    }
}
