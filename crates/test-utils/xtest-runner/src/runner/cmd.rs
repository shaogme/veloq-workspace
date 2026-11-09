use crate::cli::RunnerError;
use std::{
    fs,
    io::{Error, ErrorKind},
    path::{Path, PathBuf},
    process::{Command, ExitStatus, Output},
};

#[derive(Clone, Copy, Debug)]
pub(super) enum DockerComposeVariant {
    Standalone,
    Plugin,
}

#[derive(Clone, Debug)]
pub(super) struct CommandSpec {
    program: String,
    args: Vec<String>,
    envs: Vec<(String, String)>,
}

impl CommandSpec {
    pub(super) fn new(program: impl Into<String>, args: impl Into<Vec<String>>) -> Self {
        Self {
            program: program.into(),
            args: args.into(),
            envs: Vec::new(),
        }
    }

    pub(super) fn with_arg(mut self, arg: impl Into<String>) -> Self {
        self.args.push(arg.into());
        self
    }

    #[cfg(test)]
    pub(super) fn args(&self) -> &[String] {
        &self.args
    }

    #[cfg(test)]
    pub(super) fn program(&self) -> &str {
        &self.program
    }

    pub(super) fn display(&self) -> String {
        let env_part = self
            .envs
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
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

    pub(super) fn command_works(program: &str, args: &[&str], workspace_root: &Path) -> bool {
        Command::new(program)
            .args(args)
            .current_dir(workspace_root)
            .output()
            .map(|output| output.status.success())
            .unwrap_or(false)
    }

    pub(super) fn docker_compose_variant(workspace_root: &Path) -> Option<DockerComposeVariant> {
        if Self::command_works("docker-compose", &["version"], workspace_root) {
            return Some(DockerComposeVariant::Standalone);
        }

        if Self::command_works("docker", &["compose", "version"], workspace_root) {
            return Some(DockerComposeVariant::Plugin);
        }

        None
    }

    pub(super) fn print_output(output: &Output) {
        if !output.stdout.is_empty() {
            eprintln!("----- stdout -----");
            eprintln!("{}", String::from_utf8_lossy(&output.stdout));
        }

        if !output.stderr.is_empty() {
            eprintln!("----- stderr -----");
            eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        }
    }

    pub(super) fn workspace_root() -> Result<PathBuf, RunnerError> {
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

    pub(super) fn status(&self, workspace_root: &Path) -> Result<ExitStatus, RunnerError> {
        if !workspace_root.exists() {
            return Err(Error::new(
                ErrorKind::NotFound,
                format!("Workspace root does not exist: {:?}", workspace_root),
            )
            .into());
        }
        let mut process = Command::new(&self.program);
        process
            .args(&self.args)
            .envs(self.envs.iter().map(|(key, value)| (key, value)))
            .current_dir(workspace_root);
        Ok(process.status()?)
    }

    pub(super) fn command_output(
        command: &Self,
        workspace_root: &Path,
    ) -> Result<Output, RunnerError> {
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
            .envs(command.envs.iter().map(|(key, value)| (key, value)))
            .current_dir(workspace_root);
        Ok(process.output()?)
    }

    pub(super) fn create_overlay_image(
        base_image: &Path,
        overlay_image: &Path,
    ) -> Result<(), RunnerError> {
        let mut command = Command::new("qemu-img");
        command.args(["create", "-f", "qcow2", "-b"]);
        command.arg(base_image);
        command.args(["-F", "qcow2"]);
        command.arg(overlay_image);

        let output = command.output()?;
        if output.status.success() {
            Ok(())
        } else {
            Err(RunnerError::CreateOverlayFailed {
                code: output.status.code(),
            })
        }
    }
}
