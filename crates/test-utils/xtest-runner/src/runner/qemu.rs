use crate::RunnerError;
use std::{
    fs,
    io::{Error, ErrorKind},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct WindowsVmConfig {
    pub image_path: PathBuf,
    pub username: String,
    pub password: String,
    pub ssh_port: u16,
}

impl WindowsVmConfig {
    pub fn from_workspace(workspace_root: &Path) -> Result<Self, RunnerError> {
        let json_path = workspace_root.join("image/win2025-core-rust-gnu.json");
        let mut image_path = workspace_root.join("image/win2025-core-rust-gnu.qcow2");
        let mut username = "Administrator".to_string();
        let mut password = "Admin1234!".to_string();
        let ssh_port = 22;

        if let Ok(content) = fs::read_to_string(&json_path) {
            if let Some(path_str) = extract_json_string(&content, "path") {
                let p = PathBuf::from(path_str);
                if p.is_absolute() && p.exists() {
                    image_path = p;
                } else if workspace_root.join(&p).exists() {
                    image_path = workspace_root.join(&p);
                }
            }
            if let Some(u) = extract_json_string(&content, "username") {
                username = u;
            }
            if let Some(p) = extract_json_string(&content, "password") {
                password = p;
            }
        }

        if !image_path.exists() {
            return Err(RunnerError::WindowsImageNotFound(
                image_path.display().to_string(),
            ));
        }

        Ok(Self {
            image_path,
            username,
            password,
            ssh_port,
        })
    }
}

fn extract_json_string(json: &str, key: &str) -> Option<String> {
    let pattern = format!("\"{key}\":");
    let pos = json.find(&pattern)?;
    let rest = &json[pos + pattern.len()..];
    let quote_start = rest.find('"')?;
    let rest_after_quote = &rest[quote_start + 1..];
    let quote_end = rest_after_quote.find('"')?;
    Some(rest_after_quote[..quote_end].to_string())
}

fn allocate_free_port() -> Result<u16, Error> {
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

pub struct QemuInstance {
    child: Child,
    pub host_port: u16,
    pub config: WindowsVmConfig,
}

impl Drop for QemuInstance {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

impl QemuInstance {
    pub fn start(config: WindowsVmConfig) -> Result<Self, RunnerError> {
        let host_port = allocate_free_port().map_err(RunnerError::Io)?;
        let mut cmd = Command::new("qemu-system-x86_64");

        if Path::new("/dev/kvm").exists() {
            cmd.arg("-enable-kvm");
            cmd.args(["-cpu", "host"]);
        } else {
            cmd.args(["-accel", "tcg"]);
        }

        cmd.args(["-smp", "4"]);
        cmd.args(["-m", "4096"]);
        cmd.args([
            "-drive",
            &format!(
                "file={},format=qcow2,if=virtio",
                config.image_path.display()
            ),
        ]);
        cmd.args([
            "-netdev",
            &format!("user,id=net0,hostfwd=tcp::{host_port}-:{}", config.ssh_port),
        ]);
        cmd.args(["-device", "virtio-net-pci,netdev=net0"]);
        cmd.arg("-snapshot");
        cmd.args(["-display", "none"]);

        cmd.stdout(Stdio::null());
        cmd.stderr(Stdio::null());

        let child = cmd.spawn().map_err(|_| RunnerError::QemuNotFound)?;

        Ok(Self {
            child,
            host_port,
            config,
        })
    }

    pub fn wait_for_ssh(&self) -> Result<(), RunnerError> {
        let start = Instant::now();
        let timeout = Duration::from_secs(60);
        let poll_interval = Duration::from_millis(500);

        while start.elapsed() < timeout {
            let mut cmd = Command::new("sshpass");
            cmd.args([
                "-p",
                &self.config.password,
                "ssh",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "ConnectTimeout=2",
                "-o",
                "LogLevel=ERROR",
                "-p",
                &self.host_port.to_string(),
                &format!("{}@127.0.0.1", self.config.username),
                "echo ready",
            ]);
            cmd.stdout(Stdio::null());
            cmd.stderr(Stdio::null());

            if let Ok(status) = cmd.status()
                && status.success()
            {
                return Ok(());
            }
            sleep(poll_interval);
        }

        Err(RunnerError::VmSshTimeout)
    }

    pub fn sync_workspace(&self, workspace_root: &Path) -> Result<(), RunnerError> {
        let mut tar_child = Command::new("tar")
            .args(["--exclude=image", "--exclude=target", "-cf", "-", "."])
            .current_dir(workspace_root)
            .stdout(Stdio::piped())
            .spawn()?;

        let tar_stdout = tar_child
            .stdout
            .take()
            .ok_or_else(|| Error::new(ErrorKind::BrokenPipe, "无法获取 tar stdout"))?;

        let mut ssh_child = Command::new("sshpass")
            .args([
                "-p",
                &self.config.password,
                "ssh",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                "-p",
                &self.host_port.to_string(),
                &format!("{}@127.0.0.1", self.config.username),
                "powershell -Command \"if (-not (Test-Path 'C:\\workspace')) { New-Item -ItemType Directory -Path 'C:\\workspace' | Out-Null }; tar -xf - -C C:\\workspace\"",
            ])
            .stdin(tar_stdout)
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;

        let tar_status = tar_child.wait()?;
        let ssh_status = ssh_child.wait()?;

        if !tar_status.success() {
            return Err(RunnerError::VmSyncFailed {
                code: tar_status.code(),
            });
        }
        if !ssh_status.success() {
            return Err(RunnerError::VmSyncFailed {
                code: ssh_status.code(),
            });
        }

        Ok(())
    }

    pub fn run_in_vm(&self, forward_args: &[String]) -> Result<ExitStatus, RunnerError> {
        let escaped_args = forward_args
            .iter()
            .map(|arg| {
                if arg.contains(' ') {
                    format!("\"{arg}\"")
                } else {
                    arg.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(" ");

        let vm_cmd = format!(
            "powershell -Command \"Set-Location C:\\workspace; cargo run -p xtest-runner -- {escaped_args}\""
        );

        let status = Command::new("sshpass")
            .args([
                "-p",
                &self.config.password,
                "ssh",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-o",
                "LogLevel=ERROR",
                "-p",
                &self.host_port.to_string(),
                &format!("{}@127.0.0.1", self.config.username),
                &vm_cmd,
            ])
            .status()?;

        Ok(status)
    }
}

pub fn ensure_devbox_path(workspace_root: &Path) {
    let devbox_bin = workspace_root.join(".devbox/nix/profile/default/bin");
    if devbox_bin.exists()
        && let Some(current_path) = std::env::var_os("PATH")
    {
        let mut paths = std::env::split_paths(&current_path).collect::<Vec<_>>();
        if !paths.iter().any(|p| p == &devbox_bin) {
            paths.insert(0, devbox_bin);
            if let Ok(new_path) = std::env::join_paths(paths) {
                // SAFETY: In single-threaded runner setup before spawning threads.
                unsafe {
                    std::env::set_var("PATH", new_path);
                }
            }
        }
    }
}
