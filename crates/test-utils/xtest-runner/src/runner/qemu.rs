use super::cmd::{create_overlay_image, print_output};
use crate::RunnerError;
use std::{
    fs,
    io::{Error, ErrorKind},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    thread::sleep,
    time::{Duration, Instant},
};

#[derive(Debug, Clone)]
pub struct WindowsVmConfig {
    pub base_image_path: PathBuf,
    pub overlay_image_path: PathBuf,
    pub username: String,
    pub password: String,
    pub ssh_port: u16,
}

impl WindowsVmConfig {
    pub fn from_workspace(workspace_root: &Path) -> Result<Self, RunnerError> {
        let json_path = workspace_root.join("image/win2025-core-rust-gnu.json");
        let mut base_image_path = workspace_root.join("image/win2025-core-rust-gnu.qcow2");
        let mut overlay_image_path = None;
        let mut username = "Administrator".to_string();
        let mut password = "Admin1234!".to_string();
        let ssh_port = 22;

        if let Ok(content) = fs::read_to_string(&json_path) {
            if let Some(path_str) = extract_json_string(&content, "path") {
                let p = PathBuf::from(path_str);
                if p.is_absolute() && p.exists() {
                    base_image_path = p;
                } else if workspace_root.join(&p).exists() {
                    base_image_path = workspace_root.join(&p);
                }
            }
            if let Some(path_str) = extract_json_string(&content, "overlay_path") {
                let p = PathBuf::from(path_str);
                if p.is_absolute() {
                    overlay_image_path = Some(p);
                } else {
                    overlay_image_path = Some(workspace_root.join(&p));
                }
            }
            if let Some(u) = extract_json_string(&content, "username") {
                username = u;
            }
            if let Some(p) = extract_json_string(&content, "password") {
                password = p;
            }
        }

        if !base_image_path.exists() {
            return Err(RunnerError::WindowsImageNotFound(
                base_image_path.display().to_string(),
            ));
        }

        let overlay_image_path = overlay_image_path.unwrap_or_else(|| {
            let file_stem = base_image_path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("win2025-core-rust-gnu");
            base_image_path.with_file_name(format!("{file_stem}-fetch.qcow2"))
        });

        Ok(Self {
            base_image_path,
            overlay_image_path,
            username,
            password,
            ssh_port,
        })
    }

    pub fn ensure_overlay_image(
        &self,
        workspace_root: &Path,
        quiet: bool,
    ) -> Result<(), RunnerError> {
        if self.overlay_image_path.exists() {
            return Ok(());
        }

        if !quiet {
            eprintln!(
                "[xtest-runner] 差分镜像不存在，准备创建差分镜像: {}",
                self.overlay_image_path.display()
            );
            eprintln!(
                "[xtest-runner] 基础镜像路径: {}",
                self.base_image_path.display()
            );
        }

        if let Some(parent) = self.overlay_image_path.parent() {
            let _ = fs::create_dir_all(parent);
        }
        let _ = fs::remove_file(&self.overlay_image_path);

        if !quiet {
            eprintln!("[xtest-runner] 正在执行 qemu-img create 创建差分镜像...");
        }
        create_overlay_image(&self.base_image_path, &self.overlay_image_path)?;

        let build_res = (|| -> Result<(), RunnerError> {
            if !quiet {
                eprintln!("[xtest-runner] 正在启动 QEMU 虚拟机（写模式）以执行 cargo fetch...");
            }
            let vm = QemuInstance::start(self.clone(), false)?;

            if !quiet {
                eprintln!("[xtest-runner] 等待 Windows 虚拟机 SSH 就绪...");
            }
            vm.wait_for_ssh()?;

            if !quiet {
                eprintln!("[xtest-runner] 正在同步工作区代码至 Windows 虚拟机...");
            }
            vm.sync_workspace(workspace_root)?;

            if !quiet {
                eprintln!("[xtest-runner] 正在虚拟机内执行 cargo fetch --locked...");
            }
            let fetch_output = vm.run_powershell("cargo fetch --locked", quiet)?;
            if !fetch_output.status.success() {
                if quiet {
                    print_output(&fetch_output);
                }
                return Err(RunnerError::CargoFetchFailed {
                    code: fetch_output.status.code(),
                });
            }

            if !quiet {
                eprintln!("[xtest-runner] 正在关闭虚拟机以保存差分镜像...");
            }
            vm.shutdown_and_wait(Duration::from_secs(120))?;

            Ok(())
        })();

        if let Err(err) = build_res {
            eprintln!("[xtest-runner] 差分镜像制作失败，清理未完成的镜像文件...");
            let _ = fs::remove_file(&self.overlay_image_path);
            return Err(err);
        }

        if !quiet {
            eprintln!(
                "[xtest-runner] 差分镜像制作完成: {}",
                self.overlay_image_path.display()
            );
        }

        Ok(())
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
    pub fn start(config: WindowsVmConfig, snapshot: bool) -> Result<Self, RunnerError> {
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
                config.overlay_image_path.display()
            ),
        ]);
        cmd.args([
            "-netdev",
            &format!("user,id=net0,hostfwd=tcp::{host_port}-:{}", config.ssh_port),
        ]);
        cmd.args(["-device", "virtio-net-pci,netdev=net0"]);
        if snapshot {
            cmd.arg("-snapshot");
        }
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

    pub fn run_powershell(&self, command: &str, quiet: bool) -> Result<Output, RunnerError> {
        let vm_cmd = format!(
            "powershell -Command \"Set-Location C:\\workspace; {command}; exit $LASTEXITCODE\""
        );

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
            &vm_cmd,
        ]);

        if quiet {
            Ok(cmd.output()?)
        } else {
            let status = cmd.status()?;
            Ok(Output {
                status,
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    pub fn run_in_vm(&self, forward_args: &[String], quiet: bool) -> Result<Output, RunnerError> {
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

        let inner_cmd = format!("cargo run -q -p xtest-runner -- {escaped_args}");
        self.run_powershell(&inner_cmd, quiet)
    }

    pub fn shutdown_and_wait(mut self, timeout: Duration) -> Result<(), RunnerError> {
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
            "ConnectTimeout=5",
            "-o",
            "LogLevel=ERROR",
            "-p",
            &self.host_port.to_string(),
            &format!("{}@127.0.0.1", self.config.username),
            "shutdown /s /t 0",
        ]);
        let _ = cmd.status();

        let start = Instant::now();
        let poll_interval = Duration::from_millis(500);
        while start.elapsed() < timeout {
            if let Ok(Some(_)) = self.child.try_wait() {
                return Ok(());
            }
            sleep(poll_interval);
        }

        let _ = self.child.kill();
        let _ = self.child.wait();
        Err(RunnerError::VmShutdownTimeout)
    }
}
