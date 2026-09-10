use core::fmt;

use crate::{
    alloc_crate::{ffi::CString, vec::Vec},
    ffi::OsStr,
    io::{Error, Read, Result, Write},
    os::{
        cvt::cvt,
        unix::{
            fd::{AsRawFd, FromRawFd, IntoRawFd, OwnedFd},
            ffi::OsStrExt,
        },
    },
    path::PathBuf,
    process::{ExitStatus as PublicExitStatus, Output, Stdio},
    vec,
};

pub fn id() -> u32 {
    unsafe { libc::getpid() as u32 }
}

pub fn exit(code: i32) -> ! {
    unsafe { libc::exit(code) }
}

pub fn abort() -> ! {
    unsafe { libc::abort() }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct ExitStatus(pub(crate) libc::c_int);

impl ExitStatus {
    pub fn success(&self) -> bool {
        self.code() == Some(0)
    }

    pub fn code(&self) -> Option<i32> {
        if libc::WIFEXITED(self.0) {
            Some(libc::WEXITSTATUS(self.0))
        } else {
            None
        }
    }
}

impl fmt::Display for ExitStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(code) = self.code() {
            write!(f, "exit status: {code}")
        } else if libc::WIFSIGNALED(self.0) {
            write!(f, "signal: {}", libc::WTERMSIG(self.0))
        } else if libc::WIFSTOPPED(self.0) {
            write!(f, "stopped: {}", libc::WSTOPSIG(self.0))
        } else {
            write!(f, "unknown exit status: {}", self.0)
        }
    }
}

impl fmt::Debug for ExitStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug)]
pub struct ChildStdin(pub(crate) OwnedFd);

impl Write for ChildStdin {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let res = unsafe { libc::write(self.0.as_raw_fd(), buf.as_ptr() as *const _, buf.len()) };
        if res < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(res as usize)
        }
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ChildStdout(pub(crate) OwnedFd);

impl Read for ChildStdout {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let res = unsafe { libc::read(self.0.as_raw_fd(), buf.as_mut_ptr() as *mut _, buf.len()) };
        if res < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(res as usize)
        }
    }
}

#[derive(Debug)]
pub struct ChildStderr(pub(crate) OwnedFd);

impl Read for ChildStderr {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let res = unsafe { libc::read(self.0.as_raw_fd(), buf.as_mut_ptr() as *mut _, buf.len()) };
        if res < 0 {
            Err(Error::last_os_error())
        } else {
            Ok(res as usize)
        }
    }
}

fn create_pipe() -> Result<(OwnedFd, OwnedFd)> {
    let mut fds = [0; 2];
    cvt(unsafe { libc::pipe(fds.as_mut_ptr()) })?;
    unsafe {
        libc::fcntl(fds[0], libc::F_SETFD, libc::FD_CLOEXEC);
        libc::fcntl(fds[1], libc::F_SETFD, libc::FD_CLOEXEC);
        Ok((OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])))
    }
}

#[derive(Debug)]
pub struct Child {
    pid: libc::pid_t,
    status: Option<ExitStatus>,
    pub stdin: Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    pub stderr: Option<ChildStderr>,
}

impl Child {
    pub fn id(&self) -> u32 {
        self.pid as u32
    }

    pub fn wait(&mut self) -> Result<PublicExitStatus> {
        if let Some(status) = self.status {
            return Ok(PublicExitStatus(status));
        }
        let mut status = 0;
        cvt(unsafe { libc::waitpid(self.pid, &mut status, 0) })?;
        let exit_status = ExitStatus(status);
        self.status = Some(exit_status);
        Ok(PublicExitStatus(exit_status))
    }

    pub fn try_wait(&mut self) -> Result<Option<PublicExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(PublicExitStatus(status)));
        }
        let mut status = 0;
        let res = cvt(unsafe { libc::waitpid(self.pid, &mut status, libc::WNOHANG) })?;
        if res == 0 {
            Ok(None)
        } else {
            let exit_status = ExitStatus(status);
            self.status = Some(exit_status);
            Ok(Some(PublicExitStatus(exit_status)))
        }
    }

    pub fn kill(&mut self) -> Result<()> {
        cvt(unsafe { libc::kill(self.pid, libc::SIGKILL) }).map(drop)
    }

    pub fn wait_with_output(mut self) -> Result<Output> {
        drop(self.stdin.take());

        let mut stdout_buf = Vec::new();
        if let Some(mut out) = self.stdout.take() {
            let _ = out.read_to_end(&mut stdout_buf);
        }

        let mut stderr_buf = Vec::new();
        if let Some(mut err) = self.stderr.take() {
            let _ = err.read_to_end(&mut stderr_buf);
        }

        let status = self.wait()?;
        Ok(Output {
            status,
            stdout: stdout_buf,
            stderr: stderr_buf,
        })
    }
}

#[derive(Clone, Debug)]
pub struct Command {
    program: CString,
    args: Vec<CString>,
    current_dir: Option<CString>,
    env: Vec<(CString, Option<CString>)>,
    clear_env: bool,
    stdin: Stdio,
    stdout: Stdio,
    stderr: Stdio,
}

impl Command {
    pub fn new(program: &OsStr) -> Self {
        let p = CString::new(program.as_bytes()).unwrap_or_else(|_| CString::default());
        let arg0 = p.clone();
        Self {
            program: p,
            args: vec![arg0],
            current_dir: None,
            env: Vec::new(),
            clear_env: false,
            stdin: Stdio::Inherit,
            stdout: Stdio::Inherit,
            stderr: Stdio::Inherit,
        }
    }

    pub fn arg(&mut self, arg: &OsStr) {
        if let Ok(c) = CString::new(arg.as_bytes()) {
            self.args.push(c);
        }
    }

    pub fn current_dir(&mut self, dir: PathBuf) {
        if let Ok(c) = CString::new(dir.into_os_string().as_bytes()) {
            self.current_dir = Some(c);
        }
    }

    pub fn env(&mut self, key: &OsStr, val: &OsStr) {
        if let (Ok(k), Ok(v)) = (CString::new(key.as_bytes()), CString::new(val.as_bytes())) {
            self.env.push((k, Some(v)));
        }
    }

    pub fn env_remove(&mut self, key: &OsStr) {
        if let Ok(k) = CString::new(key.as_bytes()) {
            self.env.push((k, None));
        }
    }

    pub fn env_clear(&mut self) {
        self.clear_env = true;
        self.env.clear();
    }

    pub fn stdin(&mut self, cfg: Stdio) {
        self.stdin = cfg;
    }

    pub fn stdout(&mut self, cfg: Stdio) {
        self.stdout = cfg;
    }

    pub fn stderr(&mut self, cfg: Stdio) {
        self.stderr = cfg;
    }

    pub fn spawn(&mut self) -> Result<Child> {
        let (stdin_read, stdin_write) = match self.stdin {
            Stdio::Piped => {
                let (r, w) = create_pipe()?;
                (Some(r), Some(w))
            }
            Stdio::Null => {
                let dev_null = cvt(unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_RDONLY) })?;
                (Some(unsafe { OwnedFd::from_raw_fd(dev_null) }), None)
            }
            Stdio::Inherit => (None, None),
        };

        let (stdout_read, stdout_write) = match self.stdout {
            Stdio::Piped => {
                let (r, w) = create_pipe()?;
                (Some(r), Some(w))
            }
            Stdio::Null => {
                let dev_null = cvt(unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_WRONLY) })?;
                (None, Some(unsafe { OwnedFd::from_raw_fd(dev_null) }))
            }
            Stdio::Inherit => (None, None),
        };

        let (stderr_read, stderr_write) = match self.stderr {
            Stdio::Piped => {
                let (r, w) = create_pipe()?;
                (Some(r), Some(w))
            }
            Stdio::Null => {
                let dev_null = cvt(unsafe { libc::open(c"/dev/null".as_ptr(), libc::O_WRONLY) })?;
                (None, Some(unsafe { OwnedFd::from_raw_fd(dev_null) }))
            }
            Stdio::Inherit => (None, None),
        };

        let pid = unsafe { libc::fork() };
        if pid < 0 {
            return Err(Error::last_os_error());
        }

        if pid == 0 {
            // Child process
            if let Some(r) = stdin_read {
                unsafe { libc::dup2(r.into_raw_fd(), libc::STDIN_FILENO) };
            }
            if let Some(w) = stdout_write {
                unsafe { libc::dup2(w.into_raw_fd(), libc::STDOUT_FILENO) };
            }
            if let Some(w) = stderr_write {
                unsafe { libc::dup2(w.into_raw_fd(), libc::STDERR_FILENO) };
            }

            if let Some(ref dir) = self.current_dir {
                unsafe { libc::chdir(dir.as_ptr()) };
            }

            if self.clear_env {
                unsafe {
                    #[cfg(any(target_os = "linux", target_os = "android"))]
                    libc::clearenv();
                }
            }

            for (k, v) in &self.env {
                match v {
                    Some(val) => unsafe {
                        libc::setenv(k.as_ptr(), val.as_ptr(), 1);
                    },
                    None => unsafe {
                        libc::unsetenv(k.as_ptr());
                    },
                }
            }

            let mut argv: Vec<*const libc::c_char> = self.args.iter().map(|a| a.as_ptr()).collect();
            argv.push(core::ptr::null());

            unsafe {
                libc::execvp(self.program.as_ptr(), argv.as_ptr());
                libc::_exit(127);
            }
        }

        // Parent process
        drop(stdin_read);
        drop(stdout_write);
        drop(stderr_write);

        Ok(Child {
            pid,
            status: None,
            stdin: stdin_write.map(ChildStdin),
            stdout: stdout_read.map(ChildStdout),
            stderr: stderr_read.map(ChildStderr),
        })
    }
}
