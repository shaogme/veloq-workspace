//! A module for working with processes.

pub(crate) mod sys;

#[cfg(test)]
mod tests;

use core::fmt;

use crate::{alloc_crate::vec::Vec, ffi::OsStr, io::Result, path::Path};

pub use sys::{Child, ChildStderr, ChildStdin, ChildStdout};

/// Describes the result of a process after it has terminated.
#[derive(Copy, Clone, PartialEq, Eq)]
pub struct ExitStatus(pub(crate) sys::ExitStatus);

impl ExitStatus {
    /// Was termination successful? Returns a boolean indicating
    /// whether the process exited with success (exit code 0).
    #[inline]
    pub fn success(&self) -> bool {
        self.0.success()
    }

    /// Returns the exit code of the process, if any.
    #[inline]
    pub fn code(&self) -> Option<i32> {
        self.0.code()
    }
}

impl fmt::Display for ExitStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::Debug for ExitStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.0, f)
    }
}

/// This type represents the status code the current process can return
/// to its parent under normal termination.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExitCode(i32);

impl ExitCode {
    /// The canonical ExitCode for successful termination on this platform.
    pub const SUCCESS: ExitCode = ExitCode(0);

    /// The canonical ExitCode for unsuccessful termination on this platform.
    pub const FAILURE: ExitCode = ExitCode(1);

    /// Returns the integer exit code value.
    #[inline]
    pub const fn as_i32(&self) -> i32 {
        self.0
    }
}

impl From<u8> for ExitCode {
    #[inline]
    fn from(code: u8) -> Self {
        ExitCode(code as i32)
    }
}

/// The output of a finished process.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Output {
    /// The status (exit code) of the process.
    pub status: ExitStatus,
    /// The data that the process wrote to stdout.
    pub stdout: Vec<u8>,
    /// The data that the process wrote to stderr.
    pub stderr: Vec<u8>,
}

/// Describes what to do with a standard I/O stream for a child process.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Stdio {
    /// The child inherits from the corresponding parent descriptor.
    #[default]
    Inherit,
    /// A new pipe should be arranged to connect the parent and child processes.
    Piped,
    /// This stream will be ignored.
    Null,
}

impl Stdio {
    #[inline]
    pub const fn inherit() -> Stdio {
        Stdio::Inherit
    }

    #[inline]
    pub const fn piped() -> Stdio {
        Stdio::Piped
    }

    #[inline]
    pub const fn null() -> Stdio {
        Stdio::Null
    }
}

/// A process builder, providing fine-grained control over how a new process should be spawned.
#[derive(Clone, Debug)]
pub struct Command {
    inner: sys::Command,
}

impl Command {
    /// Constructs a new `Command` for launching the program at
    /// path `program`, with the following default configuration:
    ///
    /// * No arguments to the program
    /// * Inherit the current process's environment
    /// * Inherit the current process's working directory
    /// * Inherit stdin/stdout/stderr for [`spawn`] or [`status`], but create pipes for [`output`]
    #[inline]
    pub fn new<S: AsRef<OsStr>>(program: S) -> Command {
        Command {
            inner: sys::Command::new(program.as_ref()),
        }
    }

    /// Adds an argument to pass to the program.
    #[inline]
    pub fn arg<S: AsRef<OsStr>>(&mut self, arg: S) -> &mut Command {
        self.inner.arg(arg.as_ref());
        self
    }

    /// Adds multiple arguments to pass to the program.
    pub fn args<I, S>(&mut self, args: I) -> &mut Command
    where
        I: IntoIterator<Item = S>,
        S: AsRef<OsStr>,
    {
        for arg in args {
            self.arg(arg);
        }
        self
    }

    /// Inserts or updates an explicit environment variable mapping for the child process.
    #[inline]
    pub fn env<K: AsRef<OsStr>, V: AsRef<OsStr>>(&mut self, key: K, val: V) -> &mut Command {
        self.inner.env(key.as_ref(), val.as_ref());
        self
    }

    /// Adds or updates multiple environment variable mappings.
    pub fn envs<I, K, V>(&mut self, vars: I) -> &mut Command
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<OsStr>,
        V: AsRef<OsStr>,
    {
        for (k, v) in vars {
            self.env(k, v);
        }
        self
    }

    /// Removes an explicitly set environment variable.
    #[inline]
    pub fn env_remove<K: AsRef<OsStr>>(&mut self, key: K) -> &mut Command {
        self.inner.env_remove(key.as_ref());
        self
    }

    /// Clears the entire environment map for the child process.
    #[inline]
    pub fn env_clear(&mut self) -> &mut Command {
        self.inner.env_clear();
        self
    }

    /// Sets the working directory for the child process.
    #[inline]
    pub fn current_dir<P: AsRef<Path>>(&mut self, dir: P) -> &mut Command {
        self.inner.current_dir(dir.as_ref().to_path_buf());
        self
    }

    /// Configuration for the child process's standard input (stdin) handle.
    #[inline]
    pub fn stdin(&mut self, cfg: Stdio) -> &mut Command {
        self.inner.stdin(cfg);
        self
    }

    /// Configuration for the child process's standard output (stdout) handle.
    #[inline]
    pub fn stdout(&mut self, cfg: Stdio) -> &mut Command {
        self.inner.stdout(cfg);
        self
    }

    /// Configuration for the child process's standard error (stderr) handle.
    #[inline]
    pub fn stderr(&mut self, cfg: Stdio) -> &mut Command {
        self.inner.stderr(cfg);
        self
    }

    /// Executes the command as a child process, returning a handle to it.
    #[inline]
    pub fn spawn(&mut self) -> Result<Child> {
        self.inner.spawn()
    }

    /// Executes the command as a child process, waiting for it to finish and
    /// collecting all of its output.
    pub fn output(&mut self) -> Result<Output> {
        self.inner.stdout(Stdio::Piped);
        self.inner.stderr(Stdio::Piped);
        let child = self.spawn()?;
        child.wait_with_output()
    }

    /// Executes a command as a child process, waiting for it to finish and
    /// collecting its status.
    pub fn status(&mut self) -> Result<ExitStatus> {
        let mut child = self.spawn()?;
        child.wait()
    }
}

/// Returns the OS-assigned process identifier associated with this process.
#[inline]
pub fn id() -> u32 {
    sys::id()
}

/// Terminates the current process with the specified exit code.
#[inline]
pub fn exit(code: i32) -> ! {
    sys::exit(code)
}

/// Terminates the process in an abnormal fashion.
#[inline]
pub fn abort() -> ! {
    sys::abort()
}
