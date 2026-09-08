use core::fmt;

use crate::{
    alloc_crate::vec::Vec,
    ffi::{OsStr, OsString},
    io::{Error, Read, Result, Write},
    os::{
        cvt::cvt,
        windows::{
            ffi::OsStrExt,
            io::{AsRawHandle, FromRawHandle, OwnedHandle, RawHandle},
        },
    },
    path::PathBuf,
    process::{ExitStatus as PublicExitStatus, Output, Stdio},
    vec,
};
use windows_sys::Win32::{
    Foundation::{
        CloseHandle, HANDLE, HANDLE_FLAG_INHERIT, INVALID_HANDLE_VALUE, SetHandleInformation,
        WAIT_OBJECT_0, WAIT_TIMEOUT,
    },
    Security::SECURITY_ATTRIBUTES,
    Storage::FileSystem::{ReadFile, WriteFile},
    System::{
        Pipes::CreatePipe,
        Threading::{
            CreateProcessW, ExitProcess, GetCurrentProcessId, GetExitCodeProcess, INFINITE,
            PROCESS_INFORMATION, STARTF_USESTDHANDLES, STARTUPINFOW, TerminateProcess,
            WaitForSingleObject,
        },
    },
};

pub fn id() -> u32 {
    unsafe { GetCurrentProcessId() }
}

pub fn exit(code: i32) -> ! {
    unsafe { ExitProcess(code as u32) }
}

pub fn abort() -> ! {
    unsafe { ExitProcess(3) }
}

#[derive(Copy, Clone, PartialEq, Eq)]
pub struct ExitStatus(pub(crate) u32);

impl ExitStatus {
    pub fn success(&self) -> bool {
        self.0 == 0
    }

    pub fn code(&self) -> Option<i32> {
        Some(self.0 as i32)
    }
}

impl fmt::Display for ExitStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "exit code: {}", self.0)
    }
}

impl fmt::Debug for ExitStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

#[derive(Debug)]
pub struct ChildStdin(pub(crate) OwnedHandle);

impl Write for ChildStdin {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        let mut written = 0u32;
        let res = unsafe {
            WriteFile(
                self.0.as_raw_handle() as _,
                buf.as_ptr(),
                buf.len() as u32,
                &mut written,
                core::ptr::null_mut(),
            )
        };
        if res == 0 {
            Err(Error::last_os_error())
        } else {
            Ok(written as usize)
        }
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
pub struct ChildStdout(pub(crate) OwnedHandle);

impl Read for ChildStdout {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut bytes_read = 0u32;
        let res = unsafe {
            ReadFile(
                self.0.as_raw_handle() as _,
                buf.as_mut_ptr(),
                buf.len() as u32,
                &mut bytes_read,
                core::ptr::null_mut(),
            )
        };
        if res == 0 {
            let err = Error::last_os_error();
            // ERROR_BROKEN_PIPE is EOF
            if err.raw_os_error() == Some(109) {
                Ok(0)
            } else {
                Err(err)
            }
        } else {
            Ok(bytes_read as usize)
        }
    }
}

#[derive(Debug)]
pub struct ChildStderr(pub(crate) OwnedHandle);

impl Read for ChildStderr {
    fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let mut bytes_read = 0u32;
        let res = unsafe {
            ReadFile(
                self.0.as_raw_handle() as _,
                buf.as_mut_ptr(),
                buf.len() as u32,
                &mut bytes_read,
                core::ptr::null_mut(),
            )
        };
        if res == 0 {
            let err = Error::last_os_error();
            if err.raw_os_error() == Some(109) {
                Ok(0)
            } else {
                Err(err)
            }
        } else {
            Ok(bytes_read as usize)
        }
    }
}

fn create_pipe(ours_readable: bool) -> Result<(OwnedHandle, OwnedHandle)> {
    let mut sa: SECURITY_ATTRIBUTES = unsafe { core::mem::zeroed() };
    sa.nLength = core::mem::size_of::<SECURITY_ATTRIBUTES>() as u32;
    sa.bInheritHandle = 1;

    let mut read_handle = INVALID_HANDLE_VALUE as HANDLE;
    let mut write_handle = INVALID_HANDLE_VALUE as HANDLE;

    cvt(unsafe { CreatePipe(&mut read_handle, &mut write_handle, &sa, 0) })?;

    let (ours, theirs) = if ours_readable {
        (read_handle, write_handle)
    } else {
        (write_handle, read_handle)
    };

    cvt(unsafe { SetHandleInformation(ours, HANDLE_FLAG_INHERIT, 0) })?;

    Ok((
        unsafe { OwnedHandle::from_raw_handle(ours as RawHandle) },
        unsafe { OwnedHandle::from_raw_handle(theirs as RawHandle) },
    ))
}

#[derive(Debug)]
pub struct Child {
    handle: OwnedHandle,
    pid: u32,
    status: Option<ExitStatus>,
    pub stdin: Option<ChildStdin>,
    pub stdout: Option<ChildStdout>,
    pub stderr: Option<ChildStderr>,
}

impl Child {
    pub fn id(&self) -> u32 {
        self.pid
    }

    pub fn wait(&mut self) -> Result<PublicExitStatus> {
        if let Some(status) = self.status {
            return Ok(PublicExitStatus(status));
        }
        let res = unsafe { WaitForSingleObject(self.handle.as_raw_handle() as _, INFINITE) };
        if res != WAIT_OBJECT_0 {
            return Err(Error::last_os_error());
        }
        let mut exit_code = 0u32;
        cvt(unsafe { GetExitCodeProcess(self.handle.as_raw_handle() as _, &mut exit_code) })?;
        let status = ExitStatus(exit_code);
        self.status = Some(status);
        Ok(PublicExitStatus(status))
    }

    pub fn try_wait(&mut self) -> Result<Option<PublicExitStatus>> {
        if let Some(status) = self.status {
            return Ok(Some(PublicExitStatus(status)));
        }
        let res = unsafe { WaitForSingleObject(self.handle.as_raw_handle() as _, 0) };
        if res == WAIT_TIMEOUT {
            Ok(None)
        } else if res == WAIT_OBJECT_0 {
            let mut exit_code = 0u32;
            cvt(unsafe { GetExitCodeProcess(self.handle.as_raw_handle() as _, &mut exit_code) })?;
            let status = ExitStatus(exit_code);
            self.status = Some(status);
            Ok(Some(PublicExitStatus(status)))
        } else {
            Err(Error::last_os_error())
        }
    }

    pub fn kill(&mut self) -> Result<()> {
        cvt(unsafe { TerminateProcess(self.handle.as_raw_handle() as _, 1) }).map(drop)
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

fn append_arg(cmd: &mut Vec<u16>, arg: &OsStr) {
    if !cmd.is_empty() {
        cmd.push(b' ' as u16);
    }
    let arg_wide: Vec<u16> = arg.encode_wide().collect();
    let has_whitespace = arg_wide
        .iter()
        .any(|&c| c == b' ' as u16 || c == b'\t' as u16);
    if arg_wide.is_empty() {
        cmd.push(b'"' as u16);
        cmd.push(b'"' as u16);
        return;
    }
    if !has_whitespace && !arg_wide.contains(&(b'"' as u16)) {
        cmd.extend_from_slice(&arg_wide);
        return;
    }

    cmd.push(b'"' as u16);
    let mut backslashes = 0;
    for &c in &arg_wide {
        if c == b'\\' as u16 {
            backslashes += 1;
        } else {
            if c == b'"' as u16 {
                cmd.extend(core::iter::repeat_n(b'\\' as u16, backslashes * 2 + 1));
                cmd.push(b'"' as u16);
            } else {
                cmd.extend(core::iter::repeat_n(b'\\' as u16, backslashes));
                cmd.push(c);
            }
            backslashes = 0;
        }
    }
    cmd.extend(core::iter::repeat_n(b'\\' as u16, backslashes * 2));
    cmd.push(b'"' as u16);
}

#[derive(Clone, Debug)]
pub struct Command {
    args: Vec<OsString>,
    current_dir: Option<PathBuf>,
    stdin: Stdio,
    stdout: Stdio,
    stderr: Stdio,
}

impl Command {
    pub fn new(program: &OsStr) -> Self {
        Self {
            args: vec![program.to_os_string()],
            current_dir: None,
            stdin: Stdio::Inherit,
            stdout: Stdio::Inherit,
            stderr: Stdio::Inherit,
        }
    }

    pub fn arg(&mut self, arg: &OsStr) {
        self.args.push(arg.to_os_string());
    }

    pub fn current_dir(&mut self, dir: PathBuf) {
        self.current_dir = Some(dir);
    }

    pub fn env(&mut self, _key: &OsStr, _val: &OsStr) {}

    pub fn env_remove(&mut self, _key: &OsStr) {}

    pub fn env_clear(&mut self) {}

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
        let mut cmdline: Vec<u16> = Vec::new();
        for arg in &self.args {
            append_arg(&mut cmdline, arg);
        }
        cmdline.push(0);

        let (stdin_parent, stdin_child) = match self.stdin {
            Stdio::Piped => {
                let (ours, theirs) = create_pipe(false)?;
                (Some(ours), Some(theirs))
            }
            Stdio::Null | Stdio::Inherit => (None, None),
        };

        let (stdout_parent, stdout_child) = match self.stdout {
            Stdio::Piped => {
                let (ours, theirs) = create_pipe(true)?;
                (Some(ours), Some(theirs))
            }
            Stdio::Null | Stdio::Inherit => (None, None),
        };

        let (stderr_parent, stderr_child) = match self.stderr {
            Stdio::Piped => {
                let (ours, theirs) = create_pipe(true)?;
                (Some(ours), Some(theirs))
            }
            Stdio::Null | Stdio::Inherit => (None, None),
        };

        let mut si: STARTUPINFOW = unsafe { core::mem::zeroed() };
        si.cb = core::mem::size_of::<STARTUPINFOW>() as u32;

        let has_pipes = stdin_child.is_some() || stdout_child.is_some() || stderr_child.is_some();
        if has_pipes {
            si.dwFlags |= STARTF_USESTDHANDLES;
            if let Some(ref h) = stdin_child {
                si.hStdInput = h.as_raw_handle() as _;
            }
            if let Some(ref h) = stdout_child {
                si.hStdOutput = h.as_raw_handle() as _;
            }
            if let Some(ref h) = stderr_child {
                si.hStdError = h.as_raw_handle() as _;
            }
        }

        let mut dir_wide = self.current_dir.as_ref().map(|d| {
            let mut w: Vec<u16> = d.as_os_str().encode_wide().collect();
            w.push(0);
            w
        });
        let dir_ptr = dir_wide
            .as_mut()
            .map(|w| w.as_ptr())
            .unwrap_or(core::ptr::null());

        let mut pi: PROCESS_INFORMATION = unsafe { core::mem::zeroed() };
        let ret = unsafe {
            CreateProcessW(
                core::ptr::null(),
                cmdline.as_mut_ptr(),
                core::ptr::null(),
                core::ptr::null(),
                if has_pipes { 1 } else { 0 },
                0,
                core::ptr::null(),
                dir_ptr,
                &si,
                &mut pi,
            )
        };

        drop(stdin_child);
        drop(stdout_child);
        drop(stderr_child);

        if ret == 0 {
            return Err(Error::last_os_error());
        }

        unsafe {
            CloseHandle(pi.hThread);
        }

        let proc_handle = unsafe { OwnedHandle::from_raw_handle(pi.hProcess as RawHandle) };

        Ok(Child {
            handle: proc_handle,
            pid: pi.dwProcessId,
            status: None,
            stdin: stdin_parent.map(ChildStdin),
            stdout: stdout_parent.map(ChildStdout),
            stderr: stderr_parent.map(ChildStderr),
        })
    }
}
