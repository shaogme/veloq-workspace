use crate::{
    process::{Command, ExitCode, id},
    string::String,
};

#[test]
fn test_process_id() {
    let pid = id();
    assert!(pid > 0);
}

#[test]
fn test_exit_code() {
    assert_eq!(ExitCode::SUCCESS.as_i32(), 0);
    assert_eq!(ExitCode::FAILURE.as_i32(), 1);
}

#[test]
#[cfg(unix)]
fn test_command_unix() {
    let mut cmd = Command::new("echo");
    cmd.arg("hello_veloq");
    let out = cmd.output().expect("execute echo");
    assert!(out.status.success());
    let s = String::from_utf8(out.stdout).expect("utf8 stdout");
    assert!(s.contains("hello_veloq"));
}

#[test]
#[cfg(windows)]
fn test_command_windows() {
    let mut cmd = Command::new("cmd");
    cmd.arg("/c");
    cmd.arg("echo hello_veloq");
    let out = cmd.output().expect("execute cmd");
    assert!(out.status.success());
    let s = String::from_utf8(out.stdout).expect("utf8 stdout");
    assert!(s.contains("hello_veloq"));
}
