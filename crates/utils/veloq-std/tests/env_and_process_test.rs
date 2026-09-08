use veloq_std::{
    env::{
        args, args_os, consts, current_dir, home_dir, join_paths, remove_var, set_var, split_paths,
        temp_dir, var, var_os, vars, vars_os,
    },
    ffi::OsString,
    path::PathBuf,
    process::{Command, ExitCode, ExitStatus, id},
    string::String,
    vec,
    vec::Vec,
};

#[cfg(unix)]
use veloq_std::os::unix::process::ExitStatusExt as UnixExitStatusExt;

#[cfg(windows)]
use veloq_std::os::windows::process::ExitStatusExt as WinExitStatusExt;

#[test]
fn test_env_dirs() {
    let cur = current_dir().expect("current directory must exist");
    assert!(!cur.as_os_str().is_empty());

    let tmp = temp_dir();
    assert!(!tmp.as_os_str().is_empty());

    let _ = home_dir();
}

#[test]
fn test_env_var_lifecycle() {
    let key = "VELOQ_INTEGRATION_TEST_KEY_123";
    let val = "veloq_integration_test_val_456";

    assert_eq!(var_os(key), None);
    assert!(var(key).is_err());

    unsafe {
        set_var(key, val);
    }

    assert_eq!(var_os(key), Some(OsString::from(val)));
    assert_eq!(var(key).unwrap(), String::from(val));

    let found = vars().any(|(k, v)| k == key && v == val);
    assert!(found);

    let found_os = vars_os().any(|(k, v)| k == key && v == val);
    assert!(found_os);

    unsafe {
        remove_var(key);
    }
    assert_eq!(var_os(key), None);
    assert!(var(key).is_err());
}

#[test]
fn test_env_split_join_paths() {
    let p1 = PathBuf::from("dir1");
    let p2 = PathBuf::from("dir2");
    let p3 = PathBuf::from("dir3");

    let joined = join_paths(vec![&p1, &p2, &p3]).expect("join paths");
    let split: Vec<_> = split_paths(&joined).collect();
    assert_eq!(split.len(), 3);
    assert_eq!(split[0], p1);
    assert_eq!(split[1], p2);
    assert_eq!(split[2], p3);
}

#[test]
fn test_env_args_and_consts() {
    let os_args: Vec<_> = args_os().collect();
    let str_args: Vec<_> = args().collect();
    assert!(!os_args.is_empty());
    assert_eq!(os_args.len(), str_args.len());

    assert!(!consts::ARCH.is_empty());
    assert!(!consts::OS.is_empty());
    assert!(!consts::FAMILY.is_empty());
}

#[test]
fn test_process_basics() {
    let pid = id();
    assert!(pid > 0);

    assert_eq!(ExitCode::SUCCESS.as_i32(), 0);
    assert_eq!(ExitCode::FAILURE.as_i32(), 1);
}

#[test]
#[cfg(unix)]
fn test_unix_exit_status_ext() {
    let status = <ExitStatus as UnixExitStatusExt>::from_raw(0);
    assert!(status.success());
    assert_eq!(status.code(), Some(0));
    assert_eq!(status.into_raw(), 0);
}

#[test]
#[cfg(windows)]
fn test_windows_exit_status_ext() {
    let status = <ExitStatus as WinExitStatusExt>::from_raw(0);
    assert!(status.success());
    assert_eq!(status.code(), Some(0));
    assert_eq!(status.into_raw(), 0);
}

#[test]
#[cfg(unix)]
fn test_command_execution_unix() {
    let mut cmd = Command::new("sh");
    cmd.arg("-c");
    cmd.arg("echo veloq_sh_test");

    let output = cmd.output().expect("run sh command");
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("stdout valid utf8");
    assert!(text.contains("veloq_sh_test"));
}

#[test]
#[cfg(windows)]
fn test_command_execution_windows() {
    let mut cmd = Command::new("cmd");
    cmd.arg("/c");
    cmd.arg("echo veloq_cmd_test");

    let output = cmd.output().expect("run cmd command");
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).expect("stdout valid utf8");
    assert!(text.contains("veloq_cmd_test"));
}
