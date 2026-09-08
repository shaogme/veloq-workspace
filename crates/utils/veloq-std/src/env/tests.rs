use crate::{
    env::{
        args, args_os, consts, current_dir, join_paths, remove_var, set_var, split_paths, temp_dir,
        var, var_os, vars, vars_os,
    },
    ffi::OsString,
    path::PathBuf,
    string::String,
    vec,
    vec::Vec,
};

#[test]
fn test_temp_dir() {
    let tmp = temp_dir();
    assert!(!tmp.as_os_str().is_empty());
}

#[test]
fn test_current_dir() {
    let dir = current_dir().expect("get current_dir");
    assert!(!dir.as_os_str().is_empty());
}

#[test]
fn test_var_manipulation() {
    let key = "VELOQ_TEST_ENV_VAR_XYZ";
    let val = "12345_veloq_value";

    assert_eq!(var_os(key), None);
    assert!(var(key).is_err());

    unsafe {
        set_var(key, val);
    }

    assert_eq!(var_os(key), Some(OsString::from(val)));
    assert_eq!(var(key).unwrap(), String::from(val));

    // verify in vars()
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
fn test_split_and_join_paths() {
    let p1 = PathBuf::from("foo");
    let p2 = PathBuf::from("bar");
    let joined = join_paths(vec![&p1, &p2]).expect("join paths");
    let split: Vec<_> = split_paths(&joined).collect();
    assert_eq!(split.len(), 2);
    assert_eq!(split[0], p1);
    assert_eq!(split[1], p2);
}

#[test]
fn test_args() {
    let a_os: Vec<_> = args_os().collect();
    let a_str: Vec<_> = args().collect();
    assert!(!a_os.is_empty());
    assert_eq!(a_os.len(), a_str.len());
}

#[test]
fn test_consts() {
    assert!(!consts::ARCH.is_empty());
    assert!(!consts::OS.is_empty());
    assert!(!consts::FAMILY.is_empty());
}
