use core::ffi::CStr;

use crate::{
    alloc_crate::{ffi::CString, vec::Vec},
    env::JoinPathsError,
    ffi::{OsStr, OsString},
    fs::File,
    io::{Error, ErrorKind, Read, Result},
    os::{
        cvt::cvt,
        unix::ffi::{OsStrExt, OsStringExt},
    },
    path::{Path, PathBuf},
    sync::NativeUnpoisonedRwLock,
    vec,
};

static ENV_LOCK: NativeUnpoisonedRwLock<()> = NativeUnpoisonedRwLock::new(());

#[cfg(not(any(target_os = "freebsd", target_vendor = "apple")))]
unsafe fn environ() -> *mut *const *const libc::c_char {
    unsafe extern "C" {
        static mut environ: *const *const libc::c_char;
    }
    core::ptr::addr_of_mut!(environ)
}

#[cfg(target_vendor = "apple")]
unsafe fn environ() -> *mut *const *const libc::c_char {
    unsafe { libc::_NSGetEnviron() as *mut *const *const libc::c_char }
}

pub fn current_dir() -> Result<PathBuf> {
    let mut buf = vec![0u8; 512];
    loop {
        let ptr = unsafe { libc::getcwd(buf.as_mut_ptr() as *mut _, buf.len()) };
        if !ptr.is_null() {
            let len = unsafe { libc::strlen(ptr) };
            buf.truncate(len);
            let s: OsString = OsStringExt::from_vec(buf);
            return Ok(PathBuf::from(s));
        }
        let err = Error::last_os_error();
        if err.raw_os_error() != Some(libc::ERANGE) {
            return Err(err);
        }
        let new_cap = buf
            .capacity()
            .checked_mul(2)
            .ok_or_else(|| Error::new(ErrorKind::OutOfMemory, "path length overflow"))?;
        buf.resize(new_cap, 0);
    }
}

pub fn set_current_dir(path: &Path) -> Result<()> {
    let c_path = CString::new(path.as_bytes())
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "path contains null byte"))?;
    cvt(unsafe { libc::chdir(c_path.as_ptr()) }).map(drop)
}

pub fn current_exe() -> Result<PathBuf> {
    let mut buf = vec![0u8; 256];
    loop {
        let len = unsafe {
            libc::readlink(
                c"/proc/self/exe".as_ptr(),
                buf.as_mut_ptr() as *mut _,
                buf.len(),
            )
        };
        if len < 0 {
            return Err(Error::last_os_error());
        }
        let len = len as usize;
        if len < buf.len() {
            buf.truncate(len);
            let s: OsString = OsStringExt::from_vec(buf);
            return Ok(PathBuf::from(s));
        }
        let new_cap = buf
            .capacity()
            .checked_mul(2)
            .ok_or_else(|| Error::new(ErrorKind::OutOfMemory, "path length overflow"))?;
        buf.resize(new_cap, 0);
    }
}

pub fn temp_dir() -> PathBuf {
    if let Some(p) = var_os(OsStr::new("TMPDIR"))
        && !p.is_empty()
    {
        return PathBuf::from(p);
    }
    #[cfg(target_os = "android")]
    {
        PathBuf::from("/data/local/tmp")
    }
    #[cfg(not(target_os = "android"))]
    {
        PathBuf::from("/tmp")
    }
}

pub fn home_dir() -> Option<PathBuf> {
    if let Some(p) = var_os(OsStr::new("HOME"))
        && !p.is_empty()
    {
        return Some(PathBuf::from(p));
    }

    let mut buf = Vec::with_capacity(1024);
    let mut pwd: libc::passwd = unsafe { core::mem::zeroed() };
    let mut result: *mut libc::passwd = core::ptr::null_mut();
    let uid = unsafe { libc::getuid() };
    loop {
        buf.resize(buf.capacity(), 0);
        let res = unsafe {
            libc::getpwuid_r(
                uid,
                &mut pwd,
                buf.as_mut_ptr() as *mut _,
                buf.len(),
                &mut result,
            )
        };
        if res == 0 {
            if result.is_null() || pwd.pw_dir.is_null() {
                return None;
            }
            let dir_bytes = unsafe { CStr::from_ptr(pwd.pw_dir) }.to_bytes();
            let s: OsString = OsStringExt::from_vec(dir_bytes.to_vec());
            return Some(PathBuf::from(s));
        }
        if res != libc::ERANGE {
            return None;
        }
        let new_cap = buf.capacity().checked_mul(2)?;
        buf.reserve(new_cap - buf.capacity());
    }
}

pub fn var_os(key: &OsStr) -> Option<OsString> {
    let bytes = key.as_bytes();
    if bytes.contains(&0) || bytes.contains(&b'=') {
        return None;
    }
    let c_key = CString::new(bytes).ok()?;
    let _guard = ENV_LOCK.read();
    let ptr = unsafe { libc::getenv(c_key.as_ptr()) };
    if ptr.is_null() {
        None
    } else {
        let val_bytes = unsafe { CStr::from_ptr(ptr) }.to_bytes().to_vec();
        Some(OsStringExt::from_vec(val_bytes))
    }
}

pub fn set_var(key: &OsStr, val: &OsStr) -> Result<()> {
    let k_bytes = key.as_bytes();
    let v_bytes = val.as_bytes();
    if k_bytes.is_empty() || k_bytes.contains(&0) || k_bytes.contains(&b'=') || v_bytes.contains(&0)
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "invalid environment variable key or value",
        ));
    }
    let c_key = CString::new(k_bytes)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "key contains null"))?;
    let c_val = CString::new(v_bytes)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "val contains null"))?;
    let _guard = ENV_LOCK.write();
    cvt(unsafe { libc::setenv(c_key.as_ptr(), c_val.as_ptr(), 1) }).map(drop)
}

pub fn remove_var(key: &OsStr) -> Result<()> {
    let k_bytes = key.as_bytes();
    if k_bytes.is_empty() || k_bytes.contains(&0) || k_bytes.contains(&b'=') {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "invalid environment variable key",
        ));
    }
    let c_key = CString::new(k_bytes)
        .map_err(|_| Error::new(ErrorKind::InvalidInput, "key contains null"))?;
    let _guard = ENV_LOCK.write();
    cvt(unsafe { libc::unsetenv(c_key.as_ptr()) }).map(drop)
}

pub fn vars_os() -> Vec<(OsString, OsString)> {
    let _guard = ENV_LOCK.read();
    let mut res = Vec::new();
    unsafe {
        let mut env_ptr = *environ();
        if !env_ptr.is_null() {
            while !(*env_ptr).is_null() {
                let bytes = CStr::from_ptr(*env_ptr).to_bytes();
                if let Some(pos) = bytes.iter().position(|&b| b == b'=')
                    && pos > 0
                {
                    let k = OsStringExt::from_vec(bytes[..pos].to_vec());
                    let v = OsStringExt::from_vec(bytes[pos + 1..].to_vec());
                    res.push((k, v));
                }
                env_ptr = env_ptr.add(1);
            }
        }
    }
    res
}

pub fn split_paths(unparsed: &OsStr) -> Vec<PathBuf> {
    unparsed
        .as_bytes()
        .split(|&b| b == b':')
        .map(|piece| {
            let s: OsString = OsStringExt::from_vec(piece.to_vec());
            PathBuf::from(s)
        })
        .collect()
}

pub fn join_paths<I, T>(paths: I) -> core::result::Result<OsString, JoinPathsError>
where
    I: Iterator<Item = T>,
    T: AsRef<OsStr>,
{
    let mut res = Vec::new();
    let sep = b':';
    for (i, path) in paths.enumerate() {
        let bytes = path.as_ref().as_bytes();
        if bytes.contains(&sep) {
            return Err(JoinPathsError);
        }
        if i > 0 {
            res.push(sep);
        }
        res.extend_from_slice(bytes);
    }
    Ok(OsStringExt::from_vec(res))
}

pub fn args_os() -> Vec<OsString> {
    if let Ok(mut file) = File::open("/proc/self/cmdline") {
        let mut data = Vec::new();
        if file.read_to_end(&mut data).is_ok() && !data.is_empty() {
            let mut res = Vec::new();
            for piece in data.split(|&b| b == 0) {
                if !piece.is_empty() {
                    res.push(OsStringExt::from_vec(piece.to_vec()));
                }
            }
            if !res.is_empty() {
                return res;
            }
        }
    }

    if let Ok(exe) = current_exe() {
        vec![exe.into_os_string()]
    } else {
        Vec::new()
    }
}
