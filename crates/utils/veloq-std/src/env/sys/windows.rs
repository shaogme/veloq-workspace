use core::{mem::MaybeUninit, slice};

use crate::{
    alloc_crate::vec::Vec,
    env::JoinPathsError,
    ffi::{OsStr, OsString},
    io::{Error, ErrorKind, Result},
    os::{
        cvt::cvt,
        windows::ffi::{OsStrExt, OsStringExt},
    },
    path::{Path, PathBuf},
};
use windows_sys::Win32::{
    Foundation::{ERROR_INSUFFICIENT_BUFFER, GetLastError, SetLastError},
    Storage::FileSystem::GetTempPathW,
    System::{
        Environment::{
            FreeEnvironmentStringsW, GetCommandLineW, GetCurrentDirectoryW, GetEnvironmentStringsW,
            GetEnvironmentVariableW, SetCurrentDirectoryW, SetEnvironmentVariableW,
        },
        LibraryLoader::GetModuleFileNameW,
    },
};

pub fn fill_utf16_buf<F1, F2, T>(mut f1: F1, f2: F2) -> Result<T>
where
    F1: FnMut(*mut u16, u32) -> u32,
    F2: FnOnce(&[u16]) -> T,
{
    let mut stack_buf: [MaybeUninit<u16>; 512] = [MaybeUninit::uninit(); 512];
    let mut heap_buf: Vec<MaybeUninit<u16>> = Vec::new();
    let mut n = stack_buf.len();
    loop {
        let buf = if n <= stack_buf.len() {
            &mut stack_buf[..]
        } else {
            let extra = n - heap_buf.len();
            heap_buf.reserve(extra);
            n = heap_buf.capacity().min(u32::MAX as usize);
            unsafe {
                heap_buf.set_len(n);
            }
            &mut heap_buf[..]
        };

        unsafe {
            SetLastError(0);
        }
        let k = match f1(buf.as_mut_ptr().cast::<u16>(), n as u32) {
            0 if unsafe { GetLastError() } == 0 => 0,
            0 => return Err(Error::last_os_error()),
            len => len,
        } as usize;

        if k == n && unsafe { GetLastError() } == ERROR_INSUFFICIENT_BUFFER {
            n = n.saturating_mul(2).min(u32::MAX as usize);
        } else if k > n {
            n = k;
        } else {
            let s = unsafe { slice::from_raw_parts(buf.as_ptr().cast::<u16>(), k) };
            return Ok(f2(s));
        }
    }
}

pub fn current_dir() -> Result<PathBuf> {
    fill_utf16_buf(
        |buf, sz| unsafe { GetCurrentDirectoryW(sz, buf) },
        |s| {
            let os: OsString = OsStringExt::from_wide(s);
            PathBuf::from(os)
        },
    )
}

pub fn set_current_dir(path: &Path) -> Result<()> {
    let mut p_wide: Vec<u16> = path.as_os_str().encode_wide().collect();
    if p_wide.contains(&0) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "path contains null character",
        ));
    }
    p_wide.push(0);

    cvt(unsafe { SetCurrentDirectoryW(p_wide.as_ptr()) }).map(drop)
}

pub fn current_exe() -> Result<PathBuf> {
    fill_utf16_buf(
        |buf, sz| unsafe { GetModuleFileNameW(core::ptr::null_mut(), buf, sz) },
        |s| {
            let os: OsString = OsStringExt::from_wide(s);
            PathBuf::from(os)
        },
    )
}

pub fn temp_dir() -> PathBuf {
    fill_utf16_buf(
        |buf, sz| unsafe { GetTempPathW(sz, buf) },
        |s| {
            let os: OsString = OsStringExt::from_wide(s);
            PathBuf::from(os)
        },
    )
    .unwrap_or_else(|_| PathBuf::from("."))
}

pub fn home_dir() -> Option<PathBuf> {
    if let Some(p) = var_os(OsStr::new("USERPROFILE"))
        && !p.is_empty()
    {
        return Some(PathBuf::from(p));
    }
    match (
        var_os(OsStr::new("HOMEDRIVE")),
        var_os(OsStr::new("HOMEPATH")),
    ) {
        (Some(d), Some(p)) if !d.is_empty() && !p.is_empty() => {
            let mut buf = PathBuf::from(d);
            buf.push(p);
            Some(buf)
        }
        _ => None,
    }
}

pub fn var_os(key: &OsStr) -> Option<OsString> {
    let mut k_wide: Vec<u16> = key.encode_wide().collect();
    if k_wide.contains(&0) || k_wide.contains(&(b'=' as u16)) {
        return None;
    }
    k_wide.push(0);

    fill_utf16_buf(
        |buf, sz| unsafe { GetEnvironmentVariableW(k_wide.as_ptr(), buf, sz) },
        OsStringExt::from_wide,
    )
    .ok()
}

pub fn set_var(key: &OsStr, val: &OsStr) -> Result<()> {
    let mut k_wide: Vec<u16> = key.encode_wide().collect();
    let mut v_wide: Vec<u16> = val.encode_wide().collect();
    if k_wide.is_empty()
        || k_wide.contains(&0)
        || k_wide.contains(&(b'=' as u16))
        || v_wide.contains(&0)
    {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "invalid environment variable key or value",
        ));
    }
    k_wide.push(0);
    v_wide.push(0);

    cvt(unsafe { SetEnvironmentVariableW(k_wide.as_ptr(), v_wide.as_ptr()) }).map(drop)
}

pub fn remove_var(key: &OsStr) -> Result<()> {
    let mut k_wide: Vec<u16> = key.encode_wide().collect();
    if k_wide.is_empty() || k_wide.contains(&0) || k_wide.contains(&(b'=' as u16)) {
        return Err(Error::new(
            ErrorKind::InvalidInput,
            "invalid environment variable key",
        ));
    }
    k_wide.push(0);

    cvt(unsafe { SetEnvironmentVariableW(k_wide.as_ptr(), core::ptr::null()) }).map(drop)
}

pub fn vars_os() -> Vec<(OsString, OsString)> {
    let mut res = Vec::new();
    unsafe {
        let ch = GetEnvironmentStringsW();
        if ch.is_null() {
            return res;
        }
        let mut cur = ch;
        while *cur != 0 {
            let mut len = 0;
            while *cur.add(len) != 0 {
                len += 1;
            }
            let s = slice::from_raw_parts(cur, len);
            cur = cur.add(len + 1);

            if let Some(pos) = s[1..].iter().position(|&u| u == b'=' as u16).map(|p| p + 1) {
                let k = OsStringExt::from_wide(&s[..pos]);
                let v = OsStringExt::from_wide(&s[pos + 1..]);
                res.push((k, v));
            }
        }
        FreeEnvironmentStringsW(ch);
    }
    res
}

pub fn split_paths(unparsed: &OsStr) -> Vec<PathBuf> {
    let mut res = Vec::new();
    let mut in_progress = Vec::new();
    let mut in_quote = false;

    for b in unparsed.encode_wide() {
        if b == b'"' as u16 {
            in_quote = !in_quote;
        } else if b == b';' as u16 && !in_quote {
            let os: OsString = OsStringExt::from_wide(&in_progress);
            res.push(PathBuf::from(os));
            in_progress.clear();
        } else {
            in_progress.push(b);
        }
    }
    if !in_progress.is_empty() || !res.is_empty() {
        let os: OsString = OsStringExt::from_wide(&in_progress);
        res.push(PathBuf::from(os));
    }
    res
}

pub fn join_paths<I, T>(paths: I) -> core::result::Result<OsString, JoinPathsError>
where
    I: Iterator<Item = T>,
    T: AsRef<OsStr>,
{
    let mut joined = Vec::new();
    let sep = b';' as u16;

    for (i, path) in paths.enumerate() {
        let path = path.as_ref();
        if i > 0 {
            joined.push(sep);
        }
        let v = path.encode_wide().collect::<Vec<u16>>();
        if v.contains(&(b'"' as u16)) {
            return Err(JoinPathsError);
        } else if v.contains(&sep) {
            joined.push(b'"' as u16);
            joined.extend_from_slice(&v[..]);
            joined.push(b'"' as u16);
        } else {
            joined.extend_from_slice(&v[..]);
        }
    }

    Ok(OsStringExt::from_wide(&joined))
}

pub fn parse_lp_cmd_line(lp_cmd_line: *const u16) -> Vec<OsString> {
    if lp_cmd_line.is_null() {
        return Vec::new();
    }
    let mut code_units = Vec::new();
    let mut ptr = lp_cmd_line;
    unsafe {
        while *ptr != 0 {
            code_units.push(*ptr);
            ptr = ptr.add(1);
        }
    }
    if code_units.is_empty() {
        return Vec::new();
    }

    let mut ret_val = Vec::new();
    let mut i = 0;

    while i < code_units.len() && (code_units[i] == b' ' as u16 || code_units[i] == b'\t' as u16) {
        i += 1;
    }

    let mut in_quotes = false;
    let mut cur = Vec::new();
    while i < code_units.len() {
        let w = code_units[i];
        if w == b'"' as u16 {
            in_quotes = !in_quotes;
        } else if (w == b' ' as u16 || w == b'\t' as u16) && !in_quotes {
            i += 1;
            break;
        } else {
            cur.push(w);
        }
        i += 1;
    }
    ret_val.push(OsStringExt::from_wide(&cur));

    while i < code_units.len() {
        while i < code_units.len()
            && (code_units[i] == b' ' as u16 || code_units[i] == b'\t' as u16)
        {
            i += 1;
        }
        if i >= code_units.len() {
            break;
        }

        let mut arg = Vec::new();
        let mut in_quotes = false;
        while i < code_units.len() {
            let w = code_units[i];
            if (w == b' ' as u16 || w == b'\t' as u16) && !in_quotes {
                break;
            } else if w == b'\\' as u16 {
                let mut backslashes = 0;
                while i < code_units.len() && code_units[i] == b'\\' as u16 {
                    backslashes += 1;
                    i += 1;
                }
                if i < code_units.len() && code_units[i] == b'"' as u16 {
                    arg.extend(core::iter::repeat_n(b'\\' as u16, backslashes / 2));
                    if backslashes % 2 == 0 {
                        in_quotes = !in_quotes;
                        i += 1;
                    } else {
                        arg.push(b'"' as u16);
                        i += 1;
                    }
                } else {
                    arg.extend(core::iter::repeat_n(b'\\' as u16, backslashes));
                }
            } else if w == b'"' as u16 {
                in_quotes = !in_quotes;
                i += 1;
            } else {
                arg.push(w);
                i += 1;
            }
        }
        ret_val.push(OsStringExt::from_wide(&arg));
    }

    ret_val
}

pub fn args_os() -> Vec<OsString> {
    unsafe {
        let cmd = GetCommandLineW();
        parse_lp_cmd_line(cmd)
    }
}
