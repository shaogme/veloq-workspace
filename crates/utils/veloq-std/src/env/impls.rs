//! Inspection and manipulation of the process's environment.

mod sys;

use core::{error::Error as CoreError, fmt, marker::PhantomData, result::Result as CoreResult};

use crate::{
    alloc_crate::{string::String, vec::IntoIter},
    ffi::{OsStr, OsString},
    io::Result,
    path::{Path, PathBuf},
};

/// Possible errors from [`Env::var`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VarError {
    /// The specified environment variable was not present in the process's environment.
    NotPresent,
    /// The specified environment variable was found, but its value was not valid Unicode.
    NotUnicode(OsString),
}

impl fmt::Display for VarError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotPresent => f.write_str("environment variable not found"),
            Self::NotUnicode(_) => {
                f.write_str("environment variable was not valid unicode: possible trunkation")
            }
        }
    }
}

impl CoreError for VarError {}

/// Error returned by [`Env::join_paths`] when one of the paths contains an invalid character.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct JoinPathsError;

impl fmt::Display for JoinPathsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("path segment contains illegal characters")
    }
}

impl CoreError for JoinPathsError {}

pub struct Env;

impl Env {
    /// Returns the current working directory as a [`PathBuf`].
    #[inline]
    pub fn current_dir() -> Result<PathBuf> {
        sys::Platform::current_dir()
    }

    /// Changes the current working directory to the specified path.
    #[inline]
    pub fn set_current_dir<P: AsRef<Path>>(path: P) -> Result<()> {
        sys::Platform::set_current_dir(path.as_ref())
    }

    /// Returns the full path of the current executable.
    #[inline]
    pub fn current_exe() -> Result<PathBuf> {
        sys::Platform::current_exe()
    }

    /// Returns the path of a temporary directory.
    #[inline]
    pub fn temp_dir() -> PathBuf {
        sys::Platform::temp_dir()
    }

    /// Returns the path of the current user's home directory if known.
    #[inline]
    pub fn home_dir() -> Option<PathBuf> {
        sys::Platform::home_dir()
    }

    /// Fetches the environment variable `key` from the current process, returning
    /// [`None`] if the variable isn't set.
    #[inline]
    pub fn var_os<K: AsRef<OsStr>>(key: K) -> Option<OsString> {
        sys::Platform::var_os(key.as_ref())
    }

    /// Fetches the environment variable `key` from the current process.
    pub fn var<K: AsRef<OsStr>>(key: K) -> CoreResult<String, VarError> {
        match Self::var_os(key) {
            Some(val) => val.into_string().map_err(VarError::NotUnicode),
            None => Err(VarError::NotPresent),
        }
    }

    /// Sets the environment variable `key` to the value `value` for the currently running process.
    ///
    /// # Safety
    ///
    /// Modifying environment variables can cause data races in multi-threaded programs
    /// where other threads or C libraries access environment variables concurrently.
    #[inline]
    pub unsafe fn set_var<K: AsRef<OsStr>, V: AsRef<OsStr>>(key: K, value: V) {
        let _ = sys::Platform::set_var(key.as_ref(), value.as_ref());
    }

    /// Removes an environment variable from the current process.
    ///
    /// # Safety
    ///
    /// Modifying environment variables can cause data races in multi-threaded programs
    /// where other threads or C libraries access environment variables concurrently.
    #[inline]
    pub unsafe fn remove_var<K: AsRef<OsStr>>(key: K) {
        let _ = sys::Platform::remove_var(key.as_ref());
    }
}

/// An iterator over a snapshot of the environment variables of this process.
pub struct VarsOs {
    inner: IntoIter<(OsString, OsString)>,
}

impl Iterator for VarsOs {
    type Item = (OsString, OsString);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for VarsOs {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Env {
    /// Returns an iterator of (variable, value) pairs of OS strings for all the
    /// environment variables of the current process.
    pub fn vars_os() -> VarsOs {
        VarsOs {
            inner: sys::Platform::vars_os().into_iter(),
        }
    }
}

/// An iterator over a snapshot of the environment variables of this process as Strings.
pub struct Vars {
    inner: VarsOs,
}

impl Iterator for Vars {
    type Item = (String, String);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|(k, v)| {
            (
                k.into_string()
                    .expect("environment variable key not valid unicode"),
                v.into_string()
                    .expect("environment variable value not valid unicode"),
            )
        })
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for Vars {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Env {
    /// Returns an iterator of (variable, value) pairs of strings for all the
    /// environment variables of the current process.
    pub fn vars() -> Vars {
        Vars {
            inner: Self::vars_os(),
        }
    }
}

/// An iterator over the paths in a path list.
pub struct SplitPaths<'a> {
    inner: IntoIter<PathBuf>,
    _marker: PhantomData<&'a OsStr>,
}

impl Iterator for SplitPaths<'_> {
    type Item = PathBuf;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl Env {
    /// Parses input that represents a path list into a collection of [`PathBuf`] values.
    pub fn split_paths<T: AsRef<OsStr> + ?Sized>(unparsed: &T) -> SplitPaths<'_> {
        SplitPaths {
            inner: sys::Platform::split_paths(unparsed.as_ref()).into_iter(),
            _marker: PhantomData,
        }
    }

    /// Joins a collection of paths into a single [`OsString`].
    pub fn join_paths<I, T>(paths: I) -> CoreResult<OsString, JoinPathsError>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<OsStr>,
    {
        sys::Platform::join_paths(paths.into_iter())
    }
}

/// An iterator over the arguments of a process as [`OsString`]s.
pub struct ArgsOs {
    inner: IntoIter<OsString>,
}

impl Iterator for ArgsOs {
    type Item = OsString;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for ArgsOs {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Env {
    /// Returns the arguments that this program was started with as [`OsString`]s.
    pub fn args_os() -> ArgsOs {
        ArgsOs {
            inner: sys::Platform::args_os().into_iter(),
        }
    }
}

/// An iterator over the arguments of a process as [`String`]s.
pub struct Args {
    inner: ArgsOs,
}

impl Iterator for Args {
    type Item = String;

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|s| s.into_string().expect("argument not valid unicode"))
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ExactSizeIterator for Args {
    #[inline]
    fn len(&self) -> usize {
        self.inner.len()
    }
}

impl Env {
    /// Returns the arguments that this program was started with.
    pub fn args() -> Args {
        Args {
            inner: Self::args_os(),
        }
    }
}

/// Constants associated with the platform.
pub mod consts {
    #[cfg(target_arch = "x86_64")]
    pub const ARCH: &str = "x86_64";
    #[cfg(target_arch = "aarch64")]
    pub const ARCH: &str = "aarch64";
    #[cfg(target_arch = "x86")]
    pub const ARCH: &str = "x86";
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64", target_arch = "x86")))]
    pub const ARCH: &str = "unknown";

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub const OS: &str = "linux";
    #[cfg(target_os = "windows")]
    pub const OS: &str = "windows";
    #[cfg(target_os = "macos")]
    pub const OS: &str = "macos";
    #[cfg(not(any(target_os = "linux", target_os = "windows", target_os = "macos")))]
    pub const OS: &str = "unknown";

    #[cfg(unix)]
    pub const FAMILY: &str = "unix";
    #[cfg(windows)]
    pub const FAMILY: &str = "windows";
    #[cfg(not(any(unix, windows)))]
    pub const FAMILY: &str = "unknown";

    #[cfg(windows)]
    pub const DLL_PREFIX: &str = "";
    #[cfg(not(windows))]
    pub const DLL_PREFIX: &str = "lib";

    #[cfg(windows)]
    pub const DLL_SUFFIX: &str = ".dll";
    #[cfg(target_os = "macos")]
    pub const DLL_SUFFIX: &str = ".dylib";
    #[cfg(all(not(windows), not(target_os = "macos")))]
    pub const DLL_SUFFIX: &str = ".so";

    #[cfg(windows)]
    pub const DLL_EXTENSION: &str = "dll";
    #[cfg(target_os = "macos")]
    pub const DLL_EXTENSION: &str = "dylib";
    #[cfg(all(not(windows), not(target_os = "macos")))]
    pub const DLL_EXTENSION: &str = "so";

    #[cfg(windows)]
    pub const EXE_SUFFIX: &str = ".exe";
    #[cfg(not(windows))]
    pub const EXE_SUFFIX: &str = "";

    #[cfg(windows)]
    pub const EXE_EXTENSION: &str = "exe";
    #[cfg(not(windows))]
    pub const EXE_EXTENSION: &str = "";
}

#[cfg(test)]
mod tests {
    use crate::{
        env::{Env, consts},
        ffi::OsString,
        path::PathBuf,
        string::String,
        vec,
        vec::Vec,
    };

    #[test]
    fn test_temp_dir() {
        let tmp = Env::temp_dir();
        assert!(!tmp.as_os_str().is_empty());
    }

    #[test]
    fn test_current_dir() {
        let dir = Env::current_dir().expect("get current_dir");
        assert!(!dir.as_os_str().is_empty());
    }

    #[test]
    fn test_var_manipulation() {
        let key = "VELOQ_TEST_ENV_VAR_XYZ";
        let val = "12345_veloq_value";

        assert_eq!(Env::var_os(key), None);
        assert!(Env::var(key).is_err());

        unsafe {
            Env::set_var(key, val);
        }

        assert_eq!(Env::var_os(key), Some(OsString::from(val)));
        assert_eq!(Env::var(key).unwrap(), String::from(val));

        // verify in vars()
        let found = Env::vars().any(|(k, v)| k == key && v == val);
        assert!(found);

        let found_os = Env::vars_os().any(|(k, v)| k == key && v == val);
        assert!(found_os);

        unsafe {
            Env::remove_var(key);
        }
        assert_eq!(Env::var_os(key), None);
        assert!(Env::var(key).is_err());
    }

    #[test]
    fn test_split_and_join_paths() {
        let p1 = PathBuf::from("foo");
        let p2 = PathBuf::from("bar");
        let joined = Env::join_paths(vec![&p1, &p2]).expect("join paths");
        let split: Vec<_> = Env::split_paths(&joined).collect();
        assert_eq!(split.len(), 2);
        assert_eq!(split[0], p1);
        assert_eq!(split[1], p2);
    }

    #[test]
    fn test_args() {
        let a_os: Vec<_> = Env::args_os().collect();
        let a_str: Vec<_> = Env::args().collect();
        assert!(!a_os.is_empty());
        assert_eq!(a_os.len(), a_str.len());
    }

    #[test]
    fn test_consts() {
        assert!(!consts::ARCH.is_empty());
        assert!(!consts::OS.is_empty());
        assert!(!consts::FAMILY.is_empty());
    }
}
