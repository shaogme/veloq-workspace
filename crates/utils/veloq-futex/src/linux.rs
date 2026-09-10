use core::{
    fmt,
    mem::{align_of, offset_of, size_of},
    ptr::{null, null_mut},
    time::Duration,
};

use libc::{
    CLOCK_MONOTONIC, FUTEX_PRIVATE_FLAG, FUTEX_WAIT, FUTEX_WAKE, c_int, c_long, c_void,
    clock_gettime, timespec,
};

#[cfg(target_pointer_width = "64")]
const FUTEX_SYSCALL: c_long = libc::SYS_futex;

#[cfg(all(
    target_pointer_width = "32",
    target_arch = "riscv32",
    target_env = "musl"
))]
const FUTEX_SYSCALL: c_long = libc::SYS_futex_time64;

#[cfg(all(
    target_pointer_width = "32",
    any(
        target_arch = "riscv32",
        target_arch = "x86",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "powerpc",
        target_arch = "sparc",
        target_arch = "m68k",
        target_arch = "csky"
    ),
    any(target_env = "gnu", target_env = "musl", target_os = "android"),
    not(all(target_arch = "riscv32", target_env = "musl"))
))]
const FUTEX_SYSCALL: c_long = libc::SYS_futex;

#[cfg(target_pointer_width = "64")]
const ABI_NAME: &str = "futex/time64";

#[cfg(all(
    target_pointer_width = "32",
    target_arch = "riscv32",
    target_env = "musl"
))]
const ABI_NAME: &str = "futex_time64/kernel_timespec64";

#[cfg(all(
    target_pointer_width = "32",
    any(
        target_arch = "riscv32",
        target_arch = "x86",
        target_arch = "arm",
        target_arch = "mips",
        target_arch = "powerpc",
        target_arch = "sparc",
        target_arch = "m68k",
        target_arch = "csky"
    ),
    any(target_env = "gnu", target_env = "musl", target_os = "android"),
    not(all(target_arch = "riscv32", target_env = "musl"))
))]
const ABI_NAME: &str = "futex/kernel_timespec32";

#[cfg(not(any(
    target_pointer_width = "64",
    all(
        target_pointer_width = "32",
        target_arch = "riscv32",
        target_env = "musl"
    ),
    all(
        target_pointer_width = "32",
        any(
            target_arch = "riscv32",
            target_arch = "x86",
            target_arch = "arm",
            target_arch = "mips",
            target_arch = "powerpc",
            target_arch = "sparc",
            target_arch = "m68k",
            target_arch = "csky"
        ),
        any(target_env = "gnu", target_env = "musl", target_os = "android"),
        not(all(target_arch = "riscv32", target_env = "musl"))
    )
)))]
compile_error!(
    "veloq-futex: this Linux target has no verified futex syscall and timespec ABI mapping"
);

#[repr(C)]
#[derive(Clone, Copy)]
struct KernelTimespec32 {
    tv_sec: i32,
    tv_nsec: i32,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct KernelTimespec64 {
    tv_sec: i64,
    tv_nsec: i64,
}

const _: () = {
    assert!(size_of::<KernelTimespec32>() == 8);
    assert!(align_of::<KernelTimespec32>() == 4);
    assert!(offset_of!(KernelTimespec32, tv_sec) == 0);
    assert!(offset_of!(KernelTimespec32, tv_nsec) == 4);
    assert!(size_of::<KernelTimespec64>() == 16);
    assert!(offset_of!(KernelTimespec64, tv_sec) == 0);
    assert!(offset_of!(KernelTimespec64, tv_nsec) == 8);
};

#[cfg(target_pointer_width = "64")]
const _: () = assert!(align_of::<KernelTimespec64>() == 8);

#[cfg(target_pointer_width = "32")]
const _: () = assert!(align_of::<KernelTimespec64>() == 4);

/// 由 futex 系统调用返回的等待结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitOutcome {
    /// 内核返回、值不匹配或被唤醒；调用方必须重新检查原子状态。
    Woken,
    /// 相对 deadline 已到期。
    TimedOut,
}

/// futex 后端无法完成操作时返回的错误。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FutexError {
    /// 内核返回的 errno。
    Errno(i32),
    /// 当前内核没有提供所选的 futex ABI。
    Unsupported,
}

impl FutexError {
    /// 返回目标编译期选择的 ABI 名称。
    pub const fn abi_name(self) -> &'static str {
        ABI_NAME
    }

    /// 返回目标编译期选择的 futex 系统调用号。
    pub const fn syscall_number(self) -> c_long {
        FUTEX_SYSCALL
    }
}

impl fmt::Display for FutexError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Errno(errno) => write!(
                formatter,
                "futex ABI {} (syscall {}) returned errno {}",
                ABI_NAME, FUTEX_SYSCALL, errno
            ),
            Self::Unsupported => write!(
                formatter,
                "futex ABI {} (syscall {}) is unsupported",
                ABI_NAME, FUTEX_SYSCALL
            ),
        }
    }
}

/// 等待 futex word 的值保持为 `expected`。
///
/// # Safety
///
/// `address` 必须非空、按 4 字节对齐，并指向一个仍存活且内核可读的
/// 32 位 futex word。调用者必须保证该地址在整个系统调用期间有效。
pub unsafe fn wait(
    address: *const u32,
    expected: u32,
    timeout: Option<Duration>,
) -> Result<WaitOutcome, FutexError> {
    unsafe { wait_with_syscall(address, expected, timeout, FUTEX_SYSCALL) }
}

/// 唤醒等待指定 futex word 的线程。
///
/// # Safety
///
/// `address` 必须非空、按 4 字节对齐，并指向一个仍存活且内核可访问的
/// 32 位 futex word。调用者必须保证该地址在整个系统调用期间有效。
pub unsafe fn wake(address: *const u32, count: u32) -> Result<u32, FutexError> {
    validate_address(address)?;
    let count = count.min(c_int::MAX as u32) as c_int;
    let result = unsafe {
        libc::syscall(
            FUTEX_SYSCALL,
            address,
            FUTEX_WAKE | FUTEX_PRIVATE_FLAG,
            count,
        )
    };
    syscall_result(result)
}

unsafe fn wait_with_syscall(
    address: *const u32,
    expected: u32,
    timeout: Option<Duration>,
    syscall_number: c_long,
) -> Result<WaitOutcome, FutexError> {
    validate_address(address)?;
    let deadline = match timeout {
        Some(duration) => Some(
            monotonic_now()?
                .checked_add(duration.as_nanos())
                .ok_or(FutexError::Errno(libc::EINVAL))?,
        ),
        None => None,
    };

    loop {
        let remaining = match deadline {
            Some(deadline) => {
                let now = monotonic_now()?;
                deadline.saturating_sub(now)
            }
            None => 0,
        };
        #[allow(unused_variables)]
        let mut timeout32 = KernelTimespec32 {
            tv_sec: 0,
            tv_nsec: 0,
        };
        #[allow(unused_variables)]
        let mut timeout64 = KernelTimespec64 {
            tv_sec: 0,
            tv_nsec: 0,
        };
        let timeout_ptr = match timeout {
            None => null(),
            Some(_) => timeout_pointer(remaining, &mut timeout32, &mut timeout64)?,
        };

        let result = unsafe {
            libc::syscall(
                syscall_number,
                address,
                FUTEX_WAIT | FUTEX_PRIVATE_FLAG,
                expected as c_int,
                timeout_ptr,
                null_mut::<c_void>(),
                0,
            )
        };
        let outcome = if result >= 0 {
            WaitSyscallOutcome::Woken
        } else {
            classify_wait_errno(errno_result())?
        };
        match outcome {
            WaitSyscallOutcome::Woken => return Ok(WaitOutcome::Woken),
            WaitSyscallOutcome::TimedOut => return Ok(WaitOutcome::TimedOut),
            WaitSyscallOutcome::Interrupted => {
                if let Some(deadline) = deadline
                    && monotonic_now()? >= deadline
                {
                    return Ok(WaitOutcome::TimedOut);
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitSyscallOutcome {
    Woken,
    TimedOut,
    Interrupted,
}

fn classify_wait_errno(errno: i32) -> Result<WaitSyscallOutcome, FutexError> {
    match errno {
        libc::EAGAIN => Ok(WaitSyscallOutcome::Woken),
        libc::EINTR => Ok(WaitSyscallOutcome::Interrupted),
        libc::ETIMEDOUT => Ok(WaitSyscallOutcome::TimedOut),
        libc::ENOSYS => Err(FutexError::Unsupported),
        errno => Err(FutexError::Errno(errno)),
    }
}

fn timeout_pointer(
    remaining_nanos: u128,
    _timeout32: &mut KernelTimespec32,
    _timeout64: &mut KernelTimespec64,
) -> Result<*const c_void, FutexError> {
    let seconds = remaining_nanos / 1_000_000_000;
    let nanoseconds = (remaining_nanos % 1_000_000_000) as i32;

    #[cfg(all(
        target_pointer_width = "32",
        target_arch = "riscv32",
        target_env = "musl"
    ))]
    {
        _timeout64.tv_sec = i64::try_from(seconds).map_err(|_| FutexError::Errno(libc::EINVAL))?;
        _timeout64.tv_nsec = i64::from(nanoseconds);
        Ok(_timeout64 as *const KernelTimespec64 as *const c_void)
    }

    #[cfg(target_pointer_width = "64")]
    {
        _timeout64.tv_sec = i64::try_from(seconds).map_err(|_| FutexError::Errno(libc::EINVAL))?;
        _timeout64.tv_nsec = i64::from(nanoseconds);
        Ok(_timeout64 as *const KernelTimespec64 as *const c_void)
    }

    #[cfg(all(
        target_pointer_width = "32",
        any(
            target_arch = "riscv32",
            target_arch = "x86",
            target_arch = "arm",
            target_arch = "mips",
            target_arch = "powerpc",
            target_arch = "sparc",
            target_arch = "m68k",
            target_arch = "csky"
        ),
        any(target_env = "gnu", target_env = "musl", target_os = "android"),
        not(all(target_arch = "riscv32", target_env = "musl"))
    ))]
    {
        _timeout32.tv_sec = i32::try_from(seconds).map_err(|_| FutexError::Errno(libc::EINVAL))?;
        _timeout32.tv_nsec = nanoseconds;
        Ok(_timeout32 as *const KernelTimespec32 as *const c_void)
    }

    /*
     * The compile-time ABI guard above makes these branches exhaustive for every
     * target on which this module can be built. Keep the fallback unreachable so
     * a newly added target fails closed if the guard is changed incorrectly.
     */
    #[cfg(not(any(
        target_pointer_width = "64",
        all(
            target_pointer_width = "32",
            any(
                target_arch = "riscv32",
                target_arch = "x86",
                target_arch = "arm",
                target_arch = "mips",
                target_arch = "powerpc",
                target_arch = "sparc",
                target_arch = "m68k",
                target_arch = "csky"
            ),
            any(target_env = "gnu", target_env = "musl", target_os = "android"),
            not(all(target_arch = "riscv32", target_env = "musl"))
        )
    )))]
    unreachable!("unsupported futex ABI")
}

fn validate_address(address: *const u32) -> Result<(), FutexError> {
    if address.is_null() || !(address as usize).is_multiple_of(align_of::<u32>()) {
        Err(FutexError::Errno(libc::EINVAL))
    } else {
        Ok(())
    }
}

fn syscall_result(result: c_long) -> Result<u32, FutexError> {
    if result >= 0 {
        Ok(result as u32)
    } else {
        match errno_result() {
            libc::ENOSYS => Err(FutexError::Unsupported),
            errno => Err(FutexError::Errno(errno)),
        }
    }
}

fn monotonic_now() -> Result<u128, FutexError> {
    let mut now = timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    let result = unsafe { clock_gettime(CLOCK_MONOTONIC, &mut now) };
    if result != 0 {
        return Err(FutexError::Errno(errno_result()));
    }
    if now.tv_sec < 0 || now.tv_nsec < 0 || now.tv_nsec >= 1_000_000_000 {
        return Err(FutexError::Errno(libc::EINVAL));
    }
    (now.tv_sec as u128)
        .checked_mul(1_000_000_000)
        .and_then(|seconds| seconds.checked_add(now.tv_nsec as u128))
        .ok_or(FutexError::Errno(libc::EINVAL))
}

#[cfg(target_os = "linux")]
fn errno_result() -> i32 {
    unsafe { *libc::__errno_location() }
}

#[cfg(target_os = "android")]
fn errno_result() -> i32 {
    unsafe { *libc::__errno() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use core::sync::atomic::AtomicU32;
    use std::{string::ToString, sync::Arc, thread, time::Duration as StdDuration};

    #[test]
    fn expected_mismatch_returns_woken() {
        let value = AtomicU32::new(1);
        let result = unsafe {
            wait(
                &value as *const AtomicU32 as *const u32,
                0,
                Some(Duration::ZERO),
            )
        };
        assert_eq!(result, Ok(WaitOutcome::Woken));
    }

    #[test]
    fn timeout_returns_timed_out() {
        let value = AtomicU32::new(0);
        let result = unsafe {
            wait(
                &value as *const AtomicU32 as *const u32,
                0,
                Some(Duration::from_millis(5)),
            )
        };
        assert_eq!(result, Ok(WaitOutcome::TimedOut));
    }

    #[test]
    fn wait_is_woken_by_wake() {
        let value = Arc::new(AtomicU32::new(0));
        let waiter_value = Arc::clone(&value);
        let waiter = thread::spawn(move || unsafe {
            wait(
                waiter_value.as_ref() as *const AtomicU32 as *const u32,
                0,
                None,
            )
        });
        thread::sleep(StdDuration::from_millis(5));
        let woken = unsafe { wake(value.as_ref() as *const AtomicU32 as *const u32, 1) }
            .expect("wake failed");
        assert!(woken <= 1);
        assert_eq!(
            waiter.join().expect("waiter panicked"),
            Ok(WaitOutcome::Woken)
        );
    }

    #[test]
    fn invalid_address_is_rejected_before_syscall() {
        let result = unsafe { wait(core::ptr::null(), 0, None) };
        assert_eq!(result, Err(FutexError::Errno(libc::EINVAL)));
        assert!(
            result
                .expect_err("null address unexpectedly succeeded")
                .to_string()
                .contains("errno 22")
        );
    }

    #[test]
    fn unsupported_syscall_is_not_a_wait_outcome() {
        let value = AtomicU32::new(0);
        let result = unsafe {
            wait_with_syscall(
                &value as *const AtomicU32 as *const u32,
                0,
                Some(Duration::ZERO),
                -1,
            )
        };
        assert_eq!(result, Err(FutexError::Unsupported));
    }

    #[test]
    fn wait_errno_classification_preserves_retry_semantics() {
        assert_eq!(
            classify_wait_errno(libc::EAGAIN),
            Ok(WaitSyscallOutcome::Woken)
        );
        assert_eq!(
            classify_wait_errno(libc::EINTR),
            Ok(WaitSyscallOutcome::Interrupted)
        );
        assert_eq!(
            classify_wait_errno(libc::ETIMEDOUT),
            Ok(WaitSyscallOutcome::TimedOut)
        );
        assert_eq!(
            classify_wait_errno(libc::ENOSYS),
            Err(FutexError::Unsupported)
        );
        assert_eq!(
            classify_wait_errno(libc::EFAULT),
            Err(FutexError::Errno(libc::EFAULT))
        );
    }
}
