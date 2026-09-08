//! Windows-specific networking helpers and initialization.

use core::{
    mem,
    sync::atomic::{
        AtomicBool,
        Ordering::{AcqRel, Relaxed},
    },
};

use windows_sys::Win32::Networking::WinSock::{WSACleanup, WSADATA, WSAGetLastError, WSAStartup};

use crate::io::Error;

static WSA_STARTED: AtomicBool = AtomicBool::new(false);

/// Checks whether the Windows socket interface has been started already, and
/// if not, starts it.
#[inline]
pub fn init() {
    if !WSA_STARTED.load(Relaxed) {
        wsa_startup();
    }
}

#[cold]
fn wsa_startup() {
    unsafe {
        let mut data: WSADATA = mem::zeroed();
        let ret = WSAStartup(0x0202, &mut data);
        assert_eq!(ret, 0, "failed to initialize winsock");
        if WSA_STARTED.swap(true, AcqRel) {
            let _ = WSACleanup();
        }
    }
}

/// Returns the last error from the Windows socket interface.
#[inline]
pub fn last_error() -> Error {
    Error::from_raw_os_error(unsafe { WSAGetLastError() })
}
