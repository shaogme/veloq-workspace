pub mod common;
pub mod config;
pub mod driver;
pub mod ext;
pub mod net;
pub mod ops;
pub mod rio;
pub mod win32;

#[cfg(test)]
pub mod tests;

use windows_sys::Win32::Networking::WinSock::{WSADATA, WSAStartup};

// Re-exports for convenience and backward compatibility where appropriate
pub use config::{
    BorrowedRawHandle, BufferRegistrationMode, IoFd, IocpConfig, OwnedRawHandle, RawHandle,
    RawHandleKind, RegisteredHandle,
};
pub use driver::{CloseMode, IocpDriver, IocpOpState, SocketLifecycleHandle};
pub use net::addr::{SockAddrStorage, socket_addr_to_storage, to_socket_addr};
pub use net::socket::Socket;
pub use win32::{IoCompletionPort, OwnedHandle, SafeSocket};

#[used]
/// SAFETY: link_section .CRT$XCU is used for global initialization on Windows.
#[unsafe(link_section = ".CRT$XCU")]
static INIT_WINSOCK: unsafe extern "C" fn() = {
    /// # Safety
    ///
    /// This function performs global initialization for Winsock and must be called correctly by the CRT.
    unsafe extern "C" fn init() {
        // SAFETY: WSAStartup is required for networking on Windows.
        unsafe {
            let mut data: WSADATA = std::mem::zeroed();
            let _ = WSAStartup(0x0202, &mut data);
        }
    }
    init
};
