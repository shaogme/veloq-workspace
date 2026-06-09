use crate::error::{IocpError, IocpResult};
use crate::rio::RioError;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, INVALID_SOCKET, IPPROTO_TCP, RIO_EXTENSION_FUNCTION_TABLE,
    SIO_GET_EXTENSION_FUNCTION_POINTER, SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER, SOCK_STREAM,
    SOCKADDR, SOCKET, WSA_FLAG_OVERLAPPED, WSAID_ACCEPTEX, WSAID_CONNECTEX,
    WSAID_GETACCEPTEXSOCKADDRS, WSAID_MULTIPLE_RIO, WSAIoctl, WSASocketW,
};
use windows_sys::Win32::System::IO::OVERLAPPED;

// Function pointer types for WinSock extensions
pub(crate) type LpfnAcceptEx = unsafe extern "system" fn(
    slistensocket: SOCKET,
    sacceptsocket: SOCKET,
    lpoutputbuffer: *mut std::ffi::c_void,
    dwreceivedatalength: u32,
    dwlocaladdresslength: u32,
    dwremoteaddresslength: u32,
    lpdwbytesreceived: *mut u32,
    lpoverlapped: *mut OVERLAPPED,
) -> i32;

pub(crate) type LpfnConnectEx = unsafe extern "system" fn(
    s: SOCKET,
    name: *const SOCKADDR,
    namelen: i32,
    lpsendbuffer: *const std::ffi::c_void,
    dwsenddatalength: u32,
    lpdwbytessent: *mut u32,
    lpoverlapped: *mut OVERLAPPED,
) -> i32;

pub(crate) type LpfnGetAcceptExSockaddrs = unsafe extern "system" fn(
    lpoutputbuffer: *const std::ffi::c_void,
    dwreceivedatalength: u32,
    dwlocaladdresslength: u32,
    dwremoteaddresslength: u32,
    localsockaddr: *mut *mut SOCKADDR,
    localsockaddrlength: *mut i32,
    remotesockaddr: *mut *mut SOCKADDR,
    remotesockaddrlength: *mut i32,
);

#[derive(Clone, Copy)]
pub(crate) struct Extensions {
    pub(crate) accept_ex: LpfnAcceptEx,
    pub(crate) connect_ex: LpfnConnectEx,
    pub(crate) get_accept_ex_sockaddrs: LpfnGetAcceptExSockaddrs,
    pub(crate) rio_table: RIO_EXTENSION_FUNCTION_TABLE,
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("rio_table", &true)
            .finish_non_exhaustive()
    }
}

impl Extensions {
    pub(crate) fn new() -> IocpResult<Self> {
        // SAFETY: Calling `WSASocketW` to create a new TCP socket. The parameters
        // are standard for a overlapped socket.
        let socket = unsafe {
            let s = WSASocketW(
                AF_INET as i32,
                SOCK_STREAM,
                IPPROTO_TCP,
                std::ptr::null(),
                0,
                WSA_FLAG_OVERLAPPED,
            );
            if s == INVALID_SOCKET {
                return Err(
                    IocpError::DriverInit.io_report("WSASocketW", std::io::Error::last_os_error())
                );
            }
            crate::win32::SafeSocket(s)
        };

        let traditional = Self::load_traditional(socket.as_raw());
        let rio = Self::load_rio(socket.as_raw())?;

        // SafeSocket will be dropped here and closesocket will be called automatically.

        let (accept_ex, connect_ex, get_accept_ex_sockaddrs) = traditional?;

        Ok(Self {
            accept_ex,
            connect_ex,
            get_accept_ex_sockaddrs,
            rio_table: rio,
        })
    }

    fn load_traditional(
        socket: SOCKET,
    ) -> IocpResult<(LpfnAcceptEx, LpfnConnectEx, LpfnGetAcceptExSockaddrs)> {
        let accept_ex = Self::get_extension(socket, WSAID_ACCEPTEX)?;
        let connect_ex = Self::get_extension(socket, WSAID_CONNECTEX)?;
        let get_accept_ex_sockaddrs = Self::get_extension(socket, WSAID_GETACCEPTEXSOCKADDRS)?;

        Ok((accept_ex, connect_ex, get_accept_ex_sockaddrs))
    }

    fn load_rio(socket: SOCKET) -> IocpResult<RIO_EXTENSION_FUNCTION_TABLE> {
        // SAFETY: `RIO_EXTENSION_FUNCTION_TABLE` is a POD struct. `WSAIoctl` is
        // called to fill it. Memory layout for the struct is guaranteed to be compatible.
        unsafe {
            let mut guid = WSAID_MULTIPLE_RIO;
            let mut table: RIO_EXTENSION_FUNCTION_TABLE = std::mem::zeroed();
            // RIO_EXTENSION_FUNCTION_TABLE must have cbSize initialized
            table.cbSize = std::mem::size_of::<RIO_EXTENSION_FUNCTION_TABLE>() as u32;

            let mut bytes_returned = 0;
            let ret = WSAIoctl(
                socket,
                SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER,
                &mut guid as *mut _ as *mut _,
                std::mem::size_of_val(&guid) as u32,
                &mut table as *mut _ as *mut _,
                std::mem::size_of_val(&table) as u32,
                &mut bytes_returned,
                std::ptr::null_mut(),
                None,
            );

            if ret == 0 {
                Ok(table)
            } else {
                Err(IocpError::Rio(RioError::LibraryLoad)
                    .io_report("WSAIoctl.load_rio", std::io::Error::last_os_error()))
            }
        }
    }

    fn get_extension<T>(socket: SOCKET, guid: windows_sys::core::GUID) -> IocpResult<T> {
        let mut guid = guid;
        let mut val = std::mem::MaybeUninit::<T>::uninit();
        let mut bytes_returned = 0;

        // SAFETY: `WSAIoctl` is called with correct pointers and sizes for the requested GUID extension pointer.
        // The pointer `val.as_mut_ptr()` is a valid pointer to memory owned by the stack.
        // If WSAIoctl returns success (0), it has initialized the memory inside `val` with the function pointer.
        let ret = unsafe {
            WSAIoctl(
                socket,
                SIO_GET_EXTENSION_FUNCTION_POINTER,
                &mut guid as *mut _ as *mut _,
                std::mem::size_of_val(&guid) as u32,
                val.as_mut_ptr() as *mut _,
                std::mem::size_of::<T>() as u32,
                &mut bytes_returned,
                std::ptr::null_mut(),
                None,
            )
        };

        if ret == 0 {
            // SAFETY: WSAIoctl successfully executed and initialized the memory.
            unsafe { Ok(val.assume_init()) }
        } else {
            Err(IocpError::DriverInit
                .io_report("WSAIoctl.get_extension", std::io::Error::last_os_error()))
        }
    }
}
