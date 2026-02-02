use std::io;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, INVALID_SOCKET, IPPROTO_TCP, RIO_EXTENSION_FUNCTION_TABLE,
    SIO_GET_EXTENSION_FUNCTION_POINTER, SIO_GET_MULTIPLE_EXTENSION_FUNCTION_POINTER, SOCK_STREAM,
    SOCKADDR, SOCKET, WSA_FLAG_OVERLAPPED, WSAID_ACCEPTEX, WSAID_CONNECTEX,
    WSAID_GETACCEPTEXSOCKADDRS, WSAID_MULTIPLE_RIO, WSAIoctl, WSASocketW, closesocket,
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
pub struct Extensions {
    pub(crate) accept_ex: LpfnAcceptEx,
    pub(crate) connect_ex: LpfnConnectEx,
    pub(crate) get_accept_ex_sockaddrs: LpfnGetAcceptExSockaddrs,
    pub(crate) rio_table: Option<RIO_EXTENSION_FUNCTION_TABLE>,
}

impl std::fmt::Debug for Extensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Extensions")
            .field("rio_table", &self.rio_table.is_some())
            .finish_non_exhaustive()
    }
}

impl Extensions {
    pub(crate) fn new() -> io::Result<Self> {
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
                return Err(io::Error::last_os_error());
            }
            s
        };

        let traditional = Self::load_traditional(socket);
        let rio = Self::load_rio(socket);

        unsafe { closesocket(socket) };

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
    ) -> io::Result<(LpfnAcceptEx, LpfnConnectEx, LpfnGetAcceptExSockaddrs)> {
        unsafe {
            let accept_ex_ptr = Self::get_extension(socket, WSAID_ACCEPTEX)?;
            let connect_ex_ptr = Self::get_extension(socket, WSAID_CONNECTEX)?;
            let get_accept_ex_sockaddrs_ptr =
                Self::get_extension(socket, WSAID_GETACCEPTEXSOCKADDRS)?;

            let accept_ex =
                std::mem::transmute::<*const std::ffi::c_void, LpfnAcceptEx>(accept_ex_ptr);
            let connect_ex =
                std::mem::transmute::<*const std::ffi::c_void, LpfnConnectEx>(connect_ex_ptr);
            let get_accept_ex_sockaddrs = std::mem::transmute::<
                *const std::ffi::c_void,
                LpfnGetAcceptExSockaddrs,
            >(get_accept_ex_sockaddrs_ptr);

            Ok((accept_ex, connect_ex, get_accept_ex_sockaddrs))
        }
    }

    fn load_rio(socket: SOCKET) -> Option<RIO_EXTENSION_FUNCTION_TABLE> {
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
                Some(table)
            } else {
                let err = io::Error::last_os_error();
                eprintln!("Failed to load RIO extension table: {:?}", err);
                None
            }
        }
    }

    unsafe fn get_extension(
        socket: SOCKET,
        guid: windows_sys::core::GUID,
    ) -> io::Result<*const std::ffi::c_void> {
        let mut guid = guid;
        let mut ptr: *mut std::ffi::c_void = std::ptr::null_mut();
        let mut bytes_returned = 0;

        let ret = unsafe {
            WSAIoctl(
                socket,
                SIO_GET_EXTENSION_FUNCTION_POINTER,
                &mut guid as *mut _ as *mut _,
                std::mem::size_of_val(&guid) as u32,
                &mut ptr as *mut _ as *mut _,
                std::mem::size_of_val(&ptr) as u32,
                &mut bytes_returned,
                std::ptr::null_mut(),
                None,
            )
        };

        if ret != 0 {
            return Err(io::Error::last_os_error());
        }

        Ok(ptr as *const _)
    }
}
