pub mod fs {
    pub use std::os::windows::fs::OpenOptionsExt;
}

pub mod io {
    pub use std::os::windows::io::{
        AsRawHandle, AsRawSocket, IntoRawHandle, IntoRawSocket, RawHandle, RawSocket,
    };
}
