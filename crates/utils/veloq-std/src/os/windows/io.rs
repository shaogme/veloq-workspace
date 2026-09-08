//! Windows-specific extensions to general I/O primitives.

mod handle;
mod raw;
mod socket;

pub use handle::{
    AsHandle, BorrowedHandle, HandleOrInvalid, HandleOrNull, InvalidHandleError, NullHandleError,
    OwnedHandle,
};
pub use raw::{
    AsRawHandle, AsRawSocket, FromRawHandle, FromRawSocket, IntoRawHandle, IntoRawSocket,
    RawHandle, RawSocket,
};
pub use socket::{AsSocket, BorrowedSocket, OwnedSocket};
