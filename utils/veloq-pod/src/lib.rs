use std::fmt;
use std::mem::{align_of, size_of};

/// An error that can occur when casting between types and byte slices.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PodError {
    /// The source slice's length does not match the target type's size.
    SizeMismatch,
    /// The source slice is not properly aligned for the target type.
    AlignmentMismatch,
}

impl fmt::Display for PodError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SizeMismatch => write!(f, "size mismatch during cast"),
            Self::AlignmentMismatch => write!(f, "alignment mismatch during cast"),
        }
    }
}

impl std::error::Error for PodError {}

/// Trait for types that can be safely initialized with all zeros.
///
/// # Safety
///
/// The type must be valid when all its bits are zero.
pub unsafe trait Zeroable: Copy {}

/// Trait for Plain Old Data types that can be safely cast to/from byte slices.
///
/// # Safety
///
/// To implement this trait, the type must:
/// - Be `Zeroable`.
/// - Have no internal padding (or the padding must not matter).
/// - Have no requirements other than its size and alignment.
/// - Be valid for any bit pattern that matches its size.
pub unsafe trait Pod: Zeroable {}

// --- Implementations for primitive types ---

unsafe impl Zeroable for u8 {}
unsafe impl Pod for u8 {}
unsafe impl Zeroable for u16 {}
unsafe impl Pod for u16 {}
unsafe impl Zeroable for u32 {}
unsafe impl Pod for u32 {}
unsafe impl Zeroable for u64 {}
unsafe impl Pod for u64 {}
unsafe impl Zeroable for i8 {}
unsafe impl Pod for i8 {}
unsafe impl Zeroable for i16 {}
unsafe impl Pod for i16 {}
unsafe impl Zeroable for i32 {}
unsafe impl Pod for i32 {}
unsafe impl Zeroable for i64 {}
unsafe impl Pod for i64 {}
unsafe impl Zeroable for usize {}
unsafe impl Pod for usize {}
unsafe impl Zeroable for isize {}
unsafe impl Pod for isize {}

// --- Casting Functions ---

/// Casts a reference to a `Pod` type into a byte slice.
pub fn bytes_of<T: Pod>(val: &T) -> &[u8] {
    let ptr = val as *const T as *const u8;
    // SAFETY: T is Pod, so it's safe to treat its memory as a byte slice.
    unsafe { std::slice::from_raw_parts(ptr, size_of::<T>()) }
}

/// Casts a mutable reference to a `Pod` type into a mutable byte slice.
pub fn bytes_of_mut<T: Pod>(val: &mut T) -> &mut [u8] {
    let ptr = val as *mut T as *mut u8;
    // SAFETY: T is Pod, so it's safe to treat its memory as a byte slice.
    unsafe { std::slice::from_raw_parts_mut(ptr, size_of::<T>()) }
}

/// Attempts to cast a byte slice into a reference to a `Pod` type.
///
/// Returns an error if the slice's length or alignment is incorrect.
pub fn try_from_bytes<T: Pod>(bytes: &[u8]) -> Result<&T, PodError> {
    if bytes.len() != size_of::<T>() {
        return Err(PodError::SizeMismatch);
    }
    if !(bytes.as_ptr() as usize).is_multiple_of(align_of::<T>()) {
        return Err(PodError::AlignmentMismatch);
    }
    // SAFETY: Size and alignment are checked. T is Pod, so any bit pattern is valid.
    Ok(unsafe { &*(bytes.as_ptr() as *const T) })
}

/// Attempts to cast a mutable byte slice into a mutable reference to a `Pod` type.
///
/// Returns an error if the slice's length or alignment is incorrect.
pub fn try_from_bytes_mut<T: Pod>(bytes: &mut [u8]) -> Result<&mut T, PodError> {
    if bytes.len() != size_of::<T>() {
        return Err(PodError::SizeMismatch);
    }
    if !(bytes.as_ptr() as usize).is_multiple_of(align_of::<T>()) {
        return Err(PodError::AlignmentMismatch);
    }
    // SAFETY: Size and alignment are checked. T is Pod, so any bit pattern is valid.
    Ok(unsafe { &mut *(bytes.as_mut_ptr() as *mut T) })
}

/// Casts a byte slice into a reference to a `Pod` type, panicking on failure.
pub fn from_bytes<T: Pod>(bytes: &[u8]) -> &T {
    try_from_bytes(bytes).expect("from_bytes cast failed")
}

/// Casts a mutable byte slice into a mutable reference to a `Pod` type, panicking on failure.
pub fn from_bytes_mut<T: Pod>(bytes: &mut [u8]) -> &mut T {
    try_from_bytes_mut(bytes).expect("from_bytes_mut cast failed")
}

/// Returns a zero-initialized instance of a `Zeroable` type.
pub fn zeroed<T: Zeroable>() -> T {
    // SAFETY: T is Zeroable, so it's safe to initialize it with zeros.
    unsafe { std::mem::zeroed() }
}

/// Casts a reference to a `Pod` type to a reference of another `Pod` type.
///
/// Panics if the types have different sizes or if the alignment is incorrect.
pub fn cast_ref<T: Pod, U: Pod>(val: &T) -> &U {
    if size_of::<T>() != size_of::<U>() {
        panic!("cast_ref size mismatch");
    }
    from_bytes(bytes_of(val))
}

/// Casts a mutable reference to a `Pod` type to a mutable reference of another `Pod` type.
///
/// Panics if the types have different sizes or if the alignment is incorrect.
pub fn cast_mut<T: Pod, U: Pod>(val: &mut T) -> &mut U {
    if size_of::<T>() != size_of::<U>() {
        panic!("cast_mut size mismatch");
    }
    from_bytes_mut(bytes_of_mut(val))
}
