use veloq_pod::*;

#[test]
fn test_bytes_of() {
    let val: u32 = 0x12345678;
    let bytes = bytes_of(&val);
    assert_eq!(bytes.len(), 4);
    if cfg!(target_endian = "little") {
        assert_eq!(bytes, &[0x78, 0x56, 0x34, 0x12]);
    } else {
        assert_eq!(bytes, &[0x12, 0x34, 0x56, 0x78]);
    }
}

#[test]
fn test_bytes_of_mut() {
    let mut val: u32 = 0;
    {
        let bytes = bytes_of_mut(&mut val);
        bytes[0] = 0x11;
        bytes[1] = 0x22;
        bytes[2] = 0x33;
        bytes[3] = 0x44;
    }
    if cfg!(target_endian = "little") {
        assert_eq!(val, 0x44332211);
    } else {
        assert_eq!(val, 0x11223344);
    }
}

#[test]
fn test_try_from_bytes_success() {
    let bytes = [0x11, 0x22, 0x33, 0x44];
    let val: &u32 = try_from_bytes(&bytes).unwrap();
    if cfg!(target_endian = "little") {
        assert_eq!(*val, 0x44332211);
    } else {
        assert_eq!(*val, 0x11223344);
    }
}

#[test]
fn test_try_from_bytes_size_mismatch() {
    let bytes = [0x11, 0x22, 0x33];
    let res: Result<&u32, PodError> = try_from_bytes(&bytes);
    assert_eq!(res, Err(PodError::SizeMismatch));
}

#[test]
fn test_try_from_bytes_alignment_mismatch() {
    let bytes = [0u8; 8];
    // Create a subslice starting at offset 1 to force misalignment for u32 (align 4)
    let slice = &bytes[1..5];
    let res: Result<&u32, PodError> = try_from_bytes(slice);
    // Note: slice.as_ptr() might happen to be aligned by chance if we are extremely unlucky with stack layout,
    // but usually it will be misaligned.
    if !(slice.as_ptr() as usize).is_multiple_of(4) {
        assert_eq!(res, Err(PodError::AlignmentMismatch));
    }
}

#[test]
fn test_zeroable() {
    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct MyStruct {
        a: u32,
        b: u16,
    }
    unsafe impl Zeroable for MyStruct {}
    unsafe impl Pod for MyStruct {}

    assert_eq!(std::mem::size_of::<MyStruct>(), 8);
    let bytes = [0u8; 8];
    let s: &MyStruct = from_bytes(&bytes);
    assert_eq!(s.a, 0);
    assert_eq!(s.b, 0);
}

#[test]
fn test_mut_cast() {
    let mut bytes = [0u8; 4];
    {
        let val: &mut u32 = from_bytes_mut(&mut bytes);
        *val = 0x12345678;
    }
    if cfg!(target_endian = "little") {
        assert_eq!(bytes, [0x78, 0x56, 0x34, 0x12]);
    } else {
        assert_eq!(bytes, [0x12, 0x34, 0x56, 0x78]);
    }
}

#[test]
fn test_zeroed() {
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct MyStruct {
        a: u32,
        b: u64,
    }
    unsafe impl Zeroable for MyStruct {}

    let s: MyStruct = zeroed();
    assert_eq!(s.a, 0);
    assert_eq!(s.b, 0);
}

#[test]
fn test_cast_ref() {
    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Foo(u32);
    unsafe impl Zeroable for Foo {}
    unsafe impl Pod for Foo {}

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Bar(u32);
    unsafe impl Zeroable for Bar {}
    unsafe impl Pod for Bar {}

    let foo = Foo(0x12345678);
    let bar: &Bar = cast_ref(&foo);
    assert_eq!(bar.0, 0x12345678);
}

#[test]
fn test_cast_mut() {
    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Foo(u32);
    unsafe impl Zeroable for Foo {}
    unsafe impl Pod for Foo {}

    #[repr(C)]
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    struct Bar(u32);
    unsafe impl Zeroable for Bar {}
    unsafe impl Pod for Bar {}

    let mut foo = Foo(0);
    {
        let bar: &mut Bar = cast_mut(&mut foo);
        bar.0 = 0x87654321;
    }
    assert_eq!(foo.0, 0x87654321);
}
