use veloq_std::{
    alloc_crate::{
        borrow::Cow,
        boxed::Box,
        collections::{BTreeMap, BTreeSet},
        format,
        rc::Rc,
        sync::Arc,
        vec,
        vec::Vec,
    },
    ffi::{Display, OsStr, OsStrJoin, OsString},
};

#[test]
fn test_os_string_basic_lifecycle() {
    let mut os_str = OsString::new();
    assert!(os_str.is_empty());
    assert_eq!(os_str.len(), 0);

    os_str.push("veloq");
    assert_eq!(os_str.len(), 5);
    assert_eq!(os_str, "veloq");

    os_str.push(OsStr::new("-std"));
    assert_eq!(os_str, "veloq-std");

    let borrowed: &OsStr = os_str.as_os_str();
    assert_eq!(borrowed, "veloq-std");
    assert_eq!(borrowed.to_str(), Some("veloq-std"));

    os_str.clear();
    assert!(os_str.is_empty());
    assert_eq!(os_str.len(), 0);
}

#[test]
fn test_os_string_capacity_and_reserve() {
    let mut s = OsString::with_capacity(64);
    assert!(s.capacity() >= 64);

    s.push("performance");

    s.reserve(128);
    assert!(s.capacity() >= s.len() + 128);

    s.reserve_exact(10);
    assert!(s.capacity() >= s.len() + 10);

    assert!(s.try_reserve(10).is_ok());
    assert!(s.try_reserve_exact(10).is_ok());

    s.shrink_to_fit();
    assert!(s.capacity() >= s.len());

    s.reserve(100);
    s.shrink_to(20);
    assert!(s.capacity() >= 20);
}

#[test]
fn test_os_str_and_string_conversions() {
    let raw = "hello \u{1F980}!";
    let os_string = OsString::from(raw);
    let os_str = OsStr::new(raw);

    assert_eq!(os_string.as_os_str(), os_str);
    assert_eq!(os_str.to_str(), Some(raw));
    assert_eq!(os_str.to_string_lossy(), raw);

    let back_to_string = os_string.clone().into_string().expect("valid utf8");
    assert_eq!(back_to_string, raw);

    let bytes = os_str.as_encoded_bytes();
    let reconstructed = unsafe { OsStr::from_encoded_bytes_unchecked(bytes) };
    assert_eq!(reconstructed, os_str);

    let vec_bytes = os_string.into_encoded_bytes();
    let reconstructed_owned = unsafe { OsString::from_encoded_bytes_unchecked(vec_bytes) };
    assert_eq!(reconstructed_owned, os_str);
}

#[test]
fn test_boxed_and_shared_pointers() {
    let text = "smart pointer test";
    let os_str = OsStr::new(text);

    let boxed: Box<OsStr> = Box::from(os_str);
    assert_eq!(&*boxed, os_str);

    let cloned_box = boxed.clone();
    let unboxed: OsString = cloned_box.into_os_string();
    assert_eq!(unboxed, os_str);

    let from_owned: Box<OsStr> = unboxed.into_boxed_os_str();
    assert_eq!(&*from_owned, os_str);

    let arc: Arc<OsStr> = Arc::from(os_str);
    let rc: Rc<OsStr> = Rc::from(os_str);
    assert_eq!(&*arc, os_str);
    assert_eq!(&*rc, os_str);

    let arc2: Arc<OsStr> = Arc::from(os_str.to_os_string());
    let rc2: Rc<OsStr> = Rc::from(os_str.to_os_string());
    assert_eq!(&*arc2, os_str);
    assert_eq!(&*rc2, os_str);

    let cow_borrowed: Cow<'_, OsStr> = Cow::from(os_str);
    let cow_owned: Cow<'_, OsStr> = Cow::from(os_str.to_os_string());
    assert_eq!(cow_borrowed, cow_owned);
    assert_eq!(cow_borrowed.into_owned(), os_str);
}

#[test]
fn test_collections_and_ordering() {
    let mut set = BTreeSet::new();
    set.insert(OsString::from("banana"));
    set.insert(OsString::from("apple"));
    set.insert(OsString::from("cherry"));

    let ordered: Vec<&str> = set.iter().map(|s| s.to_str().unwrap()).collect();
    assert_eq!(ordered, vec!["apple", "banana", "cherry"]);

    let mut map = BTreeMap::new();
    map.insert(OsString::from("k1"), 100);
    map.insert(OsString::from("k2"), 200);
    assert_eq!(map.get(OsStr::new("k1")), Some(&100));
}

#[test]
fn test_split_and_slice() {
    let os_str = OsStr::new("abc🦀def");
    assert_eq!(os_str.slice_encoded_bytes(..3), "abc");
    assert_eq!(os_str.slice_encoded_bytes(3..7), "🦀");
    assert_eq!(os_str.slice_encoded_bytes(7..), "def");

    let (left, right) = os_str.split_at(3);
    assert_eq!(left, "abc");
    assert_eq!(right, "🦀def");

    assert!(os_str.split_at_checked(4).is_none()); // Mid-character index for crab
    assert!(os_str.split_at_checked(7).is_some());
}

#[test]
fn test_formatting_and_display() {
    let os_str = OsStr::new("hello display");
    let disp: Display<'_> = os_str.display();
    assert_eq!(format!("{disp}"), "hello display");
    assert_eq!(format!("{disp:?}"), "\"hello display\"");
    assert_eq!(format!("{os_str:?}"), "\"hello display\"");

    use core::fmt::Write;
    let mut buf = OsString::new();
    write!(buf, "fmt write test {}", 42).unwrap();
    assert_eq!(buf, "fmt write test 42");
}

#[test]
fn test_extend_and_join() {
    let parts = [OsStr::new("usr"), OsStr::new("local"), OsStr::new("bin")];
    assert_eq!(parts.join(OsStr::new("/")), "usr/local/bin");

    let mut base = OsString::from("root");
    base.extend([OsStr::new("/home"), OsStr::new("/user")]);
    assert_eq!(base, "root/home/user");
}

#[cfg(unix)]
#[test]
fn test_unix_extensions() {
    use veloq_std::os::unix::ffi::{OsStrExt, OsStringExt};

    let raw_bytes = b"foo\xFFbar";
    let os_str = OsStr::from_bytes(raw_bytes);
    assert_eq!(os_str.as_bytes(), raw_bytes);
    assert!(os_str.to_str().is_none());
    assert_eq!(os_str.to_string_lossy(), "foo\u{FFFD}bar");

    let os_string = OsString::from_vec(raw_bytes.to_vec());
    assert_eq!(os_string.as_bytes(), raw_bytes);
    assert_eq!(os_string.into_vec(), raw_bytes.to_vec());
}

#[cfg(windows)]
#[test]
fn test_windows_extensions() {
    use veloq_std::os::windows::ffi::{OsStrExt, OsStringExt};

    let wide = [0x0061, 0x0062, 0xD800, 0x0063];
    let os_string = OsString::from_wide(&wide);
    assert!(os_string.to_str().is_none());
    assert_eq!(os_string.to_string_lossy(), "ab\u{FFFD}c");

    let re_encoded: Vec<u16> = os_string.encode_wide().collect();
    assert_eq!(&re_encoded[..], &wide[..]);
}
