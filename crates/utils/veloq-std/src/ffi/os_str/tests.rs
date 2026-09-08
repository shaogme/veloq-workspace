use super::*;
use crate::alloc_crate::{borrow::ToOwned, boxed::Box, format, rc::Rc, sync::Arc, vec};

#[test]
fn test_os_string_with_capacity() {
    let os_string = OsString::with_capacity(0);
    assert_eq!(0, os_string.capacity());

    let os_string = OsString::with_capacity(10);
    assert!(os_string.capacity() >= 10);

    let mut os_string = OsString::with_capacity(0);
    os_string.push("abc");
    assert!(os_string.capacity() >= 3);
}

#[test]
fn test_os_string_clear() {
    let mut os_string = OsString::from("abc");
    assert_eq!(3, os_string.len());

    os_string.clear();
    assert_eq!(&os_string, "");
    assert_eq!(0, os_string.len());
}

#[test]
fn test_os_string_leak() {
    let os_string = OsString::from("have a cake");
    let leaked = os_string.leak();
    assert_eq!(leaked.as_encoded_bytes(), b"have a cake");
}

#[test]
fn test_os_string_capacity() {
    let os_string = OsString::with_capacity(0);
    assert_eq!(0, os_string.capacity());

    let os_string = OsString::with_capacity(10);
    assert!(os_string.capacity() >= 10);

    let mut os_string = OsString::with_capacity(0);
    os_string.push("abc");
    assert!(os_string.capacity() >= 3);
}

#[test]
fn test_os_string_reserve() {
    let mut os_string = OsString::new();
    assert_eq!(os_string.capacity(), 0);

    os_string.reserve(2);
    assert!(os_string.capacity() >= 2);

    for _ in 0..16 {
        os_string.push("a");
    }

    assert!(os_string.capacity() >= 16);
    os_string.reserve(16);
    assert!(os_string.capacity() >= 32);

    os_string.push("a");
    os_string.reserve(16);
    assert!(os_string.capacity() >= 33);
}

#[test]
fn test_os_string_reserve_exact() {
    let mut os_string = OsString::new();
    assert_eq!(os_string.capacity(), 0);

    os_string.reserve_exact(2);
    assert!(os_string.capacity() >= 2);

    for _ in 0..16 {
        os_string.push("a");
    }

    assert!(os_string.capacity() >= 16);
    os_string.reserve_exact(16);
    assert!(os_string.capacity() >= 32);

    os_string.push("a");
    os_string.reserve_exact(16);
    assert!(os_string.capacity() >= 33);
}

#[test]
fn test_os_string_shrink() {
    let mut s = OsString::from("foo");
    s.reserve(100);
    assert!(s.capacity() >= 100);

    s.shrink_to_fit();
    assert!(s.capacity() >= 3);

    s.reserve(100);
    s.shrink_to(10);
    assert!(s.capacity() >= 10);
}

#[test]
fn test_os_string_join() {
    let strings = [OsStr::new("hello"), OsStr::new("dear"), OsStr::new("world")];
    assert_eq!("hello", strings[..1].join(OsStr::new(" ")));
    assert_eq!("hello dear world", strings.join(OsStr::new(" ")));
    assert_eq!("hellodearworld", strings.join(OsStr::new("")));
    assert_eq!("hello.\n dear.\n world", strings.join(OsStr::new(".\n ")));

    let strings_abc = [
        OsString::from("a"),
        OsString::from("b"),
        OsString::from("c"),
    ];
    assert_eq!("a b c", strings_abc.join(OsStr::new(" ")));
}

#[test]
fn test_os_string_default() {
    let os_string: OsString = Default::default();
    assert_eq!("", &os_string);
}

#[test]
fn test_os_str_is_empty() {
    let mut os_string = OsString::new();
    assert!(os_string.is_empty());

    os_string.push("abc");
    assert!(!os_string.is_empty());

    os_string.clear();
    assert!(os_string.is_empty());
}

#[test]
fn test_os_str_len() {
    let mut os_string = OsString::new();
    assert_eq!(0, os_string.len());

    os_string.push("abc");
    assert_eq!(3, os_string.len());

    os_string.clear();
    assert_eq!(0, os_string.len());
}

#[test]
fn test_os_str_default() {
    let os_str: &OsStr = Default::default();
    assert_eq!("", os_str);
}

#[test]
fn into_boxed() {
    let orig = "Hello, world!";
    let os_str = OsStr::new(orig);
    let boxed: Box<OsStr> = Box::from(os_str);
    let os_string = os_str.to_owned().into_boxed_os_str().into_os_string();
    assert_eq!(os_str, &*boxed);
    assert_eq!(&*boxed, &*os_string);
    assert_eq!(&*os_string, os_str);
}

#[test]
fn boxed_default() {
    let boxed = <Box<OsStr>>::default();
    assert!(boxed.is_empty());
}

#[test]
fn test_os_str_clone_into() {
    let mut os_string = OsString::with_capacity(123);
    os_string.push("hello");
    let os_str = OsStr::new("bonjour");
    os_str.clone_into(&mut os_string);
    assert_eq!(os_str, os_string);
    assert!(os_string.capacity() >= 123);
}

#[test]
fn into_rc() {
    let orig = "Hello, world!";
    let os_str = OsStr::new(orig);
    let rc: Rc<OsStr> = Rc::from(os_str);
    let arc: Arc<OsStr> = Arc::from(os_str);

    assert_eq!(&*rc, os_str);
    assert_eq!(&*arc, os_str);

    let rc2: Rc<OsStr> = Rc::from(os_str.to_owned());
    let arc2: Arc<OsStr> = Arc::from(os_str.to_owned());

    assert_eq!(&*rc2, os_str);
    assert_eq!(&*arc2, os_str);
}

#[test]
fn slice_encoded_bytes() {
    let os_str = OsStr::new("123θგ🦀");
    let digits = os_str.slice_encoded_bytes(..3);
    assert_eq!(digits, "123");
    let three = os_str.slice_encoded_bytes(2..3);
    assert_eq!(three, "3");
    let theta = os_str.slice_encoded_bytes(3..5);
    assert_eq!(theta, "θ");
    let gani = os_str.slice_encoded_bytes(5..8);
    assert_eq!(gani, "გ");
    let crab = os_str.slice_encoded_bytes(8..);
    assert_eq!(crab, "🦀");
}

#[test]
#[should_panic]
fn slice_out_of_bounds() {
    let crab = OsStr::new("🦀");
    let _ = crab.slice_encoded_bytes(..5);
}

#[test]
#[should_panic]
fn slice_mid_char() {
    let crab = OsStr::new("🦀");
    let _ = crab.slice_encoded_bytes(..2);
}

#[cfg(not(windows))]
#[test]
#[should_panic(expected = "byte index 1 is not an OsStr boundary")]
fn slice_invalid_data() {
    use crate::os::unix::ffi::OsStrExt;

    let os_string = OsStr::from_bytes(b"\xFF\xFF");
    let _ = os_string.slice_encoded_bytes(1..);
}

#[cfg(not(windows))]
#[test]
#[should_panic(expected = "byte index 1 is not an OsStr boundary")]
fn slice_partial_utf8() {
    use crate::os::unix::ffi::{OsStrExt, OsStringExt};

    let part_crab = OsStr::from_bytes(&"🦀".as_bytes()[..3]);
    let mut os_string = OsString::from_vec(vec![0xFF]);
    os_string.push(part_crab);
    let _ = os_string.slice_encoded_bytes(1..);
}

#[cfg(not(windows))]
#[test]
fn slice_invalid_edge() {
    use crate::os::unix::ffi::{OsStrExt, OsStringExt};

    let os_string = OsStr::from_bytes(b"a\xFFa");
    assert_eq!(os_string.slice_encoded_bytes(..1), "a");
    assert_eq!(
        os_string.slice_encoded_bytes(1..),
        OsStr::from_bytes(b"\xFFa")
    );
    assert_eq!(
        os_string.slice_encoded_bytes(..2),
        OsStr::from_bytes(b"a\xFF")
    );
    assert_eq!(os_string.slice_encoded_bytes(2..), "a");

    let os_string = OsStr::from_bytes(&"abc🦀".as_bytes()[..6]);
    assert_eq!(os_string.slice_encoded_bytes(..3), "abc");
    assert_eq!(
        os_string.slice_encoded_bytes(3..),
        OsStr::from_bytes(b"\xF0\x9F\xA6")
    );

    let mut os_string = OsString::from_vec(vec![0xFF]);
    os_string.push("🦀");
    assert_eq!(
        os_string.slice_encoded_bytes(..1),
        OsStr::from_bytes(b"\xFF")
    );
    assert_eq!(os_string.slice_encoded_bytes(1..), "🦀");
}

#[test]
fn os_str_slice_at() {
    let input = OsStr::new("hello world");
    let (first, second) = input.split_at(5);
    assert_eq!(first, "hello");
    assert_eq!(second, " world");

    assert_eq!(
        input.split_at_checked(5),
        Some((OsStr::new("hello"), OsStr::new(" world")))
    );
    assert!(input.split_at_checked(999).is_none());
}

#[test]
fn test_os_str_ascii() {
    let mut s = OsString::from("Grüße, Jürgen ❤");
    assert!(!s.is_ascii());
    assert!(OsString::from("hello").is_ascii());

    assert_eq!("grüße, jürgen ❤", s.to_ascii_lowercase());
    assert_eq!("GRüßE, JüRGEN ❤", s.to_ascii_uppercase());

    s.make_ascii_lowercase();
    assert_eq!("grüße, jürgen ❤", s);

    s.make_ascii_uppercase();
    assert_eq!("GRüßE, JüRGEN ❤", s);

    assert!(OsString::from("Ferris").eq_ignore_ascii_case("FERRIS"));
    assert!(OsString::from("Ferrös").eq_ignore_ascii_case("FERRöS"));
    assert!(!OsString::from("Ferrös").eq_ignore_ascii_case("FERRÖS"));
}

#[test]
fn test_conversions_and_comparisons() {
    let s = "hello";
    let os_s = OsString::from(s);
    assert_eq!(os_s.to_str(), Some("hello"));
    assert_eq!(os_s.to_string_lossy(), "hello");
    assert_eq!(os_s.clone().into_string(), Ok(String::from("hello")));

    assert_eq!(os_s, s);
    assert_eq!(s, os_s);
    assert_eq!(&os_s, s);
    assert_eq!(s, &os_s);
    assert_eq!(os_s.as_os_str(), s);
    assert_eq!(s, os_s.as_os_str());

    assert!(os_s <= OsStr::new("world"));
    assert!(os_s < OsStr::new("world"));
    assert!(OsStr::new("world") > os_s);
}

#[test]
fn test_fmt_and_display() {
    let s = OsStr::new("hello world");
    assert_eq!(format!("{}", s.display()), "hello world");
    assert_eq!(format!("{:?}", s), "\"hello world\"");

    let os_string = OsString::from("test");
    assert_eq!(format!("{:?}", os_string), "\"test\"");
}

#[test]
fn test_from_iterator_and_extend() {
    let items = vec![
        OsString::from("a"),
        OsString::from("b"),
        OsString::from("c"),
    ];
    let collected: OsString = items.into_iter().collect();
    assert_eq!(collected, "abc");

    let mut base = OsString::from("start_");
    base.extend([OsStr::new("1"), OsStr::new("2")]);
    assert_eq!(base, "start_12");
}

#[test]
fn test_truncate() {
    let mut s = OsString::from("hello world");
    s.truncate(5);
    assert_eq!(s, "hello");
    s.truncate(10);
    assert_eq!(s, "hello");
}

#[cfg(windows)]
#[test]
fn test_wtf8_module_direct() {
    use super::wtf8::{CodePoint, Wtf8, Wtf8Buf};

    let mut buf = Wtf8Buf::new();
    buf.push_wtf8(Wtf8::from_str("abc"));
    assert_eq!(buf.len(), 3);
    assert_eq!(buf.to_string_lossy(), "abc");

    let surrogate = unsafe { CodePoint::from_u32_unchecked(0xD800) };
    buf.push(surrogate);
    assert_eq!(buf.to_string_lossy(), "abc\u{FFFD}");

    let wide: Vec<u16> = buf.encode_wide().collect();
    assert_eq!(wide, vec![0x61, 0x62, 0x63, 0xD800]);

    let round_trip = Wtf8Buf::from_wide(&wide);
    assert_eq!(round_trip.as_bytes(), buf.as_bytes());
}

#[cfg(windows)]
#[test]
fn test_windows_wide_extension() {
    use crate::os::windows::ffi::{OsStrExt, OsStringExt};

    let source = [
        0x0055, 0x006E, 0x0069, 0x0063, 0x006F, 0x0064, 0x0065, 0xD800,
    ];
    let os_string = OsString::from_wide(&source);
    let result: Vec<u16> = os_string.encode_wide().collect();
    assert_eq!(&source[..], &result[..]);
}
