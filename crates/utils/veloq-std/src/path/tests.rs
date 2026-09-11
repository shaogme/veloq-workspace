use core::hash::{Hash, Hasher};

use crate::alloc_crate::vec;
use crate::{
    alloc_crate::{
        boxed::Box,
        format,
        rc::Rc,
        string::{String, ToString},
        sync::Arc,
        vec::Vec,
    },
    collections::hash_map::DefaultHasher,
    ffi::{OsStr, OsString},
    path::{Component, MAIN_SEPARATOR, MAIN_SEPARATOR_STR, Path, PathBuf, is_separator},
};

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

#[test]
fn test_path_basic_and_conversions() {
    let p = Path::new("foo/bar/baz.txt");
    assert_eq!(p.to_str(), Some("foo/bar/baz.txt"));
    assert_eq!(p.to_string_lossy(), "foo/bar/baz.txt");
    assert_eq!(p.as_bytes(), b"foo/bar/baz.txt");
    assert!(!p.is_empty());

    let empty = Path::new("");
    assert!(empty.is_empty());
    assert_eq!(empty.as_bytes(), b"");

    let pb = p.to_path_buf();
    assert_eq!(pb.as_path(), p);
    assert_eq!(pb.as_os_str(), OsStr::new("foo/bar/baz.txt"));

    let pb_from_str = PathBuf::from("hello");
    assert_eq!(pb_from_str.as_path(), Path::new("hello"));

    let pb_from_string = PathBuf::from(String::from("world"));
    assert_eq!(pb_from_string.as_path(), Path::new("world"));

    let pb_from_os_str = PathBuf::from(OsStr::new("rust"));
    assert_eq!(pb_from_os_str.as_path(), Path::new("rust"));

    let pb_from_os_string = PathBuf::from(OsString::from("2024"));
    assert_eq!(pb_from_os_string.as_path(), Path::new("2024"));

    let os_string: OsString = pb.clone().into_os_string();
    assert_eq!(os_string, OsStr::new("foo/bar/baz.txt"));

    let string_res = pb.clone().into_string();
    assert_eq!(string_res, Ok(String::from("foo/bar/baz.txt")));

    let boxed: Box<Path> = Box::from(p);
    assert_eq!(&*boxed, p);
    let pb_unboxed: PathBuf = boxed.into_path_buf();
    assert_eq!(pb_unboxed.as_path(), p);

    let arc: Arc<Path> = Arc::from(p);
    assert_eq!(&*arc, p);
    let rc: Rc<Path> = Rc::from(p);
    assert_eq!(&*rc, p);
}

#[test]
fn test_path_components_and_iteration() {
    let p = Path::new("/foo/bar/baz.txt");
    let comps: Vec<Component<'_>> = p.components().collect();

    assert_eq!(
        comps,
        vec![
            Component::RootDir,
            Component::Normal(OsStr::new("foo")),
            Component::Normal(OsStr::new("bar")),
            Component::Normal(OsStr::new("baz.txt")),
        ]
    );

    let items: Vec<&OsStr> = p.iter().collect();
    assert_eq!(
        items,
        vec![
            OsStr::new(MAIN_SEPARATOR_STR),
            OsStr::new("foo"),
            OsStr::new("bar"),
            OsStr::new("baz.txt"),
        ]
    );

    let p_rel = Path::new("./a/../b");
    let rel_comps: Vec<Component<'_>> = p_rel.components().collect();
    assert_eq!(
        rel_comps,
        vec![
            Component::CurDir,
            Component::Normal(OsStr::new("a")),
            Component::ParentDir,
            Component::Normal(OsStr::new("b")),
        ]
    );

    let mut rev_iter = p.iter().rev();
    assert_eq!(rev_iter.next(), Some(OsStr::new("baz.txt")));
    assert_eq!(rev_iter.next(), Some(OsStr::new("bar")));
    assert_eq!(rev_iter.next(), Some(OsStr::new("foo")));
    assert_eq!(rev_iter.next(), Some(OsStr::new(MAIN_SEPARATOR_STR)));
    assert_eq!(rev_iter.next(), None);
}

#[test]
fn test_path_parents_and_ancestors() {
    let p = Path::new("/foo/bar/baz.txt");
    assert_eq!(p.parent(), Some(Path::new("/foo/bar")));
    assert_eq!(p.parent().unwrap().parent(), Some(Path::new("/foo")));
    assert_eq!(
        p.parent().unwrap().parent().unwrap().parent(),
        Some(Path::new("/"))
    );
    assert_eq!(
        p.parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent(),
        None
    );

    let ancestors: Vec<&Path> = p.ancestors().collect();
    assert_eq!(
        ancestors,
        vec![
            Path::new("/foo/bar/baz.txt"),
            Path::new("/foo/bar"),
            Path::new("/foo"),
            Path::new("/"),
        ]
    );

    let single = Path::new("file.txt");
    assert_eq!(single.parent(), Some(Path::new("")));
    assert_eq!(Path::new("").parent(), None);
}

#[test]
fn test_path_file_names_stems_and_extensions() {
    let p = Path::new("dir/sub/archive.tar.gz");
    assert_eq!(p.file_name(), Some(OsStr::new("archive.tar.gz")));
    assert_eq!(p.file_stem(), Some(OsStr::new("archive.tar")));
    assert_eq!(p.file_prefix(), Some(OsStr::new("archive")));
    assert_eq!(p.extension(), Some(OsStr::new("gz")));

    let dotfile = Path::new(".config.toml");
    assert_eq!(dotfile.file_name(), Some(OsStr::new(".config.toml")));
    assert_eq!(dotfile.file_stem(), Some(OsStr::new(".config")));
    assert_eq!(dotfile.file_prefix(), Some(OsStr::new(".config")));
    assert_eq!(dotfile.extension(), Some(OsStr::new("toml")));

    let no_ext = Path::new("my_program");
    assert_eq!(no_ext.file_name(), Some(OsStr::new("my_program")));
    assert_eq!(no_ext.file_stem(), Some(OsStr::new("my_program")));
    assert_eq!(no_ext.file_prefix(), Some(OsStr::new("my_program")));
    assert_eq!(no_ext.extension(), None);

    let root = Path::new("/");
    assert_eq!(root.file_name(), None);
    assert_eq!(root.file_stem(), None);
    assert_eq!(root.file_prefix(), None);
    assert_eq!(root.extension(), None);
}

#[test]
fn test_pathbuf_mutations() {
    let mut buf = PathBuf::from("base");
    buf.push("next");
    assert_eq!(buf.as_path(), Path::new("base").join("next"));

    let mut buf2 = PathBuf::from("foo/bar.txt");
    buf2.set_file_name("baz.md");
    assert_eq!(buf2.as_path(), Path::new("foo/baz.md"));

    let mut buf3 = PathBuf::from("foo/bar.txt");
    assert!(buf3.set_extension("rs"));
    assert_eq!(buf3.as_path(), Path::new("foo/bar.rs"));

    assert!(buf3.set_extension(""));
    assert_eq!(buf3.as_path(), Path::new("foo/bar"));

    let mut buf4 = PathBuf::from("archive.tar");
    assert!(buf4.add_extension("gz"));
    assert_eq!(buf4.as_path(), Path::new("archive.tar.gz"));

    let with_ext = Path::new("main.rs").with_extension("o");
    assert_eq!(with_ext.as_path(), Path::new("main.o"));

    let with_add_ext = Path::new("main.tar").with_added_extension("xz");
    assert_eq!(with_add_ext.as_path(), Path::new("main.tar.xz"));

    let with_name = Path::new("a/b/c.txt").with_file_name("d.rs");
    assert_eq!(with_name.as_path(), Path::new("a/b/d.rs"));
}

#[test]
fn test_path_prefixes_and_suffixes() {
    let p = Path::new("/var/log/syslog");
    assert!(p.starts_with("/var"));
    assert!(p.starts_with("/var/log"));
    assert!(p.starts_with("/var/log/syslog"));
    assert!(!p.starts_with("/va"));
    assert!(!p.starts_with("/log"));

    assert!(p.ends_with("syslog"));
    assert!(p.ends_with("log/syslog"));
    assert!(p.ends_with("/var/log/syslog"));
    assert!(!p.ends_with("log"));

    assert_eq!(p.strip_prefix("/var"), Ok(Path::new("log/syslog")));
    assert_eq!(p.strip_prefix("/var/log"), Ok(Path::new("syslog")));
    assert!(p.strip_prefix("var").is_err());
}

#[test]
fn test_trailing_separator() {
    let dir = Path::new("foo/bar/");
    assert!(dir.has_trailing_sep());
    assert_eq!(dir.trim_trailing_sep(), Path::new("foo/bar"));

    let file = Path::new("foo/bar");
    assert!(!file.has_trailing_sep());
    assert_eq!(file.with_trailing_sep().as_ref(), Path::new("foo/bar/"));
}

#[test]
fn test_path_comparisons_and_hashing() {
    let p1 = Path::new("foo//bar/./baz");
    let p2 = Path::new("foo/bar/baz");
    assert_eq!(p1, p2);
    assert_eq!(hash(&p1), hash(&p2));

    let pb1 = PathBuf::from("foo/bar");
    let pb2 = PathBuf::from("foo//bar");
    assert_eq!(pb1, pb2);
    assert_eq!(hash(&pb1), hash(&pb2));

    assert_eq!(Path::new("foo"), "foo");
    assert_eq!("foo", Path::new("foo"));
    assert_eq!(PathBuf::from("foo"), "foo");
    assert_eq!("foo", PathBuf::from("foo"));

    assert_eq!(*Path::new("foo"), String::from("foo"));
    assert_eq!(String::from("foo"), *Path::new("foo"));
    assert_eq!(PathBuf::from("foo"), String::from("foo"));
    assert_eq!(String::from("foo"), PathBuf::from("foo"));

    assert_eq!(Path::new("foo"), OsStr::new("foo"));
    assert_eq!(OsStr::new("foo"), Path::new("foo"));
    assert_eq!(PathBuf::from("foo"), OsStr::new("foo"));
    assert_eq!(OsStr::new("foo"), PathBuf::from("foo"));

    assert_eq!(Path::new("foo"), OsString::from("foo"));
    assert_eq!(OsString::from("foo"), Path::new("foo"));
    assert_eq!(PathBuf::from("foo"), OsString::from("foo"));
    assert_eq!(OsString::from("foo"), PathBuf::from("foo"));

    assert!(Path::new("a") < Path::new("b"));
    let (pb_a, pb_b) = (PathBuf::from("a"), PathBuf::from("b"));
    assert!(pb_a < pb_b);
}

#[test]
fn test_path_formatting() {
    let p = Path::new("some/path/file.txt");
    assert_eq!(format!("{}", p.display()), "some/path/file.txt");
    assert_eq!(format!("{:?}", p), "\"some/path/file.txt\"");
    assert_eq!(format!("{}", p), "some/path/file.txt");

    let pb = PathBuf::from("some/path/file.txt");
    assert_eq!(format!("{}", pb.display()), "some/path/file.txt");
    assert_eq!(format!("{:?}", pb), "\"some/path/file.txt\"");
    assert_eq!(format!("{}", pb), "some/path/file.txt");
}

#[test]
fn test_separator_helpers() {
    assert!(is_separator('/'));
    assert_eq!(MAIN_SEPARATOR.to_string(), MAIN_SEPARATOR_STR);
    #[cfg(windows)]
    assert!(is_separator('\\'));
}

#[test]
fn test_capacity_and_reserve() {
    let mut pb = PathBuf::with_capacity(32);
    assert!(pb.capacity() >= 32);
    pb.push("initial");
    pb.reserve(64);
    assert!(pb.capacity() >= pb.as_os_str().len() + 64);
    assert!(pb.try_reserve(10).is_ok());
    pb.shrink_to_fit();
    pb.clear();
    assert!(pb.as_path().is_empty());
}
