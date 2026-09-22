use veloq_std::{
    boxed::Box,
    string::ToString,
    sync::{Arc, NativeArc},
    vec,
};

trait Animal: Send + Sync {
    fn speak(&self) -> &'static str;
}

struct Dog {
    name: &'static str,
}

impl Animal for Dog {
    fn speak(&self) -> &'static str {
        "woof"
    }
}

#[test]
fn test_native_arc_basic() {
    let arc = NativeArc::new(42);
    assert_eq!(*arc, 42);
    assert_eq!(NativeArc::strong_count(&arc), 1);
    assert_eq!(NativeArc::weak_count(&arc), 0);

    let clone = NativeArc::clone(&arc);
    assert_eq!(NativeArc::strong_count(&arc), 2);
    assert!(NativeArc::ptr_eq(&arc, &clone));

    drop(clone);
    assert_eq!(NativeArc::strong_count(&arc), 1);
    assert_eq!(NativeArc::try_unwrap(arc), Ok(42));
}

#[test]
fn test_native_arc_cast_unsized_trait() {
    let dog = NativeArc::new(Dog { name: "Buddy" });
    assert_eq!(dog.name, "Buddy");

    let animal: NativeArc<dyn Animal> =
        unsafe { NativeArc::cast_unsized(dog, |p| p as *const dyn Animal) };
    assert_eq!(animal.speak(), "woof");
}

#[test]
fn test_native_arc_new_unsized() {
    let animal: NativeArc<dyn Animal> =
        unsafe { NativeArc::new_unsized(Dog { name: "Max" }, |p| p as *const dyn Animal) };
    assert_eq!(animal.speak(), "woof");
}

#[test]
fn test_native_arc_cast_unsized_slice() {
    let array = NativeArc::new([1, 2, 3, 4]);
    let slice: NativeArc<[i32]> = unsafe { NativeArc::cast_unsized(array, |p| p as *const [i32]) };
    assert_eq!(&*slice, &[1, 2, 3, 4]);
}

#[test]
fn test_native_arc_raw_roundtrip() {
    let arc = NativeArc::new(100);
    let ptr = NativeArc::into_raw(arc);
    let arc = unsafe { NativeArc::from_raw(ptr) };
    assert_eq!(*arc, 100);
}

#[test]
fn test_native_arc_get_mut_and_make_mut() {
    let mut arc = NativeArc::new(10);
    if let Some(val) = NativeArc::get_mut(&mut arc) {
        *val = 20;
    }
    assert_eq!(*arc, 20);

    let clone = NativeArc::clone(&arc);
    let mut arc_to_mutate = arc;
    *NativeArc::make_mut(&mut arc_to_mutate) = 30;
    assert_eq!(*arc_to_mutate, 30);
    assert_eq!(*clone, 20);
}

#[test]
fn test_native_arc_from_conversions() {
    let from_box = NativeArc::from(Box::new(123));
    assert_eq!(*from_box, 123);

    let from_slice = NativeArc::from(&[1, 2, 3][..]);
    assert_eq!(&*from_slice, &[1, 2, 3]);

    let from_str = NativeArc::from("hello");
    assert_eq!(&*from_str, "hello");

    let from_string = NativeArc::from("world".to_string());
    assert_eq!(&*from_string, "world");

    let from_vec = NativeArc::from(vec![4, 5, 6]);
    assert_eq!(&*from_vec, &[4, 5, 6]);
}

#[test]
fn test_arc_alias_cast_unsized() {
    #[cfg(feature = "loom")]
    {
        loom::model(|| {
            let arc = Arc::new(Dog { name: "Rex" });
            let animal: Arc<dyn Animal> =
                unsafe { Arc::cast_unsized(arc, |p| p as *const dyn Animal) };
            assert_eq!(animal.speak(), "woof");
        });
    }
    #[cfg(not(feature = "loom"))]
    {
        let arc = Arc::new(Dog { name: "Rex" });
        let animal: Arc<dyn Animal> = unsafe { Arc::cast_unsized(arc, |p| p as *const dyn Animal) };
        assert_eq!(animal.speak(), "woof");
    }
}

#[cfg(feature = "loom")]
#[test]
fn test_loom_arc_model() {
    use veloq_std::sync::LoomArc;

    loom::model(|| {
        let dog = LoomArc::new(Dog { name: "LoomDog" });
        assert_eq!(LoomArc::strong_count(&dog), 1);

        let animal: LoomArc<dyn Animal> =
            unsafe { LoomArc::cast_unsized(dog, |p| p as *const dyn Animal) };
        assert_eq!(animal.speak(), "woof");
    });
}
