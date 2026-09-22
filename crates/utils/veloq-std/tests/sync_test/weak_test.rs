use veloq_std::sync::{NativeArc, NativeWeak};

trait Animal: Send + Sync {
    fn speak(&self) -> &'static str;
}

struct Dog {
    name: &'static str,
}

impl Animal for Dog {
    fn speak(&self) -> &'static str {
        let _ = self.name;
        "woof"
    }
}

#[test]
fn test_native_weak_basic() {
    let arc = NativeArc::new(42);
    let weak = NativeArc::downgrade(&arc);

    assert_eq!(NativeWeak::strong_count(&weak), 1);
    assert_eq!(NativeWeak::weak_count(&weak), 1);

    let upgraded = weak.upgrade().expect("should upgrade");
    assert_eq!(*upgraded, 42);
    assert_eq!(NativeArc::strong_count(&upgraded), 2);

    drop(upgraded);
    drop(arc);

    assert_eq!(weak.upgrade(), None);
    assert_eq!(NativeWeak::strong_count(&weak), 0);
}

#[test]
fn test_native_weak_new() {
    let weak: NativeWeak<i32> = NativeWeak::new();
    assert_eq!(weak.upgrade(), None);
    assert_eq!(NativeWeak::strong_count(&weak), 0);
    assert_eq!(NativeWeak::weak_count(&weak), 0);

    let default_weak: NativeWeak<i32> = Default::default();
    assert_eq!(default_weak.upgrade(), None);
}

#[test]
fn test_native_weak_cast_unsized() {
    let dog_arc = NativeArc::new(Dog { name: "Rex" });
    let weak = NativeArc::downgrade(&dog_arc);

    let weak_animal: NativeWeak<dyn Animal> =
        unsafe { NativeWeak::cast_unsized(weak, |p| p as *const dyn Animal) };

    let upgraded = weak_animal.upgrade().expect("should upgrade trait object");
    assert_eq!(upgraded.speak(), "woof");

    drop(upgraded);
    drop(dog_arc);
    assert!(weak_animal.upgrade().is_none());
}

#[test]
fn test_native_weak_raw_roundtrip() {
    let arc = NativeArc::new(99);
    let weak = NativeArc::downgrade(&arc);

    let raw = NativeWeak::into_raw(weak);
    let weak = unsafe { NativeWeak::from_raw(raw) };

    let upgraded = weak.upgrade().expect("should upgrade");
    assert_eq!(*upgraded, 99);
}

#[test]
fn test_native_weak_ptr_eq() {
    let arc1 = NativeArc::new(10);
    let arc2 = NativeArc::new(10);

    let weak1_a = NativeArc::downgrade(&arc1);
    let weak1_b = NativeArc::downgrade(&arc1);
    let weak2 = NativeArc::downgrade(&arc2);

    assert!(NativeWeak::ptr_eq(&weak1_a, &weak1_b));
    assert!(!NativeWeak::ptr_eq(&weak1_a, &weak2));
}

#[test]
fn test_weak_alias_basic() {
    use veloq_std::sync::{Arc, Weak};

    let _empty: Weak<i32> = Weak::new();

    #[cfg(feature = "loom")]
    loom::model(|| {
        let arc = Arc::new(100);
        let weak = Arc::downgrade(&arc);
        assert_eq!(weak.upgrade().as_deref().copied(), Some(100));
        drop(arc);
        assert!(weak.upgrade().is_none());
    });

    #[cfg(not(feature = "loom"))]
    {
        let arc = Arc::new(100);
        let weak = Arc::downgrade(&arc);
        assert_eq!(weak.upgrade().as_deref().copied(), Some(100));
        drop(arc);
        assert!(weak.upgrade().is_none());
    }
}

#[cfg(feature = "loom")]
#[test]
fn test_loom_weak_model() {
    use veloq_std::sync::{LoomArc, LoomWeak};

    loom::model(|| {
        let arc = LoomArc::new(42);
        let weak = LoomArc::downgrade(&arc);

        assert_eq!(LoomWeak::strong_count(&weak), 1);
        assert_eq!(LoomWeak::weak_count(&weak), 1);

        let upgraded = weak.upgrade().expect("should upgrade");
        assert_eq!(*upgraded, 42);
        assert_eq!(LoomArc::strong_count(&upgraded), 2);

        drop(upgraded);
        drop(arc);

        assert_eq!(weak.upgrade(), None);
        assert_eq!(LoomWeak::strong_count(&weak), 0);
    });
}

#[cfg(feature = "loom")]
#[test]
fn test_loom_weak_cast_unsized() {
    use veloq_std::sync::{LoomArc, LoomWeak};

    loom::model(|| {
        let dog_arc = LoomArc::new(Dog { name: "LoomRex" });
        let weak = LoomArc::downgrade(&dog_arc);

        let weak_animal: LoomWeak<dyn Animal> =
            unsafe { LoomWeak::cast_unsized(weak, |p| p as *const dyn Animal) };

        let upgraded = weak_animal.upgrade().expect("should upgrade trait object");
        assert_eq!(upgraded.speak(), "woof");

        drop(upgraded);
        drop(dog_arc);
        assert!(weak_animal.upgrade().is_none());
    });
}

#[cfg(feature = "loom")]
#[test]
fn test_loom_weak_concurrent_upgrade_and_drop() {
    use veloq_std::sync::LoomArc;

    loom::model(|| {
        let arc = LoomArc::new(1234);
        let weak = LoomArc::downgrade(&arc);

        let t1 = loom::thread::spawn(move || {
            drop(arc);
        });

        let t2 = loom::thread::spawn(move || {
            if let Some(upgraded) = weak.upgrade() {
                assert_eq!(*upgraded, 1234);
            }
        });

        t1.join().unwrap();
        t2.join().unwrap();
    });
}
