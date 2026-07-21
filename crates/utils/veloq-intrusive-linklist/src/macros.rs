#[macro_export]
macro_rules! offset_of {
    ($Container:path, $field:ident) => {{
        // 使用 MaybeUninit 创建未初始化的实例，模拟 container
        let val = veloq_std::mem::MaybeUninit::<$Container>::uninit();
        let base_ptr = val.as_ptr();
        // 获取字段指针
        // 注意：addr_of! 是 rust 1.51+ 特性，确保不产生引用
        #[allow(unused_unsafe)]
        let field_ptr = unsafe { veloq_std::ptr::addr_of!((*base_ptr).$field) };
        (field_ptr as usize) - (base_ptr as usize)
    }};
}

#[macro_export]
macro_rules! container_of {
    ($ptr:expr, $Container:path, $field:ident) => {{
        let ptr = $ptr as *const _ as *const u8;
        let offset = $crate::offset_of!($Container, $field);
        #[allow(unused_unsafe)]
        unsafe {
            (ptr.sub(offset)) as *const $Container
        }
    }};
}

#[macro_export]
#[doc(hidden)]
macro_rules! __impl_intrusive_adapter {
    // Generic version with lifetime
    ($vis:vis $Adapter:ident < $lt:lifetime, $($gen:ident),+ >, $Node:ty, $link_field:ident, $Trait:ident, $Link:ident $(where $($wh:tt)+)?) => {
        $vis struct $Adapter< $lt, $($gen),+ >(veloq_std::marker::PhantomData<(& $lt (), $($gen),+)>);

        impl< $lt, $($gen),+ > $Adapter< $lt, $($gen),+ > {
            pub const fn new() -> Self {
                Self(veloq_std::marker::PhantomData)
            }
        }

        unsafe impl < $lt, $($gen),+ > $crate::$Trait for $Adapter < $lt, $($gen),+ > $(where $($wh)+)? {
            type Value = $Node;

            #[inline]
            unsafe fn get_link(&self, value: veloq_std::ptr::NonNull<Self::Value>) -> veloq_std::ptr::NonNull<$crate::$Link> {
                let val_ptr = value.as_ptr();
                unsafe {
                    let link_ptr = veloq_std::ptr::addr_of_mut!((*val_ptr).$link_field);
                    veloq_std::ptr::NonNull::new_unchecked(link_ptr)
                }
            }

            #[inline]
            unsafe fn get_value(&self, link: veloq_std::ptr::NonNull<$crate::$Link>) -> veloq_std::ptr::NonNull<Self::Value> {
                let link_ptr = link.as_ptr();
                unsafe {
                    let val_ptr = $crate::container_of!(link_ptr, $Node, $link_field) as *mut $Node;
                    veloq_std::ptr::NonNull::new_unchecked(val_ptr)
                }
            }
        }
    };

    // Generic version
    ($vis:vis $Adapter:ident < $($gen:ident),+ >, $Node:ty, $link_field:ident, $Trait:ident, $Link:ident $(where $($wh:tt)+)?) => {
        $vis struct $Adapter< $($gen),+ >(veloq_std::marker::PhantomData<($($gen),+)>);

        impl< $($gen),+ > $Adapter< $($gen),+ > {
            pub const fn new() -> Self {
                Self(veloq_std::marker::PhantomData)
            }
        }

        unsafe impl < $($gen),+ > $crate::$Trait for $Adapter < $($gen),+ > $(where $($wh)+)? {
            type Value = $Node;

            #[inline]
            unsafe fn get_link(&self, value: veloq_std::ptr::NonNull<Self::Value>) -> veloq_std::ptr::NonNull<$crate::$Link> {
                let val_ptr = value.as_ptr();
                unsafe {
                    let link_ptr = veloq_std::ptr::addr_of_mut!((*val_ptr).$link_field);
                    veloq_std::ptr::NonNull::new_unchecked(link_ptr)
                }
            }

            #[inline]
            unsafe fn get_value(&self, link: veloq_std::ptr::NonNull<$crate::$Link>) -> veloq_std::ptr::NonNull<Self::Value> {
                let link_ptr = link.as_ptr();
                unsafe {
                    let val_ptr = $crate::container_of!(link_ptr, $Node, $link_field) as *mut $Node;
                    veloq_std::ptr::NonNull::new_unchecked(val_ptr)
                }
            }
        }
    };

    // Non-generic version
    ($vis:vis $Adapter:ident, $Node:ty, $link_field:ident, $Trait:ident, $Link:ident) => {
        $vis struct $Adapter;

        unsafe impl $crate::$Trait for $Adapter {
            type Value = $Node;

            #[inline]
            unsafe fn get_link(&self, value: veloq_std::ptr::NonNull<Self::Value>) -> veloq_std::ptr::NonNull<$crate::$Link> {
                let val_ptr = value.as_ptr();
                unsafe {
                    let link_ptr = veloq_std::ptr::addr_of_mut!((*val_ptr).$link_field);
                    veloq_std::ptr::NonNull::new_unchecked(link_ptr)
                }
            }

            #[inline]
            unsafe fn get_value(&self, link: veloq_std::ptr::NonNull<$crate::$Link>) -> veloq_std::ptr::NonNull<Self::Value> {
                let link_ptr = link.as_ptr();
                unsafe {
                    let val_ptr = $crate::container_of!(link_ptr, $Node, $link_field) as *mut $Node;
                    veloq_std::ptr::NonNull::new_unchecked(val_ptr)
                }
            }
        }
    };
}

/// 宏用于自动生成 `Adapter` 实现
///
/// # Example
///
/// ```rust
/// use veloq_intrusive_linklist::{intrusive_adapter, Link};
///
/// pub struct MyNode {
///     link: Link,
///     data: i32,
/// }
///
/// intrusive_adapter!(pub MyAdapter = MyNode { link: Link });
///
/// let adapter = MyAdapter;
/// ```
#[macro_export]
macro_rules! intrusive_adapter {
    // Generic version with lifetime
    ($vis:vis $Adapter:ident < $lt:lifetime, $($gen:ident),+ > = $Node:ty { $link_field:ident : Link } $(where $($wh:tt)+)?) => {
        $crate::__impl_intrusive_adapter!($vis $Adapter < $lt, $($gen),+ >, $Node, $link_field, Adapter, Link $(where $($wh)+)?);
    };

    // Generic version
    ($vis:vis $Adapter:ident < $($gen:ident),+ > = $Node:ty { $link_field:ident : Link } $(where $($wh:tt)+)?) => {
        $crate::__impl_intrusive_adapter!($vis $Adapter < $($gen),+ >, $Node, $link_field, Adapter, Link $(where $($wh)+)?);
    };

    // Non-generic version
    ($vis:vis $Adapter:ident = $Node:ty { $link_field:ident : Link }) => {
        $crate::__impl_intrusive_adapter!($vis $Adapter, $Node, $link_field, Adapter, Link);
    };
}

#[macro_export]
macro_rules! concurrent_intrusive_adapter {
    // Generic version with lifetime
    ($vis:vis $Adapter:ident < $lt:lifetime, $($gen:ident),+ > = $Node:ty { $link_field:ident : ConcurrentLink } $(where $($wh:tt)+)?) => {
        $crate::__impl_intrusive_adapter!($vis $Adapter < $lt, $($gen),+ >, $Node, $link_field, ConcurrentAdapter, ConcurrentLink $(where $($wh)+)?);
    };

    // Generic version
    ($vis:vis $Adapter:ident < $($gen:ident),+ > = $Node:ty { $link_field:ident : ConcurrentLink } $(where $($wh:tt)+)?) => {
        $crate::__impl_intrusive_adapter!($vis $Adapter < $($gen),+ >, $Node, $link_field, ConcurrentAdapter, ConcurrentLink $(where $($wh)+)?);
    };

    // Non-generic version
    ($vis:vis $Adapter:ident = $Node:ty { $link_field:ident : ConcurrentLink }) => {
        $crate::__impl_intrusive_adapter!($vis $Adapter, $Node, $link_field, ConcurrentAdapter, ConcurrentLink);
    };
}

#[cfg(test)]
mod tests {
    #[repr(C)]
    struct TestStruct {
        a: u8,
        b: u32,
        c: u64,
    }

    #[test]
    fn test_offset_of() {
        let offset_a = offset_of!(TestStruct, a);
        let offset_b = offset_of!(TestStruct, b);
        let offset_c = offset_of!(TestStruct, c);

        assert_eq!(offset_a, 0);
        // padding 3 bytes between a and b
        assert_eq!(offset_b, 4);
        // padding 0 bytes if u32 is 4 aligned? u64 is 8 aligned.
        // b is 4..8. c starts at 8.
        assert_eq!(offset_c, 8);
    }

    #[test]
    fn test_container_of() {
        let val = TestStruct { a: 1, b: 2, c: 3 };
        let ptr_b = &val.b as *const u32;

        unsafe {
            let ptr_struct = container_of!(ptr_b, TestStruct, b);
            assert_eq!(&(*ptr_struct).a as *const _, &val.a as *const _);
            assert_eq!((*ptr_struct).c, 3);
        }
    }
}
