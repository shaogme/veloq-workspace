#[macro_export]
macro_rules! offset_of {
    ($Container:path, $field:ident) => {{
        // 使用 MaybeUninit 创建未初始化的实例，模拟 container
        let val = core::mem::MaybeUninit::<$Container>::uninit();
        let base_ptr = val.as_ptr();
        // 获取字段指针
        // 注意：addr_of! 是 rust 1.51+ 特性，确保不产生引用
        #[allow(unused_unsafe)]
        let field_ptr = unsafe { core::ptr::addr_of!((*base_ptr).$field) };
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
/// fn main() {
///     let adapter = MyAdapter;
/// }
/// ```
#[macro_export]
macro_rules! intrusive_adapter {
    ($vis:vis $Adapter:ident = $Node:ty { $link_field:ident : Link }) => {
        $vis struct $Adapter;

        unsafe impl $crate::Adapter for $Adapter {
            type Value = $Node;

            #[inline]
            unsafe fn get_link(&self, value: core::ptr::NonNull<Self::Value>) -> core::ptr::NonNull<$crate::Link> {
                let val_ptr = value.as_ptr();
                // SAFETY: 调用者必须保证 value 指针有效且未被借用为 &mut（除非这里就在做转换）
                // addr_of_mut! 不会在 stable 1.51 之前存在，但它是生成裸指针的安全方式
                unsafe {
                    let link_ptr = core::ptr::addr_of_mut!((*val_ptr).$link_field);
                    core::ptr::NonNull::new_unchecked(link_ptr)
                }
            }

            #[inline]
            unsafe fn get_value(&self, link: core::ptr::NonNull<$crate::Link>) -> core::ptr::NonNull<Self::Value> {
                let link_ptr = link.as_ptr();
                // container_of 返回 *const T
                unsafe {
                    let val_ptr = $crate::container_of!(link_ptr, $Node, $link_field) as *mut $Node;
                    core::ptr::NonNull::new_unchecked(val_ptr)
                }
            }
        }
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
