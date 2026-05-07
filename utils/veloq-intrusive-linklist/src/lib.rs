mod cursor;
mod list;
mod macros;

use std::ptr::NonNull;
use veloq_shim::cell::UnsafeCell;

pub use cursor::Cursor;
pub use cursor::CursorMut;
pub use list::LinkedList;

/// 适配器 Trait，用于定义对象与 Link 之间的映射关系
///
/// # Safety
/// 实现者必须保证 get_link 和 get_value 的转换是正确且内存安全的。
pub unsafe trait Adapter {
    /// 链表存储的数据类型 (例如 WaiterNode)
    type Value: ?Sized;

    /// 给定数据指针，返回该数据中 Link 字段的指针
    /// # Safety
    /// Caller must ensure values are valid.
    unsafe fn get_link(&self, value: NonNull<Self::Value>) -> NonNull<Link>;

    /// 给定 Link 指针，返回包含该 Link 的数据指针
    /// # Safety
    /// Caller must ensure links are valid.
    unsafe fn get_value(&self, link: NonNull<Link>) -> NonNull<Self::Value>;
}

/// 侵入式链表的链接节点，必须嵌入在数据结构中使用。
pub struct Link {
    // 使用 UnsafeCell 允许在只有 &Link 引用时修改指针（通常配合外层锁使用）
    pub(crate) next: UnsafeCell<Option<NonNull<Link>>>,
    pub(crate) prev: UnsafeCell<Option<NonNull<Link>>>,
    pub(crate) linked: UnsafeCell<bool>,
}

impl Link {
    #[cfg(not(feature = "loom"))]
    pub const fn new() -> Self {
        Self {
            next: UnsafeCell::new(None),
            prev: UnsafeCell::new(None),
            linked: UnsafeCell::new(false),
        }
    }

    #[cfg(feature = "loom")]
    pub fn new() -> Self {
        Self {
            next: UnsafeCell::new(None),
            prev: UnsafeCell::new(None),
            linked: UnsafeCell::new(false),
        }
    }

    /// 检查节点是否链接在某个列表中。
    #[inline]
    pub fn is_linked(&self) -> bool {
        unsafe { self.linked.with(|l| *l) }
    }

    /// 强制断开连接（unsafe，需确保已从列表中移除）
    #[inline]
    pub(crate) unsafe fn unsafe_unlink(&self) {
        unsafe {
            self.next.with_mut(|n| *n = None);
            self.prev.with_mut(|p| *p = None);
            self.linked.with_mut(|l| *l = false);
        }
    }
}

impl Drop for Link {
    fn drop(&mut self) {
        if self.is_linked() && !std::thread::panicking() {
            panic!("dropped a node that is still linked");
        }
    }
}

// 默认实现 Default
impl Default for Link {
    fn default() -> Self {
        Self::new()
    }
}

impl core::fmt::Debug for Link {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Link")
            .field("linked", &self.is_linked())
            .finish()
    }
}

unsafe impl Send for Link {}
unsafe impl Sync for Link {}
