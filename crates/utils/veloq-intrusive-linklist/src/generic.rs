use core::{
    fmt,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
};

/// 抽象链表节点的指针操作和链接状态
pub trait LinkNode: Sized {
    fn is_linked(&self) -> bool;
    /// # Safety
    /// Caller must ensure safety.
    unsafe fn set_linked(&self, val: bool);
    /// # Safety
    /// Caller must ensure pointers are valid.
    unsafe fn get_next(&self) -> Option<NonNull<Self>>;
    /// # Safety
    /// Caller must ensure pointers are valid.
    unsafe fn set_next(&self, next: Option<NonNull<Self>>);
    /// # Safety
    /// Caller must ensure pointers are valid.
    unsafe fn get_prev(&self) -> Option<NonNull<Self>>;
    /// # Safety
    /// Caller must ensure pointers are valid.
    unsafe fn set_prev(&self, prev: Option<NonNull<Self>>);
    /// # Safety
    /// Caller must ensure links are valid.
    unsafe fn unsafe_unlink(&self);
}

/// 通用适配器 Trait，定义值与通用 LinkNode 之间的映射
///
/// # Safety
/// 实现者必须保证 get_link 和 get_value 的转换是正确且内存安全的。
pub unsafe trait GenericAdapter<L: LinkNode> {
    /// 链表存储的数据类型
    type Value: ?Sized;
    /// 给定数据指针，返回该数据中 Link 字段的指针
    /// # Safety
    /// Caller must ensure values are valid.
    unsafe fn get_link(&self, value: NonNull<Self::Value>) -> NonNull<L>;
    /// 给定 Link 指针，返回包含该 Link 的数据指针
    /// # Safety
    /// Caller must ensure links are valid.
    unsafe fn get_value(&self, link: NonNull<L>) -> NonNull<Self::Value>;
}

/// 通用双向链表实现
pub struct GenericLinkedList<A: GenericAdapter<L>, L: LinkNode> {
    pub(crate) head: Option<NonNull<L>>,
    pub(crate) tail: Option<NonNull<L>>,
    pub(crate) adapter: A,
    pub(crate) len: usize,
    marker: PhantomData<Box<A::Value>>,
}

unsafe impl<A: GenericAdapter<L>, L: LinkNode> Send for GenericLinkedList<A, L> where A::Value: Send {}
unsafe impl<A: GenericAdapter<L>, L: LinkNode> Sync for GenericLinkedList<A, L> where A::Value: Sync {}

impl<A: GenericAdapter<L>, L: LinkNode> GenericLinkedList<A, L> {
    #[inline]
    pub const fn new(adapter: A) -> Self {
        Self {
            head: None,
            tail: None,
            adapter,
            len: 0,
            marker: PhantomData,
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.is_none()
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// 将节点添加到尾部
    ///
    /// # Safety
    /// 必须保证 value 在链表中存在期间有效。
    #[inline]
    pub unsafe fn push_back(&mut self, value: Pin<&mut A::Value>) {
        unsafe {
            let raw_val = value.get_unchecked_mut();
            let link_ptr = self.adapter.get_link(NonNull::from(raw_val));
            let link = link_ptr.as_ref();

            if link.is_linked() {
                panic!("Node is already linked");
            }

            let old_tail = self.tail;
            link.set_next(None);
            link.set_prev(old_tail);
            link.set_linked(true);

            if let Some(tail) = old_tail {
                tail.as_ref().set_next(Some(link_ptr));
            } else {
                self.head = Some(link_ptr);
            }

            self.tail = Some(link_ptr);
            self.len += 1;
        }
    }

    /// 将节点添加到头部
    ///
    /// # Safety
    /// 必须保证 value 在链表中存在期间有效。
    #[inline]
    pub unsafe fn push_front(&mut self, value: Pin<&mut A::Value>) {
        unsafe {
            let raw_val = value.get_unchecked_mut();
            let link_ptr = self.adapter.get_link(NonNull::from(raw_val));
            let link = link_ptr.as_ref();

            if link.is_linked() {
                panic!("Node is already linked");
            }

            let old_head = self.head;
            link.set_prev(None);
            link.set_next(old_head);
            link.set_linked(true);

            if let Some(head) = old_head {
                head.as_ref().set_prev(Some(link_ptr));
            } else {
                self.tail = Some(link_ptr);
            }

            self.head = Some(link_ptr);
            self.len += 1;
        }
    }

    /// 从头部移除节点
    #[inline]
    pub fn pop_front(&mut self) -> Option<Pin<&mut A::Value>> {
        unsafe {
            let head = self.head?;
            let head_link = head.as_ref();
            let next = head_link.get_next();

            if let Some(next_ptr) = next {
                next_ptr.as_ref().set_prev(None);
            } else {
                self.tail = None;
            }

            self.head = next;
            self.len -= 1;

            head_link.unsafe_unlink();

            let val_ptr = self.adapter.get_value(head);
            Some(Pin::new_unchecked(&mut *val_ptr.as_ptr()))
        }
    }

    /// 获取头部 Cursor (只读)
    #[inline]
    pub fn front(&self) -> GenericCursor<'_, A, L> {
        GenericCursor::new(self, self.head)
    }

    /// 获取头部 Cursor (可变)
    #[inline]
    pub fn front_mut(&mut self) -> GenericCursorMut<'_, A, L> {
        GenericCursorMut::new(self, self.head)
    }

    /// 根据数据指针创建 Cursor，用于 O(1) 移除。
    ///
    /// # Safety
    /// ptr 必须指向链表中的有效节点。
    #[inline]
    pub unsafe fn cursor_mut_from_ptr(
        &mut self,
        ptr: NonNull<A::Value>,
    ) -> GenericCursorMut<'_, A, L> {
        let link = unsafe { self.adapter.get_link(ptr) };
        GenericCursorMut::new(self, Some(link))
    }

    /// 将节点添加到尾部，并返回一个 ScopedGuard。
    ///
    /// 当 Guard 离开作用域时，节点会自动从链表中移除。
    ///
    /// # Safety
    /// 必须保证 value 在 Guard 存活期间有效。
    #[inline]
    pub unsafe fn push_back_scoped<'a>(
        &'a mut self,
        mut value: Pin<&mut A::Value>,
    ) -> GenericRemoveOnDrop<'a, A, L> {
        unsafe {
            let node_ptr = NonNull::from(value.as_mut().get_unchecked_mut());
            self.push_back(value);
            GenericRemoveOnDrop {
                list: self,
                node_ptr,
            }
        }
    }
}

impl<A: GenericAdapter<L>, L: LinkNode> Drop for GenericLinkedList<A, L> {
    fn drop(&mut self) {
        let mut current = self.head;
        self.head = None;
        self.tail = None;
        self.len = 0;

        while let Some(link_ptr) = current {
            unsafe {
                let link = link_ptr.as_ref();
                let next = link.get_next();
                link.unsafe_unlink();
                current = next;
            }
        }
    }
}

impl<A: GenericAdapter<L>, L: LinkNode> fmt::Debug for GenericLinkedList<A, L> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GenericLinkedList")
            .field("len", &self.len)
            .finish()
    }
}

/// 自动移除守卫
pub struct GenericRemoveOnDrop<'a, A: GenericAdapter<L>, L: LinkNode> {
    list: &'a mut GenericLinkedList<A, L>,
    node_ptr: NonNull<A::Value>,
}

impl<'a, A: GenericAdapter<L>, L: LinkNode> Drop for GenericRemoveOnDrop<'a, A, L> {
    fn drop(&mut self) {
        unsafe {
            let link_ptr = self.list.adapter.get_link(self.node_ptr);
            if link_ptr.as_ref().is_linked() {
                let mut cursor = self.list.cursor_mut_from_ptr(self.node_ptr);
                cursor.remove();
            }
        }
    }
}

impl<'a, A: GenericAdapter<L>, L: LinkNode> Deref for GenericRemoveOnDrop<'a, A, L> {
    type Target = GenericLinkedList<A, L>;

    fn deref(&self) -> &Self::Target {
        self.list
    }
}

impl<'a, A: GenericAdapter<L>, L: LinkNode> DerefMut for GenericRemoveOnDrop<'a, A, L> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.list
    }
}

/// 通用只读游标
pub struct GenericCursor<'a, A: GenericAdapter<L>, L: LinkNode> {
    list: &'a GenericLinkedList<A, L>,
    current: Option<NonNull<L>>,
}

impl<'a, A: GenericAdapter<L>, L: LinkNode> GenericCursor<'a, A, L> {
    #[inline]
    pub(crate) fn new(list: &'a GenericLinkedList<A, L>, current: Option<NonNull<L>>) -> Self {
        Self { list, current }
    }

    /// 获取当前指向元素的原始指针
    #[inline]
    pub fn get_raw(&self) -> Option<NonNull<A::Value>> {
        self.current
            .map(|link| unsafe { self.list.adapter.get_value(link) })
    }

    /// 获取当前指向的元素引用
    #[inline]
    pub fn get(&self) -> Option<&A::Value> {
        self.current.map(|link| unsafe {
            let value_ptr = self.list.adapter.get_value(link);
            &*value_ptr.as_ptr()
        })
    }

    /// 移动到下一个
    #[inline]
    pub fn move_next(&mut self) {
        if let Some(curr) = self.current {
            unsafe {
                self.current = curr.as_ref().get_next();
            }
        } else {
            self.current = None;
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.current.is_none()
    }
}

/// 通用可变游标
pub struct GenericCursorMut<'a, A: GenericAdapter<L>, L: LinkNode> {
    list: &'a mut GenericLinkedList<A, L>,
    current: Option<NonNull<L>>,
}

impl<'a, A: GenericAdapter<L>, L: LinkNode> GenericCursorMut<'a, A, L> {
    #[inline]
    pub(crate) fn new(list: &'a mut GenericLinkedList<A, L>, current: Option<NonNull<L>>) -> Self {
        Self { list, current }
    }

    /// 获取当前指向元素的原始指针
    #[inline]
    pub fn get_raw(&self) -> Option<NonNull<A::Value>> {
        self.current
            .map(|link| unsafe { self.list.adapter.get_value(link) })
    }

    /// 获取当前指向的元素引用
    #[inline]
    pub fn get(&self) -> Option<&A::Value> {
        self.current.map(|link| unsafe {
            let value_ptr = self.list.adapter.get_value(link);
            &*value_ptr.as_ptr()
        })
    }

    /// 获取当前指向的元素可变引用（Pinned）
    #[inline]
    pub fn get_mut(&mut self) -> Option<Pin<&mut A::Value>> {
        self.current.map(|link| unsafe {
            let value_ptr = self.list.adapter.get_value(link);
            Pin::new_unchecked(&mut *value_ptr.as_ptr())
        })
    }

    /// 移除当前指向的元素，并将游标移动到下一个元素。
    /// 返回被移除的元素。
    #[inline]
    pub fn remove(&mut self) -> Option<Pin<&mut A::Value>> {
        let current_link_ptr = self.current?;

        unsafe {
            let current_link = current_link_ptr.as_ref();
            let prev = current_link.get_prev();
            let next = current_link.get_next();

            if let Some(prev_ptr) = prev {
                prev_ptr.as_ref().set_next(next);
            } else {
                self.list.head = next;
            }

            if let Some(next_ptr) = next {
                next_ptr.as_ref().set_prev(prev);
            } else {
                self.list.tail = prev;
            }

            self.list.len -= 1;
            self.current = next;

            current_link.unsafe_unlink();

            let val_ptr = self.list.adapter.get_value(current_link_ptr);
            Some(Pin::new_unchecked(&mut *val_ptr.as_ptr()))
        }
    }

    /// 移动到下一个
    #[inline]
    pub fn move_next(&mut self) {
        if let Some(curr) = self.current {
            unsafe {
                self.current = curr.as_ref().get_next();
            }
        } else {
            self.current = None;
        }
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.current.is_none()
    }
}
