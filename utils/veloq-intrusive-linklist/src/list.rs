use crate::{
    Adapter, Link,
    cursor::{Cursor, CursorMut},
};
use core::{
    fmt,
    marker::PhantomData,
    ops::{Deref, DerefMut},
    pin::Pin,
    ptr::NonNull,
};

pub struct LinkedList<A: Adapter> {
    pub(crate) head: Option<NonNull<Link>>,
    pub(crate) tail: Option<NonNull<Link>>,
    pub(crate) adapter: A,
    pub(crate) len: usize,
    marker: PhantomData<Box<A::Value>>,
}

// 确保 Send/Sync 语义正确，取决于 Value 是否 Send/Sync
unsafe impl<A: Adapter> Send for LinkedList<A> where A::Value: Send {}
unsafe impl<A: Adapter> Sync for LinkedList<A> where A::Value: Sync {}

impl<A: Adapter> LinkedList<A> {
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
            // Unsafe: get_unchecked_mut is required to get a raw pointer for the adapter.
            // We guarantee we do not move out of this reference.
            let raw_val = value.get_unchecked_mut();
            let link_ptr = self.adapter.get_link(NonNull::from(raw_val));
            let link = link_ptr.as_ref();

            if link.is_linked() {
                panic!("Node is already linked");
            }

            let old_tail = self.tail;
            link.next.with_mut(|n| *n = None);
            link.prev.with_mut(|p| *p = old_tail);
            link.linked.with_mut(|l| *l = true);

            if let Some(tail) = old_tail {
                let tail_link = tail.as_ref();
                tail_link.next.with_mut(|n| *n = Some(link_ptr));
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
            link.prev.with_mut(|p| *p = None);
            link.next.with_mut(|n| *n = old_head);
            link.linked.with_mut(|l| *l = true);

            if let Some(head) = old_head {
                let head_link = head.as_ref();
                head_link.prev.with_mut(|p| *p = Some(link_ptr));
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
            // head 是 NonNull<Link>
            let head_link = head.as_ref();
            let next = head_link.next.with(|n| *n);

            if let Some(next_ptr) = next {
                let next_link = next_ptr.as_ref();
                next_link.prev.with_mut(|p| *p = None);
            } else {
                self.tail = None;
            }

            self.head = next;
            self.len -= 1;

            // 清理取出的 link 状态
            head_link.unsafe_unlink();

            let val_ptr = self.adapter.get_value(head);
            // Unsafe: We trust the adapter gives us a valid pointer to the object.
            // We return Pin because the object was pinned when inserted (guaranteed by API).
            Some(Pin::new_unchecked(&mut *val_ptr.as_ptr()))
        }
    }

    /// 获取头部 Cursor (只读)
    #[inline]
    pub fn front(&self) -> Cursor<'_, A> {
        let head = self.head;
        Cursor::new(self, head)
    }

    /// 获取头部 Cursor (可变)
    #[inline]
    pub fn front_mut(&mut self) -> CursorMut<'_, A> {
        let head = self.head;
        CursorMut::new(self, head)
    }

    /// 根据数据指针创建 Cursor，用于 O(1) 移除。
    ///
    /// # Safety
    /// ptr 必须指向链表中的有效节点。
    #[inline]
    pub unsafe fn cursor_mut_from_ptr(&mut self, ptr: NonNull<A::Value>) -> CursorMut<'_, A> {
        let link = unsafe { self.adapter.get_link(ptr) };
        CursorMut::new(self, Some(link))
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
    ) -> RemoveOnDrop<'a, A> {
        unsafe {
            let node_ptr = NonNull::from(value.as_mut().get_unchecked_mut());
            // 先 push
            self.push_back(value);
            RemoveOnDrop {
                list: self,
                node_ptr,
            }
        }
    }
}

pub struct RemoveOnDrop<'a, A: Adapter> {
    list: &'a mut LinkedList<A>,
    node_ptr: NonNull<A::Value>,
}

impl<'a, A: Adapter> Drop for RemoveOnDrop<'a, A> {
    fn drop(&mut self) {
        unsafe {
            // 安全性：
            // node_ptr 来自于创建 Guard 时传入的引用，由于 Guard 持有 list 的可变引用，
            // 且 Guard 的生命周期受限于 list，所以此时 list 有效。
            // 我们需要检查节点是否仍然链接在链表中。
            let link_ptr = self.list.adapter.get_link(self.node_ptr);
            if link_ptr.as_ref().is_linked() {
                let mut cursor = self.list.cursor_mut_from_ptr(self.node_ptr);
                cursor.remove();
            }
        }
    }
}

impl<'a, A: Adapter> Deref for RemoveOnDrop<'a, A> {
    type Target = LinkedList<A>;

    fn deref(&self) -> &Self::Target {
        self.list
    }
}

impl<'a, A: Adapter> DerefMut for RemoveOnDrop<'a, A> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.list
    }
}

impl<A: Adapter> Drop for LinkedList<A> {
    fn drop(&mut self) {
        // 清理链表防止 Link 数据残留
        let mut current = self.head;
        self.head = None;
        self.tail = None;
        self.len = 0;

        while let Some(link_ptr) = current {
            unsafe {
                let link = link_ptr.as_ref();
                // 必须在 unlink 之前获取 next，因为 unlink 会清除 next
                let next = link.next.with(|n| *n);
                link.unsafe_unlink();
                current = next;
            }
        }
    }
}

impl<A: Adapter> fmt::Debug for LinkedList<A> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LinkedList")
            .field("len", &self.len)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::boxed::Box;

    struct TestNode {
        val: i32,
        link: Link,
    }

    crate::intrusive_adapter!(TestAdapter = TestNode { link: Link });

    #[test]
    fn test_push_pop() {
        let mut list = LinkedList::new(TestAdapter);
        assert!(list.is_empty());

        let mut node1 = Box::pin(TestNode {
            val: 1,
            link: Link::new(),
        });
        let mut node2 = Box::pin(TestNode {
            val: 2,
            link: Link::new(),
        });

        unsafe {
            list.push_back(node1.as_mut());
            list.push_back(node2.as_mut());
        }

        assert_eq!(list.len(), 2);
        assert!(!list.is_empty());

        let popped1 = list.pop_front().unwrap();
        assert_eq!(popped1.val, 1);

        assert_eq!(list.len(), 1);

        let popped2 = list.pop_front().unwrap();
        assert_eq!(popped2.val, 2);

        assert!(list.is_empty());
        assert!(list.pop_front().is_none());
    }

    #[test]
    fn test_drop_cleans_links() {
        // Test that dropping the list unlinks existing nodes, allowing them to be relinked or dropped safely?
        // Actually, drop calls pop_front which calls unsafe_unlink.
        // We should verify that the nodes are still valid but unlinked.

        let mut list = LinkedList::new(TestAdapter);
        let mut node = Box::pin(TestNode {
            val: 10,
            link: Link::new(),
        });

        unsafe {
            list.push_back(node.as_mut());
        }
        drop(list);

        // Node is automatically dropped when `node` variable goes out of scope here.
        // We just need to verify linkage integrity via unsafe inspection if needed,
        // but with Box::pin we are limited in accessing internal link state if we don't hold ref.
        // However, node is still alive here.
        assert!(!node.link.is_linked());
    }

    #[test]
    fn test_scoped_push() {
        let mut list = LinkedList::new(TestAdapter);
        let mut node = Box::pin(TestNode {
            val: 42,
            link: Link::new(),
        });

        {
            let _guard = unsafe { list.push_back_scoped(node.as_mut()) };
            assert_eq!(_guard.len(), 1);
            assert!(node.link.is_linked());
        } // _guard dropped here, should remove node

        assert_eq!(list.len(), 0);
        assert!(!node.link.is_linked());

        // Ensure we can push it again (it was cleanly removed)
        unsafe { list.push_back(node.as_mut()) };
        assert_eq!(list.len(), 1);
        list.pop_front();
    }
}
