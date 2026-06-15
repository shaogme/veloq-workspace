use crate::{Adapter, Link, list::LinkedList};
use core::{pin::Pin, ptr::NonNull};

pub struct Cursor<'a, A: Adapter> {
    list: &'a LinkedList<A>,
    current: Option<NonNull<Link>>, // 当前指向的 Link
}

impl<'a, A: Adapter> Cursor<'a, A> {
    #[inline]
    pub(crate) fn new(list: &'a LinkedList<A>, current: Option<NonNull<Link>>) -> Self {
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

    // 移动到下一个
    #[inline]
    pub fn move_next(&mut self) {
        if let Some(curr) = self.current {
            unsafe {
                self.current = curr.as_ref().next.with(|n| *n);
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

pub struct CursorMut<'a, A: Adapter> {
    list: &'a mut LinkedList<A>,
    current: Option<NonNull<Link>>, // 当前指向的 Link
}

impl<'a, A: Adapter> CursorMut<'a, A> {
    #[inline]
    pub(crate) fn new(list: &'a mut LinkedList<A>, current: Option<NonNull<Link>>) -> Self {
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
    ///
    /// 返回 Pin<&mut T> 以防止节点在内存中被移动，这对侵入式链表至关重要。
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
            let prev = current_link.prev.with(|p| *p);
            let next = current_link.next.with(|n| *n);

            // 更新 prev 的 next
            if let Some(prev_ptr) = prev {
                let prev_link = prev_ptr.as_ref();
                prev_link.next.with_mut(|n| *n = next);
            } else {
                // 如果没有 prev，说明是 head
                self.list.head = next;
            }

            // 更新 next 的 prev
            if let Some(next_ptr) = next {
                let next_link = next_ptr.as_ref();
                next_link.prev.with_mut(|p| *p = prev);
            } else {
                // 如果没有 next，说明是 tail
                self.list.tail = prev;
            }

            self.list.len -= 1;

            // 移动 cursor 到下一个
            self.current = next;

            // 清理被移除节点的连接状态
            current_link.unsafe_unlink();

            let val_ptr = self.list.adapter.get_value(current_link_ptr);
            Some(Pin::new_unchecked(&mut *val_ptr.as_ptr()))
        }
    }

    // 移动到下一个
    #[inline]
    pub fn move_next(&mut self) {
        if let Some(curr) = self.current {
            unsafe {
                self.current = curr.as_ref().next.with(|n| *n);
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
    fn test_cursor_traversal() {
        let mut list = LinkedList::new(TestAdapter);
        let mut node1 = Box::pin(TestNode {
            val: 1,
            link: Link::new(),
        });
        let mut node2 = Box::pin(TestNode {
            val: 2,
            link: Link::new(),
        });
        let mut node3 = Box::pin(TestNode {
            val: 3,
            link: Link::new(),
        });

        unsafe {
            list.push_back(node1.as_mut());
            list.push_back(node2.as_mut());
            list.push_back(node3.as_mut());
        }

        let mut cursor = list.front_mut();

        assert_eq!(cursor.get().unwrap().val, 1);
        cursor.move_next();
        assert_eq!(cursor.get().unwrap().val, 2);
        cursor.move_next();
        assert_eq!(cursor.get().unwrap().val, 3);
        cursor.move_next();
        assert!(cursor.get().is_none());
        assert!(cursor.is_null());

        // Cleanup
        // In this test with stack pinned boxes, we rely on Drop of list to unlink,
        // and stack unwinding to drop nodes.
        // But popping check is fine.
        while let Some(popped) = list.pop_front() {
            // Just verifying it pops.
            let _ = popped;
        }
    }

    #[test]
    fn test_cursor_remove() {
        let mut list = LinkedList::new(TestAdapter);
        let mut node1 = Box::pin(TestNode {
            val: 1,
            link: Link::new(),
        });
        let mut node2 = Box::pin(TestNode {
            val: 2,
            link: Link::new(),
        });
        let mut node3 = Box::pin(TestNode {
            val: 3,
            link: Link::new(),
        });

        unsafe {
            list.push_back(node1.as_mut());
            list.push_back(node2.as_mut());
            list.push_back(node3.as_mut());
        }

        let mut cursor = list.front_mut();
        // Pointing at 1
        cursor.move_next();
        // Pointing at 2

        let removed = cursor.remove().unwrap();
        assert_eq!(removed.val, 2);

        // Cursor should now point to 3 (next element)
        // Cursor should now point to 3 (next element)
        // Cursor should now point to 3 (next element)
        assert_eq!(cursor.get().unwrap().val, 3);

        // Remove 3
        let removed3 = cursor.remove().unwrap();
        assert_eq!(removed3.val, 3);

        // Cursor null
        assert!(cursor.get().is_none());

        // List should have 1 left
        assert_eq!(list.len(), 1);
        let head = list.pop_front().unwrap();
        assert_eq!(head.val, 1);
    }

    #[test]
    fn test_readonly_cursor_traversal() {
        let mut list = LinkedList::new(TestAdapter);
        let mut node1 = Box::pin(TestNode {
            val: 10,
            link: Link::new(),
        });
        let mut node2 = Box::pin(TestNode {
            val: 20,
            link: Link::new(),
        });

        unsafe {
            list.push_back(node1.as_mut());
            list.push_back(node2.as_mut());
        }

        // Use read-only cursor
        let mut cursor = list.front();

        assert_eq!(cursor.get().unwrap().val, 10);
        cursor.move_next();
        assert_eq!(cursor.get().unwrap().val, 20);
        cursor.move_next();
        assert!(cursor.get().is_none());
        assert!(cursor.is_null());

        // List is not modified
        assert_eq!(list.len(), 2);

        // Cleanup: remove nodes from list before they are dropped
        while list.pop_front().is_some() {}
    }
}
