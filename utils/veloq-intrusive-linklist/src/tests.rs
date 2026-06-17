use crate::{Link, LinkedList};
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
    let mut list = LinkedList::new(TestAdapter);
    let mut node = Box::pin(TestNode {
        val: 10,
        link: Link::new(),
    });

    unsafe {
        list.push_back(node.as_mut());
    }
    drop(list);

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

    while let Some(popped) = list.pop_front() {
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
    cursor.move_next();

    let removed = cursor.remove().unwrap();
    assert_eq!(removed.val, 2);

    assert_eq!(cursor.get().unwrap().val, 3);

    let removed3 = cursor.remove().unwrap();
    assert_eq!(removed3.val, 3);

    assert!(cursor.get().is_none());

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

    let mut cursor = list.front();

    assert_eq!(cursor.get().unwrap().val, 10);
    cursor.move_next();
    assert_eq!(cursor.get().unwrap().val, 20);
    cursor.move_next();
    assert!(cursor.get().is_none());
    assert!(cursor.is_null());

    assert_eq!(list.len(), 2);

    while list.pop_front().is_some() {}
}
