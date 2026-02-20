use std::boxed::Box;
use veloq_intrusive_linklist::{Link, LinkedList, intrusive_adapter};

struct MyNode {
    id: usize,
    link: Link,
}

intrusive_adapter!(MyAdapter = MyNode { link: Link });

#[test]
fn test_integration_flow() {
    let mut list = LinkedList::new(MyAdapter);

    // Create nodes
    let mut nodes: Vec<_> = (0..5)
        .map(|i| {
            Box::pin(MyNode {
                id: i,
                link: Link::new(),
            })
        })
        .collect();

    // Push all
    for node in nodes.iter_mut() {
        unsafe {
            list.push_back(node.as_mut());
        }
    }

    assert_eq!(list.len(), 5);

    // Verify order and remove
    let mut count = 0;
    while let Some(ptr) = list.pop_front() {
        assert_eq!(ptr.id, count);
        count += 1;
    }

    assert_eq!(count, 5);
    assert!(list.is_empty());
}

#[test]
fn test_cursor_integration() {
    let mut list = LinkedList::new(MyAdapter);
    let mut node1 = Box::pin(MyNode {
        id: 100,
        link: Link::new(),
    });
    let mut node2 = Box::pin(MyNode {
        id: 200,
        link: Link::new(),
    });
    let mut node3 = Box::pin(MyNode {
        id: 300,
        link: Link::new(),
    });

    unsafe {
        list.push_back(node1.as_mut());
        list.push_back(node2.as_mut());
        list.push_back(node3.as_mut());
    }

    let mut cursor = list.front_mut();

    // Find 200 and remove
    while let Some(node) = cursor.get() {
        if node.id == 200 {
            let removed = cursor.remove().unwrap();
            assert_eq!(removed.id, 200);
            // After removal, cursor points to 300
        } else {
            cursor.move_next();
        }
    }

    assert_eq!(list.len(), 2);

    let v1 = list.pop_front().unwrap();
    assert_eq!(v1.id, 100);

    let v2 = list.pop_front().unwrap();
    assert_eq!(v2.id, 300);
}
