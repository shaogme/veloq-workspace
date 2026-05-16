use veloq_runtime::runtime::Runtime;
use veloq_runtime::task::yield_now;

#[test]
fn test_panic_propagation() {
    let rt = Runtime::<_, (), _>::default();
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        rt.block_on(async |ctx| {
            println!("开始测试 Panic 传播...");
            ctx.scope(async |s| {
                s.spawn_boxed(async {
                    yield_now().await;
                    println!("子任务即将 Panic...");
                    panic!("BOOM!");
                });
                println!("主任务等待 scope 结束...");
            })
            .await;
            println!("如果不传播，这里会被打印");
        });
    }));

    match result {
        Err(e) => {
            if let Some(msg) = e.downcast_ref::<&str>() {
                println!("成功捕获到传播的 Panic: {}", msg);
                assert_eq!(*msg, "BOOM!");
            } else if let Some(msg) = e.downcast_ref::<String>() {
                println!("成功捕获到传播的 Panic: {}", msg);
                assert_eq!(msg, "BOOM!");
            } else {
                println!("成功捕获到传播的 Panic (未知类型)");
            }
        }
        Ok(_) => {
            panic!("错误：Panic 没有被传播！");
        }
    }
    println!("测试通过！");
}
