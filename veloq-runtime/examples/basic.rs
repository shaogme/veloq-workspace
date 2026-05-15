use veloq_runtime::runtime::Runtime;
use veloq_runtime::task::yield_now;
use veloq_runtime::{task, task_local};

// --- 测试用例 ---

async fn work(
    ctx: veloq_runtime::runtime::RuntimeScopeContext<()>,
    id: String,
    steps: u32,
) -> String {
    for i in 1..=steps {
        yield_now().await;
        let worker_id = ctx.worker_id();
        println!(
            "  [Worker {}] [任务 {}] 进度 {}/{}",
            worker_id, id, i, steps
        );
    }
    format!("Result from {}", id)
}

fn main() {
    let rt = Runtime::default();
    rt.block_on(async |ctx| {
        println!("--- 安全异步作用域执行开始 ---");

        task_local!(
            static_node,
            work(ctx.clone(), "栈任务-Static".to_string(), 2)
        );
        task!(send_node, work(ctx.clone(), "栈Send任务".to_string(), 2));

        ctx.scope(async |my_scope| {
            let res_send = my_scope.spawn(&send_node).await.unwrap();
            println!("  >> 栈Send任务完成, 结果: {}", res_send);

            let mut handles = Vec::new();
            for i in 1..=3 {
                let h = my_scope.spawn_boxed(work(ctx.clone(), format!("堆任务-{}", i), i + 1));
                handles.push(h);
            }

            let h_static = my_scope.spawn_local(&static_node);

            let res_static = h_static.await.unwrap();
            println!("  >> 栈任务已提前完成, 结果: {}", res_static);

            for h in handles {
                let res = h.await.unwrap();
                println!("  >> 堆任务完成, 结果: {}", res);
            }

            // --- 测试业务 Result + unwrap() ---
            println!("\n  [测试] 演示业务 Result 处理...");
            ctx.scope(async |biz_scope| {
                let biz_handle = biz_scope.spawn_boxed(async {
                    yield_now().await;
                    if true {
                        Err("这是一个业务错误")
                    } else {
                        Ok("成功".to_string())
                    }
                });
                let biz_res: Result<String, &str> = biz_handle.await.unwrap();
                println!("  >> 业务任务结果: {:?}", biz_res);
            })
            .await;

            // --- 测试显式取消 (Explicit Cancellation) ---
            println!("\n  [测试] 测试显式取消：手动取消特定任务...");
            ctx.scope(async |explicit_cancel_scope| {
                let h1 = explicit_cancel_scope.spawn_boxed(async move {
                    for i in 1..=10 {
                        yield_now().await;
                        let worker_id = explicit_cancel_scope.worker_id();
                        println!("    [Worker {}] [手动取消任务] 进度 {}", worker_id, i);
                    }
                });

                // 模拟一些工作后手动取消
                yield_now().await;
                yield_now().await;
                println!("    >> 正在手动取消任务...");
                h1.cancel();

                match h1.await {
                    Err(veloq_runtime::task::TaskError::Cancelled) => {
                        println!("    >> 确认：任务已被手动取消")
                    }
                    other => println!("    >> 错误：意外的返回结果 {:?}", other),
                }
            })
            .await;

            // --- 测试异步取消通知 (Async Cancellation Notification) ---
            println!("\n  [测试] 测试异步取消通知：任务主动监听取消信号...");
            ctx.scope(async |async_notify_scope| {
                let token = async_notify_scope.cancel_token().child();
                let token_clone = token.clone();
                let h = async_notify_scope.spawn_boxed(async move {
                    let worker_id = async_notify_scope.worker_id();
                    println!(
                        "    [Worker {}] [异步监听任务] 正在等待取消信号...",
                        worker_id
                    );
                    token_clone.cancelled().await;
                    let worker_id = async_notify_scope.worker_id();
                    println!(
                        "    [Worker {}] [异步监听任务] 收到取消信号！正在清理资源...",
                        worker_id
                    );
                    "清理完成".to_string()
                });

                yield_now().await;
                yield_now().await;
                println!("    >> 触发异步取消...");
                token.cancel();

                let res = h.await.unwrap();
                println!("    >> 任务清理结果: {}", res);
            })
            .await;

            // --- 测试延迟生成的任务令牌 (Lazy Task Token) ---
            println!("\n  [测试] 测试 JoinHandle 延迟生成的取消令牌...");
            ctx.scope(async |lazy_token_scope| {
                let h = lazy_token_scope.spawn_boxed(async move {
                    yield_now().await;
                    yield_now().await;
                    let worker_id = lazy_token_scope.worker_id();
                    println!("    [Worker {}] [延迟令牌任务] 任务运行中...", worker_id);
                });

                let task_token = h.cancel_token();
                let token_clone = task_token.clone();

                lazy_token_scope.spawn_boxed(async move {
                    token_clone.cancelled().await;
                    let worker_id = lazy_token_scope.worker_id();
                    println!("    [Worker {}] [监听器] 检测到任务令牌被取消", worker_id);
                });

                yield_now().await;
                println!("    >> 正在通过 JoinHandle 取消任务...");
                h.cancel();

                let _ = h.await;
            })
            .await;

            // --- 测试定向分发 (Targeted Distribution) ---
            println!("\n  [测试] 测试定向分发：显式发送任务到 Worker 1...");
            ctx.scope(async |target_scope| {
                let mut handles = Vec::new();
                for i in 1..=3 {
                    let h = target_scope.spawn_boxed_to(1, async move || {
                        let worker_id = target_scope.worker_id();
                        println!("    [Worker {}] [定向任务-{}] 正在执行...", worker_id, i);
                    });
                    handles.push(h);
                }
                for h in handles {
                    let _ = h.await;
                }
            })
            .await;

            // --- 测试嵌套取消传播 (Nested Cancellation Propagation) ---
            println!("\n  [测试] 测试嵌套取消传播：取消父作用域应自动取消子作用域...");
            ctx.scope(async |parent_scope| {
                let token = parent_scope.cancel_token().clone();

                parent_scope.spawn_boxed(async move {
                    println!("    [父作用域] 启动子作用域...");
                    ctx.scope(async |child_scope| {
                        child_scope.spawn_boxed(async {
                            for i in 1..=100 {
                                yield_now().await;
                                if i % 10 == 0 {
                                    println!("      [子作用域任务] 运行中... {}", i);
                                }
                            }
                        });
                    })
                    .await;
                    println!("    [父作用域] 子作用域已退出");
                });

                yield_now().await;
                yield_now().await;
                println!("    >> 正在取消父作用域...");
                token.cancel();
            })
            .await;
            println!("  >> 父作用域已退出");
        })
        .await;
        println!("--- scope 结束 ---");
    });
    println!("--- 所有任务安全完成 ---");
}
