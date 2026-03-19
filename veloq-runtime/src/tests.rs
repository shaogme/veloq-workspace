mod basic;
mod buffer_test;
mod fs;
mod select_test;
mod socket_opts;
mod spawn_to_test;
mod tcp;
mod time;
mod udp;

// ============ 测试框架提取 ============

pub(crate) struct NetworkTestRunner {
    pub name: &'static str,
    pub worker_threads: usize,
    pub buffer_sizes: Vec<std::num::NonZeroUsize>,
    pub overall_timeout: std::time::Duration,
}

impl NetworkTestRunner {
    pub(crate) fn new(name: &'static str) -> Self {
        Self {
            name,
            worker_threads: 1,
            buffer_sizes: vec![
                veloq_buf::nz!(8192),
                veloq_buf::nz!(16384),
                veloq_buf::nz!(65536),
            ],
            overall_timeout: std::time::Duration::from_secs(30),
        }
    }

    pub(crate) fn worker_threads(mut self, threads: usize) -> Self {
        self.worker_threads = threads;
        self
    }

    pub(crate) fn buffer_sizes(mut self, sizes: Vec<std::num::NonZeroUsize>) -> Self {
        self.buffer_sizes = sizes;
        self
    }

    /// 执行测试任务
    pub(crate) fn run<F, Fut>(&self, test_logic: F)
    where
        F: Fn(std::num::NonZeroUsize) -> Fut + Send + Sync + Clone + 'static,
        Fut: std::future::Future<Output = ()> + 'static,
    {
        for &size in &self.buffer_sizes {
            let name = self.name;
            let worker_threads = self.worker_threads;
            let test_logic_clone = test_logic.clone();
            let overall_timeout = self.overall_timeout;

            // 使用独立的 OS 线程来完全隔离运行时的创建
            std::thread::Builder::new()
                .name(format!("{}-{}", name, size.get()))
                .spawn(move || {
                    let runtime = crate::runtime::Runtime::builder()
                        .config(crate::config::Config::default().worker_threads(worker_threads))
                        .build()
                        .unwrap();

                    let start_time = std::time::Instant::now();

                    runtime.block_on(async move {
                        // 利用我们在 time 模块中提供的 timeout 对整体测试做第一层包装
                        crate::time::timeout(overall_timeout, test_logic_clone(size))
                            .await
                            .unwrap_or_else(|_| {
                                panic!(
                                    "Test '{}' OVERALL timeout after {:?}",
                                    name, overall_timeout
                                )
                            });
                    });

                    println!(
                        "Test '{}' with BufferSize: {} completed in {:?}",
                        name,
                        size,
                        start_time.elapsed()
                    );
                })
                .unwrap()
                .join()
                .unwrap();
        }
    }
}

pub(crate) async fn timeout_op<T, Fut>(ctx: &str, phase: &str, timeout_secs: u64, fut: Fut) -> T
where
    Fut: std::future::Future<Output = T>,
{
    crate::time::timeout(std::time::Duration::from_secs(timeout_secs), fut)
        .await
        .unwrap_or_else(|_| {
            panic!(
                "Phase Timeout Error: context='{}'; phase='{}'; time_allowed={}s",
                ctx, phase, timeout_secs
            )
        })
}
