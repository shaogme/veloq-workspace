#[link(name = "kernel32")]
unsafe extern "system" {
    fn QueryPerformanceCounter(lpPerformanceCount: *mut i64) -> i32;
}

/// 获取系统时间或计数器值以用于生成随机种子
pub fn get_system_time_seed() -> u64 {
    let mut count = 0i64;
    let res = unsafe { QueryPerformanceCounter(&mut count) };
    if res != 0 {
        count as u64
    } else {
        // 退化方案：混合一个栈地址以确保一定的随机性
        let dummy = 0;
        let addr = &dummy as *const _ as u64;
        0x123456789abcdef0u64 ^ addr
    }
}
