use core::mem::MaybeUninit;

/// 获取系统时间以用于生成随机种子
pub fn get_system_time_seed() -> u64 {
    let mut ts = MaybeUninit::<libc::timespec>::uninit();
    let res = unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, ts.as_mut_ptr()) };
    if res == 0 {
        let ts = unsafe { ts.assume_init() };
        ((ts.tv_sec as u64) << 32) ^ (ts.tv_nsec as u64)
    } else {
        // 退化方案：混合一个栈地址以确保一定的随机性
        let dummy = 0;
        let addr = &dummy as *const _ as u64;
        0x123456789abcdef0u64 ^ addr
    }
}
