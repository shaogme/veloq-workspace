//! multishot 流两条实现路径共用的那几条判据。
//!
//! 放在 native 层而不是门面层，是因为它们读的是**内核那一侧的 errno**。门面层不该认识
//! errno，也不该为了认识它而按平台去挑依赖（`libc` 只在 Linux 上有）——那正是平台差异往上
//! 渗的形态之一。
//!
//! IOCP 一个 multishot 能力都没有（`capabilities()` 恒为全 `false`），门面层的
//! `AcceptStream` / `RecvStream` 在那里恒走 `Emulated`，从来不会问「内核是不是拒绝了这个
//! 能力」。所以非 Linux 的实现恒为 `false` 不是敷衍：那条路根本走不到。

use diagweave::report::Report;

use crate::error::Error as DriverError;

/// 这条完成带回来的 errno，没有则 `None`。
///
/// 驱动把 CQE 的负数结果原样存进 `error_code`（见 uring 后端的 `on_complete`），所以这里
/// 拿到的就是内核那一侧的 errno。
#[cfg(target_os = "linux")]
#[inline]
fn errno_of(err: &Report<DriverError>) -> Option<i32> {
    err.error_code().and_then(|code| i32::try_from(code).ok())
}

/// 环里没有 buffer 可挑。
///
/// 对 multishot 而言这不只是一次失败：内核**顺带把整个操作终止了**（那条 CQE 不带
/// `IORING_CQE_F_MORE`），所以流必须重新 arm 才能继续。
#[cfg(target_os = "linux")]
#[inline]
pub fn is_buffer_ring_exhausted(err: &Report<DriverError>) -> bool {
    errno_of(err) == Some(libc::ENOBUFS)
}

/// 内核不认识这个操作的 multishot 变体。
///
/// **必须靠试**：`IORING_OP_ACCEPT` 从 5.5 就在、`IORING_OP_RECV` 从 5.6 就在，而它们的
/// multishot 变体分别要 5.19 与 6.0——`IORING_REGISTER_PROBE` 只回答「这个 opcode 存不
/// 存在」，分不出这两者。于是能力集合先乐观地记上，第一次提交被 `-EINVAL` 打回来才降级。
///
/// 判据宽于必要：一个**真的**参数错误（比如 socket 根本没 listen）也会命中这里，于是能
/// 力被白白关掉。但那条路是自愈的——退回单发之后同一个错误会照样报出来，用户看到的仍是
/// 真实原因，代价只是这个 driver 之后少一次优化。反过来漏判则是硬故障：整条流在旧内核上
/// 永远只吐 `-EINVAL`。
#[cfg(target_os = "linux")]
#[inline]
pub fn is_capability_rejected(err: &Report<DriverError>) -> bool {
    matches!(errno_of(err), Some(libc::EINVAL | libc::EOPNOTSUPP))
}

/// 没有 provided buffer 环的后端上，环永远不会「被掏空」。
#[cfg(not(target_os = "linux"))]
#[inline]
pub fn is_buffer_ring_exhausted(_err: &Report<DriverError>) -> bool {
    false
}

/// 没有 multishot 能力的后端上，没有能力可降级——`capabilities()` 一开始就是全 `false`，
/// 流从第一次 poll 起就在 `Emulated` 上。
#[cfg(not(target_os = "linux"))]
#[inline]
pub fn is_capability_rejected(_err: &Report<DriverError>) -> bool {
    false
}
