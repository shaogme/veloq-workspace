//! `RuntimeCtx` 不能作为返回值逃出运行时：它指向的 `RuntimeShared` 活在 `block_on` 的栈帧里。
//! 入口闭包对上下文生命周期高阶，因此 `R` 无法提及它。

use veloq_runtime::runtime::{RuntimeBuilder, RuntimeCtx};

fn main() {
    let escaped: RuntimeCtx<'static, ()> = RuntimeBuilder::new()
        .scope(async |ctx| ctx)
        .unwrap();
    let _ = escaped.worker_count();
}
