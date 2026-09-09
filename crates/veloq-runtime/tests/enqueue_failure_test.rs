//! 队列容量耗尽 / 入队失败降级路径的回归测试。
//!
//! 验证入队失败时避免 job cell 双重释放，以及确保 scope 义务得到妥善结算（防止
//! `wait_all` 永久挂起），用 `with_queue_capacity(1)` 把它们逼出来。

use std::{convert::Infallible, num::NonZeroUsize};

use veloq_runtime::{
    Outcome,
    error::RuntimeError,
    runtime::RuntimeBuilder,
    scope,
    scope::JoinOutcome,
    scope_local,
    task::{TaskError, yield_now},
};

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("non-zero")
}

#[derive(Default, Debug)]
struct OutcomeTally {
    ok: usize,
    cancelled: usize,
    panicked: usize,
    runtime_err: usize,
}

impl OutcomeTally {
    fn record<T>(&mut self, outcome: JoinOutcome<T>) {
        match outcome {
            JoinOutcome::Ok(_) => self.ok += 1,
            JoinOutcome::TaskErr(TaskError::Cancelled) => self.cancelled += 1,
            JoinOutcome::TaskErr(TaskError::Panic) => self.panicked += 1,
            JoinOutcome::RuntimeErr(_) => self.runtime_err += 1,
        }
    }

    fn total(&self) -> usize {
        self.ok + self.cancelled + self.panicked + self.runtime_err
    }
}

/// 本地队列被打满：多余的 `spawn_boxed` 必须被明确终结（结算 scope 义务 + 让 handle 有
/// 结果），而不是让 scope 的 `remaining` 永不归零。
#[test]
fn local_queue_exhaustion_settles_scope_obligations() {
    const SPAWNS: usize = 8;

    let tally = RuntimeBuilder::new()
        .with_worker_count(Some(nz(1)))
        .with_queue_capacity(nz(1))
        .scope(async |ctx| {
            scope_local!(ctx, async |scope| {
                let mut handles = Vec::with_capacity(SPAWNS);
                for i in 0..SPAWNS {
                    handles.push(scope.spawn_boxed(async move { i }));
                }

                let mut tally = OutcomeTally::default();
                for handle in handles {
                    tally.record(handle.await);
                }
                Outcome::<_, Infallible>::Ok(tally)
            })
            .await
            .expect("local scope")
        })
        .expect("runtime");
    let tally = match tally {
        Outcome::Ok(tally) => tally,
        _ => panic!("local scope did not return a tally"),
    };

    assert_eq!(tally.total(), SPAWNS, "所有 handle 都必须结算: {tally:?}");
    assert!(tally.ok >= 1, "至少一个任务应当真正执行: {tally:?}");
    assert!(tally.cancelled > 0, "被拒绝的任务应当以取消收场: {tally:?}");
    assert_eq!(tally.runtime_err, 0, "不应出现运行时协议错误: {tally:?}");
}

/// `spawn_boxed_to` 在 pinned 队列打满时会走「路由投递失败」与「任务安装失败」两条降级
/// 路径：job cell 只允许被释放一次，且每个 handle 都必须能被 join 到结果。
#[test]
fn routed_spawn_boxed_survives_pinned_queue_exhaustion() {
    const SPAWNS: usize = 32;

    let tally = RuntimeBuilder::new()
        .with_worker_count(Some(nz(2)))
        .with_queue_capacity(nz(1))
        .scope(async |ctx| {
            scope!(ctx, async |scope| {
                let mut handles = Vec::with_capacity(SPAWNS);
                for i in 0..SPAWNS {
                    handles.push(scope.spawn_boxed_to(1, async move || {
                        yield_now().await;
                        i
                    }));
                }

                let mut tally = OutcomeTally::default();
                for handle in handles {
                    tally.record(handle.await);
                }
                Outcome::<_, Infallible>::Ok(tally)
            })
            .await
            .expect("send scope")
        })
        .expect("runtime");
    let tally = match tally {
        Outcome::Ok(tally) => tally,
        _ => panic!("send scope did not return a tally"),
    };

    assert_eq!(tally.total(), SPAWNS, "所有 handle 都必须结算: {tally:?}");
}

/// 越界 worker id 是一条纯粹的「入队前置校验失败」路径：既要报错，也要结算 scope 义务
/// （否则 `wait_all` 会挂死）。
#[test]
fn routed_spawn_to_invalid_worker_reports_error() {
    RuntimeBuilder::new()
        .with_worker_count(Some(nz(1)))
        .scope(async |ctx| {
            scope!(ctx, async |scope| {
                let handle = scope.spawn_boxed_to(usize::MAX, async || 7usize);
                match handle.await {
                    JoinOutcome::RuntimeErr(err) => {
                        assert!(
                            matches!(err.inner(), RuntimeError::WorkerIdOutOfBounds { .. }),
                            "unexpected error: {err}"
                        );
                    }
                    JoinOutcome::Ok(value) => panic!("expected error, got Ok({value})"),
                    JoinOutcome::TaskErr(err) => panic!("expected runtime error, got {err:?}"),
                }
            })
            .await
            .expect("send scope")
        })
        .expect("runtime");
}

/// 取消一批可能已被拒绝的 handle 不应 panic，也不应破坏 scope 的结算。
#[test]
fn cancelling_rejected_handles_is_safe() {
    const SPAWNS: usize = 8;

    RuntimeBuilder::new()
        .with_worker_count(Some(nz(2)))
        .with_queue_capacity(nz(1))
        .scope(async |ctx| {
            scope!(ctx, async |scope| {
                let mut handles = Vec::with_capacity(SPAWNS);
                for i in 0..SPAWNS {
                    handles.push(scope.spawn_boxed_to(1, async move || i));
                }
                for handle in &mut handles {
                    handle.cancel();
                }
                for handle in handles {
                    let _ = handle.await;
                }
            })
            .await
            .expect("send scope")
        })
        .expect("runtime");
}
