//! Field-level borrow splits for the uring submission path.
//!
//! A submission has to hold two things at once: the slot that owns the op/payload (a `&mut`
//! borrow into [`UringDriver::ops`]) and the driver state needed to turn that op into an SQE
//! and hand it to the kernel. Handing the whole `&mut UringDriver` to the second half while
//! the first is live is an aliasing violation, so the non-`ops` fields are projected into the
//! views below and the borrow checker verifies the split.

use crate::{
    config::BufferRegistrationMode,
    driver::{
        FileTable, MAX_CHUNKS, ProvidedBufGroup, UringDriver, UringRegistrationStats,
        registration::{BufferRegistrationQuarantine, REGISTER_FAILURE_RETRY_COOLDOWN},
    },
    error::{UringError, UringResult},
    op::{CompletionCleanupHintFn, UringOpRegistry},
};
use diagweave::prelude::*;
use io_uring::{IoUring, cqueue, squeue};
use tracing::{debug, trace};
use veloq_buf::{BufferRegistrar, FixedBuf, heap::ChunkId};
use veloq_driver_core::driver::{BufferRegistrationStatus, CompletionToken, OpToken};
use veloq_std::{
    collections::{BitSet, HashMap},
    format, io, ptr,
    time::Instant,
};
use veloq_wheel::Wheel;

#[cfg(feature = "test-hooks")]
use veloq_std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkRegistrationDecision {
    Fixed,
    RawFallback,
}

/// What a `IOSQE_BUFFER_SELECT` submission needs to know about the provided-buffer ring.
///
/// A `Copy` snapshot rather than a borrow of the group: `make_sqe` only reads these two
/// numbers, and copying them leaves the group itself free for the completion path to mutate.
#[derive(Debug, Clone, Copy)]
pub(crate) struct ProvidedBufSqeInfo {
    bgid: u16,
    buf_size: u32,
}

impl ProvidedBufSqeInfo {
    #[inline]
    pub(crate) fn new(bgid: u16, buf_size: u32) -> Self {
        Self { bgid, buf_size }
    }
}

/// The driver state a `make_sqe` implementation is allowed to consult.
///
/// `make_sqe` runs with the slot's op and payload borrowed mutably out of
/// [`UringDriver::ops`], so it must not be able to reach `ops` itself. These fields are
/// disjoint from it and are handed out by value or immutably: lazy chunk registration happens
/// *after* `make_sqe` returns, so nothing here needs to be mutated during SQE construction.
pub(crate) struct SqeEnv<'d> {
    pub(crate) file_table: &'d FileTable,
    registered_chunks: &'d BitSet,
    provided: Option<ProvidedBufSqeInfo>,
}

impl SqeEnv<'_> {
    /// The buffer group an `IOSQE_BUFFER_SELECT` submission should draw from, together with the
    /// size every buffer in it has.
    #[inline]
    pub(crate) fn provided_buf_info(&self, scope: &'static str) -> UringResult<(u16, u32)> {
        let info = self.provided.ok_or_else(|| {
            UringError::Unsupported
                .to_report()
                .push_ctx("scope", scope)
                .attach_note("driver has no provided buffer ring registered")
        })?;
        Ok((info.bgid, info.buf_size))
    }

    /// Whether `chunk` is already in the kernel's fixed-buffer table.
    ///
    /// An out-of-range id answers `false`, which simply selects the non-fixed opcode — the
    /// same outcome as a chunk that has not been registered yet.
    #[inline]
    pub(crate) fn is_chunk_registered(&self, chunk: ChunkId) -> bool {
        self.registered_chunks
            .get(chunk.as_usize())
            .unwrap_or(false)
    }
}

/// The driver state a completion is allowed to reach.
///
/// Mirrors [`SqeEnv`] on the other end of an operation. The completion path holds a slot
/// borrowed out of [`UringDriver::ops`] while it builds the record, so it cannot be handed the
/// whole driver either — but unlike submission it does need to *mutate* something: a CQE that
/// carries a buffer id has consumed a ring entry, and that entry has to be settled before the
/// record leaves this function.
pub(crate) struct CqeEnv<'d> {
    provided: Option<&'d mut ProvidedBufGroup>,
}

impl<'d> CqeEnv<'d> {
    #[inline]
    pub(crate) fn new(provided: Option<&'d mut ProvidedBufGroup>) -> Self {
        Self { provided }
    }

    /// Takes the buffer this CQE selected and refills its slot in the ring.
    ///
    /// `Ok(None)` means the completion selected no buffer at all — `-ENOBUFS`, or an operation
    /// that never asked for one. A CQE that *does* carry a buffer id while this driver has no
    /// ring registered is an error rather than a `None`: the kernel cannot have picked from a
    /// group that does not exist, so silently dropping it would hide a real bug.
    pub(crate) fn take_provided_buf(
        &mut self,
        flags: u32,
        res: i32,
    ) -> UringResult<Option<FixedBuf>> {
        match self.provided.as_deref_mut() {
            Some(group) => Ok(group.take_selected(flags, res)),
            None if cqueue::buffer_select(flags).is_some() => UringError::InvalidState
                .push_ctx("scope", "uring.driver.cqe_env.take_provided_buf")
                .with_ctx("cqe_flags", flags)
                .attach_note("completion selected a provided buffer but no ring is registered"),
            None => Ok(None),
        }
    }

    /// Hands the buffer this CQE selected straight back to the ring.
    ///
    /// For completions that are being discarded (cancelled, orphaned, stale token). Skipping it
    /// leaks one buffer id per discarded completion — "cancellation is not termination" in its
    /// provided-buffer form.
    pub(crate) fn return_provided_buf(&mut self, flags: u32) {
        if let Some(group) = self.provided.as_deref_mut() {
            group.return_selected(flags);
        }
    }

    /// Records that the kernel found the ring empty.
    pub(crate) fn note_exhausted(&mut self) {
        if let Some(group) = self.provided.as_deref_mut() {
            group.note_exhausted();
        }
    }
}

/// Every [`UringDriver`] field outside `ops` that the submission path touches.
///
/// Obtained together with `&mut ops` from [`UringDriver::split_for_submit`], which is what
/// lets a submission keep a slot borrow alive while it registers chunks, pushes the SQE and
/// arms software timers.
pub(crate) struct SubmitEnv<'d, 'r> {
    pub(crate) ring: &'d mut IoUring,
    pub(crate) wheel: &'d mut Wheel<OpToken>,
    completion_cleanup_hints: &'d mut HashMap<CompletionToken, Option<CompletionCleanupHintFn>>,
    file_table: &'d FileTable,
    registered_chunks: &'d mut BitSet,
    registrar: &'r (dyn BufferRegistrar + 'r),
    registration_stats: &'d mut UringRegistrationStats,
    registration_mode: BufferRegistrationMode,
    fixed_buffers_available: bool,
    fixed_buffers_failure_errno: Option<i32>,
    chunk_register_failure_at: &'d mut [Option<Instant>],
    registration_quarantine: &'d mut Option<BufferRegistrationQuarantine>,
    #[cfg(feature = "test-hooks")]
    register_buffers_update_outcomes: &'d mut VecDeque<Option<i32>>,
    #[cfg(feature = "test-hooks")]
    bitset_set_failure: &'d mut bool,
    #[cfg(feature = "test-hooks")]
    push_entry_failure: &'d mut bool,
    provided: Option<ProvidedBufSqeInfo>,
}

impl SubmitEnv<'_, '_> {
    /// Records the cleanup capability only after the SQE has entered the user-space queue.
    ///
    /// The value is optional by design: a present `None` records that this token is a known
    /// non-fd operation, whereas a missing token means that completion metadata was lost.
    #[inline]
    pub(crate) fn register_completion_cleanup_hint(
        &mut self,
        token: CompletionToken,
        hint: Option<CompletionCleanupHintFn>,
    ) {
        let previous = self.completion_cleanup_hints.insert(token, hint);
        debug_assert!(
            previous.is_none(),
            "completion cleanup hint was registered twice for one token"
        );
    }

    /// Narrows this view down to what a `make_sqe` implementation may see.
    #[inline]
    pub(crate) fn sqe_env(&self) -> SqeEnv<'_> {
        SqeEnv {
            file_table: self.file_table,
            registered_chunks: self.registered_chunks,
            provided: self.provided,
        }
    }

    /// Pushes `entry` onto the submission queue, flushing once if the ring is full.
    pub(crate) fn push_entry(&mut self, entry: squeue::Entry) -> bool {
        trace!("Pushing SQE user_data={}", entry.get_user_data());
        #[cfg(feature = "test-hooks")]
        if *self.push_entry_failure {
            *self.push_entry_failure = false;
            debug!("injected SQ push failure");
            return false;
        }

        let mut sq = self.ring.submission();

        if unsafe { sq.push(&entry) }.is_ok() {
            return true;
        }

        drop(sq);
        // push 失败意味着用户态 SQ 环被已填充、内核尚未消费的条目占满。要腾出空间只能
        // 让内核消费它们，即带 `to_submit > 0` 进 `io_uring_enter`——单纯 GETEVENTS 只
        // 收割 CQ，一条 SQE 都不会被消费，重试必然再次失败并白付一次系统调用。
        let _ = self.ring.submit();

        let mut sq = self.ring.submission();
        if unsafe { sq.push(&entry) }.is_ok() {
            return true;
        }

        debug!("SQ full even after flush");
        false
    }

    /// Registers `[ptr, ptr + len)` as the kernel's fixed buffer number `id`.
    pub(crate) fn register_buffer_backend(
        &mut self,
        id: ChunkId,
        ptr: *const u8,
        len: usize,
    ) -> UringResult<BufferRegistrationStatus> {
        let index = id.as_usize();
        if index >= MAX_CHUNKS {
            return UringError::InvalidInput
                .push_ctx("scope", "driver.register_buffer_internal")
                .with_ctx("chunk_id", index)
                .with_ctx("max_chunks", MAX_CHUNKS)
                .attach_note("chunk id exceeds maximum registered chunk count");
        }

        if ptr.is_null() || len == 0 {
            return UringError::InvalidInput
                .push_ctx("scope", "driver.register_buffer_internal")
                .with_ctx("chunk_id", index)
                .with_ctx("buffer_len", len)
                .attach_note("fixed-buffer registration requires a non-null pointer and length");
        }

        self.ensure_registration_healthy("driver.register_buffer_internal", id, None)?;

        let is_registered = self.registered_chunks.get(index).map_err(|e| {
            UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.register_buffer_internal")
                .with_ctx("chunk_index", index)
                .with_ctx("bitset_error", format!("{e:?}"))
                .attach_note("BitSet get failed")
        })?;
        if is_registered {
            return Ok(BufferRegistrationStatus::Registered);
        }

        if !self.fixed_buffers_available {
            if self.registration_mode.is_strict() {
                return Err(self.fixed_buffers_unavailable_report(
                    "driver.register_buffer_internal",
                    id,
                    None,
                ));
            }
            return self.registration_unavailable(
                id,
                self.fixed_buffers_failure_errno,
                "sparse fixed-buffer registration unavailable",
            );
        }

        if let Some(last_fail) = self.chunk_register_failure_at[index] {
            if last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN {
                self.registration_stats
                    .chunk_register_skipped_recent_failure = self
                    .registration_stats
                    .chunk_register_skipped_recent_failure
                    .saturating_add(1);
                if self.registration_mode.is_strict() {
                    return UringError::Registration
                        .push_ctx("scope", "driver.register_buffer_internal")
                        .with_ctx("chunk_id", id.raw())
                        .attach_note("recent chunk registration failure cooldown");
                }
                return self.registration_unavailable(
                    id,
                    None,
                    "recent chunk registration failure cooldown",
                );
            }
            // The cooldown expired: drop the record now instead of letting it sit here for
            // the lifetime of the driver.
            self.chunk_register_failure_at[index] = None;
        }

        let iovecs = [libc::iovec {
            iov_base: ptr as *mut _,
            iov_len: len,
        }];

        // Use register_buffers_update. The wrapper is also the deterministic test-only fault
        // injection boundary; production builds call the io_uring syscall directly.
        self.registration_stats.chunk_register_attempts = self
            .registration_stats
            .chunk_register_attempts
            .saturating_add(1);
        let register_result = self.register_buffers_update(index as u32, &iovecs);
        if let Err(e) = register_result {
            self.registration_stats.chunk_register_failures = self
                .registration_stats
                .chunk_register_failures
                .saturating_add(1);
            self.chunk_register_failure_at[index] = Some(Instant::now());
            let report = UringError::Registration
                .io_report("driver.register_buffer_internal.register_buffers_update", e);
            return if self.registration_mode.is_strict() {
                Err(report)
            } else {
                self.registration_unavailable(
                    id,
                    Self::report_errno(&report),
                    "chunk fixed-buffer registration failed",
                )
            };
        }

        if let Err(set_report) = self.mark_registered_chunk(index) {
            let zero_iovec = [libc::iovec {
                iov_base: ptr::null_mut(),
                iov_len: 0,
            }];
            match self.register_buffers_update(index as u32, &zero_iovec) {
                Ok(()) => {
                    return Err(set_report.attach_note(
                        "fixed-buffer registration was cleared after bitset update failed",
                    ));
                }
                Err(cleanup_error) => {
                    let cleanup_errno = cleanup_error.raw_os_error();
                    *self.registration_quarantine = Some(BufferRegistrationQuarantine {
                        chunk_id: id,
                        register_errno: None,
                        cleanup_errno,
                        scope: "driver.register_buffer_internal.bitset_cleanup",
                    });
                    let report = if let Some(errno) = cleanup_errno {
                        set_report.with_ctx("cleanup_errno", errno)
                    } else {
                        set_report.attach_note("fixed-buffer cleanup returned no errno")
                    };
                    return Err(report.attach_note(
                        "fixed-buffer registration cleanup failed; ring is quarantined",
                    ));
                }
            }
        }
        self.chunk_register_failure_at[index] = None;
        self.registration_stats.chunk_register_success = self
            .registration_stats
            .chunk_register_success
            .saturating_add(1);

        Ok(BufferRegistrationStatus::Registered)
    }

    /// Registers `chunk_id` on demand so the kernel can reach the buffer this SQE points at.
    ///
    /// Runs after `make_sqe`, which means the very first submission touching a chunk uses the
    /// non-fixed opcode and only later ones get `ReadFixed`/`WriteFixed`.
    pub(crate) fn ensure_chunk_registered(
        &mut self,
        chunk_id: ChunkId,
        user_data: usize,
        scope: &'static str,
    ) -> UringResult<ChunkRegistrationDecision> {
        let index = chunk_id.as_usize();
        if index >= MAX_CHUNKS {
            return UringError::InvalidInput
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", index)
                .with_ctx("max_chunks", MAX_CHUNKS)
                .with_ctx("user_data", user_data)
                .attach_note("chunk id exceeds maximum registered chunk count");
        }
        self.ensure_registration_healthy(scope, chunk_id, Some(user_data))?;
        let is_registered = self.registered_chunks.get(index).map_err(|e| {
            UringError::InvalidState
                .to_report()
                .push_ctx("scope", scope)
                .with_ctx("chunk_index", index)
                .with_ctx("bitset_error", format!("{e:?}"))
                .attach_note("BitSet get failed")
        })?;
        if is_registered {
            return Ok(ChunkRegistrationDecision::Fixed);
        }

        if !self.fixed_buffers_available {
            if self.registration_mode.is_strict() {
                return Err(self.fixed_buffers_unavailable_report(
                    scope,
                    chunk_id,
                    Some(user_data),
                ));
            }
            return Ok(self.raw_fallback(
                chunk_id,
                user_data,
                self.fixed_buffers_failure_errno,
                "sparse fixed-buffer registration unavailable",
            ));
        }

        let Some(info) = self.registrar.resolve_chunk_info(chunk_id) else {
            self.registration_stats.submission_missing_chunk_info = self
                .registration_stats
                .submission_missing_chunk_info
                .saturating_add(1);
            if self.registration_mode.is_strict() {
                return UringError::InvalidState
                    .push_ctx("scope", scope)
                    .with_ctx("chunk_id", chunk_id.raw())
                    .with_ctx("user_data", user_data)
                    .attach_note("strict mode missing chunk info for lazy registration");
            }
            return Ok(self.raw_fallback(
                chunk_id,
                user_data,
                None,
                "missing chunk info for lazy registration",
            ));
        };

        if info.id != chunk_id {
            return UringError::InvalidState
                .push_ctx("scope", scope)
                .with_ctx("chunk_id", chunk_id.raw())
                .with_ctx("resolved_chunk_id", info.id.raw())
                .with_ctx("user_data", user_data)
                .attach_note("chunk registrar returned mismatched chunk info");
        }

        // Resolving a missing local snapshot may have drained the worker's chunk message queue;
        // the backend may have registered the chunk through another path in the meantime.
        if self.is_chunk_registered(info.id) {
            return Ok(ChunkRegistrationDecision::Fixed);
        };

        match self.register_buffer_backend(info.id, info.ptr.as_ptr(), info.len.get())? {
            BufferRegistrationStatus::Registered => Ok(ChunkRegistrationDecision::Fixed),
            BufferRegistrationStatus::Unavailable => Ok(self.raw_fallback(
                chunk_id,
                user_data,
                self.fixed_buffers_failure_errno,
                "chunk fixed-buffer registration unavailable",
            )),
        }
    }

    fn registration_unavailable(
        &mut self,
        chunk_id: ChunkId,
        errno: Option<i32>,
        reason: &'static str,
    ) -> UringResult<BufferRegistrationStatus> {
        debug!(
            chunk_id = chunk_id.raw(),
            errno = ?errno,
            registration_mode = self.registration_mode.as_str(),
            fallback = true,
            reason,
            "fixed-buffer registration unavailable; using raw buffer I/O"
        );
        Ok(BufferRegistrationStatus::Unavailable)
    }

    fn fixed_buffers_unavailable_report(
        &self,
        scope: &'static str,
        chunk_id: ChunkId,
        user_data: Option<usize>,
    ) -> Report<UringError> {
        let report = UringError::Registration
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("chunk_id", chunk_id.raw())
            .with_ctx("registration_mode", self.registration_mode.as_str())
            .with_ctx("fallback", false)
            .attach_note("sparse fixed-buffer registration is unavailable");
        let report = if let Some(user_data) = user_data {
            report.with_ctx("user_data", user_data)
        } else {
            report
        };
        if let Some(errno) = self.fixed_buffers_failure_errno {
            report.with_ctx("errno", errno)
        } else {
            report
        }
    }

    fn raw_fallback(
        &mut self,
        chunk_id: ChunkId,
        user_data: usize,
        errno: Option<i32>,
        reason: &'static str,
    ) -> ChunkRegistrationDecision {
        self.registration_stats.raw_buffer_fallbacks = self
            .registration_stats
            .raw_buffer_fallbacks
            .saturating_add(1);
        debug!(
            chunk_id = chunk_id.raw(),
            user_data,
            errno = ?errno,
            registration_mode = self.registration_mode.as_str(),
            fallback = true,
            reason,
            "using raw buffer I/O"
        );
        ChunkRegistrationDecision::RawFallback
    }

    fn register_buffers_update(&mut self, index: u32, iovecs: &[libc::iovec]) -> io::Result<()> {
        #[cfg(feature = "test-hooks")]
        if let Some(outcome) = self.register_buffers_update_outcomes.pop_front() {
            if let Some(errno) = outcome {
                return Err(io::Error::from_raw_os_error(errno));
            }
            return Ok(());
        }

        // SAFETY: `iovecs` points at live chunk memory for the duration of this syscall, and the
        // caller retains ownership of that memory until every in-flight operation completes.
        unsafe {
            self.ring
                .submitter()
                .register_buffers_update(index, iovecs, None)
        }
        .map_err(io::Error::from)
    }

    #[inline]
    fn report_errno(report: &Report<UringError>) -> Option<i32> {
        report
            .error_code()
            .and_then(|code| i32::try_from(code).ok())
    }

    fn mark_registered_chunk(&mut self, index: usize) -> UringResult<()> {
        #[cfg(feature = "test-hooks")]
        if *self.bitset_set_failure {
            *self.bitset_set_failure = false;
            return UringError::InvalidState
                .push_ctx("scope", "driver.register_buffer_internal.bitset_set")
                .with_ctx("chunk_index", index)
                .attach_note("injected registered chunk bitset failure");
        }

        self.registered_chunks.set(index).map_err(|e| {
            UringError::InvalidState
                .to_report()
                .push_ctx("scope", "driver.register_buffer_internal.bitset_set")
                .with_ctx("chunk_index", index)
                .with_ctx("bitset_error", format!("{e:?}"))
                .attach_note("BitSet set failed after kernel registration")
        })
    }

    fn ensure_registration_healthy(
        &self,
        scope: &'static str,
        chunk_id: ChunkId,
        user_data: Option<usize>,
    ) -> UringResult<()> {
        let Some(quarantine) = self.registration_quarantine.as_ref() else {
            return Ok(());
        };

        let mut report = UringError::InvalidState
            .to_report()
            .push_ctx("scope", scope)
            .with_ctx("chunk_id", chunk_id.raw())
            .with_ctx("quarantine_chunk_id", quarantine.chunk_id.raw())
            .with_ctx("quarantine_scope", quarantine.scope)
            .attach_note("fixed-buffer registry is quarantined and requires ring rebuild");
        if let Some(errno) = quarantine.register_errno {
            report = report.with_ctx("registration_errno", errno);
        }
        if let Some(errno) = quarantine.cleanup_errno {
            report = report.with_ctx("cleanup_errno", errno);
        }
        if let Some(user_data) = user_data {
            Err(report.with_ctx("user_data", user_data))
        } else {
            Err(report)
        }
    }

    #[inline]
    pub(crate) fn is_chunk_registered(&self, chunk_id: ChunkId) -> bool {
        self.registered_chunks
            .get(chunk_id.as_usize())
            .unwrap_or(false)
    }
}

impl<'a> UringDriver<'a> {
    /// Splits off the op registry from the rest of the driver so a slot borrow and the ring
    /// can be held at the same time. Both halves are plain field projections, so the compiler
    /// — not a raw pointer — is what guarantees they do not alias.
    pub(crate) fn split_for_submit(&mut self) -> (&mut UringOpRegistry, SubmitEnv<'_, 'a>) {
        let view = self.buffer_registry.split_for_submit();

        (
            &mut self.ops,
            SubmitEnv {
                ring: &mut self.ring,
                wheel: self.timers.wheel_mut(),
                completion_cleanup_hints: &mut self.completion_cleanup_hints,
                file_table: &self.file_table,
                registered_chunks: view.registered_chunks,
                registrar: view.registrar,
                registration_stats: view.registration_stats,
                registration_mode: view.registration_mode,
                fixed_buffers_available: view.fixed_buffers_available,
                fixed_buffers_failure_errno: view.fixed_buffers_failure_errno,
                chunk_register_failure_at: view.chunk_register_failure_at,
                registration_quarantine: view.registration_quarantine,
                #[cfg(feature = "test-hooks")]
                register_buffers_update_outcomes: view.register_buffers_update_outcomes,
                #[cfg(feature = "test-hooks")]
                bitset_set_failure: view.bitset_set_failure,
                #[cfg(feature = "test-hooks")]
                push_entry_failure: view.push_entry_failure,
                provided: view.provided,
            },
        )
    }

    /// The submission half of [`Self::split_for_submit`], for callers that hold no slot.
    #[inline]
    pub(crate) fn submit_env(&mut self) -> SubmitEnv<'_, 'a> {
        self.split_for_submit().1
    }
}
