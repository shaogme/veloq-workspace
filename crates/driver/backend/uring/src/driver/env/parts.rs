use veloq_buf::{BufferRegistrar, heap::ChunkId};

use crate::{
    driver::{
        env::{CompletionControlView, CompletionLedgerParts},
        registration::{ChunkRegistrationRecord, ChunkRegistrationState, FileTable},
    },
    error::{UringError, UringResult},
};
use diagweave::prelude::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChunkRegistrationDecision {
    Fixed,
    RawFallback,
}

/// What a `IOSQE_BUFFER_SELECT` submission needs to know about the provided-buffer ring.
///
/// A `Copy` snapshot rather than a borrow of the group: `make_sqe` only reads these two
/// numbers, and copying them leaves the group itself free for the completion path to mutate.
/// SQE 构造只读取 group id 与 buffer 大小；复制这两个值可让完成路径继续独占修改 group。
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
/// the operation registry, so it must not be able to reach that registry itself. These fields are
/// disjoint from it and are handed out by value or immutably: lazy chunk registration happens
/// *after* `make_sqe` returns, so nothing here needs to be mutated during SQE construction.
/// `make_sqe` 持有操作 slot 的可变借用，因此只能读取彼此独立的状态；惰性 chunk 注册在构造 SQE 后进行。
pub(crate) struct SqeEnv<'d> {
    file_table: &'d FileTable,
    chunk_records: &'d [Option<ChunkRegistrationRecord>],
    registrar: &'d (dyn BufferRegistrar + 'd),
    provided: Option<ProvidedBufSqeInfo>,
}

pub(crate) struct CompletionMetadata {
    waker_buf_len: usize,
    waker_generation: u64,
}

impl CompletionMetadata {
    pub(crate) const fn new(waker_buf_len: usize, waker_generation: u64) -> Self {
        Self {
            waker_buf_len,
            waker_generation,
        }
    }

    pub(crate) const fn waker_buf_len(&self) -> usize {
        self.waker_buf_len
    }

    pub(crate) const fn waker_generation(&self) -> u64 {
        self.waker_generation
    }
}

pub(crate) struct CompletionControlParts<'d> {
    ledger: CompletionLedgerParts<'d>,
    metadata: CompletionMetadata,
}

impl<'d> CompletionControlParts<'d> {
    pub(crate) fn new(ledger: CompletionLedgerParts<'d>, metadata: CompletionMetadata) -> Self {
        Self { ledger, metadata }
    }

    pub(crate) fn into_view(self) -> CompletionControlView<'d> {
        let Self { ledger, metadata } = self;
        CompletionControlView::new(ledger, metadata)
    }
}

impl SqeEnv<'_> {
    pub(crate) fn from_parts<'d>(
        file_table: &'d FileTable,
        chunk_records: &'d [Option<ChunkRegistrationRecord>],
        registrar: &'d (dyn BufferRegistrar + 'd),
        provided: Option<ProvidedBufSqeInfo>,
    ) -> SqeEnv<'d> {
        SqeEnv {
            file_table,
            chunk_records,
            registrar,
            provided,
        }
    }

    #[inline]
    pub(crate) fn file_table(&self) -> &FileTable {
        self.file_table
    }

    /// The buffer group an `IOSQE_BUFFER_SELECT` submission should draw from, together with the
    /// size every buffer in it has.
    /// 返回 `IOSQE_BUFFER_SELECT` 提交所用的 group id 和每个 buffer 的大小。
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
    /// 越界 chunk id 返回 `false`，与尚未注册的 chunk 一样选择非 fixed opcode。
    #[inline]
    pub(crate) fn is_chunk_registered(&self, chunk: ChunkId) -> bool {
        let Some(record) = self
            .chunk_records
            .get(chunk.as_usize())
            .and_then(Option::as_ref)
        else {
            return false;
        };
        let Some(info) = self.registrar.resolve_chunk_info(chunk) else {
            return false;
        };
        record.state() == ChunkRegistrationState::Registered
            && info.id == chunk
            && record.matches(chunk, info.ptr.as_ptr(), info.len.get())
    }
}

#[cfg(test)]
impl<'d> SqeEnv<'d> {
    /// Creates the smallest submission environment needed by projection tests.
    /// 为视图投影测试创建所需的最小提交环境。
    pub(crate) fn for_test(
        file_table: &'d FileTable,
        registrar: &'d (dyn BufferRegistrar + 'd),
    ) -> Self {
        Self {
            file_table,
            chunk_records: &[],
            registrar,
            provided: None,
        }
    }

    pub(crate) fn for_test_with_provided(
        file_table: &'d FileTable,
        registrar: &'d (dyn BufferRegistrar + 'd),
        bgid: u16,
        buf_size: u32,
    ) -> Self {
        Self {
            file_table,
            chunk_records: &[],
            registrar,
            provided: Some(ProvidedBufSqeInfo::new(bgid, buf_size)),
        }
    }
}
