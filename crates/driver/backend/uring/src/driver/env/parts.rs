use veloq_buf::{RegionInfo, RegistrationCapability};

use crate::{
    driver::{
        env::{CompletionControlView, CompletionLedgerParts},
        registration::{ChunkRegistrationRecord, ChunkRegistrationState, FileTable},
    },
    error::{UringError, UringResult},
};
use diagweave::prelude::*;

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
/// disjoint from it and are handed out by value or immutably: fixed-buffer registration is
/// completed during pool startup, so SQE construction only validates the worker ledger.
/// `make_sqe` 持有操作 slot 的可变借用，因此只能读取彼此独立的状态；固定 buffer 注册在 pool 启动时完成，构造 SQE 时只校验 worker 账本。
pub(crate) struct SqeEnv<'d> {
    file_table: &'d FileTable,
    chunk_records: &'d [Option<ChunkRegistrationRecord>],
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
        provided: Option<ProvidedBufSqeInfo>,
    ) -> SqeEnv<'d> {
        SqeEnv {
            file_table,
            chunk_records,
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

    /// Validate explicit registration metadata against the worker ledger.
    /// 根据 worker 账本校验 buffer 显式携带的注册区域。
    #[inline]
    pub(crate) fn fixed_buffer_index(&self, info: RegionInfo) -> UringResult<Option<u16>> {
        let RegistrationCapability::RegisteredChunk(chunk) = info.registration else {
            return Ok(None);
        };
        if chunk != info.id {
            return UringError::InvalidState
                .push_ctx("scope", "uring.sqe_env.fixed_buffer_index")
                .with_ctx("declared_chunk_id", chunk.raw())
                .with_ctx("resolved_chunk_id", info.id.raw())
                .attach_note("registered buffer capability does not match its backing region");
        }
        let Some(record) = self
            .chunk_records
            .get(chunk.as_usize())
            .and_then(Option::as_ref)
        else {
            return UringError::InvalidState
                .push_ctx("scope", "uring.sqe_env.fixed_buffer_index")
                .with_ctx("chunk_id", chunk.raw())
                .attach_note("registered buffer has no worker registration record");
        };
        if record.state() != ChunkRegistrationState::Registered
            || !record.matches(chunk, info.base_ptr.as_ptr(), info.region_len.get())
        {
            return UringError::InvalidState
                .push_ctx("scope", "uring.sqe_env.fixed_buffer_index")
                .with_ctx("chunk_id", chunk.raw())
                .with_ctx("region_ptr", info.base_ptr.as_ptr() as usize)
                .with_ctx("region_len", info.region_len.get())
                .attach_note("registered buffer identity does not match the worker ledger");
        }
        Ok(Some(chunk.raw()))
    }
}

#[cfg(test)]
impl<'d> SqeEnv<'d> {
    /// Creates the smallest submission environment needed by projection tests.
    /// 为视图投影测试创建所需的最小提交环境。
    pub(crate) fn for_test(file_table: &'d FileTable) -> Self {
        Self {
            file_table,
            chunk_records: &[],
            provided: None,
        }
    }

    pub(crate) fn for_test_with_provided(
        file_table: &'d FileTable,
        bgid: u16,
        buf_size: u32,
    ) -> Self {
        Self {
            file_table,
            chunk_records: &[],
            provided: Some(ProvidedBufSqeInfo::new(bgid, buf_size)),
        }
    }
}
