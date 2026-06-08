use diagweave::set;

set! {
    pub BufError = {
        #[display("Layout error: {0}")]
        Layout(#[from] std::alloc::LayoutError),

        #[display("Out of memory")]
        Oom,

        #[display("IO error: {0}")]
        Io(#[from] std::io::Error),

        #[display("Allocation failed: {0}")]
        AllocFailed(String),

        #[display("Chunk memory size too small for sharding")]
        ChunkTooSmall,

        #[display("Chunk memory size must be page aligned: {size}")]
        PageUnaligned { size: usize },

        #[display("Chunk {chunk_id} missing")]
        ChunkMissing { chunk_id: crate::heap::ChunkId },

        #[display("FixedBuf has invalid ChunkID")]
        InvalidChunkId,

        #[display("GlobalSlotPool ID overflow")]
        IdOverflow,
    }
}

pub type BufResult<T> = Result<T, diagweave::report::Report<BufError>>;
