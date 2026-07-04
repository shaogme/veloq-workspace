use diagweave::set;
use veloq_std::string::String;

set! {
    pub SystemError = {
        #[display("OS error {0}")]
        Os(i32),
    }

    pub BufError = {
        #[display("Layout error: {0}")]
        Layout(#[from] veloq_std::alloc::LayoutError),

        #[display("Out of memory")]
        Oom,

        #[display("System error: {0}")]
        System(#[from] SystemError),

        #[display("Other error: {0}")]
        Other(String),

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
