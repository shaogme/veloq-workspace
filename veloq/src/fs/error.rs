use diagweave::set;

set! {
    pub FsError = {
        #[display("Path too long for buffer")]
        PathTooLong,

        #[display("Register files returned empty")]
        RegisterFailed,

        #[display("Op buffer lost")]
        OpBufferLost,

        #[display("failed to fill whole buffer")]
        UnexpectedEof,

        #[display("failed to write whole buffer")]
        WriteZero,
    }
}
