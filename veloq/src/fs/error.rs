use diagweave::set;

set! {
    pub FsError = {
        #[display("Path too long for buffer")]
        PathTooLong,

        #[display("Register files returned empty")]
        RegisterFailed,

        #[display("Op buffer lost")]
        OpBufferLost,
    }
}
