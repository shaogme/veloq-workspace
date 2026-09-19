use diagweave::set;
use veloq_std::io::Error as IoError;

set! {
    pub TcpError = {
        #[display("Accept op lost")]
        AcceptOpLost,

        #[display("Accept completed without remote address")]
        AcceptMissingRemoteAddr,

        #[display("a provided-buffer recv completed without a buffer")]
        ProvidedBufferMissing,
    }

    pub UdpError = {
        #[display("socket receive is already owned")]
        ReceiveAlreadyOwned,

        #[display("invalid UDP receive configuration")]
        ReceiveConfigInvalid,

        #[display("UDP receiver is not ready")]
        ReceiverNotReady,

        #[display("initial UDP receive submit failed")]
        InitialReceiveSubmitFailed,

        #[display("UDP receive queue is stalled")]
        ReceiveQueueStalled,

        #[display("UDP receive buffer is exhausted")]
        ReceiveBufferExhausted,

        #[display("UDP receive replacement submit failed")]
        ReplacementSubmitFailed,

        #[display("UDP receive OS error ({code})")]
        ReceiveOsError { code: i32 },

        #[display("UDP receive was cancelled")]
        ReceiveCancelled,

        #[display("UDP receive request context is corrupt")]
        ReceiveContextCorrupt,

        #[display("UDP receive is unavailable on this socket")]
        SocketReceiveUnavailable,

        #[display("UDP receive close drain timed out")]
        CloseDrainTimeout,
    }

    pub NetError = TcpError | UdpError | {
        #[display("socket registration requires socket handle")]
        InvalidSocketHandle,

        #[display("register_files returned empty")]
        RegistrationEmpty,

        #[display("local addr is unavailable for this socket")]
        LocalAddrUnavailable,

        #[display("no address provided")]
        NoAddressProvided,

        #[display("Op buffer lost")]
        OpBufferLost,

        #[display("failed to fill whole buffer")]
        UnexpectedEof,

        #[display("failed to write whole buffer")]
        WriteZero,

        #[display("failed to resolve address")]
        ToSocketAddrs(IoError),
    }
}
