use diagweave::set;
use veloq_std::io::Error as IoError;

set! {
    pub TcpError = {
        #[display("Accept op lost")]
        AcceptOpLost,

        #[display("Accept completed without remote address")]
        AcceptMissingRemoteAddr,

        #[display("provided buffers are not available on this runtime")]
        ProvidedBuffersUnavailable,

        #[display("a provided-buffer recv completed without a buffer")]
        ProvidedBufferMissing,
    }

    pub UdpError = {
        #[display("UdpRecvFrom op lost")]
        UdpRecvFromOpLost,

        #[display("driver must populate UdpRecvFrom::addr before completion")]
        UdpRecvFromMissingAddr,
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
