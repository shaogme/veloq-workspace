use diagweave::set;

set! {
    pub NetError = {
        #[display("socket registration requires socket handle")]
        InvalidSocketHandle,

        #[display("register_files returned empty")]
        RegistrationEmpty,

        #[display("local addr is unavailable for this socket")]
        LocalAddrUnavailable,

        #[display("no address provided")]
        NoAddressProvided,

        #[display("Accept op lost")]
        AcceptOpLost,

        #[display("Accept completed without remote address")]
        AcceptMissingRemoteAddr,

        #[display("Op buffer lost")]
        OpBufferLost,

        #[display("UdpRecvFrom op lost")]
        UdpRecvFromOpLost,

        #[display("driver must populate UdpRecvFrom::addr before completion")]
        UdpRecvFromMissingAddr,

        #[display("failed to fill whole buffer")]
        UnexpectedEof,

        #[display("failed to write whole buffer")]
        WriteZero,

        #[display("failed to resolve address")]
        ToSocketAddrs(std::io::Error),
    }
}
