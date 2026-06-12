use diagweave::set;

set! {
    pub TcpError = {
        #[display("Accept op lost")]
        AcceptOpLost,

        #[display("Accept completed without remote address")]
        AcceptMissingRemoteAddr,
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
        ToSocketAddrs(std::io::Error),
    }
}
