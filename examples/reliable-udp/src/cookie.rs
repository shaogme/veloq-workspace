use hmac::{Hmac, Mac};
use sha2::Sha256;
use veloq::std::{
    error::Error as StdError,
    fmt,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

use crate::packet::{ConnectionId, VERSION};

pub const COOKIE_KEY_LEN: usize = 32;
pub const COOKIE_VERSION: u8 = 1;
const COOKIE_TAG_LEN: usize = 16;
pub const COOKIE_LEN: usize = 32;
const COOKIE_DOMAIN: &[u8] = b"veloq-reliable-udp/cookie/v3";
const RESERVED_LEN: usize = 4;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CookieKey {
    key_id: u8,
    secret: [u8; COOKIE_KEY_LEN],
}

impl CookieKey {
    pub fn new(key_id: u8, secret: [u8; COOKIE_KEY_LEN]) -> Result<Self, CookieError> {
        if key_id == 0 {
            return Err(CookieError::InvalidKeyId);
        }
        Ok(Self { key_id, secret })
    }

    pub fn from_slice(key_id: u8, secret: &[u8]) -> Result<Self, CookieError> {
        let secret: [u8; COOKIE_KEY_LEN] = secret
            .try_into()
            .map_err(|_| CookieError::InvalidKeyLength)?;
        Self::new(key_id, secret)
    }

    pub const fn key_id(self) -> u8 {
        self.key_id
    }
}

impl fmt::Debug for CookieKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CookieKey")
            .field("key_id", &self.key_id)
            .field("secret", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CookieError {
    InvalidKeyId,
    InvalidKeyLength,
    DuplicateKeyId,
    ZeroTtl,
    TtlTooLong,
}

impl fmt::Display for CookieError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidKeyId => f.write_str("cookie key ID must be non-zero"),
            Self::InvalidKeyLength => {
                write!(f, "cookie key must be {COOKIE_KEY_LEN} bytes")
            }
            Self::DuplicateKeyId => f.write_str("current and previous cookie keys must differ"),
            Self::ZeroTtl => f.write_str("cookie TTL must be non-zero"),
            Self::TtlTooLong => f.write_str("cookie TTL is too long"),
        }
    }
}

impl StdError for CookieError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CookieKeyRing {
    current: CookieKey,
    previous: Option<CookieKey>,
}

impl CookieKeyRing {
    pub fn new(current: CookieKey, previous: Option<CookieKey>) -> Result<Self, CookieError> {
        if previous.is_some_and(|key| key.key_id == current.key_id || key.secret == current.secret)
        {
            return Err(CookieError::DuplicateKeyId);
        }
        Ok(Self { current, previous })
    }

    pub const fn current(&self) -> CookieKey {
        self.current
    }

    pub const fn previous(&self) -> Option<CookieKey> {
        self.previous
    }

    pub fn rotate(&self, current: CookieKey) -> Result<Self, CookieError> {
        Self::new(current, Some(self.current))
    }

    fn find(&self, key_id: u8) -> Option<CookieKey> {
        if self.current.key_id == key_id {
            Some(self.current)
        } else {
            self.previous.filter(|key| key.key_id == key_id)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CookieConfig {
    pub ttl: Duration,
    pub clock_skew: Duration,
}

impl Default for CookieConfig {
    fn default() -> Self {
        Self {
            ttl: Duration::from_secs(5),
            clock_skew: Duration::from_secs(1),
        }
    }
}

impl CookieConfig {
    pub fn validate(&self, tick: Duration) -> Result<(), CookieError> {
        if self.ttl.is_zero() {
            return Err(CookieError::ZeroTtl);
        }
        if self.ttl <= tick || self.ttl.as_millis() > u128::from(u64::MAX) {
            return Err(CookieError::TtlTooLong);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CookieInput {
    pub source: SocketAddr,
    pub connection_id: ConnectionId,
    pub client_receive_window: u16,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub struct CookieToken([u8; COOKIE_LEN]);

impl CookieToken {
    pub const fn as_bytes(&self) -> &[u8; COOKIE_LEN] {
        &self.0
    }

    pub fn parse(bytes: &[u8]) -> Result<Self, CookieValidation> {
        let bytes: [u8; COOKIE_LEN] = bytes
            .try_into()
            .map_err(|_| CookieValidation::Invalid(CookieInvalidReason::Malformed))?;
        if bytes[0] != COOKIE_VERSION || bytes[28..28 + RESERVED_LEN].iter().any(|byte| *byte != 0)
        {
            return Err(CookieValidation::Invalid(CookieInvalidReason::Malformed));
        }
        Ok(Self(bytes))
    }

    pub fn issued_at_ms(&self) -> u64 {
        u64::from_le_bytes(self.0[2..10].try_into().expect("cookie issued time"))
    }

    pub fn client_receive_window(&self) -> u16 {
        u16::from_le_bytes(self.0[10..12].try_into().expect("cookie window"))
    }

    pub fn key_id(&self) -> u8 {
        self.0[1]
    }
}

impl fmt::Debug for CookieToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CookieToken")
            .field("cookie_version", &self.0[0])
            .field("key_id", &self.key_id())
            .field("issued_at_ms", &self.issued_at_ms())
            .field("client_receive_window", &self.client_receive_window())
            .field("tag", &"[redacted]")
            .finish()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CookieInvalidReason {
    Malformed,
    KeyUnavailable,
    Expired,
    Future,
    WrongParameters,
    InvalidMac,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CookieValidation {
    Valid {
        client_receive_window: u16,
        issued_at_ms: u64,
    },
    Invalid(CookieInvalidReason),
}

pub fn issue_cookie(key_ring: &CookieKeyRing, input: CookieInput, now: Duration) -> CookieToken {
    let issued_at_ms = duration_millis(now);
    let mut token = [0u8; COOKIE_LEN];
    token[0] = COOKIE_VERSION;
    token[1] = key_ring.current.key_id;
    token[2..10].copy_from_slice(&issued_at_ms.to_le_bytes());
    token[10..12].copy_from_slice(&input.client_receive_window.to_le_bytes());
    let tag = compute_tag(&key_ring.current, &token, input);
    token[12..12 + COOKIE_TAG_LEN].copy_from_slice(&tag[..COOKIE_TAG_LEN]);
    CookieToken(token)
}

pub fn validate_cookie(
    key_ring: &CookieKeyRing,
    config: CookieConfig,
    input: CookieInput,
    token: CookieToken,
    now: Duration,
) -> CookieValidation {
    let issued_at_ms = token.issued_at_ms();
    let now_ms = duration_millis(now);
    let skew_ms = duration_millis(config.clock_skew);
    if issued_at_ms > now_ms.saturating_add(skew_ms) {
        return CookieValidation::Invalid(CookieInvalidReason::Future);
    }
    if now_ms.saturating_sub(issued_at_ms) > duration_millis(config.ttl) {
        return CookieValidation::Invalid(CookieInvalidReason::Expired);
    }
    if token.client_receive_window() != input.client_receive_window {
        return CookieValidation::Invalid(CookieInvalidReason::WrongParameters);
    }
    let Some(key) = key_ring.find(token.key_id()) else {
        return CookieValidation::Invalid(CookieInvalidReason::KeyUnavailable);
    };
    let expected = compute_tag(&key, token.as_bytes(), input);
    let valid = constant_time_eq(&token.as_bytes()[12..28], &expected[..COOKIE_TAG_LEN]);
    if !valid {
        return CookieValidation::Invalid(CookieInvalidReason::InvalidMac);
    }
    CookieValidation::Valid {
        client_receive_window: token.client_receive_window(),
        issued_at_ms,
    }
}

fn compute_tag(key: &CookieKey, token: &[u8; COOKIE_LEN], input: CookieInput) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(&key.secret).expect("fixed HMAC key length");
    mac.update(COOKIE_DOMAIN);
    mac.update(&token[0..2]);
    mac.update(
        &u64::from_le_bytes(token[2..10].try_into().expect("cookie issued time")).to_be_bytes(),
    );
    mac.update(&u16::from_le_bytes(token[10..12].try_into().expect("cookie window")).to_be_bytes());
    encode_socket_addr(&mut mac, input.source);
    mac.update(&input.connection_id.get().to_be_bytes());
    mac.update(&[VERSION]);
    mac.finalize().into_bytes().into()
}

fn encode_socket_addr(mac: &mut HmacSha256, source: SocketAddr) {
    match source.ip() {
        IpAddr::V4(address) => {
            mac.update(&[4]);
            mac.update(&address.octets());
        }
        IpAddr::V6(address) => {
            mac.update(&[6]);
            mac.update(&address.octets());
        }
    }
    mac.update(&source.port().to_be_bytes());
}

fn duration_millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut difference = 0u8;
    for (left, right) in left.iter().zip(right) {
        difference |= left ^ right;
    }
    difference == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use veloq::std::net::{Ipv4Addr, SocketAddrV4};

    fn key(id: u8, value: u8) -> CookieKey {
        CookieKey::new(id, [value; COOKIE_KEY_LEN]).expect("test key")
    }

    fn input() -> CookieInput {
        CookieInput {
            source: SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::LOCALHOST, 1234)),
            connection_id: ConnectionId::new(7).expect("connection ID"),
            client_receive_window: 32,
        }
    }

    #[test]
    fn cookie_binds_input_and_rotates_keys() {
        let ring = CookieKeyRing::new(key(1, 7), None).expect("key ring");
        let token = issue_cookie(&ring, input(), Duration::from_secs(1));
        assert!(matches!(
            validate_cookie(
                &ring,
                CookieConfig::default(),
                input(),
                token,
                Duration::from_secs(2)
            ),
            CookieValidation::Valid { .. }
        ));
        let mut changed = input();
        changed.connection_id = ConnectionId::new(8).expect("connection ID");
        assert!(matches!(
            validate_cookie(
                &ring,
                CookieConfig::default(),
                changed,
                token,
                Duration::from_secs(2)
            ),
            CookieValidation::Invalid(CookieInvalidReason::InvalidMac)
        ));
        let rotated = ring.rotate(key(2, 8)).expect("rotation");
        assert!(matches!(
            validate_cookie(
                &rotated,
                CookieConfig::default(),
                input(),
                token,
                Duration::from_secs(2)
            ),
            CookieValidation::Valid { .. }
        ));
    }

    #[test]
    fn cookie_rejects_expired_and_future_tokens() {
        let ring = CookieKeyRing::new(key(1, 7), None).expect("key ring");
        let token = issue_cookie(&ring, input(), Duration::from_secs(1));
        let config = CookieConfig {
            ttl: Duration::from_secs(1),
            clock_skew: Duration::ZERO,
        };
        assert!(matches!(
            validate_cookie(&ring, config, input(), token, Duration::from_millis(2_001)),
            CookieValidation::Invalid(CookieInvalidReason::Expired)
        ));
        assert!(matches!(
            validate_cookie(&ring, config, input(), token, Duration::ZERO),
            CookieValidation::Invalid(CookieInvalidReason::Future)
        ));
    }
}
