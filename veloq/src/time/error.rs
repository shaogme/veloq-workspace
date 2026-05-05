use std::fmt;
use std::io;

#[derive(Debug, PartialEq, Eq)]
pub struct Elapsed(());

impl Elapsed {
    pub(crate) fn new() -> Self {
        Elapsed(())
    }
}

impl fmt::Display for Elapsed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "deadline has elapsed")
    }
}

impl std::error::Error for Elapsed {}

impl From<Elapsed> for io::Error {
    fn from(_: Elapsed) -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, "deadline has elapsed")
    }
}
