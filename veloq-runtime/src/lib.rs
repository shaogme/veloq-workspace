pub mod config;
pub mod fs;
pub mod io;
pub mod local {
    pub use veloq_local::*;
}
pub mod macros;
pub mod net;
pub mod runtime;
pub mod sync {
    pub use veloq_sync::*;
}
pub mod time;

// Re-export key functions for convenient access
pub use runtime::{JoinHandle, LocalJoinHandle};
pub use runtime::{LocalExecutor, Runtime}; // Export Runtime for config usage
pub use runtime::{RuntimeContext, spawn, spawn_local, spawn_to, yield_now};

#[cfg(test)]
mod tests {
    mod basic;
    mod fs;
    mod select_test;
    mod spawn_to_test;
    mod socket_opts;
    mod tcp;
    mod time;
    mod udp;
}
