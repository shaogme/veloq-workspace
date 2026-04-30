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

#[cfg(test)]
mod tests;

// Re-export key functions for convenient access
pub use runtime::{JoinHandle, LocalJoinHandle};
pub use runtime::{LocalExecutor, Runtime}; // Export Runtime for config usage
pub use runtime::{RuntimeContext, spawn, spawn_eager, spawn_local, spawn_to, yield_now};
