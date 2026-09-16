#![no_std]

extern crate alloc;

mod config;
mod error;
mod id;
mod level;
mod wheel;

pub use config::{WheelConfig, WheelConfigBuilder};
pub use error::{ConfigError, TimerError};
pub use id::TimerId;
pub use wheel::{AdvanceReport, Expired, Wheel};
