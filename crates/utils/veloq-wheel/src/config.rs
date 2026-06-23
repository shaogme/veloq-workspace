//! Timer Configuration Module
//!
//! Provides hierarchical configuration structure and Builder pattern for configuring timing wheel, service, and batch processing behavior.
//!
//! 定时器配置模块，提供分层的配置结构和 Builder 模式，用于配置时间轮、服务和批处理行为。
use std::fmt;
use std::time::Duration;

/// Timing Wheel Configuration
///
/// Used to configure parameters for hierarchical timing wheel. The system only supports hierarchical mode.
///
/// # 时间轮配置
///
/// 用于配置分层时间轮的参数。系统只支持分层模式。
///
/// # Examples (示例)
/// ```no_run
/// use veloq_wheel::WheelConfig;
/// use std::time::Duration;
///
/// // Use default configuration (使用默认配置，分层模式)
/// let config = WheelConfig::default();
///
/// // Use Builder to customize configuration (使用 Builder 自定义配置)
/// let config = WheelConfig::builder()
///     .l0_tick_duration(Duration::from_millis(20))
///     .l0_slot_count(1024)
///     .l1_tick_duration(Duration::from_secs(2))
///     .l1_slot_count(128)
///     .build()
///     .unwrap();
/// ```
#[derive(Debug, Clone)]
pub struct WheelConfig {
    /// Duration of each tick in L0 layer, bottom layer
    ///
    /// L0 层每个 tick 的持续时间
    pub l0_tick_duration: Duration,

    /// Number of slots in L0 layer, must be power of 2
    ///
    /// L0 层槽位数，必须是 2 的幂
    pub l0_slot_count: usize,

    /// Duration of each tick in L1 layer, upper layer
    ///
    /// L1 层每个 tick 的持续时间
    pub l1_tick_duration: Duration,

    /// Number of slots in L1 layer, must be power of 2
    ///
    /// L1 层槽位数，必须是 2 的幂
    pub l1_slot_count: usize,
}

impl Default for WheelConfig {
    fn default() -> Self {
        Self {
            l0_tick_duration: Duration::from_millis(10),
            l0_slot_count: 512,
            l1_tick_duration: Duration::from_secs(1),
            l1_slot_count: 64,
        }
    }
}

impl WheelConfig {
    /// Create configuration builder (创建配置构建器)
    pub fn builder() -> WheelConfigBuilder {
        WheelConfigBuilder::default()
    }
}

/// Timing Wheel Configuration Builder
#[derive(Debug, Clone)]
pub struct WheelConfigBuilder {
    l0_tick_duration: Duration,
    l0_slot_count: usize,
    l1_tick_duration: Duration,
    l1_slot_count: usize,
}

impl Default for WheelConfigBuilder {
    fn default() -> Self {
        Self {
            l0_tick_duration: Duration::from_millis(10),
            l0_slot_count: 512,
            l1_tick_duration: Duration::from_secs(1),
            l1_slot_count: 64,
        }
    }
}

impl WheelConfigBuilder {
    /// Set L0 layer tick duration
    pub fn l0_tick_duration(mut self, duration: Duration) -> Self {
        self.l0_tick_duration = duration;
        self
    }

    /// Set L0 layer slot count
    pub fn l0_slot_count(mut self, count: usize) -> Self {
        self.l0_slot_count = count;
        self
    }

    /// Set L1 layer tick duration
    pub fn l1_tick_duration(mut self, duration: Duration) -> Self {
        self.l1_tick_duration = duration;
        self
    }

    /// Set L1 layer slot count
    pub fn l1_slot_count(mut self, count: usize) -> Self {
        self.l1_slot_count = count;
        self
    }

    /// Build and validate configuration
    ///
    /// # Returns
    /// - `Ok(WheelConfig)`: Configuration is valid
    /// - `Err(ConfigError)`: Configuration validation failed
    ///
    /// # Validation Rules
    /// - L0 tick duration must be greater than 0
    /// - L1 tick duration must be greater than 0
    /// - L0 slot count must be greater than 0 and power of 2
    /// - L1 slot count must be greater than 0 and power of 2
    /// - L1 tick must be an integer multiple of L0 tick
    pub fn build(self) -> Result<WheelConfig, ConfigError> {
        // Validate L0 layer configuration
        if self.l0_tick_duration.is_zero() {
            return Err(ConfigError::InvalidConfiguration {
                field: "l0_tick_duration".to_string(),
                reason: "L0 layer tick duration must be greater than 0".to_string(),
            });
        }

        if self.l0_slot_count == 0 {
            return Err(ConfigError::InvalidSlotCount {
                slot_count: self.l0_slot_count,
                reason: "L0 layer slot count must be greater than 0",
            });
        }

        if !self.l0_slot_count.is_power_of_two() {
            return Err(ConfigError::InvalidSlotCount {
                slot_count: self.l0_slot_count,
                reason: "L0 layer slot count must be power of 2",
            });
        }

        // Validate L1 layer configuration
        if self.l1_tick_duration.is_zero() {
            return Err(ConfigError::InvalidConfiguration {
                field: "l1_tick_duration".to_string(),
                reason: "L1 layer tick duration must be greater than 0".to_string(),
            });
        }

        if self.l1_slot_count == 0 {
            return Err(ConfigError::InvalidSlotCount {
                slot_count: self.l1_slot_count,
                reason: "L1 layer slot count must be greater than 0",
            });
        }

        if !self.l1_slot_count.is_power_of_two() {
            return Err(ConfigError::InvalidSlotCount {
                slot_count: self.l1_slot_count,
                reason: "L1 layer slot count must be power of 2",
            });
        }

        // Validate L1 tick is an integer multiple of L0 tick
        let l0_ms = self.l0_tick_duration.as_millis() as u64;
        let l1_ms = self.l1_tick_duration.as_millis() as u64;
        if !l1_ms.is_multiple_of(l0_ms) {
            return Err(ConfigError::InvalidConfiguration {
                field: "l1_tick_duration".to_string(),
                reason: format!(
                    "L1 tick duration ({} ms) must be an integer multiple of L0 tick duration ({} ms)",
                    l1_ms, l0_ms
                ),
            });
        }

        Ok(WheelConfig {
            l0_tick_duration: self.l0_tick_duration,
            l0_slot_count: self.l0_slot_count,
            l1_tick_duration: self.l1_tick_duration,
            l1_slot_count: self.l1_slot_count,
        })
    }
}

/// Config Error Type
///
/// 配置错误类型
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// Invalid slot count (must be a power of 2 and greater than 0)
    ///
    /// 无效的槽位数 (必须是 2 的幂且大于 0)
    InvalidSlotCount {
        slot_count: usize,
        reason: &'static str,
    },

    /// Configuration validation failed
    ///
    /// 配置验证失败
    InvalidConfiguration { field: String, reason: String },
}

impl fmt::Display for ConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ConfigError::InvalidSlotCount { slot_count, reason } => {
                write!(
                    f,
                    "Invalid slot count {}: {} (无效的槽位数 {}: {})",
                    slot_count, reason, slot_count, reason
                )
            }
            ConfigError::InvalidConfiguration { field, reason } => {
                write!(
                    f,
                    "Configuration validation failed ({}): {} (配置验证失败 ({}): {})",
                    field, reason, field, reason
                )
            }
        }
    }
}

impl std::error::Error for ConfigError {}
