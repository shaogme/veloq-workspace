use error_stack::Report;
use std::fmt;

#[derive(Debug, Clone)]
pub struct RioDiag {
    scope: &'static str,
    fields: Vec<(&'static str, String)>,
}

impl RioDiag {
    pub fn new(scope: &'static str) -> Self {
        Self {
            scope,
            fields: Vec::new(),
        }
    }

    pub fn field(mut self, key: &'static str, value: impl ToString) -> Self {
        self.fields.push((key, value.to_string()));
        self
    }

    #[inline]
    pub fn wsa_class_from_text(text: &str) -> &'static str {
        if text.contains("wsa_class=zero_wsa") || text.contains("WSAGetLastError=0") {
            "zero_wsa"
        } else {
            "nonzero_or_unknown_wsa"
        }
    }
}

impl fmt::Display for RioDiag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rio_diag(scope={}", self.scope)?;
        for (k, v) in &self.fields {
            write!(f, ", {}={}", k, v)?;
        }
        write!(f, ")")
    }
}

/// RIO 模块特定的错误上下文
#[derive(Debug)]
pub enum RioError {
    /// RIO 库 or 函数指针加载失败 (如 GetExtensionFunctionPointer 失败)
    LibraryLoad,
    /// 注册内存缓冲区（Buffer）失败
    BufferRegistration,
    /// 创建 RIO 完成队列（CQ）失败
    CqCreation,
    /// 创建 RIO 请求队列（RQ）失败
    RqCreation,
    /// 数据路径操作失败（发送/接收提交）
    Datapath,
    /// 资源分配失败（如超出 RIO 限制）
    ResourceExhaustion,
    /// 内部逻辑一致性错误
    Internal,
}

impl fmt::Display for RioError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LibraryLoad => write!(f, "RIO library or function pointers load failure"),
            Self::BufferRegistration => write!(f, "failed to register memory buffer for RIO"),
            Self::CqCreation => write!(f, "failed to create RIO completion queue"),
            Self::RqCreation => write!(f, "failed to create RIO request queue"),
            Self::Datapath => write!(f, "RIO datapath operation error"),
            Self::ResourceExhaustion => write!(f, "RIO resource limit reached"),
            Self::Internal => write!(f, "RIO internal inconsistency"),
        }
    }
}

impl std::error::Error for RioError {}

/// RIO 模块专用的 Result 类型
pub type RioResult<T> = Result<T, Report<RioError>>;

/// 提供将 RioResult 转换为外部 io::Error 的扩展能力
pub trait RioReportExt {
    fn to_io_error(self, detail: impl Into<String>) -> std::io::Error;
}

impl RioReportExt for Report<RioError> {
    fn to_io_error(self, detail: impl Into<String>) -> std::io::Error {
        use crate::common::{IocpErrorContext, io_error};
        let detail = detail.into();
        // 保持与 common.rs 的结构化日志兼容
        let io_err = std::io::Error::other(format!("{self:#}"));
        io_error(IocpErrorContext::Rio, io_err, detail)
    }
}

/// 提供将 RioResult 转换为外部 io::Result 的扩展能力
pub trait RioResultExt<T> {
    fn to_io_result(self, detail: impl Into<String>) -> std::io::Result<T>;
}

impl<T> RioResultExt<T> for RioResult<T> {
    fn to_io_result(self, detail: impl Into<String>) -> std::io::Result<T> {
        self.map_err(|e| e.to_io_error(detail))
    }
}
