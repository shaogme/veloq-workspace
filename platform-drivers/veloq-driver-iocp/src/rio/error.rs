use error_stack::Report;
use std::fmt;

/// RIO 诊断信息，用于携带结构化的上下文
#[derive(Debug, Clone)]
pub struct RioDiag {
    pub scope: &'static str,
    pub error_code: Option<u32>,
    pub original_message: Option<String>,
    pub fields: Vec<(&'static str, String)>,
}

impl RioDiag {
    pub fn new(scope: &'static str) -> Self {
        Self {
            scope,
            error_code: None,
            original_message: None,
            fields: Vec::new(),
        }
    }

    pub fn with_error(mut self, code: u32, msg: impl ToString) -> Self {
        self.error_code = Some(code);
        self.original_message = Some(msg.to_string());
        self
    }

    pub fn field(mut self, key: &'static str, value: impl ToString) -> Self {
        self.fields.push((key, value.to_string()));
        self
    }
}

impl fmt::Display for RioDiag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "rio_diag(scope={}", self.scope)?;
        if let Some(code) = self.error_code {
            write!(f, ", error_code={}", code)?;
        }
        if let Some(ref msg) = self.original_message {
            write!(f, ", original_error={}", msg)?;
        }
        for (k, v) in &self.fields {
            write!(f, ", {}={}", k, v)?;
        }
        write!(f, ")")
    }
}

/// RIO 模块特定的错误上下文
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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
    /// 操作不支持 (如 RIO 未能初始化)
    NotSupported,
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
            Self::NotSupported => write!(f, "RIO not supported or initialized"),
            Self::Internal => write!(f, "RIO internal inconsistency"),
        }
    }
}

impl std::error::Error for RioError {}

/// RIO 模块专用的 Result 类型
pub type RioResult<T> = Result<T, Report<RioError>>;

#[derive(Debug)]
pub struct RioIoError {
    pub report: Report<RioError>,
    pub detail: String,
}

impl fmt::Display for RioIoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "RIO error ({}): {:#}", self.detail, self.report)
    }
}

impl std::error::Error for RioIoError {}

/// 提供将 RioResult 转换为外部 io::Error 的扩展能力
pub trait RioReportExt {
    fn to_io_error(self, detail: impl Into<String>) -> std::io::Error;
    fn has_wsa_error(&self, code: u32) -> bool;
}

impl RioReportExt for Report<RioError> {
    fn to_io_error(self, detail: impl Into<String>) -> std::io::Error {
        use crate::common::IocpErrorContext;
        let detail = detail.into();

        // 我们在这里模拟 common::io_error 的逻辑，但保留 RioIoError 类型
        let os_code = self
            .frames()
            .find_map(|f| f.downcast_ref::<RioDiag>().and_then(|d| d.error_code));

        tracing::error!(
            context = %IocpErrorContext::Rio,
            detail = %detail,
            os_error = ?os_code,
            report = ?&self,
            "RIO error report"
        );

        let rio_io_err = RioIoError {
            report: self,
            detail: detail.clone(),
        };

        std::io::Error::other(rio_io_err)
    }

    fn has_wsa_error(&self, code: u32) -> bool {
        self.frames().any(|f| {
            if let Some(diag) = f.downcast_ref::<RioDiag>() {
                diag.error_code == Some(code)
            } else {
                false
            }
        })
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
