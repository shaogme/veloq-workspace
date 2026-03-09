use error_stack::Report;
use std::fmt;
use std::io;

#[derive(Debug, Clone, Copy)]
pub enum IocpErrorContext {
    DriverInit,
    CompletionWait,
    Submission,
    Rio,
    ResolveFd,
}

impl fmt::Display for IocpErrorContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DriverInit => f.write_str("IOCP driver initialization failed"),
            Self::CompletionWait => f.write_str("IOCP completion wait failed"),
            Self::Submission => f.write_str("IOCP operation submission failed"),
            Self::Rio => f.write_str("RIO operation failed"),
            Self::ResolveFd => f.write_str("failed to resolve IO handle"),
        }
    }
}

impl std::error::Error for IocpErrorContext {}

fn sanitize_field(s: &str) -> String {
    s.replace('\n', "\\n").replace('\r', "\\r")
}

fn extract_structured_field<'a>(s: &'a str, key: &str) -> Option<&'a str> {
    s.split("; ").find_map(|part| part.strip_prefix(key))
}

fn parse_nested_source(source: &str) -> (String, Option<i32>) {
    if !source.starts_with("context=") {
        return (source.to_string(), None);
    }

    let nested_source = extract_structured_field(source, "source=");
    let nested_os = extract_structured_field(source, "os_error=").and_then(|v| {
        if v == "none" {
            None
        } else {
            v.parse::<i32>().ok()
        }
    });

    match nested_source {
        Some("none") | None => (source.to_string(), nested_os),
        Some(val) => (val.to_string(), nested_os),
    }
}

fn structured_line(
    ctx: IocpErrorContext,
    detail: &str,
    source: Option<&str>,
    os_code: Option<i32>,
) -> String {
    let source = source.map(sanitize_field).unwrap_or_else(|| "none".to_string());
    let os_code = os_code
        .map(|v| v.to_string())
        .unwrap_or_else(|| "none".to_string());
    format!(
        "context={ctx}; detail={}; source={source}; os_error={os_code}",
        sanitize_field(detail)
    )
}

pub(crate) fn io_error(
    ctx: IocpErrorContext,
    err: io::Error,
    detail: impl Into<String>,
) -> io::Error {
    let detail = detail.into();
    let raw_source = err.to_string();
    let (source, nested_os) = parse_nested_source(&raw_source);
    let os_code = err.raw_os_error().or(nested_os);
    let report = Report::new(err)
        .change_context(ctx)
        .attach(detail.clone());
    let msg = structured_line(ctx, &detail, Some(&source), os_code);
    tracing::error!(
        context = %ctx,
        detail = %detail,
        source = %raw_source,
        os_error = ?os_code,
        report = ?report,
        "IOCP error report"
    );
    io::Error::other(msg)
}

pub(crate) fn io_msg(ctx: IocpErrorContext, detail: impl Into<String>) -> io::Error {
    let detail = detail.into();
    let report = Report::new(ctx).attach(detail.clone());
    let msg = structured_line(ctx, &detail, None, None);
    tracing::error!(
        context = %ctx,
        detail = %detail,
        report = ?report,
        "IOCP error report"
    );
    io::Error::other(msg)
}
