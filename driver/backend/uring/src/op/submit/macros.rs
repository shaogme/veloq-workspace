macro_rules! impl_lifecycle {
    ($drop_fn:ident, $variant:ident, direct_fd) => {
        pub(crate) unsafe fn $drop_fn(_op: &mut UringOp) {}
    };
    ($drop_fn:ident, $variant:ident, nested_fd) => {
        pub(crate) unsafe fn $drop_fn(_op: &mut UringOp) {}
    };
    ($drop_fn:ident, $variant:ident, no_fd) => {
        pub(crate) unsafe fn $drop_fn(_op: &mut UringOp) {}
    };
}

macro_rules! impl_default_completion {
    ($fn_name:ident) => {
        pub(crate) unsafe fn $fn_name(
            _op: &mut UringOp,
            _payload: &mut UringUserPayload,
            result: i32,
        ) -> DriverResult<usize> {
            if result >= 0 {
                Ok(result as usize)
            } else {
                Err(UringError::CompletionWait
                    .report(
                        concat!("uring.op.submit.", stringify!($fn_name)),
                        "kernel completion returned error",
                    )
                    .set_error_code(-result))
            }
        }
    };
}
