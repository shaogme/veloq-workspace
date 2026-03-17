use std::io;
use veloq_driver_core::driver::{
    CompletionEvent, CompletionRecord, CompletionSidecar, SharedCompletionQueue,
    SharedCompletionTable, encode_completion_token,
};

#[inline]
pub(crate) fn io_result_to_event_res(res: &io::Result<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => -e.raw_os_error().unwrap_or(1),
    }
}

#[inline]
pub(crate) fn completion_record(sidecar: CompletionSidecar) -> CompletionRecord {
    CompletionRecord {
        event: CompletionEvent {
            user_data: encode_completion_token(sidecar.user_data, sidecar.generation),
            res: sidecar.res,
            flags: sidecar.flags,
        },
        payload: sidecar.payload,
        detail: sidecar.detail,
    }
}

#[inline]
pub(crate) fn push_completion_event_shared(
    queue: &SharedCompletionQueue,
    table: &SharedCompletionTable,
    record: CompletionRecord,
) {
    table.record_completion_with_data(record.event, record.payload, record.detail);
    queue.push(record.event);
}
