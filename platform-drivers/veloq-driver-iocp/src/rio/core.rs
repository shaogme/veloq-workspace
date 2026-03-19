//! Core context encoding, registry ownership, and kernel dispatch wrappers.

pub(crate) mod registry;
pub(crate) mod submit_ops;

use crate::rio::RioState;
use crate::rio::runtime::pool::POOL_CTX_TAG;
use std::io;

#[derive(Clone, Copy)]
pub(crate) enum RioCompletionKind {
    Pool {
        actor_id: u32,
        generation: u32,
    },
    Op {
        user_data: usize,
        generation: u32,
        ctx_ptr: *mut RioOpRequestContext,
    },
}

#[repr(C)]
pub(crate) struct RioOpRequestContext {
    pub(crate) user_data: usize,
    pub(crate) generation: u32,
}

pub(crate) struct RioOpCtxGuard(pub(crate) *mut RioOpRequestContext);

impl Drop for RioOpCtxGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            unsafe { drop(Box::from_raw(self.0)) };
            self.0 = std::ptr::null_mut();
        }
    }
}

#[inline]
pub(crate) fn rio_result_to_event_res(res: &io::Result<usize>) -> i32 {
    match res {
        Ok(v) => (*v).min(i32::MAX as usize) as i32,
        Err(e) => -e.raw_os_error().unwrap_or(1).abs(),
    }
}

impl RioState {
    #[inline]
    pub(crate) fn encode_req_ctx(user_data: usize, generation: u32) -> *const std::ffi::c_void {
        let ctx = Box::new(RioOpRequestContext {
            user_data,
            generation,
        });
        let raw = Box::into_raw(ctx);
        debug_assert_eq!((raw as usize) & POOL_CTX_TAG, 0);
        raw.cast::<std::ffi::c_void>()
    }

    #[inline]
    pub(crate) fn decode_req_ctx(ctx: u64) -> Option<RioCompletionKind> {
        if ctx == 0 {
            return None;
        }
        let raw = ctx as usize;
        if (raw & POOL_CTX_TAG) == POOL_CTX_TAG {
            let token = ((raw >> 1) & 0xffff_ffff) as u32;
            let actor_id = ((raw >> 33) & 0xffff_ffff) as u32;
            if token == 0 || actor_id == 0 {
                return None;
            }
            return Some(RioCompletionKind::Pool {
                actor_id,
                generation: token,
            });
        }
        let ctx_ptr = raw as *mut RioOpRequestContext;
        if ctx_ptr.is_null() {
            return None;
        }
        let op_ctx = unsafe { &*ctx_ptr };
        Some(RioCompletionKind::Op {
            user_data: op_ctx.user_data,
            generation: op_ctx.generation,
            ctx_ptr,
        })
    }

    #[inline]
    pub(crate) fn free_op_req_ctx(ctx: u64) {
        if ctx == 0 {
            return;
        }
        let raw = ctx as usize;
        if (raw & POOL_CTX_TAG) == POOL_CTX_TAG {
            return;
        }
        let ptr = raw as *mut RioOpRequestContext;
        if !ptr.is_null() {
            unsafe { drop(Box::from_raw(ptr)) };
        }
    }

    #[inline]
    pub(crate) fn decode_pool_context(ctx: u64) -> Option<(u32, u32)> {
        if ctx == 0 {
            return None;
        }
        let raw = ctx as usize;
        if (raw & POOL_CTX_TAG) != POOL_CTX_TAG {
            return None;
        }
        let token = ((raw >> 1) & 0xffff_ffff) as u32;
        let actor_id = ((raw >> 33) & 0xffff_ffff) as u32;
        if token == 0 || actor_id == 0 {
            return None;
        }
        Some((actor_id, token))
    }

    pub(crate) fn last_wsa_error() -> io::Error {
        io::Error::from_raw_os_error(unsafe {
            windows_sys::Win32::Networking::WinSock::WSAGetLastError()
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_ctx_roundtrip_decode_and_free() {
        let ptr = RioState::encode_req_ctx(11, 17);
        let decoded = RioState::decode_req_ctx(ptr as u64);
        assert!(matches!(
            decoded,
            Some(RioCompletionKind::Op {
                user_data: 11,
                generation: 17,
                ..
            })
        ));
        RioState::free_op_req_ctx(ptr as u64);
    }

    #[test]
    fn pool_ctx_decode_valid() {
        let token = 7_u32;
        let actor_id = 9_u32;
        let raw = (((actor_id as usize) << 33) | ((token as usize) << 1) | POOL_CTX_TAG) as u64;
        assert_eq!(RioState::decode_pool_context(raw), Some((actor_id, token)));
        assert!(matches!(
            RioState::decode_req_ctx(raw),
            Some(RioCompletionKind::Pool {
                actor_id: 9,
                generation: 7,
            })
        ));
    }

    #[test]
    fn pool_ctx_decode_rejects_invalid_zero_fields() {
        let zero_token = (((9_usize) << 33) | POOL_CTX_TAG) as u64;
        let zero_actor = (((7_usize) << 1) | POOL_CTX_TAG) as u64;
        assert!(RioState::decode_pool_context(zero_token).is_none());
        assert!(RioState::decode_pool_context(zero_actor).is_none());
        assert!(RioState::decode_req_ctx(zero_token).is_none());
        assert!(RioState::decode_req_ctx(zero_actor).is_none());
    }

    #[test]
    fn rio_result_translation_behaviour() {
        assert_eq!(rio_result_to_event_res(&Ok(5)), 5);
        assert_eq!(
            rio_result_to_event_res(&Ok((i32::MAX as usize) + 10)),
            i32::MAX
        );
        let err = io::Error::from_raw_os_error(10022);
        assert_eq!(rio_result_to_event_res(&Err(err)), -10022);
    }

    #[test]
    fn free_pool_tagged_context_is_noop() {
        let tagged = (((1_usize) << 33) | ((1_usize) << 1) | POOL_CTX_TAG) as u64;
        RioState::free_op_req_ctx(tagged);
    }
}
