//! Core context encoding, registry ownership, and kernel dispatch wrappers.

pub(crate) mod registry;
pub(crate) mod submit_ops;

use crate::rio::runtime::pool::POOL_CTX_TAG;
use crate::rio::{ActorKey, RioState};
use crate::rio::error::{RioError, RioDiag};
use error_stack::Report;
use std::io;

#[derive(Clone, Copy)]
pub(crate) enum RioCompletionKind {
    Pool {
        actor_key: ActorKey,
        generation: u32,
        ctx_ptr: *mut RioPoolRequestContext,
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

#[repr(C)]
pub(crate) struct RioPoolRequestContext {
    pub(crate) actor_key: ActorKey,
    pub(crate) generation: u32,
}

pub(crate) struct RioOpCtxGuard(pub(crate) *mut RioOpRequestContext);
pub(crate) struct RioPoolCtxGuard(pub(crate) *mut RioPoolRequestContext);

impl Drop for RioOpCtxGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: self.0 was created from Box::into_raw in encode_req_ctx.
            unsafe { drop(Box::from_raw(self.0)) };
            self.0 = std::ptr::null_mut();
        }
    }
}

impl Drop for RioPoolCtxGuard {
    fn drop(&mut self) {
        if !self.0.is_null() {
            // SAFETY: self.0 was created from Box::into_raw in encode_pool_req_ctx.
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
            let ctx_ptr = (raw & !POOL_CTX_TAG) as *mut RioPoolRequestContext;
            if ctx_ptr.is_null() {
                return None;
            }
            // SAFETY: ctx_ptr is a valid pointer to RioPoolRequestContext if pool tagged.
            let pool_ctx = unsafe { &*ctx_ptr };
            return Some(RioCompletionKind::Pool {
                actor_key: pool_ctx.actor_key,
                generation: pool_ctx.generation,
                ctx_ptr,
            });
        }
        let ctx_ptr = raw as *mut RioOpRequestContext;
        if ctx_ptr.is_null() {
            return None;
        }
        // SAFETY: ctx_ptr is a valid pointer to RioOpRequestContext if it's not a pool context.
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
            // SAFETY: ptr was created from Box::into_raw in encode_req_ctx.
            unsafe { drop(Box::from_raw(ptr)) };
        }
    }

    #[inline]
    pub(crate) fn encode_pool_req_ctx(
        actor_key: ActorKey,
        generation: u32,
    ) -> *const std::ffi::c_void {
        let ctx = Box::new(RioPoolRequestContext {
            actor_key,
            generation,
        });
        let raw = Box::into_raw(ctx) as usize;
        debug_assert_eq!(raw & POOL_CTX_TAG, 0);
        (raw | POOL_CTX_TAG) as *const std::ffi::c_void
    }
    
    #[inline]
    pub(crate) fn last_wsa_error() -> io::Error {
        // SAFETY: WSAGetLastError is safe to call.
        io::Error::from_raw_os_error(unsafe {
            windows_sys::Win32::Networking::WinSock::WSAGetLastError()
        })
    }

    pub(crate) fn last_wsa_report(context: RioError, scope: &'static str) -> Report<RioError> {
        let err = Self::last_wsa_error();
        let code = err.raw_os_error().map(|c| c as u32).unwrap_or(0);
        let diag = RioDiag::new(scope)
            .with_error(code, &err);
        error_stack::Report::new(context).attach(diag)
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
        let actor_key = ActorKey::default();
        let raw = RioState::encode_pool_req_ctx(actor_key, token) as u64;
        let decoded = RioState::decode_req_ctx(raw);
        match decoded {
            Some(RioCompletionKind::Pool {
                actor_key: k,
                generation,
                ctx_ptr,
            }) => {
                assert_eq!(k, actor_key);
                assert_eq!(generation, token);
                let _guard = RioPoolCtxGuard(ctx_ptr);
            }
            _ => panic!("pool context should decode"),
        }
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
        let tagged = 1_u64;
        RioState::free_op_req_ctx(tagged);
    }
}
