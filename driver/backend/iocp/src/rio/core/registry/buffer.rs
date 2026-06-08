use super::{HEAP_REGISTRATION_CACHE_LIMIT, REGISTER_FAILURE_RETRY_COOLDOWN, RioRegistry};
use crate::rio::RioEnv;
use crate::rio::core::submit_ops::{RioBufferId, RioProvider};
use crate::rio::error::{RioError, RioResult};
use diagweave::prelude::*;
use std::time::Instant;
use veloq_buf::{FixedBuf, PoolKind};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RioHeapBufferKey {
    pub(crate) ptr: usize,
    pub(crate) cap: usize,
    pub(crate) cookie: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioHeapLeaseToken {
    pub(crate) key: RioHeapBufferKey,
    pub(crate) id: RioBufferId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) struct RioChunkRegistrationKey {
    pub(crate) id: u16,
    pub(crate) generation: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RioChunkLeaseToken {
    pub(crate) key: RioChunkRegistrationKey,
    pub(crate) id: RioBufferId,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RioBufferLeaseToken {
    Chunk(RioChunkLeaseToken),
    Heap(RioHeapLeaseToken),
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RioBufferRegistration {
    pub(crate) id: RioBufferId,
    pub(crate) active_refs: usize,
    pub(crate) retired: bool,
}

impl RioBufferRegistration {
    pub(crate) fn new(id: RioBufferId) -> Self {
        Self {
            id,
            active_refs: 0,
            retired: false,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RioChunkRegistration {
    pub(crate) generation: u64,
    pub(crate) registration: RioBufferRegistration,
}

impl RioRegistry {
    pub(crate) fn resolve_buffer_id(
        &mut self,
        buf: &FixedBuf,
        env: RioEnv<'_>,
    ) -> RioResult<(RioBufferId, usize, Option<RioBufferLeaseToken>)> {
        let info = buf.resolve_region_info();

        if info.pool_kind == PoolKind::Heap {
            return self.resolve_heap_id(buf, info.offset, env);
        }

        let mut lease = self.current_chunk_lease(info.id);

        if lease.is_none()
            && let Some(chunk_info) = env.registrar.resolve_chunk_info(info.id)
        {
            self.register_chunk(
                info.id,
                (chunk_info.ptr.as_ptr(), chunk_info.len.get()),
                env,
            )?;
            lease = self.current_chunk_lease(info.id);
        }

        match lease {
            Some(RioBufferLeaseToken::Chunk(lease)) => Ok((
                lease.id,
                info.offset,
                Some(RioBufferLeaseToken::Chunk(lease)),
            )),
            Some(RioBufferLeaseToken::Heap(_)) => {
                debug_assert!(false, "resolved heap lease from chunk registry");
                RioError::Internal
                    .with_ctx("chunk_id", info.id as usize)
                    .attach_note("RIO chunk registration resolved to heap lease")
            }
            None => RioError::Internal
                .with_ctx("chunk_id", info.id as usize)
                .attach_note("RIO chunk not registered"),
        }
    }

    pub(crate) fn register_chunk(
        &mut self,
        id: u16,
        mem: (*const u8, usize),
        env: RioEnv<'_>,
    ) -> RioResult<()> {
        if let Some(last_fail) = self.chunk_register_failures_recent.get(&id)
            && last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN
        {
            self.registration_stats
                .chunk_register_skipped_recent_failure = self
                .registration_stats
                .chunk_register_skipped_recent_failure
                .saturating_add(1);
            return RioError::ResourceExhaustion
                .with_ctx("chunk_id", id as usize)
                .attach_note("RIO chunk registration skipped due to recent failure");
        }

        let (ptr, len) = mem;
        let id_idx = id as usize;

        if id_idx >= self.chunk_registry.len() {
            self.chunk_registry.resize(id_idx + 1, None);
        }

        self.registration_stats.chunk_register_attempts = self
            .registration_stats
            .chunk_register_attempts
            .saturating_add(1);

        let buf_id = match env.dispatch.register_buffer(ptr, len as u32) {
            Ok(id) => id,
            Err(e) => {
                self.registration_stats.chunk_register_failures = self
                    .registration_stats
                    .chunk_register_failures
                    .saturating_add(1);
                self.chunk_register_failures_recent
                    .insert(id, Instant::now());
                return Err(e)
                    .with_ctx("chunk_id", id as usize)
                    .with_ctx("buffer_length", len)
                    .attach_note("RIORegisterBuffer failed for chunk");
            }
        };

        let generation = self.next_registration_generation();
        let previous = self.chunk_registry[id_idx].replace(RioChunkRegistration {
            generation,
            registration: RioBufferRegistration::new(buf_id),
        });
        if let Some(previous) = previous {
            let key = RioChunkRegistrationKey {
                id,
                generation: previous.generation,
            };
            self.retire_chunk_registration(key, previous.registration, env);
        }
        self.chunk_register_failures_recent.remove(&id);
        self.registration_stats.chunk_register_success = self
            .registration_stats
            .chunk_register_success
            .saturating_add(1);
        Ok(())
    }

    pub(crate) fn next_registration_generation(&mut self) -> u64 {
        self.next_registration_generation = self.next_registration_generation.wrapping_add(1);
        if self.next_registration_generation == 0 {
            self.next_registration_generation = 1;
        }
        self.next_registration_generation
    }

    pub(crate) fn current_chunk_lease(&self, id: u16) -> Option<RioBufferLeaseToken> {
        let entry = self.chunk_registry.get(id as usize)?.as_ref()?;
        Some(RioBufferLeaseToken::Chunk(RioChunkLeaseToken {
            key: RioChunkRegistrationKey {
                id,
                generation: entry.generation,
            },
            id: entry.registration.id,
        }))
    }

    pub(crate) fn retire_chunk_registration(
        &mut self,
        key: RioChunkRegistrationKey,
        mut registration: RioBufferRegistration,
        env: RioEnv<'_>,
    ) {
        if registration.active_refs == 0 {
            env.dispatch.deregister_buffer(registration.id);
            return;
        }
        registration.retired = true;
        self.retired_chunk_registrations.insert(key, registration);
    }

    pub(crate) fn resolve_heap_id(
        &mut self,
        buf: &FixedBuf,
        offset: usize,
        env: RioEnv<'_>,
    ) -> RioResult<(RioBufferId, usize, Option<RioBufferLeaseToken>)> {
        let key = RioHeapBufferKey {
            ptr: buf.as_ptr() as usize,
            cap: buf.capacity(),
            cookie: buf.resolve_region_info().cookie,
        };
        if let Some(entry) = self.heap_rio_bufs.get(&key) {
            let lease = RioHeapLeaseToken { key, id: entry.id };
            return Ok((entry.id, offset, Some(RioBufferLeaseToken::Heap(lease))));
        }

        if let Some(last_fail) = self.heap_register_failures_recent.get(&key)
            && last_fail.elapsed() < REGISTER_FAILURE_RETRY_COOLDOWN
        {
            self.registration_stats.heap_register_skipped_recent_failure = self
                .registration_stats
                .heap_register_skipped_recent_failure
                .saturating_add(1);
            return RioError::ResourceExhaustion
                .with_ctx("registration_mode", env.registration_mode.as_str())
                .with_ctx("buffer_ptr", key.ptr)
                .with_ctx("buffer_capacity", key.cap)
                .with_ctx("buffer_cookie", key.cookie)
                .attach_note("RIO heap registration skipped due to recent failure");
        }

        self.retire_heap_cache_for_insert(env);

        let id = self.register_heap_raw(buf, key, env)?;
        let lease = RioHeapLeaseToken { key, id };
        Ok((id, offset, Some(RioBufferLeaseToken::Heap(lease))))
    }

    pub(crate) fn register_heap_raw(
        &mut self,
        buf: &FixedBuf,
        key: RioHeapBufferKey,
        env: RioEnv<'_>,
    ) -> RioResult<RioBufferId> {
        self.registration_stats.heap_register_attempts = self
            .registration_stats
            .heap_register_attempts
            .saturating_add(1);

        let id = match env
            .dispatch
            .register_buffer(buf.as_ptr(), buf.capacity() as u32)
        {
            Ok(id) => id,
            Err(e) => {
                self.registration_stats.heap_register_failures = self
                    .registration_stats
                    .heap_register_failures
                    .saturating_add(1);
                self.heap_register_failures_recent
                    .insert(key, Instant::now());
                return Err(e)
                    .with_ctx("registration_mode", env.registration_mode.as_str())
                    .with_ctx("buffer_ptr", key.ptr)
                    .with_ctx("buffer_capacity", key.cap)
                    .with_ctx("buffer_cookie", key.cookie)
                    .attach_note("RIORegisterBuffer failed for heap buffer");
            }
        };

        self.heap_rio_bufs
            .insert(key, RioBufferRegistration::new(id));
        self.heap_register_failures_recent.remove(&key);
        self.registration_stats.heap_register_success = self
            .registration_stats
            .heap_register_success
            .saturating_add(1);
        Ok(id)
    }

    pub(crate) fn commit_buffer_lease(&mut self, lease: Option<RioBufferLeaseToken>) {
        let Some(lease) = lease else {
            return;
        };
        match lease {
            RioBufferLeaseToken::Chunk(lease) => self.commit_chunk_lease(lease),
            RioBufferLeaseToken::Heap(lease) => self.commit_heap_lease(lease),
        }
    }

    pub(crate) fn commit_chunk_lease(&mut self, lease: RioChunkLeaseToken) {
        if let Some(entry) = self.current_chunk_registration_mut(lease.key) {
            if entry.id != lease.id {
                debug_assert!(false, "committed stale RIO chunk lease");
                return;
            }
            entry.active_refs = entry.active_refs.saturating_add(1);
            return;
        }

        let Some(entry) = self.retired_chunk_registrations.get_mut(&lease.key) else {
            debug_assert!(false, "committed unknown RIO chunk lease");
            return;
        };
        if entry.id != lease.id {
            debug_assert!(false, "committed stale RIO chunk lease");
            return;
        }
        entry.active_refs = entry.active_refs.saturating_add(1);
    }

    pub(crate) fn commit_heap_lease(&mut self, lease: RioHeapLeaseToken) {
        let Some(entry) = self.heap_rio_bufs.get_mut(&lease.key) else {
            debug_assert!(false, "committed unknown RIO heap lease");
            return;
        };
        if entry.id != lease.id {
            debug_assert!(false, "committed stale RIO heap lease");
            return;
        }
        entry.active_refs = entry.active_refs.saturating_add(1);
    }

    pub(crate) fn release_buffer_lease(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
        env: RioEnv<'_>,
    ) {
        if let Some(id) = self.release_buffer_lease_inner(lease) {
            env.dispatch.deregister_buffer(id);
        }
    }

    pub(crate) fn release_buffer_lease_deferred(&mut self, lease: Option<RioBufferLeaseToken>) {
        if let Some(id) = self.release_buffer_lease_inner(lease) {
            self.pending_deregistrations.push(id);
        }
    }

    pub(crate) fn release_buffer_lease_inner(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
    ) -> Option<RioBufferId> {
        let lease = lease?;
        match lease {
            RioBufferLeaseToken::Chunk(lease) => self.release_chunk_lease_inner(lease),
            RioBufferLeaseToken::Heap(lease) => self.release_heap_lease_inner(lease),
        }
    }

    pub(crate) fn release_chunk_lease_inner(
        &mut self,
        lease: RioChunkLeaseToken,
    ) -> Option<RioBufferId> {
        let remove_current;
        if let Some(entry) = self.current_chunk_registration_mut(lease.key) {
            if entry.id != lease.id {
                debug_assert!(false, "released stale RIO chunk lease");
                return None;
            }
            debug_assert!(entry.active_refs > 0, "released inactive RIO chunk lease");
            if entry.active_refs > 0 {
                entry.active_refs -= 1;
            }
            remove_current = entry.active_refs == 0 && entry.retired;
        } else if let Some(entry) = self.retired_chunk_registrations.get_mut(&lease.key) {
            if entry.id != lease.id {
                debug_assert!(false, "released stale RIO chunk lease");
                return None;
            }
            debug_assert!(entry.active_refs > 0, "released inactive RIO chunk lease");
            if entry.active_refs > 0 {
                entry.active_refs -= 1;
            }
            if entry.active_refs == 0 && entry.retired {
                return self
                    .retired_chunk_registrations
                    .remove(&lease.key)
                    .map(|entry| entry.id);
            }
            return None;
        } else {
            debug_assert!(false, "released unknown RIO chunk lease");
            return None;
        }

        if remove_current {
            return self
                .chunk_registry
                .get_mut(lease.key.id as usize)
                .and_then(Option::take)
                .map(|entry| entry.registration.id);
        }
        None
    }

    pub(crate) fn release_heap_lease_inner(
        &mut self,
        lease: RioHeapLeaseToken,
    ) -> Option<RioBufferId> {
        let mut remove = false;
        if let Some(entry) = self.heap_rio_bufs.get_mut(&lease.key) {
            if entry.id != lease.id {
                debug_assert!(false, "released stale RIO heap lease");
                return None;
            }
            debug_assert!(entry.active_refs > 0, "released inactive RIO heap lease");
            if entry.active_refs > 0 {
                entry.active_refs -= 1;
            }
            remove = entry.active_refs == 0 && entry.retired;
        }

        if remove {
            return self.heap_rio_bufs.remove(&lease.key).map(|entry| entry.id);
        }
        None
    }

    pub(crate) fn current_chunk_registration_mut(
        &mut self,
        key: RioChunkRegistrationKey,
    ) -> Option<&mut RioBufferRegistration> {
        self.chunk_registry
            .get_mut(key.id as usize)?
            .as_mut()
            .filter(|entry| entry.generation == key.generation)
            .map(|entry| &mut entry.registration)
    }

    pub(crate) fn retire_heap_cache_for_insert(&mut self, env: RioEnv<'_>) {
        if self.heap_rio_bufs.len() < HEAP_REGISTRATION_CACHE_LIMIT {
            return;
        }

        let mut idle_keys = Vec::new();
        for (key, entry) in &mut self.heap_rio_bufs {
            if entry.active_refs == 0 {
                idle_keys.push(*key);
            } else {
                entry.retired = true;
            }
        }

        for key in idle_keys {
            if let Some(entry) = self.heap_rio_bufs.remove(&key) {
                env.dispatch.deregister_buffer(entry.id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rio::core::registry::test_helpers::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn rio_chunk_retired_registration_waits_for_last_lease() {
        let _guard = lock_dispatch_state();
        reset_dispatch_state();
        let dispatch = test_dispatch();
        let env = test_env(&dispatch);
        let mut registry = RioRegistry::new(32, 1);
        let chunk_id = 3;
        let key = RioChunkRegistrationKey {
            id: chunk_id,
            generation: 1,
        };
        registry.chunk_registry.resize(chunk_id as usize + 1, None);
        registry.chunk_registry[chunk_id as usize] = Some(RioChunkRegistration {
            generation: key.generation,
            registration: RioBufferRegistration::new(RioBufferId(41 as _)),
        });
        let lease = registry.current_chunk_lease(chunk_id);

        registry.commit_buffer_lease(lease);
        let previous = registry.chunk_registry[chunk_id as usize]
            .take()
            .expect("chunk registration");
        registry.retire_chunk_registration(key, previous.registration, env);

        assert!(deregistered_ids().is_empty());
        assert!(registry.retired_chunk_registrations.contains_key(&key));

        registry.release_buffer_lease(lease, env);

        assert_eq!(deregistered_ids(), vec![41]);
        assert!(!registry.retired_chunk_registrations.contains_key(&key));
    }

    #[test]
    fn rio_heap_retired_registration_deregisters_on_release() {
        let _guard = lock_dispatch_state();
        reset_dispatch_state();
        let dispatch = test_dispatch();
        let env = test_env(&dispatch);
        let mut registry = RioRegistry::new(32, 1);
        let key = RioHeapBufferKey {
            ptr: 1,
            cap: 8,
            cookie: 13,
        };
        let lease = Some(RioBufferLeaseToken::Heap(RioHeapLeaseToken {
            key,
            id: RioBufferId(77 as _),
        }));
        registry
            .heap_rio_bufs
            .insert(key, RioBufferRegistration::new(RioBufferId(77 as _)));

        registry.commit_buffer_lease(lease);
        registry
            .heap_rio_bufs
            .get_mut(&key)
            .expect("heap registration")
            .retired = true;
        registry.release_buffer_lease(lease, env);

        assert_eq!(deregistered_ids(), vec![77]);
        assert!(!registry.heap_rio_bufs.contains_key(&key));
    }

    #[test]
    fn rio_chunk_register_failure_keeps_existing_registration_current() {
        let _guard = lock_dispatch_state();
        reset_dispatch_state();
        let dispatch = test_dispatch();
        let env = test_env(&dispatch);
        let mut registry = RioRegistry::new(32, 1);
        let chunk_id = 2;
        registry.chunk_registry.resize(chunk_id as usize + 1, None);
        registry.chunk_registry[chunk_id as usize] = Some(RioChunkRegistration {
            generation: 1,
            registration: RioBufferRegistration::new(RioBufferId(55 as _)),
        });
        let byte = 0_u8;
        REGISTER_FAILS.store(true, Ordering::SeqCst);

        registry
            .register_chunk(chunk_id, (&byte as *const u8, 1), env)
            .expect_err("failed registration should be reported");

        let current = registry.chunk_registry[chunk_id as usize]
            .expect("existing chunk registration should remain current");
        assert_eq!(current.registration.id, RioBufferId(55 as _));
        assert!(registry.pending_deregistrations.is_empty());
        assert!(deregistered_ids().is_empty());
    }
}
