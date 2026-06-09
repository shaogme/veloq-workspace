use super::{HEAP_REGISTRATION_CACHE_LIMIT, REGISTER_FAILURE_RETRY_COOLDOWN, RioRegistry};
use crate::rio::RioEnv;
use crate::rio::core::{RioBufferId, RioProvider};
use crate::rio::error::{RioError, RioResult};
use diagweave::prelude::*;
use std::time::Instant;
use veloq_buf::heap::ChunkId;
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
    pub(crate) id: ChunkId,
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

    fn acquire_ref(&mut self) -> RioResult<()> {
        self.active_refs = self.active_refs.checked_add(1).ok_or_else(|| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("rio_buffer_id", self.id.0 as usize)
                .with_ctx("active_refs", self.active_refs)
                .attach_note("RIO buffer registration active refcount overflow")
        })?;
        Ok(())
    }

    fn release_ref(&mut self) -> bool {
        if self.active_refs == 0 {
            return false;
        }
        self.active_refs -= 1;
        true
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct RioChunkRegistration {
    pub(crate) generation: u64,
    pub(crate) registration: RioBufferRegistration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RioChunkRegistrationLocation {
    Current,
    Retired,
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
            Some(lease) => Ok((
                lease.id,
                info.offset,
                Some(RioBufferLeaseToken::Chunk(lease)),
            )),
            None => RioError::Internal
                .with_ctx("chunk_id", info.id.raw())
                .attach_note("RIO chunk not registered"),
        }
    }

    pub(crate) fn register_chunk(
        &mut self,
        id: ChunkId,
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
                .with_ctx("chunk_id", id.raw())
                .attach_note("RIO chunk registration skipped due to recent failure");
        }

        let (ptr, len) = mem;
        let id_idx = id.as_usize();

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
                    .with_ctx("chunk_id", id.raw())
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

    pub(crate) fn current_chunk_lease(&self, id: ChunkId) -> Option<RioChunkLeaseToken> {
        let entry = self.chunk_registry.get(id.as_usize())?.as_ref()?;
        Some(RioChunkLeaseToken {
            key: RioChunkRegistrationKey {
                id,
                generation: entry.generation,
            },
            id: entry.registration.id,
        })
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

    pub(crate) fn acquire_buffer_lease(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
    ) -> RioResult<()> {
        let Some(lease) = lease else {
            return Ok(());
        };
        match lease {
            RioBufferLeaseToken::Chunk(lease) => self.acquire_chunk_lease(lease),
            RioBufferLeaseToken::Heap(lease) => self.acquire_heap_lease(lease),
        }
    }

    pub(crate) fn acquire_chunk_lease(&mut self, lease: RioChunkLeaseToken) -> RioResult<()> {
        let (_location, entry) = self.chunk_registration_for_lease_mut(lease, "acquire")?;
        entry.acquire_ref()
    }

    pub(crate) fn acquire_heap_lease(&mut self, lease: RioHeapLeaseToken) -> RioResult<()> {
        let entry = self.heap_registration_for_lease_mut(lease, "acquire")?;
        entry.acquire_ref()
    }

    pub(crate) fn release_buffer_lease(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
        env: RioEnv<'_>,
    ) -> RioResult<()> {
        if let Some(id) = self.release_buffer_lease_inner(lease)? {
            env.dispatch.deregister_buffer(id);
        }
        Ok(())
    }

    pub(crate) fn release_buffer_lease_deferred(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
    ) -> RioResult<()> {
        if let Some(id) = self.release_buffer_lease_inner(lease)? {
            self.pending_deregistrations.push(id);
        }
        Ok(())
    }

    pub(crate) fn release_buffer_lease_inner(
        &mut self,
        lease: Option<RioBufferLeaseToken>,
    ) -> RioResult<Option<RioBufferId>> {
        let Some(lease) = lease else {
            return Ok(None);
        };
        match lease {
            RioBufferLeaseToken::Chunk(lease) => self.release_chunk_lease_inner(lease),
            RioBufferLeaseToken::Heap(lease) => self.release_heap_lease_inner(lease),
        }
    }

    pub(crate) fn release_chunk_lease_inner(
        &mut self,
        lease: RioChunkLeaseToken,
    ) -> RioResult<Option<RioBufferId>> {
        let (location, remove) = {
            let (location, entry) = self.chunk_registration_for_lease_mut(lease, "release")?;
            if !entry.release_ref() {
                return Err(Self::chunk_lease_error(
                    "release",
                    lease,
                    "RIO chunk lease release found no active refs",
                ));
            }
            (location, entry.active_refs == 0 && entry.retired)
        };

        if !remove {
            return Ok(None);
        }

        let deregister = match location {
            RioChunkRegistrationLocation::Current => self
                .chunk_registry
                .get_mut(lease.key.id.as_usize())
                .and_then(Option::take)
                .map(|entry| entry.registration.id),
            RioChunkRegistrationLocation::Retired => self
                .retired_chunk_registrations
                .remove(&lease.key)
                .map(|entry| entry.id),
        };
        Ok(deregister)
    }

    pub(crate) fn release_heap_lease_inner(
        &mut self,
        lease: RioHeapLeaseToken,
    ) -> RioResult<Option<RioBufferId>> {
        let remove = {
            let entry = self.heap_registration_for_lease_mut(lease, "release")?;
            if !entry.release_ref() {
                return Err(Self::heap_lease_error(
                    "release",
                    lease,
                    "RIO heap lease release found no active refs",
                ));
            }
            entry.active_refs == 0 && entry.retired
        };

        if remove {
            return Ok(self.heap_rio_bufs.remove(&lease.key).map(|entry| entry.id));
        }
        Ok(None)
    }

    pub(crate) fn current_chunk_registration_mut(
        &mut self,
        key: RioChunkRegistrationKey,
    ) -> Option<&mut RioBufferRegistration> {
        self.chunk_registry
            .get_mut(key.id.as_usize())?
            .as_mut()
            .filter(|entry| entry.generation == key.generation)
            .map(|entry| &mut entry.registration)
    }

    fn chunk_registration_for_lease_mut(
        &mut self,
        lease: RioChunkLeaseToken,
        action: &'static str,
    ) -> RioResult<(RioChunkRegistrationLocation, &mut RioBufferRegistration)> {
        let location = self.locate_chunk_lease(lease, action)?;
        match location {
            RioChunkRegistrationLocation::Current => {
                let entry = self
                    .current_chunk_registration_mut(lease.key)
                    .ok_or_else(|| {
                        Self::chunk_lease_error(
                            action,
                            lease,
                            "current RIO chunk registration disappeared after lease lookup",
                        )
                    })?;
                Ok((location, entry))
            }
            RioChunkRegistrationLocation::Retired => {
                let entry = self
                    .retired_chunk_registrations
                    .get_mut(&lease.key)
                    .ok_or_else(|| {
                        Self::chunk_lease_error(
                            action,
                            lease,
                            "retired RIO chunk registration disappeared after lease lookup",
                        )
                    })?;
                Ok((location, entry))
            }
        }
    }

    fn locate_chunk_lease(
        &self,
        lease: RioChunkLeaseToken,
        action: &'static str,
    ) -> RioResult<RioChunkRegistrationLocation> {
        if let Some(entry) = self
            .chunk_registry
            .get(lease.key.id.as_usize())
            .and_then(Option::as_ref)
            .filter(|entry| entry.generation == lease.key.generation)
        {
            if entry.registration.id != lease.id {
                return Err(Self::chunk_lease_error(
                    action,
                    lease,
                    "RIO chunk lease buffer id is stale",
                ));
            }
            return Ok(RioChunkRegistrationLocation::Current);
        }

        let Some(entry) = self.retired_chunk_registrations.get(&lease.key) else {
            return Err(Self::chunk_lease_error(
                action,
                lease,
                "RIO chunk lease registration is unknown",
            ));
        };
        if entry.id != lease.id {
            return Err(Self::chunk_lease_error(
                action,
                lease,
                "retired RIO chunk lease buffer id is stale",
            ));
        }
        Ok(RioChunkRegistrationLocation::Retired)
    }

    fn heap_registration_for_lease_mut(
        &mut self,
        lease: RioHeapLeaseToken,
        action: &'static str,
    ) -> RioResult<&mut RioBufferRegistration> {
        let Some(entry) = self.heap_rio_bufs.get_mut(&lease.key) else {
            return Err(Self::heap_lease_error(
                action,
                lease,
                "RIO heap lease registration is unknown",
            ));
        };
        if entry.id != lease.id {
            return Err(Self::heap_lease_error(
                action,
                lease,
                "RIO heap lease buffer id is stale",
            ));
        }
        Ok(entry)
    }

    fn chunk_lease_error(
        action: &'static str,
        lease: RioChunkLeaseToken,
        note: &'static str,
    ) -> Report<RioError> {
        RioError::Internal
            .to_report()
            .with_ctx("rio_buffer_lease_action", action)
            .with_ctx("chunk_id", lease.key.id.as_usize())
            .with_ctx("chunk_generation", lease.key.generation)
            .with_ctx("rio_buffer_id", lease.id.0 as usize)
            .attach_note(note)
    }

    fn heap_lease_error(
        action: &'static str,
        lease: RioHeapLeaseToken,
        note: &'static str,
    ) -> Report<RioError> {
        RioError::Internal
            .to_report()
            .with_ctx("rio_buffer_lease_action", action)
            .with_ctx("buffer_ptr", lease.key.ptr)
            .with_ctx("buffer_capacity", lease.key.cap)
            .with_ctx("buffer_cookie", lease.key.cookie)
            .with_ctx("rio_buffer_id", lease.id.0 as usize)
            .attach_note(note)
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
    use super::super::test_helpers::*;
    use super::*;
    use std::sync::atomic::Ordering;

    #[test]
    fn rio_chunk_retired_registration_waits_for_last_lease() {
        let _guard = lock_dispatch_state();
        reset_dispatch_state();
        let dispatch = test_dispatch();
        let env = test_env(&dispatch);
        let mut registry = RioRegistry::new(32, 1);
        let chunk_id = ChunkId::from_raw(3);
        let chunk_index = chunk_id.as_usize();
        let key = RioChunkRegistrationKey {
            id: chunk_id,
            generation: 1,
        };
        registry.chunk_registry.resize(chunk_index + 1, None);
        registry.chunk_registry[chunk_index] = Some(RioChunkRegistration {
            generation: key.generation,
            registration: RioBufferRegistration::new(RioBufferId(41 as _)),
        });
        let lease = registry
            .current_chunk_lease(chunk_id)
            .map(RioBufferLeaseToken::Chunk);

        registry
            .acquire_buffer_lease(lease)
            .expect("chunk lease acquire should succeed");
        let previous = registry.chunk_registry[chunk_index]
            .take()
            .expect("chunk registration");
        registry.retire_chunk_registration(key, previous.registration, env);

        assert!(deregistered_ids().is_empty());
        assert!(registry.retired_chunk_registrations.contains_key(&key));

        registry
            .release_buffer_lease(lease, env)
            .expect("chunk lease release should succeed");

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

        registry
            .acquire_buffer_lease(lease)
            .expect("heap lease acquire should succeed");
        registry
            .heap_rio_bufs
            .get_mut(&key)
            .expect("heap registration")
            .retired = true;
        registry
            .release_buffer_lease(lease, env)
            .expect("heap lease release should succeed");

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
        let chunk_id = ChunkId::from_raw(2);
        let chunk_index = chunk_id.as_usize();
        registry.chunk_registry.resize(chunk_index + 1, None);
        registry.chunk_registry[chunk_index] = Some(RioChunkRegistration {
            generation: 1,
            registration: RioBufferRegistration::new(RioBufferId(55 as _)),
        });
        let byte = 0_u8;
        REGISTER_FAILS.store(true, Ordering::SeqCst);

        registry
            .register_chunk(chunk_id, (&byte as *const u8, 1), env)
            .expect_err("failed registration should be reported");

        let current = registry.chunk_registry[chunk_index]
            .expect("existing chunk registration should remain current");
        assert_eq!(current.registration.id, RioBufferId(55 as _));
        assert!(registry.pending_deregistrations.is_empty());
        assert!(deregistered_ids().is_empty());
    }
}
