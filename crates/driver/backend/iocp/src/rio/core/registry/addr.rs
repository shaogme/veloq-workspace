use crate::{
    net::addr::SockAddrStorage,
    rio::{
        RioEnv,
        core::{RioBufferId, RioProvider},
        error::{RioError, RioResult},
    },
};
use diagweave::prelude::*;
use veloq_pod::{bytes_of, bytes_of_mut};
use veloq_std::{
    boxed::Box,
    collections::HashSet,
    mem::{replace, size_of, size_of_val},
    vec,
    vec::Vec,
};
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, RIO_BUF, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
};

#[derive(Clone, Copy)]
pub(crate) struct RioAddrReservation {
    pub(crate) slot: usize,
    pub(crate) generation: u64,
    pub(crate) rio_buf: RIO_BUF,
}

pub(super) struct RioAddressRegistry {
    addr_slots: Box<[SockAddrStorage]>,
    addr_slot_in_use: Vec<bool>,
    addr_slot_generations: Vec<u64>,
    addr_free_slots: Vec<usize>,
    addr_buffer_id: RioBufferId,
}

impl RioAddressRegistry {
    pub(super) fn new(capacity: usize) -> Self {
        let capacity = capacity.max(1);
        let mut addr_free_slots = Vec::with_capacity(capacity);
        for slot in (0..capacity).rev() {
            addr_free_slots.push(slot);
        }

        Self {
            addr_slots: vec![SockAddrStorage::default(); capacity].into_boxed_slice(),
            addr_slot_in_use: vec![false; capacity],
            addr_slot_generations: vec![0; capacity],
            addr_free_slots,
            addr_buffer_id: RioBufferId::INVALID,
        }
    }

    pub(crate) fn prepare_send_addr(
        &mut self,
        addr: &SockAddrStorage,
        addr_len: i32,
        env: RioEnv<'_>,
    ) -> RioResult<RioAddrReservation> {
        let rio_addr_len = Self::validate_send_addr(addr, addr_len)?;
        let reservation = self.allocate_addr_slot(env)?;
        let copy_len = (addr_len as usize).min(rio_addr_len as usize);
        let destination = &mut self.addr_slots[reservation.slot];
        *destination = SockAddrStorage::default();
        bytes_of_mut(destination)[..copy_len].copy_from_slice(&bytes_of(addr)[..copy_len]);
        Ok(RioAddrReservation {
            rio_buf: RIO_BUF {
                Length: rio_addr_len,
                ..reservation.rio_buf
            },
            ..reservation
        })
    }

    pub(crate) fn prepare_recv_addr(&mut self, env: RioEnv<'_>) -> RioResult<RioAddrReservation> {
        let reservation = self.allocate_addr_slot(env)?;
        self.addr_slots[reservation.slot] = SockAddrStorage::default();
        Ok(reservation)
    }

    pub(crate) fn copy_addr_slot_to(
        &self,
        reservation: RioAddrReservation,
        dst: &mut SockAddrStorage,
    ) -> RioResult<()> {
        if !self.addr_reservation_is_active(reservation) {
            return Err(
                self.addr_lease_error(reservation, "copy from inactive or stale address lease")
            );
        }
        let Some(src) = self.addr_slots.get(reservation.slot) else {
            return RioError::Internal
                .with_ctx("addr_slot", reservation.slot)
                .attach_note("RIO address slot out of bounds");
        };
        *dst = *src;
        Ok(())
    }

    pub(crate) fn free_addr_reservation(
        &mut self,
        reservation: Option<RioAddrReservation>,
    ) -> RioResult<()> {
        let Some(reservation) = reservation else {
            return Ok(());
        };
        let Some(in_use) = self.addr_slot_in_use.get_mut(reservation.slot) else {
            return Err(self.addr_lease_error(reservation, "address lease slot is out of bounds"));
        };
        if !*in_use {
            return Err(self.addr_lease_error(reservation, "address lease was already released"));
        }
        if self.addr_slot_generations[reservation.slot] != reservation.generation {
            return Err(self.addr_lease_error(reservation, "address lease generation is stale"));
        }
        *in_use = false;
        self.addr_free_slots.push(reservation.slot);
        Ok(())
    }

    fn addr_reservation_is_active(&self, reservation: RioAddrReservation) -> bool {
        self.addr_slot_in_use
            .get(reservation.slot)
            .is_some_and(|in_use| {
                *in_use && self.addr_slot_generations[reservation.slot] == reservation.generation
            })
    }

    fn addr_lease_error(
        &self,
        reservation: RioAddrReservation,
        note: &'static str,
    ) -> Report<RioError> {
        RioError::Internal
            .to_report()
            .with_ctx("addr_slot", reservation.slot)
            .with_ctx("addr_generation", reservation.generation)
            .attach_note(note)
    }

    pub(crate) fn allocate_addr_slot(&mut self, env: RioEnv<'_>) -> RioResult<RioAddrReservation> {
        let Some(&slot) = self.addr_free_slots.last() else {
            return RioError::ResourceExhaustion
                .with_ctx("addr_capacity", self.addr_slots.len())
                .attach_note("RIO address scratch buffer exhausted");
        };
        let offset = Self::addr_slot_offset(slot)?;
        let buffer_id = self.ensure_addr_buffer_registered(env)?;
        let slot = self.addr_free_slots.pop().ok_or_else(|| {
            RioError::Internal
                .to_report()
                .attach_note("RIO address free-list changed during allocation")
        })?;
        let generation = self.addr_slot_generations[slot].wrapping_add(1).max(1);
        self.addr_slot_generations[slot] = generation;
        self.addr_slot_in_use[slot] = true;
        Ok(RioAddrReservation {
            slot,
            generation,
            rio_buf: RIO_BUF {
                BufferId: buffer_id.raw(),
                Offset: offset,
                Length: size_of::<SOCKADDR_INET>() as u32,
            },
        })
    }

    pub(crate) fn ensure_addr_buffer_registered(
        &mut self,
        env: RioEnv<'_>,
    ) -> RioResult<RioBufferId> {
        if !self.addr_buffer_id.is_invalid() {
            return Ok(self.addr_buffer_id);
        }

        let len = size_of_val(&*self.addr_slots);
        let len_u32 = u32::try_from(len).map_err(|_| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("addr_buffer_length", len)
                .attach_note("RIO address scratch buffer too large")
        })?;
        let id = env
            .dispatch
            .register_buffer(self.addr_slots.as_ptr().cast::<u8>(), len_u32)
            .with_ctx("buffer_length", len)
            .attach_note("RIORegisterBuffer failed for address scratch buffer")?;
        self.addr_buffer_id = id;
        Ok(id)
    }

    pub(crate) fn validate_send_addr(addr: &SockAddrStorage, addr_len: i32) -> RioResult<u32> {
        if addr_len < 0 {
            return RioError::InvalidInput
                .with_ctx("address_len", addr_len)
                .attach_note("RIO send_to invalid negative address length");
        }
        if (addr_len as usize) < size_of::<SOCKADDR>() {
            return RioError::InvalidInput
                .with_ctx("address_len", addr_len)
                .with_ctx("min_address_len", size_of::<SOCKADDR>())
                .attach_note("RIO send_to address too short for SOCKADDR");
        }
        let family = addr.family();
        let min_len = match family {
            AF_INET => size_of::<SOCKADDR_IN>(),
            AF_INET6 => size_of::<SOCKADDR_IN6>(),
            _ => {
                return RioError::InvalidInput
                    .with_ctx("address_family", family)
                    .attach_note("RIO unsupported address family");
            }
        };
        if (addr_len as usize) < min_len {
            return RioError::InvalidInput
                .with_ctx("address_len", addr_len)
                .with_ctx("min_address_len", min_len)
                .attach_note("RIO send_to invalid address length");
        }

        Ok(size_of::<SOCKADDR_INET>() as u32)
    }

    pub(crate) fn addr_slot_offset(slot: usize) -> RioResult<u32> {
        let offset = slot
            .checked_mul(size_of::<SockAddrStorage>())
            .ok_or(RioError::ResourceExhaustion)
            .attach_note("RIO address slot offset overflow")?;
        u32::try_from(offset).map_err(|_| {
            RioError::ResourceExhaustion
                .to_report()
                .with_ctx("addr_slot", slot)
                .with_ctx("addr_slot_offset", offset)
                .attach_note("RIO address slot offset exceeds u32")
        })
    }

    pub(crate) fn reset_addr_slots(&mut self) {
        self.addr_free_slots.clear();
        for (slot, in_use) in self.addr_slot_in_use.iter_mut().enumerate().rev() {
            *in_use = false;
            self.addr_free_slots.push(slot);
        }
    }

    pub(super) fn active_slot_count(&self) -> usize {
        self.addr_slot_in_use
            .iter()
            .filter(|in_use| **in_use)
            .count()
    }

    #[cfg(test)]
    pub(super) fn free_slot_count(&self) -> usize {
        self.addr_free_slots.len()
    }

    #[cfg(test)]
    pub(super) fn slot_in_use(&self, slot: usize) -> bool {
        self.addr_slot_in_use.get(slot).copied().unwrap_or(false)
    }

    pub(super) fn deregister_buffer(&mut self, env: RioEnv<'_>, deregistered: &mut HashSet<usize>) {
        let id = replace(&mut self.addr_buffer_id, RioBufferId::INVALID);
        if !id.is_invalid() && deregistered.insert(id.raw() as usize) {
            env.dispatch.deregister_buffer(id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{RioRegistry, RioRegistryTestSupport};
    use super::{
        AF_INET, AF_INET6, RioError, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
        SockAddrStorage,
    };
    use veloq_std::mem::{size_of, zeroed};

    #[test]
    fn rio_send_addr_validation_rejects_short_sockaddr_before_family_read() {
        let address = SockAddrStorage::default();
        let err = RioRegistry::validate_send_addr(&address, 1)
            .expect_err("short sockaddr should fail before reading sa_family");

        assert_eq!(*err.inner(), RioError::InvalidInput);
    }

    #[test]
    fn rio_send_addr_validation_rejects_invalid_lengths_and_families() {
        // SAFETY: SOCKADDR is a plain WinSock header and zero bytes are valid here. / 安全性：SOCKADDR 是普通 WinSock 头部，全零内容在此有效。
        let mut sockaddr: SockAddrStorage = unsafe { zeroed() };
        let err = RioRegistry::validate_send_addr(&sockaddr, -1)
            .expect_err("negative sockaddr length should fail");
        assert_eq!(*err.inner(), RioError::InvalidInput);

        sockaddr.0.ss_family = AF_INET6;
        let err = RioRegistry::validate_send_addr(&sockaddr, size_of::<SOCKADDR>() as i32)
            .expect_err("IPv6 sockaddr shorter than SOCKADDR_IN6 should fail");
        assert_eq!(*err.inner(), RioError::InvalidInput);

        sockaddr.0.ss_family = 0x7fff;
        let err = RioRegistry::validate_send_addr(&sockaddr, size_of::<SOCKADDR>() as i32)
            .expect_err("unsupported sockaddr family should fail");
        assert_eq!(*err.inner(), RioError::InvalidInput);
    }

    #[test]
    fn rio_send_addr_validation_accepts_ipv4_and_ipv6_lengths() {
        // SAFETY: SOCKADDR_IN is POD and the test sets its family. / 安全性：SOCKADDR_IN 是 POD，测试显式设置了地址族。
        let mut ipv4: SockAddrStorage = unsafe { zeroed() };
        ipv4.0.ss_family = AF_INET;
        let rio_len = RioRegistry::validate_send_addr(&ipv4, size_of::<SOCKADDR_IN>() as i32)
            .expect("valid IPv4 sockaddr should pass");
        assert_eq!(rio_len, size_of::<SOCKADDR_INET>() as u32);

        // SAFETY: SOCKADDR_IN6 is POD and the test sets its family. / 安全性：SOCKADDR_IN6 是 POD，测试显式设置了地址族。
        let mut ipv6: SockAddrStorage = unsafe { zeroed() };
        ipv6.0.ss_family = AF_INET6;
        let rio_len = RioRegistry::validate_send_addr(&ipv6, size_of::<SOCKADDR_IN6>() as i32)
            .expect("valid IPv6 sockaddr should pass");
        assert_eq!(rio_len, size_of::<SOCKADDR_INET>() as u32);
    }

    #[test]
    fn address_slots_use_exclusive_generation_checked_leases() {
        let _guard = RioRegistryTestSupport::lock_dispatch_state();
        RioRegistryTestSupport::reset_dispatch_state();
        let dispatch = RioRegistryTestSupport::test_dispatch();
        let env = RioRegistryTestSupport::test_env(&dispatch);
        let mut registry = RioRegistry::new(32, 1);

        let first = registry
            .prepare_recv_addr(env)
            .expect("first address lease should succeed");
        assert!(registry.prepare_recv_addr(env).is_err());
        registry
            .free_addr_reservation(Some(first))
            .expect("first address lease should release");
        assert!(registry.free_addr_reservation(Some(first)).is_err());

        let second = registry
            .prepare_recv_addr(env)
            .expect("slot should be reusable after release");
        assert_ne!(first.generation, second.generation);
        assert!(registry.free_addr_reservation(Some(first)).is_err());
        registry
            .free_addr_reservation(Some(second))
            .expect("second address lease should release");
        assert_eq!(registry.addresses.addr_free_slots.len(), 1);
        assert!(RioRegistryTestSupport::deregistered_ids().is_empty());
    }

    #[test]
    fn address_registration_failure_does_not_consume_a_slot() {
        let _guard = RioRegistryTestSupport::lock_dispatch_state();
        RioRegistryTestSupport::reset_dispatch_state();
        let dispatch = RioRegistryTestSupport::test_dispatch();
        let env = RioRegistryTestSupport::test_env(&dispatch);
        let mut registry = RioRegistry::new(32, 1);
        RioRegistryTestSupport::fail_next_register();

        assert!(registry.prepare_recv_addr(env).is_err());
        assert_eq!(registry.addresses.addr_free_slots.len(), 1);
        assert!(!registry.addresses.addr_slot_in_use[0]);
        assert!(registry.addresses.addr_buffer_id.is_invalid());
    }
}
