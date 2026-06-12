use super::RioRegistry;
use crate::net::addr::SockAddrStorage;
use crate::rio::RioEnv;
use crate::rio::core::{RioBufferId, RioProvider};
use crate::rio::error::{RioError, RioResult};
use diagweave::prelude::*;
use windows_sys::Win32::Networking::WinSock::{
    AF_INET, AF_INET6, RIO_BUF, SOCKADDR, SOCKADDR_IN, SOCKADDR_IN6, SOCKADDR_INET,
};

#[derive(Clone, Copy)]
pub(crate) struct RioAddrReservation {
    pub(crate) slot: usize,
    pub(crate) rio_buf: RIO_BUF,
}

impl RioRegistry {
    pub(crate) fn prepare_send_addr(
        &mut self,
        addr_ptr: *const std::ffi::c_void,
        addr_len: i32,
        env: RioEnv<'_>,
    ) -> RioResult<RioAddrReservation> {
        let rio_addr_len = Self::validate_send_addr(addr_ptr, addr_len)?;
        let reservation = self.allocate_addr_slot(env)?;
        let dst = (&mut self.addr_slots[reservation.slot] as *mut SockAddrStorage).cast::<u8>();
        let copy_len = (addr_len as usize).min(rio_addr_len as usize);
        // SAFETY: `dst` points at an owned scratch slot, and `addr_ptr` was
        // validated as non-null with at least `copy_len` readable bytes.
        unsafe {
            std::ptr::write_bytes(dst, 0, std::mem::size_of::<SockAddrStorage>());
            std::ptr::copy_nonoverlapping(addr_ptr.cast::<u8>(), dst, copy_len);
        }
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
        let dst = (&mut self.addr_slots[reservation.slot] as *mut SockAddrStorage).cast::<u8>();
        // SAFETY: `dst` points at an owned scratch slot.
        unsafe {
            std::ptr::write_bytes(dst, 0, std::mem::size_of::<SockAddrStorage>());
        }
        Ok(reservation)
    }

    pub(crate) fn copy_addr_slot_to(
        &self,
        slot: usize,
        dst: *mut SockAddrStorage,
    ) -> RioResult<()> {
        if dst.is_null() {
            return RioError::Internal
                .attach_note("RIO recv_from completion missing output address");
        }
        let Some(src) = self.addr_slots.get(slot) else {
            return RioError::Internal
                .with_ctx("addr_slot", slot)
                .attach_note("RIO address slot out of bounds");
        };
        // SAFETY: `src` is a live scratch slot and `dst` points at the op payload.
        unsafe {
            std::ptr::copy_nonoverlapping(src as *const SockAddrStorage, dst, 1);
        }
        Ok(())
    }

    pub(crate) fn free_addr_slot(&mut self, slot: Option<usize>) {
        let Some(slot) = slot else {
            return;
        };
        if let Some(in_use) = self.addr_slot_in_use.get_mut(slot)
            && *in_use
        {
            *in_use = false;
            self.addr_free_slots.push(slot);
        }
    }

    pub(crate) fn allocate_addr_slot(&mut self, env: RioEnv<'_>) -> RioResult<RioAddrReservation> {
        let buffer_id = self.ensure_addr_buffer_registered(env)?;
        let Some(slot) = self.addr_free_slots.pop() else {
            return RioError::ResourceExhaustion
                .with_ctx("addr_capacity", self.addr_slots.len())
                .attach_note("RIO address scratch buffer exhausted");
        };
        self.addr_slot_in_use[slot] = true;
        let offset = Self::addr_slot_offset(slot)?;
        Ok(RioAddrReservation {
            slot,
            rio_buf: RIO_BUF {
                BufferId: buffer_id.0,
                Offset: offset,
                Length: std::mem::size_of::<SOCKADDR_INET>() as u32,
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

        let len = std::mem::size_of_val(&*self.addr_slots);
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

    pub(crate) fn validate_send_addr(
        addr_ptr: *const std::ffi::c_void,
        addr_len: i32,
    ) -> RioResult<u32> {
        if addr_ptr.is_null() {
            return RioError::InvalidInput.attach_note("RIO send_to received null address");
        }
        if addr_len < 0 {
            return RioError::InvalidInput
                .with_ctx("address_len", addr_len)
                .attach_note("RIO send_to invalid negative address length");
        }
        if (addr_len as usize) < std::mem::size_of::<SOCKADDR>() {
            return RioError::InvalidInput
                .with_ctx("address_len", addr_len)
                .with_ctx("min_address_len", std::mem::size_of::<SOCKADDR>())
                .attach_note("RIO send_to address too short for SOCKADDR");
        }
        // SAFETY: addr_ptr is non-null and at least SOCKADDR-sized; read_unaligned avoids
        // imposing alignment requirements on future raw-pointer callers.
        let family = unsafe {
            std::ptr::addr_of!((*(addr_ptr as *const SOCKADDR)).sa_family).read_unaligned()
        };
        let min_len = match family {
            AF_INET => std::mem::size_of::<SOCKADDR_IN>(),
            AF_INET6 => std::mem::size_of::<SOCKADDR_IN6>(),
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

        Ok(std::mem::size_of::<SOCKADDR_INET>() as u32)
    }

    pub(crate) fn addr_slot_offset(slot: usize) -> RioResult<u32> {
        let offset = slot
            .checked_mul(std::mem::size_of::<SockAddrStorage>())
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
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rio_send_addr_validation_rejects_short_sockaddr_before_family_read() {
        let bytes = [0_u8; 1];
        let err = RioRegistry::validate_send_addr(bytes.as_ptr().cast(), bytes.len() as i32)
            .expect_err("short sockaddr should fail before reading sa_family");

        assert_eq!(*err.inner(), RioError::InvalidInput);
    }

    #[test]
    fn rio_send_addr_validation_rejects_invalid_lengths_and_families() {
        // SAFETY: SOCKADDR is a plain WinSock address header and all-zero bytes are valid here.
        let mut sockaddr: SOCKADDR = unsafe { std::mem::zeroed() };
        let err = RioRegistry::validate_send_addr((&sockaddr as *const SOCKADDR).cast(), -1)
            .expect_err("negative sockaddr length should fail");
        assert_eq!(*err.inner(), RioError::InvalidInput);

        sockaddr.sa_family = AF_INET6;
        let err = RioRegistry::validate_send_addr(
            (&sockaddr as *const SOCKADDR).cast(),
            std::mem::size_of::<SOCKADDR>() as i32,
        )
        .expect_err("IPv6 sockaddr shorter than SOCKADDR_IN6 should fail");
        assert_eq!(*err.inner(), RioError::InvalidInput);

        sockaddr.sa_family = 0x7fff;
        let err = RioRegistry::validate_send_addr(
            (&sockaddr as *const SOCKADDR).cast(),
            std::mem::size_of::<SOCKADDR>() as i32,
        )
        .expect_err("unsupported sockaddr family should fail");
        assert_eq!(*err.inner(), RioError::InvalidInput);
    }

    #[test]
    fn rio_send_addr_validation_accepts_ipv4_and_ipv6_lengths() {
        // SAFETY: SOCKADDR_IN is POD; the test fills the family field explicitly.
        let mut ipv4: SOCKADDR_IN = unsafe { std::mem::zeroed() };
        ipv4.sin_family = AF_INET;
        let rio_len = RioRegistry::validate_send_addr(
            (&ipv4 as *const SOCKADDR_IN).cast(),
            std::mem::size_of::<SOCKADDR_IN>() as i32,
        )
        .expect("valid IPv4 sockaddr should pass");
        assert_eq!(rio_len, std::mem::size_of::<SOCKADDR_INET>() as u32);

        // SAFETY: SOCKADDR_IN6 is POD; the test fills the family field explicitly.
        let mut ipv6: SOCKADDR_IN6 = unsafe { std::mem::zeroed() };
        ipv6.sin6_family = AF_INET6;
        let rio_len = RioRegistry::validate_send_addr(
            (&ipv6 as *const SOCKADDR_IN6).cast(),
            std::mem::size_of::<SOCKADDR_IN6>() as i32,
        )
        .expect("valid IPv6 sockaddr should pass");
        assert_eq!(rio_len, std::mem::size_of::<SOCKADDR_INET>() as u32);
    }
}
