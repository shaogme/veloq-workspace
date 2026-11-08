use veloq_std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    vec::Vec,
};

use crate::os::Platform;

const DEFAULT_LOCKED_MEMORY_LIMIT: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockedMemoryError {
    InvalidRange,
    MissingLockedFootprint,
    InvalidPageSize,
    ArithmeticOverflow,
    ReservationAllocationFailed,
}

impl fmt::Display for LockedMemoryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRange => formatter.write_str("locked memory range is empty or null"),
            Self::MissingLockedFootprint => {
                formatter.write_str("large-page allocation requires its complete locked footprint")
            }
            Self::InvalidPageSize => formatter.write_str("OS page size is unavailable"),
            Self::ArithmeticOverflow => formatter.write_str("locked memory charge overflow"),
            Self::ReservationAllocationFailed => {
                formatter.write_str("locked memory reservation allocation failed")
            }
        }
    }
}

struct BudgetState {
    limit: usize,
    used: AtomicUsize,
}

#[derive(Clone)]
pub struct LockedMemoryBudget {
    state: Arc<BudgetState>,
}

pub struct LockedMemoryPermit {
    state: Arc<BudgetState>,
    charge: usize,
}

impl LockedMemoryBudget {
    pub fn new(limit_bytes: usize) -> Self {
        Self {
            state: Arc::new(BudgetState {
                limit: limit_bytes,
                used: AtomicUsize::new(0),
            }),
        }
    }

    pub fn default_limit() -> Self {
        Self::new(DEFAULT_LOCKED_MEMORY_LIMIT)
    }

    pub fn limit_bytes(&self) -> usize {
        self.state.limit
    }

    pub fn used_bytes(&self) -> usize {
        self.state.used.load(Ordering::Acquire)
    }

    pub fn available_bytes(&self) -> usize {
        self.limit_bytes().saturating_sub(self.used_bytes())
    }

    pub fn required_charge(&self, ptr: *const u8, len: usize) -> Result<usize, LockedMemoryError> {
        let page_size = Platform::page_size().ok_or(LockedMemoryError::InvalidPageSize)?;
        Self::range_charge(ptr as usize, len, page_size)
    }

    pub fn try_reserve_range(
        &self,
        ptr: *const u8,
        len: usize,
    ) -> Result<Option<LockedMemoryPermit>, LockedMemoryError> {
        let charge = self.required_charge(ptr, len)?;
        Ok(self.try_reserve_charge(charge))
    }

    pub fn try_reserve_repeated(
        &self,
        charge_per_region: usize,
        count: usize,
    ) -> Result<Option<Vec<LockedMemoryPermit>>, LockedMemoryError> {
        let total_charge = charge_per_region
            .checked_mul(count)
            .ok_or(LockedMemoryError::ArithmeticOverflow)?;
        if count == 0 {
            return Ok(Some(Vec::new()));
        }
        if charge_per_region == 0 {
            return Err(LockedMemoryError::InvalidRange);
        }
        let mut permits = Vec::new();
        permits
            .try_reserve_exact(count)
            .map_err(|_| LockedMemoryError::ReservationAllocationFailed)?;
        if !self.try_add(total_charge) {
            return Ok(None);
        }
        for _ in 0..count {
            permits.push(LockedMemoryPermit {
                state: self.state.clone(),
                charge: charge_per_region,
            });
        }
        Ok(Some(permits))
    }

    pub fn try_reserve_repeated_unaligned(
        &self,
        region_len: usize,
        count: usize,
    ) -> Result<Option<Vec<LockedMemoryPermit>>, LockedMemoryError> {
        let page_size = Platform::page_size().ok_or(LockedMemoryError::InvalidPageSize)?;
        let charge = Self::worst_case_range_charge(region_len, page_size)?;
        self.try_reserve_repeated(charge, count)
    }

    fn range_charge(
        start: usize,
        len: usize,
        page_size: usize,
    ) -> Result<usize, LockedMemoryError> {
        if start == 0 || len == 0 {
            return Err(LockedMemoryError::InvalidRange);
        }
        if page_size == 0 || !page_size.is_power_of_two() {
            return Err(LockedMemoryError::InvalidPageSize);
        }
        let end = start
            .checked_add(len)
            .ok_or(LockedMemoryError::ArithmeticOverflow)?;
        let aligned_start = start & !(page_size - 1);
        let aligned_end = end
            .checked_add(page_size - 1)
            .ok_or(LockedMemoryError::ArithmeticOverflow)?
            & !(page_size - 1);
        aligned_end
            .checked_sub(aligned_start)
            .ok_or(LockedMemoryError::ArithmeticOverflow)
    }

    fn worst_case_range_charge(len: usize, page_size: usize) -> Result<usize, LockedMemoryError> {
        if len == 0 {
            return Err(LockedMemoryError::InvalidRange);
        }
        if page_size == 0 || !page_size.is_power_of_two() {
            return Err(LockedMemoryError::InvalidPageSize);
        }
        let max_end = len
            .checked_add(page_size - 1)
            .ok_or(LockedMemoryError::ArithmeticOverflow)?;
        let rounded = max_end
            .checked_add(page_size - 1)
            .ok_or(LockedMemoryError::ArithmeticOverflow)?
            & !(page_size - 1);
        if rounded == 0 {
            return Err(LockedMemoryError::ArithmeticOverflow);
        }
        Ok(rounded)
    }

    fn try_reserve_charge(&self, charge: usize) -> Option<LockedMemoryPermit> {
        if self.try_add(charge) {
            Some(LockedMemoryPermit {
                state: self.state.clone(),
                charge,
            })
        } else {
            None
        }
    }

    fn try_add(&self, charge: usize) -> bool {
        let mut used = self.state.used.load(Ordering::Acquire);
        loop {
            let Some(next) = used.checked_add(charge) else {
                return false;
            };
            if next > self.state.limit {
                return false;
            }
            match self.state.used.compare_exchange_weak(
                used,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(observed) => used = observed,
            }
        }
    }
}

impl Default for LockedMemoryBudget {
    fn default() -> Self {
        Self::default_limit()
    }
}

impl fmt::Debug for LockedMemoryBudget {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LockedMemoryBudget")
            .field("limit_bytes", &self.limit_bytes())
            .field("used_bytes", &self.used_bytes())
            .finish()
    }
}

impl LockedMemoryPermit {
    pub fn charge_bytes(&self) -> usize {
        self.charge
    }

    pub fn belongs_to(&self, budget: &LockedMemoryBudget) -> bool {
        Arc::ptr_eq(&self.state, &budget.state)
    }
}

impl fmt::Debug for LockedMemoryPermit {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("LockedMemoryPermit")
            .field("charge_bytes", &self.charge)
            .finish()
    }
}

impl Drop for LockedMemoryPermit {
    fn drop(&mut self) {
        let previous = self.state.used.fetch_sub(self.charge, Ordering::AcqRel);
        debug_assert!(previous >= self.charge);
    }
}
