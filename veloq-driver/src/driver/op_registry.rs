use super::stable_slab::StableSlab;
use crate::driver::PlatformOp;
use std::ops::{Index, IndexMut};
use std::task::Waker;

pub struct OpEntry<Op: PlatformOp, P> {
    pub waker: Option<Waker>,
    pub resources: Option<Op>,
    pub platform_data: P,
}

impl<Op: PlatformOp, P> OpEntry<Op, P> {
    pub fn new(resources: Option<Op>, platform_data: P) -> Self {
        Self {
            waker: None,
            resources,
            platform_data,
        }
    }
}

pub struct OpRegistry<Op: PlatformOp, P> {
    slab: StableSlab<OpEntry<Op, P>>,
}

impl<Op: PlatformOp, P> OpRegistry<Op, P> {
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            slab: StableSlab::with_capacity(capacity),
        }
    }

    pub fn insert(&mut self, entry: OpEntry<Op, P>) -> usize {
        self.slab.insert(entry)
    }

    pub fn get(&self, user_data: usize) -> Option<&OpEntry<Op, P>> {
        self.slab.get(user_data)
    }

    pub fn get_mut(&mut self, user_data: usize) -> Option<&mut OpEntry<Op, P>> {
        self.slab.get_mut(user_data)
    }

    pub fn contains(&self, user_data: usize) -> bool {
        self.slab.contains(user_data)
    }

    #[allow(dead_code)]
    pub fn remove(&mut self, user_data: usize) -> OpEntry<Op, P> {
        self.slab.remove(user_data)
    }

    #[cfg(target_os = "linux")]
    pub fn is_empty(&self) -> bool {
        self.slab.is_empty()
    }

    #[cfg(target_os = "windows")]
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (usize, &mut OpEntry<Op, P>)> {
        self.slab.iter_mut()
    }

    /// Helper for RIO registration: access underlying slab pages
    #[cfg(target_os = "windows")]
    pub fn get_page_slice(&self, page_idx: usize) -> Option<(*const u8, usize)> {
        self.slab.get_page_slice(page_idx)
    }

    #[cfg(target_os = "windows")]
    pub fn page_count(&self) -> usize {
        self.slab.page_count()
    }

    // Expose mask to drivers
    #[cfg(target_os = "windows")]
    pub const INDEX_MASK: usize = StableSlab::<OpEntry<Op, P>>::index_mask();
    #[cfg(target_os = "windows")]
    pub const PAGE_SHIFT: usize = StableSlab::<OpEntry<Op, P>>::PAGE_SHIFT;
}

impl<Op: PlatformOp, P> Index<usize> for OpRegistry<Op, P> {
    type Output = OpEntry<Op, P>;

    fn index(&self, index: usize) -> &Self::Output {
        &self.slab[index]
    }
}

impl<Op: PlatformOp, P> IndexMut<usize> for OpRegistry<Op, P> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.slab[index]
    }
}
