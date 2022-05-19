use std::{
    ops::{Deref, DerefMut},
    sync::Arc,
};

use memmap2::MmapMut;

use super::p_manager::PManager;

pub type PageId = u32;

pub const PAGE_SIZE: usize = 4096;

pub trait PageRef {
    fn load_from(mmap: &MmapMut) -> Self;
}

pub struct PageInner<T: PageRef, T1: PManager> {
    pub(crate) page_id: PageId,
    pub(crate) obj: T,
    pub(crate) mmap: Arc<MmapMut>,
    pub(crate) pm: Arc<T1>,
}

impl<T: PageRef, T1: PManager> PageInner<T, T1> {
    #[inline]
    pub fn read(&self) -> &T {
        &self.obj
    }

    #[inline]
    pub fn write<'a>(&'a mut self) -> PageWriteGuard<'a, T, T1> {
        self.pm.write_start(self.page_id);
        PageWriteGuard { page: self }
    }
}

pub struct PageWriteGuard<'a, T: PageRef, T1: PManager> {
    page: &'a mut PageInner<T, T1>,
}

impl<'a, T: PageRef, T1: PManager> Drop for PageWriteGuard<'a, T, T1> {
    fn drop(&mut self) {
        self.page
            .pm
            .write_ok(self.page.page_id, self.page.mmap.clone());
    }
}

impl<'a, T: PageRef, T1: PManager> Deref for PageWriteGuard<'a, T, T1> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.page.obj
    }
}

impl<'a, T: PageRef, T1: PManager> DerefMut for PageWriteGuard<'a, T, T1> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page.obj
    }
}
