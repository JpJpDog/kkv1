use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
    sync::Arc,
    thread::{spawn, JoinHandle},
};

use dashmap::{DashMap};
use memmap2::{MmapMut, MmapOptions};

use crate::persistencer::Persistencer;

pub type PageId = u32;

pub const PAGE_SIZE: usize = 4096;

pub struct PageManager {
    written: DashMap<PageId, Arc<MmapMut>>,
    flushing: Arc<DashMap<PageId, Arc<MmapMut>>>,
    persistencer: Arc<Persistencer>,
}

impl PageManager {
    pub fn new(root_dir: &str) -> Self {
        Self {
            written: DashMap::new(),
            flushing: Arc::new(DashMap::new()),
            persistencer: Arc::new(Persistencer::new(root_dir)),
        }
    }

    pub fn load(root_dir: &str) -> Self {
        Self {
            written: DashMap::new(),
            flushing: Arc::new(DashMap::new()),
            persistencer: Arc::new(Persistencer::load(root_dir)),
        }
    }

    pub fn new_page<T: PageRef>(pm: Arc<Self>, page_id: PageId) -> Page<T> {
        let mmap = Arc::new(pm.persistencer.new_mmap(page_id));
        Page {
            dirty: false,
            page_id,
            obj: T::load_from(&mmap),
            mmap,
            pm,
        }
    }

    pub unsafe fn load_page<T: PageRef>(pm: Arc<PageManager>, page_id: PageId) -> Page<T> {
        let mmap = if let Some(page) = pm.written.get(&page_id) {
            page.value().clone()
        } else if let Some(page) = pm.flushing.get(&page_id) {
            let mut mmap = MmapOptions::new().len(PAGE_SIZE).map_anon().unwrap();
            mmap.clone_from_slice(page.value());
            Arc::new(mmap)
        } else {
            Arc::new(pm.persistencer.load_mmap(page_id))
        };
        Page {
            dirty: false,
            page_id,
            obj: T::load_from(&mmap),
            mmap,
            pm,
        }
    }

    pub fn flush_pages(&self) -> Option<JoinHandle<()>> {
        if !self.flushing.is_empty() {
            return None;
        }
        let mut keys = HashSet::new();
        self.written.iter().for_each(|e| {
            keys.insert(*e.key());
        });
        for k in keys {
            if let Some((page_id, mmap)) = self.written.remove(&k) {
                self.flushing.insert(page_id, mmap);
            }
        }
        let persistencer = self.persistencer.clone();
        let flushing = self.flushing.clone();
        Some(spawn(move || persistencer.flush(flushing)))
    }
}

pub trait PageRef {
    fn load_from(mmap: &MmapMut) -> Self;
}
pub struct PageWriteGuard<'a, T: PageRef> {
    page: &'a mut Page<T>,
}

impl<'a, T: PageRef> Drop for PageWriteGuard<'a, T> {
    fn drop(&mut self) {
        self.page
            .pm
            .written
            .insert(self.page.page_id, self.page.mmap.clone());
    }
}

impl<'a, T: PageRef> Deref for PageWriteGuard<'a, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.page.obj
    }
}

impl<'a, T: PageRef> DerefMut for PageWriteGuard<'a, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page.obj
    }
}

pub struct Page<T: PageRef> {
    dirty: bool,
    page_id: PageId,
    obj: T,
    mmap: Arc<MmapMut>,
    pm: Arc<PageManager>,
}

impl<T: PageRef> Page<T> {
    #[inline]
    pub fn read(&self) -> &T {
        &self.obj
    }

    #[inline]
    pub fn write<'a>(&'a mut self) -> PageWriteGuard<'a, T> {
        self.pm.written.remove(&self.page_id);
        PageWriteGuard { page: self }
    }
}
