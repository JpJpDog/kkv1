use std::{
    ops::{Deref, DerefMut},
    sync::{
        atomic::{AtomicBool, AtomicPtr, Ordering},
        Arc,
    },
    thread::{spawn, JoinHandle},
};

use dashmap::DashMap;
use memmap2::{MmapMut, MmapOptions};

use crate::persistencer::Persistencer;

pub type PageId = u32;

pub const PAGE_SIZE: usize = 4096;

pub struct PageManager {
    written: Arc<AtomicPtr<DashMap<PageId, Arc<MmapMut>>>>,
    log_flag: Arc<AtomicBool>,
    flush_flag: Arc<AtomicBool>,
    logging: Arc<AtomicPtr<DashMap<PageId, Arc<MmapMut>>>>,
    flushing: Arc<AtomicPtr<DashMap<PageId, Arc<MmapMut>>>>,
    persistencer: Arc<Persistencer>,
}

impl PageManager {
    pub fn new(root_dir: &str) -> Self {
        Self {
            written: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new())))),
            log_flag: Arc::new(AtomicBool::new(false)),
            flush_flag: Arc::new(AtomicBool::new(false)),
            logging: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new())))),
            flushing: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new())))),
            persistencer: Arc::new(Persistencer::new(root_dir)),
        }
    }

    pub fn load(root_dir: &str) -> Self {
        Self {
            written: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new())))),
            log_flag: Arc::new(AtomicBool::new(false)),
            flush_flag: Arc::new(AtomicBool::new(false)),
            logging: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new())))),
            flushing: Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new())))),
            persistencer: Arc::new(Persistencer::load(root_dir)),
        }
    }

    pub fn new_page<T: PageRef>(pm: Arc<Self>, page_id: PageId) -> Page<T> {
        let mmap = Arc::new(pm.persistencer.new_mmap(page_id));
        Page {
            page_id,
            obj: T::load_from(&mmap),
            mmap,
            pm,
        }
    }

    pub unsafe fn load_page<T: PageRef>(pm: Arc<PageManager>, page_id: PageId) -> Page<T> {
        let mmap = if let Some(page) = (&*pm.written.load(Ordering::SeqCst)).get(&page_id) {
            page.value().clone()
        } else if let Some(page) = (&*pm.logging.load(Ordering::SeqCst)).get(&page_id) {
            let mut mmap = MmapOptions::new().len(PAGE_SIZE).map_anon().unwrap();
            mmap.clone_from_slice(page.value());
            Arc::new(mmap)
        } else if let Some(page) = (&*pm.flushing.load(Ordering::SeqCst)).get(&page_id) {
            let mut mmap = MmapOptions::new().len(PAGE_SIZE).map_anon().unwrap();
            let pp = page.value();
            mmap.clone_from_slice(page.value());
            Arc::new(mmap)
        } else {
            Arc::new(pm.persistencer.load_mmap(page_id))
        };
        Page {
            page_id,
            obj: T::load_from(&mmap),
            mmap,
            pm,
        }
    }

    pub fn flush_pages(&self) -> Option<JoinHandle<()>> {
        if let Err(_e) =
            self.log_flag
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
        {
            return None;
        }
        let persistencer = self.persistencer.clone();
        let written = self.written.clone();
        let logging = self.logging.clone();
        let flushing = self.flushing.clone();
        let log_flag = self.log_flag.clone();
        let flush_flag = self.flush_flag.clone();
        Some(spawn(move || {
            let logging1 = logging.load(Ordering::SeqCst);
            logging.store(written.load(Ordering::SeqCst), Ordering::SeqCst);
            written.store(Box::into_raw(Box::new(DashMap::new())), Ordering::SeqCst);
            drop(unsafe { Box::from_raw(logging1) });
            persistencer.log(unsafe { &*logging.load(Ordering::SeqCst) });
            while let Err(_e) =
                flush_flag.compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            {
            }
            let flushing1 = flushing.load(Ordering::SeqCst);
            flushing.store(logging.load(Ordering::SeqCst), Ordering::SeqCst);
            logging.store(Box::into_raw(Box::new(DashMap::new())), Ordering::SeqCst);
            drop(unsafe { Box::from_raw(flushing1) });
            log_flag.store(false, Ordering::SeqCst);
            persistencer.flush(unsafe { &*flushing.load(Ordering::SeqCst) });
            // drop(unsafe { Box::from_raw(flushing.load(Ordering::SeqCst)) });
            flush_flag.store(false, Ordering::SeqCst);
        }))
    }
}

impl Drop for PageManager {
    fn drop(&mut self) {
        unsafe {
            drop(Box::from_raw(self.written.load(Ordering::SeqCst)));
            drop(Box::from_raw(self.logging.load(Ordering::SeqCst)));
            drop(Box::from_raw(self.flushing.load(Ordering::SeqCst)));
        }
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
        unsafe { &*self.page.pm.written.load(Ordering::SeqCst) }
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
        unsafe { &*self.pm.written.load(Ordering::SeqCst) }.remove(&self.page_id);
        PageWriteGuard { page: self }
    }
}
