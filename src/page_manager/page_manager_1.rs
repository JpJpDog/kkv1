use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
    thread::{spawn, JoinHandle},
};

use dashmap::DashMap;
use memmap2::{MmapMut, MmapOptions};

use super::{
    p_manager::{FHandler, PManager},
    page::{Page, PageId, PageRef, PAGE_SIZE},
    persistencer::Persistencer,
};

pub struct PageManager1 {
    flushing: Arc<Mutex<()>>,
    written_map: DashMap<PageId, Arc<MmapMut>>,
    flush_map: Arc<DashMap<PageId, Arc<MmapMut>>>,
    persistencer: Arc<Persistencer>,
}

pub struct FlushHandler1 {
    handler: Option<JoinHandle<()>>,
}

impl FHandler for FlushHandler1 {
    fn join(&mut self) {
        self.handler.take().unwrap().join().unwrap();
    }
}

impl PManager for PageManager1 {
    type FlushHandler = FlushHandler1;

    #[inline]
    fn new(root_dir: &str) -> Self {
        Self::init(root_dir, true)
    }

    #[inline]
    unsafe fn load(root_dir: &str) -> Self {
        Self::init(root_dir, false)
    }

    fn new_page<T: PageRef>(arc_self: Arc<Self>, page_id: PageId) -> Page<T, Self>
    where
        Self: Sized,
    {
        let mmap = Arc::new(arc_self.persistencer.new_mmap(page_id));
        Page {
            page_id,
            obj: T::load_from(&mmap),
            mmap,
            pm: arc_self,
        }
    }

    unsafe fn load_page<T: PageRef>(arc_self: Arc<Self>, page_id: PageId) -> Page<T, Self>
    where
        Self: Sized,
    {
        let mmap = if let Some(page) = arc_self.written_map.get(&page_id) {
            page.value().clone()
        } else if let Some(page) = arc_self.flush_map.get(&page_id) {
            let mut mmap = MmapOptions::new().len(PAGE_SIZE).map_anon().unwrap();
            mmap.clone_from_slice(page.value());
            Arc::new(mmap)
        } else {
            Arc::new(arc_self.persistencer.load_mmap(page_id))
        };
        Page {
            page_id,
            obj: T::load_from(&mmap),
            mmap,
            pm: arc_self,
        }
    }

    #[inline]
    fn flush(&self) -> Self::FlushHandler {
        self.inner_flush(false).unwrap()
    }

    #[inline]
    fn write_start(&self, page_id: PageId) {
        self.written_map.remove(&page_id);
    }

    #[inline]
    fn write_ok(&self, page_id: PageId, mmap: Arc<MmapMut>) {
        self.written_map.insert(page_id, mmap);
    }
}

impl PageManager1 {
    fn init(root_dir: &str, init: bool) -> Self {
        Self {
            flushing: Arc::new(Mutex::new(())),
            written_map: DashMap::new(),
            flush_map: Arc::new(DashMap::new()),
            persistencer: Arc::new(if init {
                Persistencer::new(root_dir)
            } else {
                Persistencer::load(root_dir)
            }),
        }
    }

    fn inner_flush(&self, is_try: bool) -> Option<FlushHandler1> {
        let flushing = self.flushing.clone();
        let persistencer = self.persistencer.clone();
        let flush_map = self.flush_map.clone();
        let written_map = self.written_map.clone();
        let handler = spawn(move || {
            let _guard = if is_try {
                if let Ok(g) = flushing.try_lock() {
                    g
                } else {
                    return;
                }
            } else {
                flushing.lock().unwrap()
            };
            assert!(flush_map.is_empty());
            let mut keys = HashSet::new();
            written_map.iter().for_each(|e| {
                keys.insert(*e.key());
            });
            for k in keys {
                if let Some((page_id, mmap)) = written_map.remove(&k) {
                    flush_map.insert(page_id, mmap);
                }
            }
            persistencer.log(&flush_map);
            persistencer.finish_log();
            persistencer.flush(&flush_map);
            flush_map.clear();
        });
        Some(FlushHandler1 {
            handler: Some(handler),
        })
    }

    #[inline]
    pub fn try_flush(&self) -> Option<FlushHandler1> {
        self.inner_flush(true)
    }
}
