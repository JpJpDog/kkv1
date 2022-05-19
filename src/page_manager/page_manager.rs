use std::{
    sync::{
        atomic::{AtomicPtr, Ordering},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
};

use crossbeam::{
    channel::{unbounded, Receiver, Sender},
    select,
};
use dashmap::DashMap;
use memmap2::{MmapMut, MmapOptions};

use super::{
    p_manager::{FHandler, PManager},
    page::{PageInner, PageId, PageRef, PAGE_SIZE},
    persistencer::Persistencer,
};

pub struct PageManager1 {
    written_map: Arc<AtomicPtr<DashMap<PageId, Arc<MmapMut>>>>,
    tmp_map: AtomicPtr<DashMap<PageId, Arc<MmapMut>>>,
    logging_map: Arc<AtomicPtr<DashMap<PageId, Arc<MmapMut>>>>,
    log_map: Arc<AtomicPtr<DashMap<PageId, Arc<MmapMut>>>>,
    flush_map: Arc<AtomicPtr<DashMap<PageId, Arc<MmapMut>>>>,
    set_flush: Arc<Mutex<()>>,
    persistencer: Arc<Persistencer>,
    flush_handler: Option<JoinHandle<()>>,
    log_handler: Option<JoinHandle<()>>,
    start_log_tx: Sender<Sender<()>>,
    kill_log_tx: Sender<()>,
    kill_flush_tx: Sender<()>,
}

pub struct FlushHandler1 {
    finish_flush_rx: Receiver<()>,
}

impl FHandler for FlushHandler1 {
    fn join(&mut self) {
        self.finish_flush_rx.recv().unwrap();
    }
}

impl PManager for PageManager1 {
    type FlushHandler = FlushHandler1;

    fn new(root_dir: &str) -> Self {
        Self::init(root_dir, false)
    }

    unsafe fn load(root_dir: &str) -> Self {
        Self::init(root_dir, true)
    }

    fn new_page<T: PageRef>(arc_self: Arc<Self>, page_id: PageId) -> PageInner<T, Self>
    where
        Self: Sized,
    {
        let mmap = Arc::new(arc_self.persistencer.new_mmap(page_id));
        PageInner {
            page_id,
            obj: T::load_from(&mmap),
            mmap,
            pm: arc_self,
        }
    }

    unsafe fn load_page<T: PageRef>(arc_self: Arc<Self>, page_id: PageId) -> PageInner<T, Self>
    where
        Self: Sized,
    {
        let mmap = if let Some(page) = (&*arc_self.written_map.load(Ordering::SeqCst)).get(&page_id)
        {
            page.value().clone()
        } else {
            if let Some(page) = if let Some(page) =
                (&*arc_self.tmp_map.load(Ordering::SeqCst)).get(&page_id)
            {
                Some(page)
            } else if let Some(page) = (&*arc_self.logging_map.load(Ordering::SeqCst)).get(&page_id)
            {
                Some(page)
            } else if let Some(page) = (&*arc_self.log_map.load(Ordering::SeqCst)).get(&page_id) {
                Some(page)
            } else if let Some(page) = (&*arc_self.flush_map.load(Ordering::SeqCst)).get(&page_id) {
                Some(page)
            } else {
                None
            } {
                let mut mmap = MmapOptions::new().len(PAGE_SIZE).map_anon().unwrap();
                mmap.clone_from_slice(page.value());
                Arc::new(mmap)
            } else {
                Arc::new(arc_self.persistencer.load_mmap(page_id))
            }
        };
        PageInner {
            page_id,
            obj: T::load_from(&mmap),
            mmap,
            pm: arc_self,
        }
    }

    fn flush(&self) -> Self::FlushHandler {
        let set_flush_g = self.set_flush.lock().unwrap();
        let logging_map = unsafe { &mut *self.logging_map.load(Ordering::SeqCst) };
        if logging_map.is_empty() {
            let p = self
                .logging_map
                .swap(self.written_map.load(Ordering::SeqCst), Ordering::SeqCst);
            self.written_map.store(p, Ordering::SeqCst);
        } else {
            let written_map = self.written_map.load(Ordering::SeqCst);
            let p = self
                .tmp_map
                .swap(self.written_map.load(Ordering::SeqCst), Ordering::SeqCst);
            self.written_map.store(p, Ordering::SeqCst);
            let tmp_map = unsafe { &*written_map };
            for e in tmp_map {
                logging_map.insert(*e.key(), e.value().clone());
            }
            tmp_map.clear();
        }
        drop(set_flush_g);
        let (finish_flush_tx, finish_flush_rx) = unbounded();
        self.start_log_tx.send(finish_flush_tx).unwrap();
        FlushHandler1 { finish_flush_rx }
    }

    #[inline]
    fn write_start(&self, page_id: PageId) {
        unsafe { &*self.written_map.load(Ordering::SeqCst) }.remove(&page_id);
    }

    #[inline]
    fn write_ok(&self, page_id: PageId, mmap: Arc<MmapMut>) {
        unsafe { &*self.written_map.load(Ordering::SeqCst) }.insert(page_id, mmap);
    }
}

impl PageManager1 {
    fn init(root_dir: &str, load: bool) -> Self {
        let written_map = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new()))));
        let tmp_map = AtomicPtr::new(Box::into_raw(Box::new(DashMap::new())));
        let logging_map = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new()))));
        let log_map = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new()))));
        let flush_map = Arc::new(AtomicPtr::new(Box::into_raw(Box::new(DashMap::new()))));
        let set_flush = Arc::new(Mutex::new(()));
        let persistencer = Arc::new(if load {
            Persistencer::load(root_dir)
        } else {
            Persistencer::new(root_dir)
        });
        let (start_log_tx, start_log_rx) = unbounded();
        let (start_flush_tx, start_flush_rx) = unbounded::<Sender<()>>();
        let (finish_flush_tx1, finish_flush_rx1) = unbounded();
        let (kill_log_tx, kill_log_rx) = unbounded();
        let (kill_flush_tx, kill_flush_rx) = unbounded();

        finish_flush_tx1.send(()).unwrap();
        let log_handler = Some({
            let log_map1 = log_map.clone();
            let logging_map1 = logging_map.clone();
            let persistencer1 = persistencer.clone();
            let flush_map1 = flush_map.clone();
            let set_flush1 = set_flush.clone();
            thread::spawn(move || loop {
                select! {
                    recv(start_log_rx) -> finish_flush_tx => {
                        let set_flush_g =  set_flush1.lock().unwrap();
                        let p = logging_map1.swap(log_map1.load(Ordering::SeqCst), Ordering::SeqCst);
                        log_map1.store(p, Ordering::SeqCst);
                        drop(set_flush_g);
                        persistencer1.log(unsafe { &*p });
                        finish_flush_rx1.recv().unwrap();
                        persistencer1.finish_log();
                        let p = flush_map1.swap(log_map1.load(Ordering::SeqCst), Ordering::SeqCst);
                        log_map1.store(p, Ordering::SeqCst);
                        start_flush_tx.send(finish_flush_tx.unwrap()).unwrap();
                    },
                    recv(kill_log_rx) -> _ => {
                        break;
                    }
                }
            })
        });
        let flush_handler = Some({
            let persistencer1 = persistencer.clone();
            let flush_map1 = flush_map.clone();
            thread::spawn(move || loop {
                select! {
                    recv (start_flush_rx) -> finish_flush_tx => {
                        let finish_flush_tx = finish_flush_tx.unwrap();
                        persistencer1.flush(unsafe { &*flush_map1.load(Ordering::SeqCst) });
                        unsafe { &*flush_map1.load(Ordering::SeqCst) }.clear();
                        finish_flush_tx.send(()).unwrap();
                        finish_flush_tx1.send(()).unwrap();
                    }
                    recv (kill_flush_rx) -> _ => {
                        break;
                    }
                }
            })
        });
        Self {
            written_map,
            tmp_map,
            logging_map,
            log_map,
            flush_map,
            set_flush,
            persistencer,
            log_handler,
            flush_handler,
            start_log_tx,
            kill_log_tx,
            kill_flush_tx,
        }
    }
}

impl Drop for PageManager1 {
    fn drop(&mut self) {
        self.kill_flush_tx.send(()).unwrap();
        self.kill_log_tx.send(()).unwrap();
        self.log_handler.take().unwrap().join().unwrap();
        self.flush_handler.take().unwrap().join().unwrap();
        unsafe {
            drop(Box::from_raw(self.written_map.load(Ordering::SeqCst)));
            drop(Box::from_raw(self.log_map.load(Ordering::SeqCst)));
            drop(Box::from_raw(self.flush_map.load(Ordering::SeqCst)));
        }
    }
}
