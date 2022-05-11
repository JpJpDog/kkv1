use std::{
    collections::HashSet,
    fs::remove_dir_all,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    thread::{spawn, JoinHandle},
};

use memmap2::MmapMut;
use rand::random;

use crate::page::{Page, PageManager, PageRef};

struct TestPageRef {
    a: *mut u32,
}

impl TestPageRef {
    fn a(&self) -> &u32 {
        unsafe { &*self.a }
    }
    fn a_mut(&mut self) -> &mut u32 {
        unsafe { &mut *self.a }
    }
}

impl PageRef for TestPageRef {
    fn load_from(mmap: &MmapMut) -> Self {
        let (_p, aa, _s) = unsafe { mmap.align_to::<u32>() };
        Self {
            a: aa.as_ptr() as *mut _,
        }
    }
}

#[test]
fn test1() {
    let root_dir = "test_dir/test1";
    if Path::exists(Path::new(root_dir)) {
        remove_dir_all(root_dir).unwrap();
    }
    let pm = Arc::new(PageManager::new(root_dir));
    let mut page: Page<TestPageRef> = PageManager::new_page(pm.clone(), 0);
    *page.write().a_mut() = 1;
    let h = pm.flush_pages().unwrap();
    h.join().unwrap();

    let a = page.read().a();
    assert_eq!(*a, 1);

    let pm = Arc::new(PageManager::load(root_dir));
    let page: Page<TestPageRef> = unsafe { PageManager::load_page(pm.clone(), 0) };
    let a = page.read().a();
    assert_eq!(*a, 1);
}

fn test11() {
    let root_dir = "test_dir/test1";
    let pm = Arc::new(PageManager::load(root_dir));
    let page: Page<TestPageRef> = unsafe { PageManager::load_page(pm.clone(), 0) };
    let a = page.read().a();
    assert_eq!(*a, 1);
}

#[test]
fn test2() {
    let root_dir = "test_dir/test2";
    if Path::exists(Path::new(root_dir)) {
        remove_dir_all(root_dir).unwrap();
    }
    let pm = Arc::new(PageManager::new(root_dir));
    let thread_n = 10;
    let page_n_per_thread = 2000;
    let mut handlers = Vec::new();
    let cnt = Arc::new(AtomicU64::new(1));
    let handler: Arc<Mutex<Option<JoinHandle<()>>>> = Arc::new(Mutex::new(None));
    for i in 0..thread_n {
        let start = i * page_n_per_thread;
        let cnt = cnt.clone();
        let pm = pm.clone();
        let handler = handler.clone();
        handlers.push(spawn(move || {
            for id in start..start + page_n_per_thread {
                let _page = PageManager::new_page::<TestPageRef>(pm.clone(), id);
            }
            let mut set = HashSet::new();
            for _j in 0..1000000 {
                let old = cnt.fetch_add(1, Ordering::SeqCst);
                if old % 10000 == 0 {
                    let mut h_g = handler.lock().unwrap();
                    let h = h_g.take();
                    if let Some(h) = h {
                        h.join().unwrap();
                    }
                    *h_g = pm.flush_pages();
                    assert!(h_g.is_some());
                }
                let op = random::<u32>() % 10;
                let id = start + random::<u32>() % page_n_per_thread;
                let mut page = unsafe { PageManager::load_page::<TestPageRef>(pm.clone(), id) };
                if op < 6 {
                    let a = page.read().a();
                    assert_eq!(*a, if set.get(&id).is_some() { 1 } else { 0 });
                } else {
                    if set.get(&id).is_some() {
                        set.remove(&id);
                        *page.write().a_mut() = 0;
                    } else {
                        set.insert(id);
                        *page.write().a_mut() = 1;
                    }
                }
            }
        }));
    }
    for h in handlers {
        h.join().unwrap();
    }
}
