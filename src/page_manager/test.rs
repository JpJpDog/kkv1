use std::{
    collections::HashSet,
    fs::remove_dir_all,
    path::Path,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
};

use memmap2::MmapMut;
use rand::random;
use tokio::io::AsyncWriteExt;

use super::{
    p_manager::{FHandler, PManager},
    page::PageRef,
    page_manager_2::{FlushHandler2 as FlushHandler, PageManager2 as PageManager},
    // page_manager_1::{FlushHandler1 as FlushHandler, PageManager1 as PageManager},
};

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
fn pm_simple() {
    let test_dir = "test_dir/pm1";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let pm = Arc::new(PageManager::new(test_dir));
    let mut page = PageManager::new_page::<TestPageRef>(pm.clone(), 0);
    *page.write().a_mut() = 1;
    let mut handler = pm.flush();
    handler.join();
    let a = page.read().a();
    assert_eq!(*a, 1);

    let pm = Arc::new(unsafe { PageManager::load(test_dir) });
    let page = unsafe { PageManager::load_page::<TestPageRef>(pm.clone(), 0) };
    let a = page.read().a();
    assert_eq!(*a, 1);
}

// #[test]
// fn test11() {
//     let test_dir = "test_dir/pm2";
//     let pm = Arc::new(PageManager::load(test_dir));
//     let page: Page<TestPageRef> = unsafe { PageManager::load_page(pm.clone(), 0) };
// }

#[test]
fn pm_complicated() {
    let test_dir = "test_dir/pm2";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let pm = Arc::new(PageManager::new(test_dir));
    let thread_n = 4;
    let page_n_per_thread = 2000;
    let mut handlers = Vec::new();
    let cnt = Arc::new(AtomicU64::new(1));
    for i in 0..thread_n {
        let start = i * page_n_per_thread;
        let cnt = cnt.clone();
        let pm = pm.clone();
        let h = thread::spawn(move || {
            let mut guard: Option<FlushHandler> = None;
            for id in start..start + page_n_per_thread {
                let _page = PageManager::new_page::<TestPageRef>(pm.clone(), id);
            }
            let mut set = HashSet::new();
            for _j in 0..100000 {
                let old = cnt.fetch_add(1, Ordering::SeqCst);
                if old % 100 == 0 {
                    if let Some(mut f) = guard {
                        f.join();
                    }
                    guard = Some(pm.flush());
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
            if let Some(mut f) = guard {
                f.join();
            }
        });
        handlers.push(h);
    }
    for h in handlers {
        h.join().unwrap();
    }
}
