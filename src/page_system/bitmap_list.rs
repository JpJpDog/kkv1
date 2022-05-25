use std::sync::Arc;

use crate::page_manager::{page::PageId, PageManager};

use super::bitmap::{BitMap, BmKey};

/// A array of bitmaps that can self increase when id is not enough
pub struct BitMapList {
    cap: BmKey,
    bms: Vec<BitMap>,
    pm: Arc<PageManager>,
}

impl BitMapList {
    /// new a `BitMapList` with one bitmap which page_id is `boot_id`.
    /// unsafe if `boot_id` is in use
    pub unsafe fn new(boot_id: PageId, pm: Arc<PageManager>) -> Self {
        let mut bms = Vec::new();
        let mut bm = BitMap::new(boot_id, pm.clone());
        assert_eq!(bm.mark().unwrap() as PageId, boot_id);
        let cap = bm.capacity();
        bms.push(bm);
        Self { cap, bms, pm }
    }

    /// load `BitMapList` from disk which head id is `boot_id`.
    /// unsafe if `boot_id` is wrong
    pub unsafe fn load(boot_id: PageId, pm: Arc<PageManager>) -> Self {
        let mut bms = Vec::new();
        let mut bm = BitMap::load(boot_id, pm.clone());
        let cap = bm.capacity();
        let mut len = bm.len();
        bms.push(bm);
        let mut idx = 0;
        // when bitmap is full, then the last key is the key of the page of next bitmap
        while len == cap {
            let next_id = (idx + 1) * cap as PageId - 1;
            bm = BitMap::load(next_id, pm.clone());
            assert_eq!(bm.capacity(), cap);
            len = bm.len();
            bms.push(bm);
            idx += 1;
        }
        Self { cap, bms, pm }
    }

    /// Mark a unused key
    pub fn mark(&mut self) -> PageId {
        let mut idx = 0;
        loop {
            let bm = &mut self.bms[idx];
            let len = bm.len();
            if len < self.cap {
                let id = bm.mark().unwrap();
                let mut res = idx as PageId * self.cap as PageId + id as PageId;
                // last bm is almost full (one key left), alloc new bitmap by the last one.
                if len + 1 == self.cap && idx == self.bms.len() - 1 {
                    let mut new_bm = unsafe { BitMap::new(res, self.pm.clone()) };
                    let id = new_bm.mark().unwrap();
                    res = self.cap as PageId * (idx + 1) as PageId + id as PageId;
                    self.bms.push(new_bm);
                }
                return res;
            }
            idx += 1;
        }
    }

    /// Unmark `id`. If `id` is not in use, return false, else return true.
    pub fn unmark(&mut self, id: PageId) -> bool {
        let idx = (id / self.cap as PageId) as usize;
        let id = (id % self.cap as PageId) as BmKey;
        let bm = &mut self.bms[idx];
        if !bm.unmark(id) {
            return false;
        }
        // if last bitmap if empty, free that page
        if bm.len() == 0 && idx == self.bms.len() - 1 {
            let del_bm = self.bms.pop().unwrap();
            assert!(self.unmark(del_bm.page_id()));
            // todo detele page
            // self.pm.delete_page(del_bm.into_page());
        }
        true
    }

    /// Check if `id` is used
    pub fn check(&self, id: PageId) -> bool {
        let idx = (id / self.cap as PageId) as usize;
        let id = (id % self.cap as PageId) as BmKey;
        if idx >= self.bms.len() {
            return false;
        }
        self.bms[idx].check(id)
    }
}

#[cfg(test)]
mod test {
    use std::{fs::remove_dir_all, path::Path};

    use crate::page_manager::p_manager::{FHandler, PManager};

    use super::*;

    #[test]
    fn test_seq() {
        let test_dir = "./test_dir/bitmap_list1";
        if Path::exists(Path::new(test_dir)) {
            remove_dir_all(test_dir).unwrap();
        }
        let pm = Arc::new(PageManager::new(test_dir));
        let mut bms = unsafe { BitMapList::new(0, pm) };
        assert!(bms.check(0));
        let nn = 10000;
        let mut ids = Vec::new();
        for _i in 0..nn {
            ids.push(bms.mark());
        }
        for id in 0..nn {
            assert!(bms.check(id));
        }
        let ids1 = &ids[0..ids.len() / 2];
        for id in ids1 {
            bms.unmark(*id);
        }
        for _i in 0..ids1.len() {
            bms.mark();
        }
        for id in ids {
            assert!(bms.unmark(id));
        }
        assert!(bms.check(0));
        assert_eq!(bms.bms.len(), 1);
    }

    #[test]
    fn test_persist() {
        let test_dir = "./test_dir/bitmap_list2";
        if Path::exists(Path::new(test_dir)) {
            remove_dir_all(test_dir).unwrap();
        }
        unsafe {
            let pm = Arc::new(PageManager::new(test_dir));
            let mut bms = BitMapList::new(0, pm.clone());
            let nn = 10000;
            let mut ids = Vec::new();
            for _i in 0..nn {
                ids.push(bms.mark());
            }
            let len = ids.len() / 2;
            for _i in 0..len {
                assert!(bms.unmark(ids.pop().unwrap()));
            }
            pm.flush().join();

            let pm = Arc::new(PageManager::load(test_dir));
            let mut bms = BitMapList::load(0, pm.clone());
            for id in ids {
                assert!(bms.unmark(id));
            }
            pm.flush().join();

            let pm = Arc::new(PageManager::load(test_dir));
            let bms = BitMapList::load(0, pm);
            assert!(bms.check(0));
            assert_eq!(bms.bms.len(), 1);
        }
    }
}
