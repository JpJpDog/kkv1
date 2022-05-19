use std::{mem::size_of, ptr::NonNull, sync::Arc};

use memmap2::MmapMut;

use super::page_manager::page::{Page, PageId, PageManager, PageRef};

pub type BmKey = u16;

pub struct BitMapPageMeta {
    len: BmKey,
}

pub struct BitMapPageRef {
    meta: NonNull<BitMapPageMeta>,
    map: NonNull<[u8]>,
}

unsafe impl Send for BitMapPageRef {}

unsafe impl Sync for BitMapPageRef {}

impl PageRef for BitMapPageRef {
    fn load_from(mmap: &MmapMut) -> Self {
        let ptr = mmap.as_ref();
        let (meta, ptr) = ptr.split_at(size_of::<BitMapPageRef>());
        let meta = NonNull::new(meta.as_ptr() as *mut BitMapPageMeta).unwrap();
        let map = NonNull::new(ptr as *const _ as *mut _).unwrap();
        Self { meta, map }
    }
}

pub type BitMapPage = Page<BitMapPageRef>;

pub struct BitMap {
    bm_page: Page<BitMapPageRef>,
    // cache of bitmap. Every 8 bit in `bm_page.map` corresponds to one bit in the lowest layer cache.
    // If cache is 1, it means all 8 bit in `bm_page.map` has be allocated already.
    indexes: Vec<Box<[u8]>>,
}

impl BitMap {
    /// Init a new bm from `page_id`.
    /// unsafe if `page_id` is in use
    pub unsafe fn new(page_id: PageId, pm: Arc<PageManager>) -> Self {
        let mut bm_page: BitMapPage = PageManager::new_page(pm, page_id);
        let mut bm_ref = bm_page.write();
        // init the data on mmap.
        bm_ref.meta.as_mut().len = 0;
        bm_ref.map.as_mut().fill(0);
        let indexes = Self::load_indexes(&bm_ref);
        drop(bm_ref);
        Self { bm_page, indexes }
    }

    /// Load a bm from disk by `page_id`.
    /// unsafe if `page_id` is not in use
    pub unsafe fn load(page_id: PageId, pm: Arc<PageManager>) -> Self {
        let mut bm_page: BitMapPage = PageManager::load_page(pm, page_id);
        let bm_ref = bm_page.write();
        let indexes = Self::load_indexes(&bm_ref);
        drop(bm_ref);
        Self { bm_page, indexes }
    }

    /// Load the cache (index) from `bm_ref.map`. Return vector of index
    fn load_indexes(bm_ref: &BitMapPageRef) -> Vec<Box<[u8]>> {
        let mut indexes = Vec::new();
        let mut ptr = unsafe { bm_ref.map.as_ref() };
        while ptr.len() > 1 {
            let mut index = unsafe { Box::new_uninit_slice(ptr.len().div_ceil(8)).assume_init() };
            index.fill(u8::MAX); // fill with 1 so the out of bound cache is init
            for (i, k) in ptr.iter().enumerate() {
                if *k != u8::MAX {
                    index[i / 8] &= !(1 << (i % 8));
                }
            }
            indexes.push(index);
            ptr = indexes.last().unwrap().as_ref();
        }
        assert_eq!(indexes.last().unwrap().len(), 1); //the top cache less than 8 bit
        indexes
    }

    #[inline]
    fn meta(&self) -> &BitMapPageMeta {
        unsafe { self.bm_page.read().meta.as_ref() }
    }

    #[inline]
    fn meta_mut(&mut self) -> &mut BitMapPageMeta {
        unsafe { self.bm_page.write().meta.as_mut() }
    }

    #[inline]
    fn map(&self) -> &[u8] {
        unsafe { self.bm_page.read().map.as_ref() }
    }

    #[inline]
    fn map_mut(&mut self) -> &mut [u8] {
        unsafe { self.bm_page.write().map.as_mut() }
    }

    /// Check if `key` is already in use
    #[inline]
    pub fn check(&self, key: BmKey) -> bool {
        self.map()[key as usize / 8] & (1 << (key % 8)) != 0
    }

    /// Mark a new BmKey. If no key if left, `None` return.
    pub fn mark(&mut self) -> Option<BmKey> {
        if self.indexes.last().unwrap()[0] == u8::MAX {
            return None;
        }
        self.meta_mut().len += 1;
        let mut at = 0;
        //search from the top cache for 0 (not all allocated)
        for index in self.indexes.iter().rev() {
            assert_ne!(index[at], u8::MAX);
            for i in 0..8 {
                if (index[at] & (1 << i)) == 0 {
                    at = at * 8 + i;
                    break;
                }
            }
        }
        let map_mut = self.map_mut();
        assert_ne!(map_mut[at], u8::MAX);
        //write key to bottom
        let mut key = 0;
        for i in 0..8 {
            if (map_mut[at] & (1 << i)) == 0 {
                map_mut[at] |= 1 << i;
                key = at * 8 + i;
                break;
            }
        }
        //update the cache
        if map_mut[at] == u8::MAX {
            let mut key1 = key / 8;
            for index in self.indexes.iter_mut() {
                index[key1 / 8] |= 1 << (key1 % 8);
                if index[key1 / 8] != u8::MAX {
                    break;
                }
                key1 /= 8;
            }
        }
        Some(key as BmKey)
    }

    /// Unmark `key`. If `key` is in use, return true, else return false.
    pub fn unmark(&mut self, key: BmKey) -> bool {
        self.meta_mut().len -= 1;
        let key = key as usize;
        let map_mut = self.map_mut();
        let update_cache = map_mut[key / 8] == u8::MAX;
        if (map_mut[key / 8] | (1 << (key % 8))) == 0 {
            return false;
        }
        map_mut[key / 8] &= !(1 << (key % 8));
        if update_cache {
            let mut key1 = key / 8;
            let mut i = 0;
            let indexes_len = self.indexes.len();
            loop {
                let index = &mut self.indexes[i];
                let flag = index[key1 / 8] == u8::MAX;
                index[key1 / 8] &= !(1 << (key1 % 8));
                i += 1;
                key1 /= 8;
                if !flag || i == indexes_len {
                    break;
                }
            }
        }
        true
    }

    #[inline]
    pub fn len(&self) -> BmKey {
        self.meta().len
    }

    #[inline]
    pub fn capacity(&self) -> BmKey {
        self.map().len() as BmKey * 8
    }

    #[inline]
    pub fn page_id(&self) -> PageId {
        self.bm_page.page_id()
    }

    #[inline]
    pub fn into_page(self) -> BitMapPage {
        self.bm_page
    }
}

#[cfg(test)]
mod test {

    use std::{path::Path, fs::remove_dir_all};

    use rand::{prelude::StdRng, Rng, SeedableRng};
    use super::*;

    #[test]
    fn test_seq() {
        let test_dir = "test_dir/bitmap1";
        if Path::exists(Path::new(test_dir)) {
            remove_dir_all(test_dir).unwrap();
        }
        let pm = Arc::new(PageManager::new(test_dir));
        let mut bm = unsafe { BitMap::new(0, pm.clone()) };
        let cap = bm.capacity();
        for _j in 0..2 {
            for i in 0..cap {
                let key = bm.mark().unwrap();
                assert_eq!(key, i as BmKey);
            }
            pm.flush();
            bm = unsafe { BitMap::load(0, pm.clone()) };
            assert!(bm.mark().is_none());
            for i in 0..cap {
                assert!(bm.unmark(i as BmKey));
            }
        }
    }

    #[test]
    fn test_random() {
        let test_dir = "test_dir/bitmap2";
        if Path::exists(Path::new(test_dir)) {
            remove_dir_all(test_dir).unwrap();
        }
        let pm = Arc::new(PageManager::new(test_dir));
        let mut bm = unsafe { BitMap::new(0, pm.clone()) };
        let test_n = 10000;
        let mut keys = Vec::new();
        let mut rng = StdRng::seed_from_u64(0);
        for _i in 0..test_n {
            let op = rng.gen::<u32>() % 40;
            if op < 20 {
                if let Some(key) = bm.mark() {
                    keys.push(key);
                }
            } else if op <= 30 {
                if keys.len() > 0 {
                    let idx = rng.gen::<usize>() % keys.len();
                    let key = keys[idx];
                    assert!(bm.check(key));
                }
            } else if op < 38 {
                if keys.len() > 0 {
                    let len = keys.len();
                    let idx = rng.gen::<usize>() % len;
                    keys.swap(idx, len - 1);
                    let key = keys.pop().unwrap();
                    assert!(bm.unmark(key));
                }
            } else {
                pm.flush();
                bm = unsafe { BitMap::load(0, pm.clone()) };
            }
            assert_eq!(bm.meta().len, keys.len() as BmKey);
        }
    }
}
