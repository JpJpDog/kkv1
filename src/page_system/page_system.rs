use std::sync::{Arc, RwLock};

use super::bitmap_list::BitMapList;

use crate::page_manager::{
    p_manager::PManager,
    page::{Page, PageId, PageRef},
    page_manager::{FlushHandler, PageManager},
};

pub struct PageSystem {
    pm: Arc<PageManager>,
    bms: RwLock<BitMapList>,
}

impl PageSystem {
    const BOOT_PAGE_ID: PageId = 0;

    pub fn new(root_dir: &str) -> Self {
        let pm = Arc::new(PageManager::new(root_dir));
        let bms = unsafe { BitMapList::new(Self::BOOT_PAGE_ID, pm.clone()) };
        let bms = RwLock::new(bms);
        Self { bms, pm }
    }

    pub unsafe fn load(root_dir: &str) -> Self {
        let pm = Arc::new(PageManager::load(root_dir));
        let bms = BitMapList::load(Self::BOOT_PAGE_ID, pm.clone());
        let bms = RwLock::new(bms);
        Self { bms, pm }
    }

    #[inline]
    pub fn new_page<T: PageRef>(&self) -> Page<T, PageManager> {
        let page_id = self.bms.write().unwrap().mark();
        PageManager::new_page(self.pm.clone(), page_id)
    }

    #[inline]
    pub unsafe fn load_page<T: PageRef>(&self, page_id: PageId) -> Page<T, PageManager> {
        assert!(self.bms.read().unwrap().check(page_id));
        PageManager::load_page(self.pm.clone(), page_id)
    }

    #[inline]
    pub fn delete_page<T: PageRef>(&self, page: &Page<T, PageManager>) {
        assert!(self.bms.write().unwrap().unmark(page.page_id));
        // todo! delete page in page_manager
        // self.pm.delete_page(page)
    }

    #[inline]
    pub fn flush(&self) -> FlushHandler {
        self.pm.flush()
    }
}
