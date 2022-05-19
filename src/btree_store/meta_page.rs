use std::ptr::NonNull;

use memmap2::MmapMut;

use super::page_manager::page::{Page, PageId, PageRef};

/// meta data of btree that need to store on disk
pub struct BTreeMeta<K> {
    pub root: PageId,
    pub depth: usize,
    pub len: usize,
    pub min_key: K,
}

pub struct BTreeMetaRef<K> {
    meta: NonNull<BTreeMeta<K>>,
}

unsafe impl<K> Send for BTreeMetaRef<K> {}

unsafe impl<K> Sync for BTreeMetaRef<K> {}

impl<K> PageRef for BTreeMetaRef<K> {
    fn load_from(mmap: &MmapMut) -> Self {
        let ptr = mmap.as_ptr() as *mut BTreeMeta<K>;
        Self {
            meta: NonNull::new(ptr).unwrap(),
        }
    }
}

impl<K> BTreeMetaRef<K> {
    #[inline]
    pub fn meta(&self) -> &BTreeMeta<K> {
        unsafe { self.meta.as_ref() }
    }

    #[inline]
    pub fn meta_mut(&mut self) -> &mut BTreeMeta<K> {
        unsafe { self.meta.as_mut() }
    }
}

pub type MetaPage<K> = Page<BTreeMetaRef<K>>;
