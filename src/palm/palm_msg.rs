use std::ptr::NonNull;

use crate::page_manager::page::PageId;

////////////////////////////////////////////////////////////

pub struct FindReqs<K> {
    pub keys: NonNull<[K]>,
    pub idx: usize,
}

unsafe impl<K> Send for FindReqs<K> {}

unsafe impl<K> Sync for FindReqs<K> {}

pub struct FindRsps {
    pub find: Vec<Vec<(PageId, usize)>>,
    pub idx: usize,
}

////////////////////////////////////////////////////////////
pub enum PALMCmd<K, V> {
    Insert { key: K, val: NonNull<V> },
    Get { key: K, dest: NonNull<V> },
    Remove { key: K },
}

impl<K, V> PALMCmd<K, V> {
    pub fn key(&self) -> &K {
        match self {
            PALMCmd::Insert { key, val: _ } => key,
            PALMCmd::Get { key, dest: _ } => key,
            PALMCmd::Remove { key } => key,
        }
    }
}

#[derive(Clone)]
pub enum PALMResult {
    Insert,
    Get(bool),
}

pub struct DataReq<K, V> {
    pub cmds: NonNull<[PALMCmd<K, V>]>,
    pub page_id: PageId,
}

pub struct DataReqs<K, V> {
    pub reqs: NonNull<[DataReq<K, V>]>,
    pub off: usize,
    pub idx: usize,
}

unsafe impl<K, V> Send for DataReqs<K, V> {}

unsafe impl<K, V> Sync for DataReqs<K, V> {}

pub struct DataRsps<K> {
    pub results: Vec<PALMResult>,
    pub inners: Vec<(InnerCmd<K>, usize)>,
    pub off: usize,
    pub idx: usize,
}

unsafe impl<K> Send for DataRsps<K> {}

unsafe impl<K> Sync for DataRsps<K> {}

////////////////////////////////////////////////////////////

pub enum InnerCmd<K> {
    Insert { key: K, id: PageId },
    Remove { key: K },
}

impl<K> InnerCmd<K> {
    pub fn key(&self) -> &K {
        match self {
            InnerCmd::Insert { key, id: _ } => key,
            InnerCmd::Remove { key } => key,
        }
    }
}

unsafe impl<K> Send for InnerCmd<K> {}

unsafe impl<K> Sync for InnerCmd<K> {}

pub struct InnerReq<K> {
    pub cmds: Vec<(InnerCmd<K>, usize)>,
    pub page_id: PageId,
}

pub struct InnerReqs<K> {
    pub reqs: NonNull<[InnerReq<K>]>,
    pub idx: usize,
}

unsafe impl<K> Send for InnerReqs<K> {}

unsafe impl<K> Sync for InnerReqs<K> {}

pub struct InnerRsps<K> {
    pub inners: Vec<(InnerCmd<K>, usize)>,
    pub idx: usize,
}
