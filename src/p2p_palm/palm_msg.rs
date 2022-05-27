use std::{cell::UnsafeCell, ptr::NonNull, sync::Arc};

use crate::btree_node::NodeId;

#[derive(Clone)]
pub enum PALMReq<K, V> {
    Insert { key: K, val: NonNull<V> },
    Get { key: K, dest: NonNull<V> },
    Remove { key: K },
}

impl<K, V> PALMReq<K, V> {
    pub fn key(&self) -> &K {
        match self {
            PALMReq::Insert { key, val: _ } => key,
            PALMReq::Get { key, dest: _ } => key,
            PALMReq::Remove { key } => key,
        }
    }
}

#[derive(Clone)]
pub enum PALMRsp {
    Insert,
    Get(bool),
    Remove,
}

pub struct DataReq<K, V> {
    pub reqs: Arc<Vec<PALMReq<K, V>>>,
    pub find_result: Arc<UnsafeCell<Vec<Vec<NodeId>>>>,
    pub off: usize,
    pub len: usize,
    pub used_thread_n: usize,
}

unsafe impl<K, V> Send for DataReq<K, V> {}

pub struct DataRsp<K> {
    pub rsps: Vec<PALMRsp>,
    pub inner_reqs: Vec<PALMInnerReq<K>>,
    pub inner_req_idxes: Vec<usize>,
}

#[derive(Clone)]
pub enum PALMInnerReq<K> {
    Insert { key: K, node_id: NodeId },
    Remove { key: K },
}

impl<K> PALMInnerReq<K> {
    pub fn key(&self) -> &K {
        match self {
            PALMInnerReq::Insert { key, node_id: _ } => key,
            PALMInnerReq::Remove { key } => key,
        }
    }
}

pub struct InnerReq<K> {
    pub reqs: Arc<Vec<PALMInnerReq<K>>>,
    pub idxes: Arc<Vec<usize>>,
    pub node_ids: Arc<Vec<NodeId>>,
    pub off: usize,
    pub len: usize,
}

pub struct InnerRsp<K> {
    pub inner_reqs: Vec<PALMInnerReq<K>>,
    pub inner_req_idxes: Vec<usize>,
}

pub struct NewRootReq<K> {
    pub reqs: Vec<PALMInnerReq<K>>,
}

pub struct NewRootRsp<K> {
    pub inner_reqs: Vec<PALMInnerReq<K>>,
}

pub enum WorkerReq<K, V> {
    Data(DataReq<K, V>),
    Inner(InnerReq<K>),
    NewRoot(NewRootReq<K>),
    End,
}

pub enum WorkerRsp<K> {
    Data(DataRsp<K>),
    Inner(InnerRsp<K>),
    NewRoot,
}
