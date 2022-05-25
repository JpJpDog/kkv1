use std::sync::Arc;

use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

use crate::{
    btree_util::{btree_store::RawBTreeStore, node_container::NodeContainer},
    page_manager::page::PageId,
};

use super::palm_msg::{
    DataReq, DataReqs, DataRsps, FindReqs, FindRsps, InnerCmd, InnerReq, InnerReqs, InnerRsps,
    PALMCmd, PALMResult,
};

pub struct PALMWorker<K: Clone + Ord, V: Clone> {
    store: Arc<RawBTreeStore<K, V>>,
    find_req_rx: Receiver<FindReqs<K>>,
    find_rsp_tx: Sender<FindRsps>,
    data_req_rx: Receiver<DataReqs<K, V>>,
    data_rsp_tx: Sender<DataRsps<K>>,
    inner_req_rx: Receiver<InnerReqs<K>>,
    inner_rsp_tx: Sender<InnerRsps<K>>,
    end_rx: Receiver<()>,
}

impl<K: Clone + Ord + Default, V: Clone + Default> PALMWorker<K, V> {
    pub fn new(
        store: Arc<RawBTreeStore<K, V>>,
        find_req_rx: Receiver<FindReqs<K>>,
        find_rsp_tx: Sender<FindRsps>,
        data_req_rx: Receiver<DataReqs<K, V>>,
        data_rsp_tx: Sender<DataRsps<K>>,
        inner_req_rx: Receiver<InnerReqs<K>>,
        inner_rsp_tx: Sender<InnerRsps<K>>,
        end_rx: Receiver<()>,
    ) -> Self {
        Self {
            store,
            find_req_rx,
            find_rsp_tx,
            data_req_rx,
            data_rsp_tx,
            inner_req_rx,
            inner_rsp_tx,
            end_rx,
        }
    }

    pub fn routine(&self) {
        loop {
            select! {
                recv(self.find_req_rx) -> msg => {
                    let req = msg.unwrap();
                    let find = self.find(unsafe { req.keys.as_ref() });
                    self.find_rsp_tx.send(FindRsps{ find, idx: req.idx}).unwrap();
                },
                recv(self.data_req_rx) -> msg => {
                    let req = msg.unwrap();
                    let (results, inners) = self.data_op(unsafe {req.reqs.as_ref()}, req.off);
                    let rsp = DataRsps{ results, inners, off: req.off, idx: req.idx };
                    self.data_rsp_tx.send(rsp).unwrap();
                }
                recv(self.inner_req_rx) -> msg => {
                    let req = msg.unwrap();
                    let inners = self.inner_op(unsafe {req.reqs.as_ref()});
                    let rsp = InnerRsps { inners, idx: req.idx};
                    self.inner_rsp_tx.send(rsp).unwrap();
                },
                recv(self.end_rx) -> _msg => {
                    break;
                }
            }
        }
    }

    /// Find node id from btree according to the `keys`. first return the node id corresponding to every key.
    /// then return the parent node id of every distinct child node
    fn find(&self, keys: &[K]) -> Vec<Vec<(PageId, usize)>> {
        // println!("find {}", keys.len());
        let meta = self.store.meta.try_read().unwrap();
        let meta_g = meta.read().meta();
        let depth = meta_g.depth;
        let mut page_ids = vec![(meta_g.root, keys.len())];
        let mut page_ids_list = Vec::new();
        for _i in 0..depth - 1 {
            let mut page_ids1 = Vec::new();
            let mut k1 = 0;
            for (id, req_n) in page_ids.iter() {
                let inner = self.store.load_inner(*id).unwrap();
                for key in keys.iter().skip(k1).take(*req_n) {
                    let id = inner.read().get(key).unwrap().val;
                    if let Some((last_id, n)) = page_ids1.last_mut() {
                        if *last_id == id {
                            *n += 1;
                        } else {
                            page_ids1.push((id, 1));
                        }
                    } else {
                        page_ids1.push((id, 1));
                    }
                }
                k1 += req_n;
            }
            page_ids_list.push(page_ids);
            page_ids = page_ids1;
        }
        page_ids_list.push(page_ids);
        page_ids_list
    }

    fn data_op(
        &self,
        reqs: &[DataReq<K, V>],
        mut off: usize,
    ) -> (Vec<PALMResult>, Vec<(InnerCmd<K>, usize)>) {
        let mut results = Vec::new();
        let mut inner_cmds = Vec::new();
        for req in reqs.iter() {
            let mut datas = vec![self.store.load_data(req.page_id).unwrap()];
            let mut data_ids = vec![(req.page_id, 0)];
            let cmds = unsafe { req.cmds.as_ref() };
            for cmd in cmds {
                let mut data_idx = 1;
                while data_idx < datas.len() {
                    if &datas[data_idx].read().first().unwrap().key > cmd.key() {
                        break;
                    }
                    data_idx += 1;
                }
                let mut data = datas[data_idx - 1].clone();
                let res = match cmd {
                    PALMCmd::Insert { key, val } => {
                        let val = unsafe { val.as_ref() };
                        if !data.write().insert(key, val) {
                            let (rid, mut right) = self.store.new_data();
                            data_ids.insert(data_idx, (rid, off));
                            data.write().split_to(right.write(), None);
                            // insert the new kv
                            if &right.read().first().unwrap().key > key {
                                data.write().insert(key, val);
                            } else {
                                right.write().insert(key, val);
                            }
                            datas.insert(data_idx, right);
                        }
                        PALMResult::Insert
                    }
                    PALMCmd::Get { key, mut dest } => {
                        let kv = data.read().get(key);
                        if let Some(kv) = kv {
                            if &kv.key == key {
                                unsafe { *dest.as_mut() = kv.val.clone() };
                                PALMResult::Get(true)
                            } else {
                                PALMResult::Get(false)
                            }
                        } else {
                            PALMResult::Get(false)
                        }
                    }
                    PALMCmd::Remove { key: _ } => todo!(),
                };
                results.push(res);
                off += 1;
            }
            for i in 1..datas.len() {
                let key = datas[i].read().first().unwrap().key.clone();
                let (id, idx) = data_ids[i];
                inner_cmds.push((InnerCmd::Insert { key, id }, idx));
            }
        }
        (results, inner_cmds)
    }

    fn inner_op(&self, reqs: &[InnerReq<K>]) -> Vec<(InnerCmd<K>, usize)> {
        let mut inner_cmds = Vec::new();
        for req in reqs {
            let mut inners = vec![self.store.load_inner(req.page_id).unwrap()];
            let mut inner_ids = vec![(req.page_id, 0)];
            for (cmd, idx) in req.cmds.iter() {
                let mut inner_idx = 1;
                while inner_idx < inners.len() {
                    if &inners[inner_idx].read().first().unwrap().key > cmd.key() {
                        break;
                    }
                    inner_idx += 1;
                }
                let mut inner = inners[inner_idx - 1].clone();
                match cmd {
                    InnerCmd::Insert { key, id } => {
                        if !inner.write().insert(key, id) {
                            let (rid, mut right) = self.store.new_inner();
                            inner_ids.insert(inner_idx, (rid, *idx));
                            inner.write().split_to(right.write(), None);
                            // insert new kv
                            if &right.read().first().unwrap().key > key {
                                inner.write().insert(key, id);
                            } else {
                                right.write().insert(key, id);
                            }
                            inners.insert(inner_idx, right);
                        }
                    }
                    InnerCmd::Remove { key: _ } => todo!(),
                }
                for i in 1..inners.len() {
                    let key = inners[i].read().first().unwrap().key.clone();
                    let id = inner_ids[i].0;
                    inner_cmds.push((InnerCmd::Insert { key, id }, inner_ids[i].1));
                }
            }
        }
        inner_cmds
    }
}
