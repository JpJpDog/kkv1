use std::sync::{Arc, RwLock};

use crossbeam::{
    channel::{Receiver, Sender},
    select,
};

use crate::{
    btree_node::{btree_node::{InnerNode, DataNode}, btree_store::BTreeStore},
    page_manager::page::PageId,
};

use super::palm_msg::{
    DataReq, DataReqs, DataRsps, FindReqs, FindRsps, InnerCmd, InnerReq, InnerReqs, InnerRsps,
    PALMCmd, PALMResult,
};

pub struct PALMWorker<K: Clone + Ord, V: Clone> {
    store: Arc<BTreeStore<K, V>>,
    find_req_rx: Receiver<FindReqs<K>>,
    find_rsp_tx: Sender<FindRsps>,
    data_req_rx: Receiver<DataReqs<K, V>>,
    data_rsp_tx: Sender<DataRsps<K>>,
    inner_req_rx: Receiver<InnerReqs<K>>,
    inner_rsp_tx: Sender<InnerRsps<K>>,
    end_rx: Receiver<()>,
}

impl<K: Clone + Ord, V: Clone> PALMWorker<K, V> {
    pub fn new(
        store: Arc<BTreeStore<K, V>>,
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
                let inner_g = inner.try_read().unwrap();
                for j in k1..k1 + req_n {
                    let id = inner_g.get(&keys[j]).unwrap().val;
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
            let datas = vec![self.store.load_data(req.page_id).unwrap()];
            let mut data_gs = vec![datas[0].try_write().unwrap()];
            let mut data_ids = vec![(req.page_id, 0)];
            let cmds = unsafe { req.cmds.as_ref() };
            for cmd in cmds {
                let mut data_idx = 1;
                while data_idx < data_gs.len() {
                    if &data_gs[data_idx].first().unwrap().key > cmd.key() {
                        break;
                    }
                    data_idx += 1;
                }
                let data_g = &mut data_gs[data_idx - 1];
                let res = match cmd {
                    PALMCmd::Insert { key, val } => {
                        let val = unsafe { val.as_ref() };
                        if !data_g.insert(key, val) {
                            let (rid, right) = self.store.new_data();
                            data_ids.insert(data_idx, (rid, off));
                            // safe because `data_gs` owns RwLockWriteGuard that refers to RwLock 'inside' Arc, which will not change when Vec is inserted a new elem
                            unsafe {
                                &mut *(&datas as *const _ as *mut Vec<Arc<RwLock<DataNode<K, V>>>>)
                            }
                            .insert(data_idx, right);
                            let mut right_g = datas[data_idx].try_write().unwrap();
                            data_g.split_to(&mut right_g, None);
                            // insert the new kv
                            if &right_g.first().unwrap().key > key {
                                data_g.insert(key, val);
                            } else {
                                right_g.insert(key, val);
                            }
                            data_gs.insert(data_idx, right_g);
                        }
                        PALMResult::Insert
                    }
                    PALMCmd::Get { key, mut dest } => {
                        let kv = data_g.get(key);
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
            for i in 1..data_gs.len() {
                let key = data_gs[i].first().unwrap().key.clone();
                let (id, idx) = data_ids[i];
                inner_cmds.push((InnerCmd::Insert { key, id }, idx));
            }
        }
        (results, inner_cmds)
    }

    fn inner_op(&self, reqs: &[InnerReq<K>]) -> Vec<(InnerCmd<K>, usize)> {
        // let mut cnt = 0;
        // reqs.iter().for_each(|e| cnt += e.cmds.len());
        // println!("inner {}", cnt);
        let mut inner_cmds = Vec::new();
        for req in reqs {
            let inners = vec![self.store.load_inner(req.page_id).unwrap()];
            let mut inner_gs = vec![inners[0].try_write().unwrap()];
            let mut inner_ids = vec![(req.page_id, 0)];
            for (cmd, idx) in req.cmds.iter() {
                let mut inner_idx = 1;
                while inner_idx < inner_gs.len() {
                    if &inner_gs[inner_idx].first().unwrap().key > cmd.key() {
                        break;
                    }
                    inner_idx += 1;
                }
                let inner_g = &mut inner_gs[inner_idx - 1];
                match cmd {
                    InnerCmd::Insert { key, id } => {
                        if !inner_g.insert(key, id) {
                            let (rid, right) = self.store.new_inner();
                            inner_ids.insert(inner_idx, (rid, *idx));
                            unsafe {
                                &mut *(&inners as *const _ as *mut Vec<Arc<RwLock<InnerNode<K>>>>)
                            }
                            .insert(inner_idx, right);
                            let mut right_g = inners[inner_idx].try_write().unwrap();
                            inner_g.split_to(&mut right_g, None);
                            // insert new kv
                            if &right_g.first().unwrap().key > key {
                                inner_g.insert(key, id);
                            } else {
                                right_g.insert(key, id);
                            }
                            inner_gs.insert(inner_idx, right_g);
                        }
                    }
                    InnerCmd::Remove { key } => todo!(),
                }
                for i in 1..inner_gs.len() {
                    let key = inner_gs[i].first().unwrap().key.clone();
                    let id = inner_ids[i].0;
                    inner_cmds.push((InnerCmd::Insert { key, id }, inner_ids[i].1));
                }
            }
        }
        inner_cmds
    }
}
