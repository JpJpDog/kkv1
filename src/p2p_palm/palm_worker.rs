use std::sync::{
    mpsc::{Receiver, Sender},
    Arc,
};

use crate::{
    btree_node::NodeId,
    btree_util::{btree_store::RawBTreeStore, node_container::NodeContainer},
    p2p_palm::palm_msg::{DataRsp, InnerRsp, PALMInnerReq},
};

use super::palm_msg::{PALMReq, PALMRsp, WorkerReq, WorkerRsp};

pub struct PALMWorker<K: Clone + Ord, V: Clone> {
    pub worker_id: usize,
    pub store: Arc<RawBTreeStore<K, V>>,
    pub worker_req_rx: Receiver<WorkerReq<K, V>>,
    pub worker_rsp_tx: Sender<WorkerRsp<K>>,
    pub prev_tx: Option<Sender<NodeId>>,
    pub next_tx: Option<Sender<NodeId>>,
    pub prev_rx: Option<Receiver<NodeId>>,
    pub next_rx: Option<Receiver<NodeId>>,
}

impl<K: Clone + Ord + Default, V: Clone + Default> PALMWorker<K, V> {
    pub fn check(&self) {
        let id = self.worker_id;
        self.prev_tx.as_ref().map(|t| t.send(id as u32));
        self.next_tx.as_ref().map(|t| t.send(id as u32));
        if let Some(r) = &self.prev_rx {
            let pid = r.recv().unwrap() as usize;
            // println!("{} 's pev is {}", id, pid);
            assert_eq!(pid + 1, id);
        }
        if let Some(r) = &self.next_rx {
            let nid = r.recv().unwrap() as usize;
            // println!("{} 's next is {}", self.worker_id, nid);
            assert_eq!(nid - 1, id);
        }
    }

    pub fn routine(&self) {
        loop {
            let msg = self.worker_req_rx.recv().unwrap();
            match msg {
                WorkerReq::Data(data_req) => {
                    let find_result = unsafe { &mut (*data_req.find_result.get()) };
                    let (off, len) = (data_req.off, data_req.len);
                    let mut keys = Vec::new();
                    for req in data_req.reqs.iter().skip(off).take(len) {
                        keys.push(req.key().clone());
                    }
                    let result = &mut find_result[off..off + len];
                    // println!("{} find {}", self.worker_id, off);
                    self.find(keys, result);
                    let (start, end) =
                        self.sync_and_redistribute(off, len, find_result, data_req.used_thread_n);

                    // println!("{}, {}; {}, {}", off, len, start, end);
                    let data_rsp = if start <= end {
                        let (off, len) = (start, end - start + 1);
                        let reqs = &data_req.reqs[off..off + len];
                        let result = &mut find_result[off..off + len];
                        let (rsps, inner_reqs, mut inner_req_idxes) = self.data_work(reqs, result);
                        for i in inner_req_idxes.iter_mut() {
                            *i += off;
                        }
                        DataRsp {
                            rsps,
                            inner_reqs,
                            inner_req_idxes,
                        }
                    } else {
                        DataRsp {
                            rsps: Vec::new(),
                            inner_reqs: Vec::new(),
                            inner_req_idxes: Vec::new(),
                        }
                    };
                    self.worker_rsp_tx.send(WorkerRsp::Data(data_rsp)).unwrap();
                }
                WorkerReq::Inner(inner_req) => {
                    let (off, len) = (inner_req.off, inner_req.len);
                    let (inner_reqs, inner_req_idxes) = self.inner_work(
                        &inner_req.reqs[off..off + len],
                        &inner_req.idxes[off..off + len],
                        &inner_req.node_ids[off..off + len],
                    );
                    let inner_rsp = InnerRsp {
                        inner_reqs,
                        inner_req_idxes,
                    };
                    self.worker_rsp_tx
                        .send(WorkerRsp::Inner(inner_rsp))
                        .unwrap();
                }
                WorkerReq::NewRoot(new_root_req) => {
                    self.new_root(new_root_req.reqs);
                    self.worker_rsp_tx.send(WorkerRsp::NewRoot).unwrap();
                }
                WorkerReq::End => break,
            }
        }
    }

    fn find(&self, keys: Vec<K>, result: &mut [Vec<NodeId>]) {
        let meta_g = self.store.meta.read().unwrap();
        let meta = meta_g.read().meta();
        let root_id = meta.root;
        let depth = meta.depth;

        assert_eq!(result.len(), keys.len());
        // assert_eq!(result[0].len(), depth);

        result.iter_mut().for_each(|r| r.push(root_id));
        for j in 1..depth {
            let mut parent_id = *result[0].last().unwrap();
            let mut parent = self.store.load_inner(parent_id).unwrap();
            for (i, key) in keys.iter().cloned().enumerate() {
                if result[i][j - 1] != parent_id {
                    parent_id = *result[i].last().unwrap();
                    parent = self.store.load_inner(parent_id).unwrap();
                }
                let kv = parent.read().get(&key).unwrap();
                result[i].push(kv.val);
            }
        }
    }

    fn sync_and_redistribute(
        &self,
        off: usize,
        len: usize,
        find_result: &Vec<Vec<NodeId>>,
        used_thread_n: usize,
    ) -> (usize, usize) {
        let find_first = *find_result[off].last().unwrap();
        let find_last = *find_result[off + len - 1].last().unwrap();
        // println!("{} first: {}, last: {}", off, find_first, find_last);
        let normal = find_first != find_last || self.next_tx.is_none();
        assert!(used_thread_n > self.worker_id);
        if used_thread_n != self.worker_id + 1 {
            if let Some(t) = &self.next_tx {
                t.send(find_last).unwrap();
                // println!("{} send next", off);
            }
        }
        let mut next_first = None;
        if !normal {
            if used_thread_n != self.worker_id + 1 {
                // println!("{} wait next", off);
                next_first = Some(self.next_rx.as_ref().unwrap().recv().unwrap());
                // println!("{} next arrive", off);
            }
            if let Some(t) = &self.prev_tx {
                t.send(find_first).unwrap();
                // println!("{} send prev", off);
            }
        } else {
            if let Some(t) = &self.prev_tx {
                t.send(find_first).unwrap();
                // println!("{} send prev", off);
            }
            if used_thread_n != self.worker_id + 1 {
                next_first = self.next_rx.as_ref().map(|r| {
                    // println!("{} wait next", off);
                    let n = r.recv().unwrap();
                    // println!("{} next arrive", off);
                    n
                });
            }
        }
        let prev_last = self.prev_rx.as_ref().map(|r| {
            // println!("{} wait prev", off);
            let p = r.recv().unwrap();
            // println!("{} prev arrive", off);
            p
        });

        let mut start = off;
        if let Some(p) = prev_last {
            if p == find_first {
                for ids in find_result.iter().skip(off).take(len) {
                    let id = *ids.last().unwrap();
                    if id != find_first {
                        break;
                    }
                    start += 1;
                }
            }
        }
        let mut end = off + len - 1;
        if start <= end {
            if let Some(n) = next_first {
                if n == find_last {
                    for ids in find_result.iter().skip(off + len) {
                        let id = *ids.last().unwrap();
                        if id != find_last {
                            break;
                        }
                        end += 1;
                    }
                }
            }
        }
        (start, end)
    }

    fn new_root(&self, mut reqs: Vec<PALMInnerReq<K>>) {
        loop {
            let mut inner_reqs = Vec::new();
            let (root_id, mut root) = self.store.new_inner();
            let mut meta = self.store.meta.write().unwrap();
            let mut meta = meta.write();
            let meta = meta.meta_mut();
            root.write().insert(&meta.min_key, &meta.root);
            let mut inners = vec![root];
            for req in reqs {
                let mut inner_idx = None;
                for (i, d) in inners.iter_mut().enumerate() {
                    let first_key = &d.read().first().unwrap().key;
                    if first_key > req.key() {
                        inner_idx = Some(i - 1);
                        break;
                    }
                }
                let inner_idx = inner_idx.unwrap_or(inners.len() - 1);
                let inner = &mut inners[inner_idx];

                match req {
                    PALMInnerReq::Insert { key, node_id: val } => {
                        if inner.write().insert(&key, &val) {
                            continue;
                        }
                        let (new_id, mut new_inner) = self.store.new_inner();
                        // !todo!
                        inner.write().split_to(new_inner.write(), None);
                        let mid_key = new_inner.read().first().unwrap().key.clone();
                        assert!(mid_key != key);
                        if mid_key < key {
                            assert!(new_inner.write().insert(&key, &val));
                        } else {
                            assert!(inner.write().insert(&key, &val));
                        }
                        inner_reqs.push(PALMInnerReq::Insert {
                            key: mid_key,
                            node_id: new_id,
                        });
                        inners.insert(inner_idx + 1, new_inner);
                    }
                    PALMInnerReq::Remove { key: _ } => panic!(),
                }
            }
            meta.depth += 1;
            meta.root = root_id;
            reqs = inner_reqs;
            if reqs.is_empty() {
                break;
            }
        }
    }

    fn inner_work(
        &self,
        reqs: &[PALMInnerReq<K>],
        req_idxes: &[usize],
        req_node_ids: &[NodeId],
    ) -> (Vec<PALMInnerReq<K>>, Vec<usize>) {
        let mut inner_reqs = Vec::new();
        let mut inner_req_idxes = Vec::new();
        let mut cur_id = req_node_ids[0];
        let mut inners = vec![self.store.load_inner(cur_id).unwrap()];
        for (i, req) in reqs.iter().enumerate() {
            let node_id = req_node_ids[i];
            if node_id != cur_id {
                cur_id = node_id;
                inners = vec![self.store.load_inner(cur_id).unwrap()];
            }
            let mut inner_idx = None;
            for (i, d) in inners.iter_mut().enumerate() {
                let first_key = &d.read().first().unwrap().key;
                if first_key > req.key() {
                    inner_idx = Some(i - 1);
                    break;
                }
            }
            let inner_idx = inner_idx.unwrap_or(inners.len() - 1);
            let inner = &mut inners[inner_idx];

            match req {
                PALMInnerReq::Insert { key, node_id: val } => {
                    if inner.write().insert(key, val) {
                        continue;
                    }
                    let (new_id, mut new_inner) = self.store.new_inner();
                    // ! todo! need old right here to update old_right's prev node id
                    inner.write().split_to(new_inner.write(), None);
                    let mid_key = new_inner.read().first().unwrap().key.clone();
                    assert!(mid_key != *key);
                    if mid_key < *key {
                        assert!(new_inner.write().insert(key, val));
                    } else {
                        assert!(inner.write().insert(key, val));
                    }
                    inner_reqs.push(PALMInnerReq::Insert {
                        key: mid_key,
                        node_id: new_id,
                    });
                    inner_req_idxes.push(req_idxes[i]);
                    inners.insert(inner_idx + 1, new_inner);
                }
                PALMInnerReq::Remove { key: _ } => todo!(),
            }
        }
        (inner_reqs, inner_req_idxes)
    }

    fn data_work(
        &self,
        reqs: &[PALMReq<K, V>],
        find_reqs: &[Vec<NodeId>],
    ) -> (Vec<PALMRsp>, Vec<PALMInnerReq<K>>, Vec<usize>) {
        let mut rsps = Vec::new();
        let mut inner_reqs = Vec::new();
        let mut inner_req_idxes = Vec::new();
        let mut cur_id = *find_reqs.first().unwrap().last().unwrap();
        let mut datas = vec![self.store.load_data(cur_id).unwrap()];
        for (i, req) in reqs.iter().enumerate() {
            let node_id = *find_reqs[i].last().unwrap();
            if node_id != cur_id {
                cur_id = node_id;
                datas = vec![self.store.load_data(cur_id).unwrap()];
            }
            let mut data_idx = None;
            for (i, d) in datas.iter_mut().enumerate() {
                let first_key = &d.read().first().unwrap().key;
                if first_key > req.key() {
                    data_idx = Some(i - 1);
                    break;
                }
            }
            let data_idx = data_idx.unwrap_or(datas.len() - 1);
            let data = &mut datas[data_idx];

            match req {
                PALMReq::Insert { key, val } => {
                    if !data.write().insert(key, unsafe { val.as_ref() }) {
                        let (new_id, mut new_data) = self.store.new_data();
                        // !! todo!() add old right
                        data.write().split_to(new_data.write(), None);
                        let mid_key = new_data.read().first().unwrap().key.clone();
                        assert!(mid_key != *key);
                        if mid_key < *key {
                            assert!(new_data.write().insert(key, unsafe { val.as_ref() }));
                        } else {
                            assert!(data.write().insert(key, unsafe { val.as_ref() }));
                        }
                        inner_reqs.push(PALMInnerReq::Insert {
                            key: mid_key,
                            node_id: new_id,
                        });
                        inner_req_idxes.push(i);
                        datas.insert(data_idx + 1, new_data);
                    }
                    rsps.push(PALMRsp::Insert);
                }
                PALMReq::Get { key, mut dest } => {
                    if let Some(kv) = data.read().get(key) {
                        if &kv.key == key {
                            *unsafe { dest.as_mut() } = kv.val.clone();
                            rsps.push(PALMRsp::Get(true));
                        } else {
                            rsps.push(PALMRsp::Get(false));
                        }
                    } else {
                        rsps.push(PALMRsp::Get(false));
                    }
                }
                PALMReq::Remove { key: _ } => todo!(),
            }
        }
        assert_eq!(rsps.len(), reqs.len());
        (rsps, inner_reqs, inner_req_idxes)
    }
}
