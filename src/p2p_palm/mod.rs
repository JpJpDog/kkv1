pub mod palm_msg;
mod palm_worker;
#[cfg(test)]
mod test;

use std::{
    cell::UnsafeCell,
    sync::{
        mpsc::{channel, Receiver, Sender},
        Arc,
    },
    thread::{self, JoinHandle},
};

use crate::{
    btree_util::btree_store::{RawBTreeStore, DEFAULT_BTREE_STORE_CONFIG},
    page_manager::FlushHandler,
};

use self::{
    palm_msg::{DataReq, InnerReq, NewRootReq, PALMReq, PALMRsp, WorkerReq, WorkerRsp},
    palm_worker::PALMWorker,
};

pub struct P2PPALMConfig {
  pub   thread_n: usize,
    pub node_cap: usize,
}

pub const DEFAULT_P2P_PALM_CONFIG: P2PPALMConfig = P2PPALMConfig {
    thread_n: 15,
    node_cap: 0,
};

pub struct P2PPALMTree<K: Ord + Clone, V: Clone> {
    pub store: Arc<RawBTreeStore<K, V>>,
    config: P2PPALMConfig,
    worker_req_txs: Vec<Sender<WorkerReq<K, V>>>,
    worker_rsp_rxs: Vec<Receiver<WorkerRsp<K>>>,
    worker_handlers: Vec<Option<JoinHandle<()>>>,
}

impl<
        K: Ord + Clone + Send + Sync + 'static + Default,
        V: Clone + Send + Sync + 'static + Default,
    > P2PPALMTree<K, V>
{
    fn init(config: P2PPALMConfig, store: Arc<RawBTreeStore<K, V>>) -> Self {
        let thread_n = config.thread_n;

        let mut to_prev_list = Vec::new(); // idx is its receiver
        let mut to_next_list = Vec::new(); // idx is its sender
        for i in 0..thread_n - 1 {
            to_prev_list.push(Some(channel()).unzip());
            to_next_list.push(Some(channel()).unzip());
        }
        let mut worker_req_txs = Vec::new();
        let mut worker_rsp_rxs = Vec::new();
        let mut worker_handlers = Vec::new();
        for i in 0..thread_n {
            let (worker_req_tx, worker_req_rx) = channel();
            let (worker_rsp_tx, worker_rsp_rx) = channel();
            worker_req_txs.push(worker_req_tx);
            worker_rsp_rxs.push(worker_rsp_rx);
            let prev_tx = (i > 0).then(|| to_prev_list[i - 1].0.take().unwrap());
            let next_tx = (i < thread_n - 1).then(|| to_next_list[i].0.take().unwrap());
            let prev_rx = (i > 0).then(|| to_next_list[i - 1].1.take().unwrap());
            let next_rx = (i < thread_n - 1).then(|| to_prev_list[i].1.take().unwrap());
            let store1 = store.clone();
            let worker = PALMWorker::<K, V> {
                worker_id: i,
                store: store1,
                prev_tx,
                next_tx,
                prev_rx,
                next_rx,
                worker_req_rx,
                worker_rsp_tx,
            };
            worker_handlers.push(Some(thread::spawn(move || {
                worker.check();
                worker.routine();
            })));
        }
        Self {
            store,
            config,
            worker_handlers,
            worker_req_txs,
            worker_rsp_rxs,
        }
    }

    pub unsafe fn load(root_dir: &str, config: P2PPALMConfig) -> Self {
        let mut store_config = DEFAULT_BTREE_STORE_CONFIG;
        store_config.node_cap = config.node_cap;
        let store = Arc::new(RawBTreeStore::load(root_dir, store_config));
        Self::init(config, store)
    } 
    pub fn new(root_dir: &str, min_key: K, config: P2PPALMConfig) -> Self {
        let mut store_config = DEFAULT_BTREE_STORE_CONFIG;
        store_config.node_cap = config.node_cap;
        let store = Arc::new(RawBTreeStore::new(root_dir, store_config, min_key));
        Self::init(config, store)
    }

    pub fn op(&self, mut reqs: Vec<PALMReq<K, V>>) -> Vec<PALMRsp> {
        let mut req_idxes = Vec::from_iter(0..reqs.len());
        req_idxes.sort_by(|i1, i2| reqs[*i1].key().cmp(reqs[*i2].key()));
        reqs.sort_by(|c1, c2| c1.key().cmp(c2.key()));
        let reqs = Arc::new(reqs);

        let req_n = reqs.len();
        let thread_n = self.config.thread_n;
        let req_per_thread = req_n.div_ceil(thread_n);
        let used_thread_n = req_n.div_ceil(req_per_thread);

        let mut cur_off = 0;
        let find_result = Arc::new(UnsafeCell::new(vec![Vec::new(); req_n]));
        for tx in self.worker_req_txs.iter().take(used_thread_n) {
            let len = std::cmp::min(req_per_thread, req_n - cur_off);
            assert!(len > 0);
            let req = DataReq {
                reqs: reqs.clone(),
                off: cur_off,
                len,
                find_result: find_result.clone(),
                used_thread_n,
            };
            cur_off += len;
            tx.send(WorkerReq::Data(req)).unwrap();
        }

        let mut idxes = Vec::new();
        let mut reqs = Vec::new();
        let mut rsps = Vec::new();

        for rx in self.worker_rsp_rxs.iter().take(used_thread_n) {
            let msg = rx.recv().unwrap();
            match msg {
                WorkerRsp::Data(mut inner_rsp) => {
                    rsps.append(&mut inner_rsp.rsps);
                    reqs.append(&mut inner_rsp.inner_reqs);
                    idxes.append(&mut inner_rsp.inner_req_idxes);
                }
                WorkerRsp::Inner(_) => panic!(),
                WorkerRsp::NewRoot => panic!(),
            }
        }
        assert_eq!(rsps.len(), req_n);

        let mut rsp1 = Box::new_uninit_slice(rsps.len());
        for (i, rsp) in rsps.into_iter().enumerate() {
            rsp1[req_idxes[i]].write(rsp);
        }
        let result = unsafe { rsp1.assume_init() }.into_vec();

        let find_result = unsafe { &*find_result.get() };
        let depth = find_result[0].len();
        let mut dep = depth - 1;
        while !reqs.is_empty() && dep >= 1 {
            dep -= 1;
            let mut node_ids = Vec::new();
            let mut cur_id = find_result[idxes[0]][dep];
            let mut chunk_locs = vec![0];
            for (i, idx) in idxes.iter().enumerate() {
                let node_id = find_result[*idx][dep];
                if node_id != cur_id {
                    cur_id = node_id;
                    chunk_locs.push(i);
                }
                node_ids.push(node_id);
            }
            let mut off_list = Vec::new();
            for loc in chunk_locs
                .iter()
                .step_by(chunk_locs.len().div_ceil(thread_n))
            {
                off_list.push(*loc);
            }
            let mut inner_reqs = Vec::new();
            {
                let reqs = Arc::new(reqs);
                let idxes = Arc::new(idxes);
                let node_ids = Arc::new(node_ids);
                for (i, off) in off_list.iter().enumerate() {
                    let len = off_list.get(i + 1).cloned().unwrap_or(reqs.len()) - off;
                    inner_reqs.push(InnerReq {
                        reqs: reqs.clone(),
                        idxes: idxes.clone(),
                        node_ids: node_ids.clone(),
                        off: off_list[i],
                        len,
                    });
                }
            }
            let used_thread_n = inner_reqs.len();
            for (i, req) in inner_reqs.into_iter().enumerate() {
                self.worker_req_txs[i].send(WorkerReq::Inner(req)).unwrap();
            }
            reqs = Vec::new();
            idxes = Vec::new();
            for rx in self.worker_rsp_rxs.iter().take(used_thread_n) {
                let msg = rx.recv().unwrap();
                match msg {
                    WorkerRsp::Data(_) => panic!(),
                    WorkerRsp::Inner(mut inner_rsp) => {
                        reqs.append(&mut inner_rsp.inner_reqs);
                        idxes.append(&mut inner_rsp.inner_req_idxes);
                    }
                    WorkerRsp::NewRoot => panic!(),
                }
            }
        }
        if !reqs.is_empty() {
            self.worker_req_txs[0]
                .send(WorkerReq::NewRoot(NewRootReq { reqs }))
                .unwrap();
            let msg = self.worker_rsp_rxs[0].recv().unwrap();
            match msg {
                WorkerRsp::Data(_) => panic!(),
                WorkerRsp::Inner(_) => panic!(),
                WorkerRsp::NewRoot => {}
            }
        }
        result
    }

    pub fn flush(&self) -> FlushHandler {
        self.store.update_cache();
        self.store.flush()
    }
}

impl<K: Ord + Clone, V: Clone> Drop for P2PPALMTree<K, V> {
    fn drop(&mut self) {
        for end_tx in self.worker_req_txs.iter() {
            end_tx.send(WorkerReq::End).unwrap();
        }
        for handler in self.worker_handlers.iter_mut() {
            handler.take().unwrap().join().unwrap();
        }
    }
}
