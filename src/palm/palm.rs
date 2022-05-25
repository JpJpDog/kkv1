use crate::btree_util::btree_store::{BTreeStore, RawBTreeStore, DEFAULT_BTREE_STORE_CONFIG};
use crate::btree_util::node_container::NodeContainer;
use crate::page_manager::page::PageId;
use crate::page_manager::FlushHandler;
use crate::palm::palm_msg::{DataReq, InnerReq};
use crossbeam::channel::unbounded;
use crossbeam::channel::{Receiver, Sender};

use std::{
    cmp::{max, min},
    ptr::NonNull,
    sync::Arc,
    thread::{spawn, JoinHandle},
};

use super::{
    palm_msg::{DataReqs, DataRsps, FindReqs, FindRsps, InnerReqs, InnerRsps, PALMCmd, PALMResult},
    palm_worker::PALMWorker,
};

pub struct PALMConfig {
    pub thread_n: usize,
    pub min_find_per_thread: usize,
    pub node_cap: usize,
}

pub const DEFAULT_PALM_CONFIG: PALMConfig = PALMConfig {
    thread_n: 15,
    min_find_per_thread: 1,
    node_cap: 0,
};

////////////////////////////////////////////////////////////

pub struct PALMTree<K: 'static + Ord + Send + Sync + Clone, V: 'static + Clone + Send + Sync>
{
    config: PALMConfig,
    find_req_txs: Vec<Sender<FindReqs<K>>>,
    find_rsp_rx: Receiver<FindRsps>,
    data_req_txs: Vec<Sender<DataReqs<K, V>>>,
    data_rsp_rx: Receiver<DataRsps<K>>,
    inner_req_txs: Vec<Sender<InnerReqs<K>>>,
    inner_rsp_rx: Receiver<InnerRsps<K>>,
    handlers: Vec<JoinHandle<()>>,
    end_txs: Vec<Sender<()>>,
    pub store: Arc<RawBTreeStore<K, V>>,
}

impl<K: 'static + Ord + Send + Sync + Clone, V: 'static + Clone + Send + Sync>
    PALMTree<K, V>
{
    pub fn new(root_dir: &str, min_key: K, config: PALMConfig) -> Self {
        let mut store_config = DEFAULT_BTREE_STORE_CONFIG;
        store_config.node_cap = config.node_cap;
        let store = Arc::new(BTreeStore::new(root_dir, store_config, min_key));
        Self::new_with_store(store, config)
    }

    pub fn load(root_dir: &str, config: PALMConfig) -> Self {
        let mut store_config = DEFAULT_BTREE_STORE_CONFIG;
        store_config.node_cap = config.node_cap;
        let store = Arc::new(unsafe { BTreeStore::load(root_dir, store_config) });
        Self::new_with_store(store, config)
    }

    fn new_with_store(store: Arc<RawBTreeStore<K, V>>, config: PALMConfig) -> Self {
        let mut handlers = Vec::new();
        let mut find_req_txs = Vec::new();
        let (find_rsp_tx, find_rsp_rx) = unbounded();
        let mut data_req_txs = Vec::new();
        let (data_rsp_tx, data_rsp_rx) = unbounded();
        let mut inner_req_txs = Vec::new();
        let (inner_rsp_tx, inner_rsp_rx) = unbounded();
        let mut end_txs = Vec::new();
        for _i in 0..config.thread_n {
            let store1 = store.clone();
            let (find_req_tx, find_req_rx) = unbounded();
            find_req_txs.push(find_req_tx);
            let (data_req_tx, data_req_rx) = unbounded();
            data_req_txs.push(data_req_tx);
            let (inner_req_tx, inner_req_rx) = unbounded();
            inner_req_txs.push(inner_req_tx);
            let (end_tx, end_rx) = unbounded();
            end_txs.push(end_tx);
            let find_rsp_tx1 = find_rsp_tx.clone();
            let data_rsp_tx1 = data_rsp_tx.clone();
            let inner_rsp_tx1 = inner_rsp_tx.clone();
            let handler = spawn(move || {
                let slave = PALMWorker::new(
                    store1,
                    find_req_rx,
                    find_rsp_tx1,
                    data_req_rx,
                    data_rsp_tx1,
                    inner_req_rx,
                    inner_rsp_tx1,
                    end_rx,
                );
                slave.routine();
            });
            handlers.push(handler);
        }
        Self {
            config,
            handlers,
            find_req_txs,
            find_rsp_rx,
            data_req_txs,
            data_rsp_rx,
            inner_req_txs,
            inner_rsp_rx,
            end_txs,
            store,
        }
    }

    fn get_chunks<T>(v: &mut [T], max_chunk_n: usize, min_chunk_len: usize) -> Vec<&mut [T]> {
        let chunk_len = max(v.len() / max_chunk_n, min_chunk_len);
        let chunk_n = min(max_chunk_n, v.len() / chunk_len);
        let mut result = Vec::new();
        let mut v_ref = v;
        let mut v_ref1;
        for _i in 0..chunk_n - 1 {
            (v_ref1, v_ref) = v_ref.split_at_mut(chunk_len);
            result.push(v_ref1);
        }
        result.push(v_ref);
        assert!(result.len() <= max_chunk_n);
        result
    }

    fn merge_find_rsp(
        find_rsps: &Vec<Vec<Vec<(PageId, usize)>>>,
        depth: usize,
    ) -> Vec<(PageId, usize)> {
        let mut finds = Vec::new();
        for find_rsp in find_rsps.iter() {
            let find = &find_rsp[depth - 1];
            for (id, req_n) in find.iter().cloned() {
                if let Some((id1, req_n1)) = finds.last_mut() {
                    if *id1 != id {
                        finds.push((id, req_n));
                    } else {
                        *req_n1 += req_n;
                    }
                } else {
                    finds.push((id, req_n));
                }
            }
        }
        finds
    }

    pub fn op(&self, mut cmds: Vec<PALMCmd<K, V>>) -> Vec<PALMResult> {
        // stable sort cmds by key
        let mut cmd_idxes = Vec::from_iter(0..cmds.len());
        cmd_idxes.sort_by(|i1, i2| cmds[*i1].key().cmp(cmds[*i2].key()));
        cmds.sort_by(|c1, c2| c1.key().cmp(c2.key()));
        // make find requests for each worker
        let mut keys = Box::new_uninit_slice(cmds.len());
        for (i, cmd) in cmds.iter().enumerate() {
            keys[i].write(cmd.key().clone());
        }
        let mut keys = unsafe { keys.assume_init() }.into_vec();
        let find_reqs_list = {
            let key_chunks = Self::get_chunks(
                keys.as_mut_slice(),
                self.config.thread_n,
                self.config.min_find_per_thread,
            );
            let mut find_reqs_list = Box::new_uninit_slice(key_chunks.len());
            for (idx, key_chunk) in key_chunks.into_iter().enumerate() {
                find_reqs_list[idx].write(FindReqs {
                    keys: NonNull::new(key_chunk).unwrap(),
                    idx,
                });
            }
            unsafe { find_reqs_list.assume_init() }.into_vec()
        };
        // allocate find work to workers
        let find_rsps = {
            let find_reqs_len = find_reqs_list.len();
            for (i, reqs) in find_reqs_list.into_iter().enumerate() {
                self.find_req_txs[i].send(reqs).unwrap();
            }
            let mut find_rsps = Box::new_uninit_slice(find_reqs_len);
            for _i in 0..find_reqs_len {
                let rsp = self.find_rsp_rx.recv().unwrap();
                find_rsps[rsp.idx].write(rsp.find);
            }
            unsafe { find_rsps.assume_init() }.into_vec()
        };
        let mut depth = find_rsps[0].len();
        // make data requests for each worker
        let mut data_reqs = {
            let finds = Self::merge_find_rsp(&find_rsps, depth);
            let mut data_reqs = Vec::new();
            let mut cmds_ref = cmds.as_mut_slice();
            let mut cmds_ref1;
            for (page_id, req_n) in finds {
                (cmds_ref1, cmds_ref) = cmds_ref.split_at_mut(req_n);
                data_reqs.push(DataReq {
                    cmds: NonNull::new(cmds_ref1).unwrap(),
                    page_id,
                });
            }
            data_reqs
        };
        let data_reqs_list = {
            let data_reqs_chunks = Self::get_chunks(&mut data_reqs, self.config.thread_n, 1);
            let mut off = 0;
            let mut data_reqs_list = Box::new_uninit_slice(data_reqs_chunks.len());
            for (idx, data_req_chunk) in data_reqs_chunks.into_iter().enumerate() {
                data_reqs_list[idx].write(DataReqs {
                    reqs: NonNull::new(data_req_chunk).unwrap(),
                    off,
                    idx,
                });
                data_req_chunk
                    .iter()
                    .for_each(|e| off += unsafe { e.cmds.as_ref() }.len());
            }
            unsafe { data_reqs_list.assume_init() }.into_vec()
        };
        // allocate data work to workers
        let (results, mut inners_list) = {
            let data_reqs_len = data_reqs_list.len();
            for (i, reqs) in data_reqs_list.into_iter().enumerate() {
                self.data_req_txs[i].send(reqs).unwrap();
            }
            let mut results1 =
                unsafe { Box::new_uninit_slice(cmds.len()).assume_init() }.into_vec();
            let mut inners_list = Box::new_uninit_slice(data_reqs_len);
            for _i in 0..data_reqs_len {
                let rsp = self.data_rsp_rx.recv().unwrap();
                results1[rsp.off..rsp.off + rsp.results.len()]
                    .clone_from_slice(rsp.results.as_slice());
                inners_list[rsp.idx].write(rsp.inners);
            }
            let mut results = Box::new_uninit_slice(results1.len());
            for (i, r) in results1.into_iter().enumerate() {
                results[cmd_idxes[i]].write(r);
            }
            let results = unsafe { results.assume_init() }.into_vec();
            let inners_list = unsafe { inners_list.assume_init() }.into_vec();
            (results, inners_list)
        };
        let mut inners;
        loop {
            inners = Vec::new();
            for mut inners1 in inners_list {
                inners.append(&mut inners1);
            }
            if inners.is_empty() || depth <= 1 {
                break;
            }
            depth -= 1;
            // make inner requests for each worker
            let mut inner_reqs = {
                let finds = Self::merge_find_rsp(&find_rsps, depth);
                let mut find_idx = 0;
                let mut cur_n = 0;
                let mut cmds = Vec::new();
                let mut inner_reqs = Vec::new();
                let mut pid = 0;
                for (inner, idx) in inners {
                    if idx >= cur_n {
                        if !cmds.is_empty() {
                            inner_reqs.push(InnerReq { cmds, page_id: pid });
                            cmds = Vec::new();
                        }
                        loop {
                            cur_n += finds[find_idx].1;
                            find_idx += 1;
                            if idx < cur_n {
                                pid = finds[find_idx - 1].0;
                                break;
                            }
                        }
                    }
                    cmds.push((inner, idx));
                }
                inner_reqs.push(InnerReq { cmds, page_id: pid });
                inner_reqs
            };
            let inner_reqs_list = {
                let inner_reqs_chunks = Self::get_chunks(&mut inner_reqs, self.config.thread_n, 1);
                let mut inner_reqs_list = Vec::new();
                for (i, inner_reqs_chunk) in inner_reqs_chunks.into_iter().enumerate() {
                    inner_reqs_list.push(InnerReqs {
                        reqs: NonNull::new(inner_reqs_chunk).unwrap(),
                        idx: i,
                    });
                }
                inner_reqs_list
            };
            // allocate inner work to worker
            inners_list = {
                let inner_reqs_len = inner_reqs_list.len();
                for (i, reqs) in inner_reqs_list.into_iter().enumerate() {
                    self.inner_req_txs[i].send(reqs).unwrap();
                }
                let mut inners_list = Box::new_uninit_slice(inner_reqs_len);
                for _i in 0..inner_reqs_len {
                    let rsp = self.inner_rsp_rx.recv().unwrap();
                    inners_list[rsp.idx].write(rsp.inners);
                }
                unsafe { inners_list.assume_init() }.into_vec()
            };
        }
        while !inners.is_empty() {
            // new inner node
            let (new_id, mut new_inner) = self.store.new_inner();
            {
                let mut meta_g = self.store.meta.try_write().unwrap();
                let mut meta = meta_g.write();
                let old_id = meta.meta().root;
                meta.meta_mut().depth += 1;
                meta.meta_mut().root = new_id;
                let min_key = &meta.meta().min_key;
                assert!(new_inner.write().insert(min_key, &old_id));
            }
            // make inner reqs to new node
            let mut inner_req = vec![InnerReq {
                cmds: inners,
                page_id: new_id,
            }];
            let inner_reqs = InnerReqs {
                reqs: NonNull::new(inner_req.as_mut_slice()).unwrap(),
                idx: 0, // useless idx here
            };
            self.inner_req_txs[0].send(inner_reqs).unwrap();
            let req = self.inner_rsp_rx.recv().unwrap();
            inners = req.inners;
        }
        results
    }

    pub fn flush(&mut self) -> FlushHandler {
        self.store.update_cache();
        self.store.flush()
    }
}

impl<K: 'static + Ord + Send + Sync + Clone, V: 'static + Clone + Send + Sync> Drop
    for PALMTree<K, V>
{
    fn drop(&mut self) {
        for end_tx in self.end_txs.iter() {
            end_tx.send(()).unwrap();
        }
        while let Some(h) = self.handlers.pop() {
            h.join().unwrap();
        }
    }
}

#[cfg(test)]
mod test {
    use rand::{prelude::StdRng, Rng, SeedableRng};

    use super::PALMTree;

    #[test]
    fn test_chunk() {
        let mut v = Vec::new();
        let mut rng = StdRng::seed_from_u64(0);
        let test_n = 100;
        for _i in 0..test_n {
            v.push(rng.gen::<u32>());
        }
        let v1 = v.clone();
        let args = vec![(10, 5), (11, 5), (11, 10), (11, 11)];
        for (max_chunk_n, min_chunk_len) in args {
            let chunks = PALMTree::<u32, u32>::get_chunks(&mut v, max_chunk_n, min_chunk_len);
            let mut cnt = 0;
            for chunk in chunks {
                print!("{}\t", chunk.len());
                for e in chunk {
                    assert_eq!(e, &v1[cnt]);
                    cnt += 1;
                }
            }
            println!();
        }
    }
}
