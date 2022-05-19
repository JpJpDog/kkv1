use std::{
    sync::{Arc, RwLock, RwLockWriteGuard},
    thread::JoinHandle,
};

use super::{
    btree_node::btree_node::{DataNode, InnerNode, Node},
    btree_store::{
        btree_store::{BTreeStore, DEFAULT_BTREE_STORE_CONFIG},
        meta_page::MetaPage,
    },
    page_manager::page::PageId,
    util::KV,
};

#[derive(Clone, Eq, PartialEq)]
enum ChildLoc {
    First,
    Mid,
    Last,
}

#[derive(Clone)]
pub struct BTreeConfig {
    pub optimistic: bool,
    pub node_cap: usize,
}

pub const DEFAULT_BTREE_CONFIG: BTreeConfig = BTreeConfig {
    optimistic: true,
    node_cap: 0,
};

enum RemoveRtn<K> {
    Remove { key: K },
    ChangeKey { old: K, key: K },
}

#[derive(Clone)]
pub struct BTree<K: Clone + PartialOrd, V: Clone> {
    pub store: Arc<BTreeStore<K, V>>,
    config: BTreeConfig,
}

impl<K: Clone + PartialOrd, V: Clone> BTree<K, V> {
    pub fn new(root_dir: &str, min_key: K, config: BTreeConfig) -> Self {
        let mut store_config = DEFAULT_BTREE_STORE_CONFIG;
        store_config.node_cap = config.node_cap;
        Self {
            config,
            store: Arc::new(BTreeStore::new(root_dir, store_config, min_key)),
        }
    }

    pub unsafe fn load(root_dir: &str, config: BTreeConfig) -> Self {
        let mut store_config = DEFAULT_BTREE_STORE_CONFIG;
        store_config.node_cap = config.node_cap;
        Self {
            config,
            store: Arc::new(BTreeStore::load(root_dir, store_config)),
        }
    }

    pub fn get(&self, key: &K, dest: &mut V) -> bool {
        let meta_g = self.store.meta.read().unwrap();
        let meta = meta_g.read().meta();
        assert!(&meta.min_key < key);
        let mut depth = meta.depth;
        let mut id = meta.root;
        let mut meta_g = Some(meta_g);
        let mut inners = vec![None; depth];
        let mut inners_ref = inners.as_mut_slice();
        let mut inner_g;
        while depth > 1 {
            inners_ref[0] = Some(self.store.load_inner(id)).unwrap();
            let inner;
            (inner, inners_ref) = inners_ref.split_first_mut().unwrap();
            inner_g = inner.as_ref().unwrap().read().unwrap();
            if meta_g.is_some() {
                meta_g = None;
            }
            id = inner_g.get(key).unwrap().val;
            depth -= 1;
        }
        let data = self.store.load_data(id).unwrap();
        let data_g = data.read().unwrap();
        let kv = data_g.get(key).unwrap();
        if &kv.key == key {
            *dest = kv.val.clone();
            true
        } else {
            false
        }
    }

    #[inline]
    fn insert_safe<V1: Clone>(node: &Node<K, V1>) -> bool {
        node.len() < node.capacity()
    }

    #[inline]
    fn remove_safe<V1: Clone>(node: &Node<K, V1>) -> bool {
        node.len() > (node.capacity() + 1) / 2
    }

    #[inline]
    fn try_route<'a>(
        &self,
        key: &K,
        data: &'a mut Option<Arc<RwLock<DataNode<K, V>>>>,
        remove_check: bool,
    ) -> Option<RwLockWriteGuard<'a, DataNode<K, V>>> {
        let meta_g = self.store.meta.read().unwrap();
        let meta = meta_g.read().meta();
        assert!(&meta.min_key < key);
        let depth = meta.depth;
        let mut id = meta.root;
        let mut _meta_g = Some(meta_g);

        let mut inners = vec![None; depth - 1];
        let mut inners_ref = inners.as_mut_slice();
        let mut inner;
        let mut _inner_g;
        for i in 0..depth - 1 {
            inners_ref[0] = self.store.load_inner(id);
            (inner, inners_ref) = inners_ref.split_first_mut().unwrap();
            let inner_g1 = inner.as_mut().unwrap().read().unwrap();
            if i == 0 {
                _meta_g = None;
            } else {
                _inner_g = None;
            }
            _inner_g = Some(inner_g1);
            id = _inner_g.as_ref().unwrap().get(key).unwrap().val;
        }

        *data = self.store.load_data(id);
        let data_g = data.as_ref().unwrap().write().unwrap();
        let mut try_ok = false;
        if remove_check {
            if depth == 1 || Self::remove_safe(&data_g) && &data_g.first().unwrap().key != key {
                _meta_g = None;
                try_ok = true;
            }
        } else if Self::insert_safe(&data_g) {
            _inner_g = None;
            try_ok = true;
        }
        if try_ok {
            Some(data_g)
        } else {
            None
        }
    }

    #[inline]
    fn get_route<'a>(
        &self,
        key: &K,
        inners: &'a mut Vec<Option<Arc<RwLock<InnerNode<K>>>>>,
        data: &'a mut Option<Arc<RwLock<DataNode<K, V>>>>,
        remove_check: bool,
    ) -> (
        Option<RwLockWriteGuard<MetaPage<K>>>,
        Vec<(RwLockWriteGuard<'a, InnerNode<K>>, ChildLoc)>,
        RwLockWriteGuard<'a, DataNode<K, V>>,
    ) {
        let meta_g = self.store.meta.write().unwrap();
        let mut meta_g = Some(meta_g);
        let meta = meta_g.as_ref().unwrap().read().meta();
        assert!(&meta.min_key < key);
        let depth = meta.depth;
        let mut id = meta.root;
        *inners = vec![None; depth - 1];
        let mut inner_gs = Vec::new();
        let mut inners_ref = inners.as_mut_slice();
        let mut inner;
        for i in 0..depth - 1 {
            inners_ref[0] = self.store.load_inner(id);
            (inner, inners_ref) = inners_ref.split_first_mut().unwrap();
            let inner_g = inner.as_mut().unwrap().write().unwrap();
            if remove_check {
                if (i == 0 && inner_g.len() > 2)
                    || Self::remove_safe(&inner_g) && &inner_g.first().unwrap().key != key
                {
                    meta_g = None;
                    inner_gs.clear();
                }
            } else if Self::insert_safe(&inner_g) {
                meta_g = None;
                inner_gs.clear();
            }
            id = inner_g.get(key).unwrap().val;
            let loc = if id == inner_g.first().unwrap().val {
                ChildLoc::First
            } else if id == inner_g.last().unwrap().val {
                ChildLoc::Last
            } else {
                ChildLoc::Mid
            };
            inner_gs.push((inner_g, loc));
        }
        *data = self.store.load_data(id);
        let data_g = data.as_ref().unwrap().write().unwrap();
        if remove_check {
            if depth == 1 || Self::remove_safe(&data_g) && &data_g.first().unwrap().key != key {
                meta_g = None;
                inner_gs.clear();
            }
        } else if Self::insert_safe(&data_g) {
            meta_g = None;
            inner_gs.clear();
        }
        // print!("{} ", inner_gs.len());
        (meta_g, inner_gs, data_g)
    }

    fn data_insert(&self, key: &K, val: &V, data_g: &mut DataNode<K, V>) -> Option<KV<K, PageId>> {
        if data_g.insert(key, val) {
            assert!(data_g.len() <= data_g.capacity());
            return None;
        }
        let (id2, data2) = self.store.new_data();
        let mut data2_g = data2.write().unwrap();
        let data3 = self.store.load_data(data_g.next_id());
        let mut data3_g = data3.as_ref().map(|d| d.write().unwrap());
        data_g.split_to(&mut data2_g, data3_g.as_mut().map(|g| &mut **g));
        if key < &data2_g.first().unwrap().key {
            assert!(data_g.insert(key, val));
        } else {
            assert!(data2_g.insert(key, val));
        }
        let k2 = data2_g.first().unwrap().key.clone();
        Some(KV::new(k2, id2))
    }

    fn inner_insert(
        &self,
        key: &K,
        id: &PageId,
        inner_g: &mut InnerNode<K>,
    ) -> Option<KV<K, PageId>> {
        if inner_g.insert(key, id) {
            return None;
        }
        let (id2, inner2) = self.store.new_inner();
        let mut inner2_g = inner2.write().unwrap();
        let inner3 = self.store.load_inner(inner_g.next_id());
        let mut inner3_g = inner3.as_ref().map(|g| g.write().unwrap());
        inner_g.split_to(&mut inner2_g, inner3_g.as_mut().map(|g| &mut **g));
        if key < &inner2_g.first().unwrap().key {
            assert!(inner_g.insert(key, id));
        } else {
            assert!(inner2_g.insert(key, id));
        }
        let k2 = inner2_g.first().unwrap().key.clone();
        Some(KV::new(k2, id2))
    }

    pub fn insert(&self, key: &K, val: &V) {
        let mut data = None;
        if self.config.optimistic {
            if let Some(mut data_g) = self.try_route(key, &mut data, false) {
                assert!(data_g.insert(key, val));
                return;
            }
        }
        let mut inners = Vec::new();
        let (meta_g, mut inner_gs, mut data_g) = self.get_route(key, &mut inners, &mut data, false);

        let rtn = self.data_insert(key, val, &mut data_g);
        if rtn.is_none() {
            return;
        }
        let mut kv2 = rtn.unwrap();
        while let Some((mut inner_g, _loc)) = inner_gs.pop() {
            let rtn = self.inner_insert(&kv2.key, &kv2.val, &mut inner_g);
            if rtn.is_none() {
                return;
            }
            kv2 = rtn.unwrap();
        }
        let mut meta_g = meta_g.unwrap();
        let root_id = meta_g.read().meta().root;
        let (new_root_id, root_node) = self.store.new_inner();
        let mut root_node_g = root_node.write().unwrap();
        let mut meta_gg = meta_g.write();
        let meta = meta_gg.meta_mut();
        meta.root = new_root_id;
        meta.depth += 1;
        root_node_g.insert(&meta.min_key, &root_id);
        root_node_g.insert(&kv2.key, &kv2.val);
    }

    fn data_remove(
        &self,
        key: &K,
        mut data_g: &mut DataNode<K, V>,
        loc: ChildLoc,
    ) -> Vec<RemoveRtn<K>> {
        let mut rtns = Vec::new();
        let change = &data_g.first().unwrap().key == key;
        data_g.remove(key);
        if change {
            let new_k = data_g.first().unwrap().key.clone();
            rtns.push(RemoveRtn::ChangeKey {
                old: key.clone(),
                key: new_k,
            });
        }
        if Self::remove_safe(data_g) {
            return rtns;
        }
        let mut right = None;
        let mut left = None;
        if loc != ChildLoc::Last {
            let rid = data_g.next_id();
            let r = self.store.load_data(rid).unwrap();
            {
                let mut r_g = r.write().unwrap();
                if Self::remove_safe(&r_g) {
                    let kv = r_g.first().unwrap();
                    assert!(data_g.insert(&kv.key, &kv.val));
                    let old = kv.key.clone();
                    r_g.pop_front();
                    let key = r_g.first().unwrap().key.clone();
                    rtns.push(RemoveRtn::ChangeKey { old, key });
                    return rtns;
                }
            }
            right = Some(r);
        }
        if loc != ChildLoc::First {
            let lid = data_g.prev_id();
            let l = self.store.load_data(lid).unwrap();
            {
                let mut l_g = l.write().unwrap();
                if Self::remove_safe(&l_g) {
                    let kv = l_g.last().unwrap();
                    let old = data_g.first().unwrap().key.clone();
                    assert!(data_g.insert(&kv.key, &kv.val));
                    let key = kv.key.clone();
                    l_g.pop_back();
                    rtns.push(RemoveRtn::ChangeKey { old, key });
                    return rtns;
                }
            }
            left = Some(l);
        }
        if let Some(r) = right {
            let mut right_g = r.write().unwrap();
            let remove_key = right_g.first().unwrap().key.clone();
            let rr_id = right_g.next_id();
            let rright = self.store.load_data(rr_id);
            let mut rright_g = rright.as_ref().map(|r| r.write().unwrap());
            data_g.merge_with(&mut right_g, rright_g.as_mut().map(|r_g| &mut **r_g));
            self.store.delete_data(&right_g);
            rtns.push(RemoveRtn::Remove { key: remove_key });
            return rtns;
        }
        let l = left.unwrap();
        let mut left_g = l.write().unwrap();
        let remove_key = data_g.first().unwrap().key.clone();
        let rid = data_g.next_id();
        let right = self.store.load_data(rid);
        let mut right_g = right.as_ref().map(|r| r.write().unwrap());
        left_g.merge_with(&mut data_g, right_g.as_mut().map(|r_g| &mut **r_g));
        self.store.delete_data(&data_g);
        rtns.push(RemoveRtn::Remove { key: remove_key });
        return rtns;
    }

    fn inner_remove(
        &self,
        key: &K,
        mut inner_g: &mut InnerNode<K>,
        loc: ChildLoc,
    ) -> Vec<RemoveRtn<K>> {
        let mut rtns = Vec::new();
        let change = &inner_g.first().unwrap().key == key;
        inner_g.remove(key);
        if change {
            let new_k = inner_g.first().unwrap().key.clone();
            rtns.push(RemoveRtn::ChangeKey {
                old: key.clone(),
                key: new_k,
            });
        }
        if Self::remove_safe(inner_g) {
            return rtns;
        }
        let mut right = None;
        let mut left = None;
        if loc != ChildLoc::Last {
            let rid = inner_g.next_id();
            let r = self.store.load_inner(rid).unwrap();
            {
                let mut r_g = r.write().unwrap();
                if Self::remove_safe(&r_g) {
                    let kv = r_g.first().unwrap();
                    assert!(inner_g.insert(&kv.key, &kv.val));
                    let old = kv.key.clone();
                    r_g.pop_front();
                    let key = r_g.first().unwrap().key.clone();
                    rtns.push(RemoveRtn::ChangeKey { old, key });
                    return rtns;
                }
            }
            right = Some(r);
        }
        if loc != ChildLoc::First {
            let lid = inner_g.prev_id();
            let l = self.store.load_inner(lid).unwrap();
            {
                let mut l_g = l.write().unwrap();
                if Self::remove_safe(&l_g) {
                    let kv = l_g.last().unwrap();
                    let old = inner_g.first().unwrap().key.clone();
                    assert!(inner_g.insert(&kv.key, &kv.val));
                    let key = kv.key.clone();
                    l_g.pop_back();
                    rtns.push(RemoveRtn::ChangeKey { old, key });
                    return rtns;
                }
            }
            left = Some(l);
        }
        if let Some(r) = right {
            let mut right_g = r.write().unwrap();
            let remove_key = right_g.first().unwrap().key.clone();
            let rr_id = right_g.next_id();
            let rright = self.store.load_inner(rr_id);
            let mut rright_g = rright.as_ref().map(|r| r.write().unwrap());
            inner_g.merge_with(&mut right_g, rright_g.as_mut().map(|r_g| &mut **r_g));
            self.store.delete_inner(&right_g);
            rtns.push(RemoveRtn::Remove { key: remove_key });
            return rtns;
        }
        let l = left.unwrap();
        let mut left_g = l.write().unwrap();
        let remove_key = inner_g.first().unwrap().key.clone();
        let rid = inner_g.next_id();
        let right = self.store.load_inner(rid);
        let mut right_g = right.as_ref().map(|r| r.write().unwrap());
        left_g.merge_with(&mut inner_g, right_g.as_mut().map(|r_g| &mut **r_g));
        self.store.delete_inner(&inner_g);
        rtns.push(RemoveRtn::Remove { key: remove_key });
        return rtns;
    }

    pub fn remove(&self, key: &K) {
        let mut data = None;
        if let Some(mut data_g) = self.try_route(key, &mut data, true) {
            data_g.remove(key);
            return;
        }
        let mut inners = Vec::new();
        let (mut meta_g, mut inner_gs, mut data_g) =
            self.get_route(key, &mut inners, &mut data, true);
        if inner_gs.is_empty() {
            data_g.remove(key);
            return;
        }
        let (mut inner_g, loc) = inner_gs.pop().unwrap();
        let mut rtn = self.data_remove(key, &mut data_g, loc);
        while let Some((parent_g, loc)) = inner_gs.pop() {
            if rtn.is_empty() {
                return;
            }
            let mut rtn1 = Vec::new();
            for r in rtn {
                match r {
                    RemoveRtn::Remove { key } => {
                        rtn1.append(&mut self.inner_remove(&key, &mut inner_g, loc.clone()))
                    }
                    RemoveRtn::ChangeKey { old, key } => {
                        inner_g.change_key(&old, key.clone());
                        if &key == &inner_g.first().unwrap().key {
                            rtn1.push(RemoveRtn::ChangeKey { old, key });
                        }
                    }
                }
            }
            rtn = rtn1;
            inner_g = parent_g;
        }
        for r in rtn {
            match r {
                RemoveRtn::Remove { key } => {
                    inner_g.remove(&key);
                }
                RemoveRtn::ChangeKey { old, key } => inner_g.change_key(&old, key),
            }
        }
        if inner_g.len() == 1 {
            let new_root = inner_g.first().unwrap().val;
            meta_g.as_mut().unwrap().write().meta_mut().root = new_root;
            meta_g.unwrap().write().meta_mut().depth -= 1;
            self.store.delete_inner(&inner_g);
        }
    }

    #[inline]
    pub fn flush_pages(&mut self) -> Option<JoinHandle<()>> {
        self.store.flush()
    }

    #[inline]
    pub fn update_cache(&self) {
        self.store.update_data_cache();
    }
}
