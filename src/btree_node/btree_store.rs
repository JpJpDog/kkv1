use std::{
    fmt::Debug,
    hash::Hash,
    mem::MaybeUninit,
    sync::{Arc, RwLock},
};

use dashmap::{DashMap, DashSet};

use crate::{
    btree_node::btree_node::NodeCursor,
    btree_util::{lru_cache::LRUCache, meta_page::MetaPage},
    page_manager::{page::PageId, FlushHandler},
    page_system::page_system::PageSystem,
};

use super::btree_node::{DataNode, InnerNode};

pub struct BTreeConfig {
    pub inner_node_n: usize,
    pub data_node_n: usize,
    pub data_cache_n: usize,
    pub node_cap: usize,
}

pub const DEFAULT_BTREE_STORE_CONFIG: BTreeConfig = BTreeConfig {
    inner_node_n: 1,
    data_node_n: 1,
    data_cache_n: 1024,
    node_cap: 0,
};

pub struct BTreeStore<K: Clone + PartialOrd, V: Clone> {
    pub meta: RwLock<MetaPage<K>>,
    config: BTreeConfig,
    ps: Arc<PageSystem>,
    inner_cache: DashMap<PageId, Arc<RwLock<InnerNode<K>>>>,
    data_cache: RwLock<LRUCache<PageId, Arc<RwLock<DataNode<K, V>>>>>,
    updates: DashSet<PageId>,
}

impl<K: Clone + PartialOrd, V: Clone> BTreeStore<K, V> {
    const META_PAGE_ID: PageId = 1;

    pub fn new(root_dir: &str, config: BTreeConfig, min_key: K) -> Self {
        let ps = Arc::new(PageSystem::new(root_dir));
        let mut meta_page: MetaPage<K> = ps.new_page();
        assert_eq!(meta_page.page_id, Self::META_PAGE_ID);

        let mut data_cache;
        let (root_id, mut root_node) =
            DataNode::<K, V>::new(&ps.clone(), config.data_node_n, config.node_cap);
        let val = unsafe { MaybeUninit::uninit().assume_init() };
        root_node.insert(&min_key, &val);
        {
            let mut meta_g = meta_page.write();
            let meta = meta_g.meta_mut();
            meta.root = root_id;
            meta.depth = 1;
            meta.len = 0;
            meta.min_key = min_key;
        }

        let root_node = Arc::new(RwLock::new(root_node));
        data_cache = LRUCache::new(config.data_cache_n);
        data_cache.put(root_id, root_node);

        Self {
            config,
            ps,
            meta: RwLock::new(meta_page),
            inner_cache: DashMap::new(),
            data_cache: RwLock::new(data_cache),
            updates: DashSet::new(),
        }
    }

    pub unsafe fn load(root_dir: &str, config: BTreeConfig) -> Self {
        let ps = Arc::new(PageSystem::load(root_dir));
        let meta_page: MetaPage<K> = ps.load_page(Self::META_PAGE_ID);
        assert_eq!(meta_page.page_id, Self::META_PAGE_ID);

        let data_cache;

        data_cache = LRUCache::new(config.data_cache_n);
        Self {
            config,
            ps,
            meta: RwLock::new(meta_page),
            inner_cache: DashMap::new(),
            data_cache: RwLock::new(data_cache),
            updates: DashSet::new(),
        }
    }

    pub fn load_inner(&self, page_id: PageId) -> Option<Arc<RwLock<InnerNode<K>>>> {
        if page_id == PageId::MAX {
            return None;
        }
        if let Some(inner) = self.inner_cache.get(&page_id) {
            return Some(inner.clone());
        }
        let inner = unsafe {
            InnerNode::<K>::load(
                &self.ps,
                self.config.inner_node_n,
                page_id,
                self.config.node_cap,
            )
        };
        let inner = Arc::new(RwLock::new(inner));
        self.inner_cache.insert(page_id, inner.clone());
        Some(inner)
    }

    pub fn load_data(&self, page_id: PageId) -> Option<Arc<RwLock<DataNode<K, V>>>> {
        if page_id == PageId::MAX {
            return None;
        }
        if let Some(data) = self.data_cache.read().unwrap().get(&page_id) {
            self.updates.insert(page_id);
            return Some(data.clone());
        }
        let data = unsafe {
            DataNode::<K, V>::load(
                &self.ps,
                self.config.data_node_n,
                page_id,
                self.config.node_cap,
            )
        };
        let data = Arc::new(RwLock::new(data));
        self.data_cache.write().unwrap().put(page_id, data.clone());
        Some(data)
    }

    pub fn new_inner(&self) -> (PageId, Arc<RwLock<InnerNode<K>>>) {
        let (id, node) =
            InnerNode::<K>::new(&self.ps, self.config.inner_node_n, self.config.node_cap);
        let node = Arc::new(RwLock::new(node));
        self.inner_cache.insert(id, node.clone());
        (id, node)
    }

    pub fn new_data(&self) -> (PageId, Arc<RwLock<DataNode<K, V>>>) {
        let (id, node) =
            DataNode::<K, V>::new(&self.ps, self.config.data_node_n, self.config.node_cap);
        let node = Arc::new(RwLock::new(node));
        self.data_cache.write().unwrap().put(id, node.clone());
        (id, node)
    }

    pub fn delete_inner(&self, node: &InnerNode<K>) {
        self.inner_cache.remove(&node.node_id());
        node.delete(&self.ps);
    }

    pub fn delete_data(&self, node: &DataNode<K, V>) {
        self.data_cache.write().unwrap().delete(&node.node_id());
        node.delete(&self.ps);
    }

    pub fn update_data_cache(&self) {
        let mut keys = Vec::new();
        for id in self.updates.iter() {
            keys.push(*id);
        }
        self.updates.clear();
        self.data_cache.write().unwrap().update(keys);
    }

    /// unsafe if any function is called before this function returns
    #[inline]
    pub fn flush(&self) -> FlushHandler {
        self.ps.flush()
    }
}

impl<K: Copy + PartialOrd + Eq + Hash + Debug, V: Clone> BTreeStore<K, V> {
    #[allow(dead_code)]
    pub fn dump(&self) {
        let mut ids = Vec::new();
        let mut ids2 = Vec::new();
        let meta = self.meta.read().unwrap();
        let meta = meta.read().meta();
        ids.push(meta.root);
        let depth = meta.depth;
        println!("!------------------------");
        for _i in 0..depth - 1 {
            for id in ids {
                let node = self.load_inner(id).unwrap();
                let node_g = node.read().unwrap();
                print!("[<{} {},{}>", id, node_g.prev_id(), node_g.next_id());
                let mut cursor = NodeCursor::new(&node_g);
                loop {
                    cursor.move_next();
                    if cursor.is_null() {
                        break;
                    }
                    let kv = cursor.cur();
                    print!("({:?} {:?})", kv.key, kv.val);
                    ids2.push(kv.val);
                }
                print!("] ");
            }
            println!();
            ids = ids2;
            ids2 = Vec::new();
        }
        for id in ids {
            let node = self.load_data(id).unwrap();
            let node_g = node.read().unwrap();
            print!("[<{} {},{}>", id, node_g.prev_id(), node_g.next_id());
            // let list = node_g.dir().list();
            // let mut p = list.null();
            let mut cursor = NodeCursor::new(&node_g);
            loop {
                cursor.move_next();
                if cursor.is_null() {
                    break;
                }
                print!("{:?} ", cursor.cur().key);
            }
            print!("] ");
        }
        println!();
        println!("------------------------!");
    }

    #[allow(dead_code)]
    pub fn check(&self) {
        let meta_g = self.meta.read().unwrap();
        let meta = meta_g.read().meta();
        let depth = meta.depth;
        let mut ids = Vec::new();
        let mut ids1 = Vec::new();
        ids.push((meta.root, meta.min_key, None));
        for _i in 0..depth - 1 {
            let mut next_node_id = None;
            for (id, left, right) in ids {
                let node = self.load_inner(id).unwrap();
                let node_g = node.read().unwrap();
                assert!(node_g.len() >= 1);

                if let Some(next) = next_node_id {
                    assert_eq!(next, node_g.node_id());
                }
                node_g.page_list.check();
                let mut cursor = NodeCursor::new(&node_g);
                cursor.move_next();
                let kv = cursor.cur();
                let mut val = kv.val;
                assert_eq!(left, kv.key);
                let mut prev = kv.key;
                cursor.move_next();
                let mut cnt = 1;
                while !cursor.is_null() {
                    cnt += 1;
                    let kv = cursor.cur();
                    assert!(prev < kv.key);
                    let old_prev = prev;
                    prev = kv.key;
                    ids1.push((val, old_prev, Some(prev)));
                    val = kv.val;
                    cursor.move_next();
                }
                assert_eq!(node_g.len(), cnt);
                cursor.move_prev();
                let kv = cursor.cur();
                if let Some(r) = right {
                    assert!(kv.key < r);
                }
                ids1.push((val, prev, right));

                if let Some(next) = next_node_id {
                    assert_eq!(next, id);
                }
                next_node_id = Some(node_g.next_id());
            }
            assert_eq!(next_node_id.unwrap(), PageId::MAX);
            ids = ids1;
            ids1 = Vec::new();
        }
        let mut next_node_id = None;
        for (id, left, right) in ids {
            let node = self.load_data(id).unwrap();
            let node_g = node.read().unwrap();
            assert!(node_g.len() >= 1);

            if let Some(next) = next_node_id {
                assert_eq!(next, node_g.node_id());
            }

            node_g.page_list.check();
            let mut cursor = NodeCursor::new(&node_g);
            cursor.move_next();
            let kv = cursor.cur();
            assert_eq!(left, kv.key);
            let mut prev = kv.key;
            cursor.move_next();
            let mut cnt = 1;
            while !cursor.is_null() {
                cnt += 1;
                let kv = cursor.cur();
                assert!(prev < kv.key);
                prev = kv.key;
                cursor.move_next();
            }
            assert_eq!(node_g.len(), cnt);
            cursor.move_prev();
            let kv = cursor.cur();
            if let Some(r) = right {
                assert!(kv.key < r);
            }
            if let Some(next) = next_node_id {
                assert_eq!(next, id);
            }
            next_node_id = Some(node_g.next_id());
        }
        assert_eq!(next_node_id.unwrap(), PageId::MAX);
    }
}
