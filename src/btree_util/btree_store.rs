use std::{
    fmt::Debug,
    hash::Hash,
    marker::PhantomData,
    sync::{Arc, RwLock},
};

use dashmap::{DashMap, DashSet};

use crate::{
    btree_node::{DataNode, InnerNode, NodeCursor, NodeId},
    btree_util::{lru_cache::LRUCache, meta_page::MetaPage},
    page_manager::FlushHandler,
    page_system::PageSystem,
};

use super::node_container::{LockNodeContainer, NodeContainer, RawNodeContainer};

pub struct BTreeStoreConfig {
    pub inner_node_n: usize,
    pub data_node_n: usize,
    pub data_cache_n: usize,
    pub node_cap: usize,
}
pub const DEFAULT_BTREE_STORE_CONFIG: BTreeStoreConfig = BTreeStoreConfig {
    inner_node_n: 1,
    data_node_n: 1,
    data_cache_n: 1024,
    node_cap: 0,
};

pub struct BTreeStore<
    K: Clone + PartialOrd,
    V: Clone,
    IC: NodeContainer<K, NodeId>,
    DC: NodeContainer<K, V>,
> {
    pub meta: RwLock<MetaPage<K>>,
    config: BTreeStoreConfig,
    ps: Arc<PageSystem>,
    inner_cache: DashMap<NodeId, IC>,
    data_cache: RwLock<LRUCache<NodeId, DC>>,
    updates: DashSet<NodeId>,
    _p: PhantomData<V>,
}

impl<
        K: Clone + PartialOrd + Default,
        V: Clone + Default + Default,
        IC: NodeContainer<K, NodeId>,
        DC: NodeContainer<K, V>,
    > BTreeStore<K, V, IC, DC>
{
    pub fn new(root_dir: &str, config: BTreeStoreConfig, min_key: K) -> Self {
        let ps = Arc::new(PageSystem::new(root_dir));
        let mut meta_page: MetaPage<K> = ps.new_page();
        assert_eq!(meta_page.page_id, Self::META_PAGE_ID);

        let (root_id, mut root_node) =
            DataNode::<K, V>::new(&ps, config.data_node_n, config.node_cap);
        let val = V::default();
        root_node.insert(&min_key, &val);
        {
            let mut meta_g = meta_page.write();
            let meta = meta_g.meta_mut();
            meta.root = root_id;
            meta.depth = 1;
            meta.len = 0;
            meta.min_key = min_key;
        }
        let root_node = DC::new(root_node);
        let mut data_cache = LRUCache::new(config.data_cache_n);
        data_cache.put(root_id, root_node);
        Self {
            config,
            ps,
            meta: RwLock::new(meta_page),
            inner_cache: DashMap::new(),
            data_cache: RwLock::new(data_cache),
            updates: DashSet::new(),
            _p: PhantomData,
        }
    }
}

impl<
        K: Clone + PartialOrd + Default,
        V: Clone + Default,
        IC: NodeContainer<K, NodeId>,
        DC: NodeContainer<K, V>,
    > BTreeStore<K, V, IC, DC>
{
    const META_PAGE_ID: NodeId = 1;

    pub unsafe fn load(root_dir: &str, config: BTreeStoreConfig) -> Self {
        let ps = Arc::new(PageSystem::load(root_dir));
        let meta_page: MetaPage<K> = ps.load_page(Self::META_PAGE_ID);
        assert_eq!(meta_page.page_id, Self::META_PAGE_ID);

        let data_cache = LRUCache::new(config.data_cache_n);
        Self {
            config,
            ps,
            meta: RwLock::new(meta_page),
            inner_cache: DashMap::new(),
            data_cache: RwLock::new(data_cache),
            updates: DashSet::new(),
            _p: PhantomData,
        }
    }

    pub fn load_inner(&self, node_id: NodeId) -> Option<IC> {
        if node_id == NodeId::MAX {
            return None;
        }
        if let Some(inner) = self.inner_cache.get(&node_id) {
            return Some(inner.clone());
        }
        let inner = unsafe {
            InnerNode::<K>::load(
                &self.ps,
                self.config.inner_node_n,
                node_id,
                self.config.node_cap,
            )
        };
        let inner = IC::new(inner);
        self.inner_cache.insert(node_id, inner.clone());
        Some(inner)
    }

    pub fn load_data(&self, node_id: NodeId) -> Option<DC> {
        if node_id == NodeId::MAX {
            return None;
        }
        if let Some(data) = self.data_cache.read().unwrap().get(&node_id) {
            self.updates.insert(node_id);
            return Some(data);
        }
        let data = unsafe {
            DataNode::<K, V>::load(
                &self.ps,
                self.config.data_node_n,
                node_id,
                self.config.node_cap,
            )
        };
        let data = DC::new(data);
        self.data_cache.write().unwrap().put(node_id, data.clone());
        Some(data)
    }

    pub fn new_inner(&self) -> (NodeId, IC) {
        let (id, node) =
            InnerNode::<K>::new(&self.ps, self.config.inner_node_n, self.config.node_cap);
        let node = IC::new(node);
        self.inner_cache.insert(id, node.clone());
        (id, node)
    }

    pub fn new_data(&self) -> (NodeId, DC) {
        let (id, node) =
            DataNode::<K, V>::new(&self.ps, self.config.data_node_n, self.config.node_cap);
        let node = DC::new(node);
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

    pub fn update_cache(&self) {
        let mut keys = Vec::new();
        for id in self.updates.iter() {
            keys.push(*id);
        }
        self.updates.clear();
        self.data_cache.write().unwrap().update(keys);
    }

    #[inline]
    pub fn flush(&self) -> FlushHandler {
        self.ps.flush()
    }
}

impl<
        K: Copy + PartialOrd + Eq + Hash + Debug + Default,
        V: Clone + Default,
        IC: NodeContainer<K, NodeId>,
        DC: NodeContainer<K, V>,
    > BTreeStore<K, V, IC, DC>
{
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
                let node_g = node.read();
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
            let node_g = node.read();
            print!("[<{} {},{}>", id, node_g.prev_id(), node_g.next_id());
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
                let node_g = node.read();
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
            assert_eq!(next_node_id.unwrap(), NodeId::MAX);
            ids = ids1;
            ids1 = Vec::new();
        }
        let mut next_node_id = None;
        for (id, left, right) in ids {
            let node = self.load_data(id).unwrap();
            let node_g = node.read();
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
        assert_eq!(next_node_id.unwrap(), NodeId::MAX);
    }
}

pub type RawBTreeStore<K, V> =
    BTreeStore<K, V, RawNodeContainer<K, NodeId>, RawNodeContainer<K, V>>;

pub type LockBTreeStore<K, V> =
    BTreeStore<K, V, LockNodeContainer<K, NodeId>, LockNodeContainer<K, V>>;
