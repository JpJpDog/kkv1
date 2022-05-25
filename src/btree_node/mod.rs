mod node_page_list;
mod skip_list;

use crate::{page_manager::page::PageId, page_system::PageSystem, util::KV};

use self::{
    node_page_list::{Block, BlockId, NodePageList},
    skip_list::{SkipList, SkipListCursor},
};

pub type NodeId = PageId;

/// BTreeNode based on NodePageList with a memory directory implemented in skiplist
pub struct Node<K: PartialOrd + Clone, V: Clone> {
    pub page_list: NodePageList<K, V>,
    dir: SkipList<K, BlockId>,
}

impl<K: PartialOrd + Clone + Default, V: Clone> Node<K, V> {
    pub fn new(ps: &PageSystem, node_n: usize, cap: usize) -> (PageId, Self) {
        let (node_id, page_list) = NodePageList::new(ps, node_n, cap as BlockId);
        // init the null node of skiplist with the null node of node_page_list
        let dir = SkipList::new(KV::new(K::default(), NodePageList::<K, V>::NULL_ID));
        (node_id, Self { page_list, dir })
    }

    pub unsafe fn load(ps: &PageSystem, node_n: usize, node_id: PageId, cap: usize) -> Self {
        let page_list = NodePageList::load(ps, node_n, node_id, cap as BlockId);
        // init the null node of skiplist with the null node of node_page_list
        let mut dir = SkipList::new(KV::new(K::default(), NodePageList::<K, V>::NULL_ID));
        // traverse the blocks in node_page_list to init the memory directory
        let mut block_id = page_list.meta().head;
        while block_id != NodePageList::<K, V>::NULL_ID {
            let block: &Block<K, V> = page_list.block(block_id);
            dir.insert(block.kv.key.clone(), block_id);
            block_id = block.next;
        }
        Self { page_list, dir }
    }

    pub fn delete(&self, ps: &PageSystem) {
        self.page_list.delete(ps);
    }

    /// get is node that less than or equal to `key`. if no such kv, return `None`
    #[inline]
    pub fn get(&self, key: &K) -> Option<&KV<K, V>> {
        let cursor = self.dir.get(key);
        if cursor.is_null() {
            None
        } else {
            let block_id = cursor.cur().val;
            Some(&self.page_list.block(block_id).kv)
        }
    }

    /// insert `kv` into node. if there is no extra space return `false`
    pub fn insert(&mut self, key: &K, val: &V) -> bool {
        unsafe {
            return if let Some(new_id) = self.page_list.alloc() {
                // if there is enough space
                self.page_list.set_block_kv(new_id, key, val);
                let (mut cursor, old_v) = self.dir.insert(key.clone(), new_id);
                cursor.move_prev();
                let prev_id = cursor.cur().val;
                if let Some(del_id) = old_v {
                    self.page_list.remove_after(prev_id);
                    // free old block if already exist
                    self.page_list.free(del_id);
                }
                self.page_list.insert_after(prev_id, new_id);
                true
            } else {
                let cursor = self.dir.get(key);
                if &cursor.cur().key == key {
                    let block_id = cursor.cur().val;
                    self.page_list.set_block_kv(block_id, key, val);
                }
                // no enough space but can find old value
                return false;
            };
        }
    }

    /// remove kv by `key`. return `false` if no such kv in node
    pub fn remove(&mut self, key: &K) -> bool {
        let (cursor, old_v) = self.dir.remove(key);
        if let Some(del_id) = old_v {
            unsafe {
                let prev_id = cursor.cur().val;
                self.page_list.remove_after(prev_id);
                self.page_list.free(del_id);
            }
            true
        } else {
            false
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.dir.len()
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        self.page_list.capacity()
    }

    #[inline]
    pub fn first(&self) -> Option<&KV<K, V>> {
        self.dir.first().map(|kv| {
            let block = self.page_list.block(kv.val);
            &block.kv
        })
    }

    #[inline]
    pub fn last(&self) -> Option<&KV<K, V>> {
        self.dir.last().map(|kv| {
            let block = self.page_list.block(kv.val);
            &block.kv
        })
    }

    #[inline]
    pub fn pop_front(&mut self) {
        if let Some(kv) = self.dir.first() {
            let k = kv.key.clone();
            self.remove(&k);
        }
    }

    #[inline]
    pub fn pop_back(&mut self) {
        if let Some(kv) = self.dir.last() {
            let k = kv.key.clone();
            self.remove(&k);
        }
    }

    #[inline]
    pub fn set_next_id(&mut self, next_id: PageId) {
        self.page_list.set_last_next(next_id);
    }

    #[inline]
    pub fn next_id(&self) -> PageId {
        self.page_list.last_next()
    }

    #[inline]
    pub fn set_prev_id(&mut self, prev_id: PageId) {
        self.page_list.set_first_prev(prev_id);
    }

    #[inline]
    pub fn prev_id(&self) -> PageId {
        self.page_list.first_prev()
    }

    #[inline]
    pub fn node_id(&self) -> PageId {
        self.page_list.first_id()
    }

    #[inline]
    pub fn dir(&self) -> &SkipList<K, BlockId> {
        &self.dir
    }
}

/// A cursor to visit data in `Node` in order
pub struct NodeCursor<'a, K: PartialOrd + Clone, V: Clone> {
    dir_cursor: SkipListCursor<'a, K, BlockId>,
    page_list: &'a NodePageList<K, V>,
}

impl<'a, K: Clone + PartialOrd, V: Clone> NodeCursor<'a, K, V> {
    pub fn new(node: &'a Node<K, V>) -> Self {
        Self {
            dir_cursor: SkipListCursor::new(&node.dir),
            page_list: &node.page_list,
        }
    }

    #[inline]
    pub fn cur(&self) -> &'a KV<K, V> {
        let block_id = self.dir_cursor.cur().val;
        &self.page_list.block(block_id).kv
    }

    #[inline]
    pub fn is_null(&self) -> bool {
        self.dir_cursor.is_null()
    }

    #[inline]
    pub fn move_next(&mut self) {
        self.dir_cursor.move_next();
    }

    #[inline]
    pub fn move_prev(&mut self) {
        self.dir_cursor.move_prev();
    }
}

pub type InnerNode<K> = Node<K, PageId>;

pub type DataNode<K, V> = Node<K, V>;

impl<K: PartialOrd + Clone + Default, V: Clone> Node<K, V> {
    /// when `self` is full, move half of the kvs to `right`
    pub fn split_to(&mut self, right: &mut Self, old_right: Option<&mut Self>) {
        assert_eq!(self.len(), right.capacity());
        assert_eq!(right.len(), 0);
        if let Some(old_right) = old_right {
            old_right.set_prev_id(right.node_id());
        }
        right.set_next_id(self.next_id());
        right.set_prev_id(self.node_id());
        self.set_next_id(right.node_id());
        let n1 = (self.capacity() + 1) / 2;
        let n2 = self.capacity() - n1;
        for _i in 0..n2 {
            let kv = self.last().unwrap();
            right.insert(&kv.key, &kv.val);
            self.pop_back();
        }
    }

    /// when `self` and `right` can be put in one node, merge kvs in `self`
    pub fn merge_with(&mut self, right: &mut Self, new_right: Option<&mut Self>) {
        assert_eq!(self.capacity(), right.capacity());
        assert!(self.len() + right.len() <= self.capacity());
        self.set_next_id(right.next_id());
        if let Some(new_right) = new_right {
            new_right.set_prev_id(self.node_id());
        }
        while right.len() != 0 {
            let kv = right.first().unwrap();
            self.insert(&kv.key, &kv.val);
            right.pop_front();
        }
    }
}

impl<K: PartialOrd + Clone + Default> InnerNode<K> {
    /// change key of kv from `old` to `key`
    pub fn change_key(&mut self, old: &K, key: K) {
        let val = self.get(old).unwrap().val;
        self.remove(old);
        self.insert(&key, &val);
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, fs::remove_dir_all, path::Path, sync::Arc};

    use rand::{prelude::StdRng, Rng, SeedableRng};

    use crate::{
        page_manager::{p_manager::FHandler, FlushHandler},
        page_system::PageSystem,
    };

    use super::*;

    #[tokio::test]
    async fn test_random() {
        let test_dir = "./test_dir/btree_node1";
        let node_n = 10;
        if Path::exists(Path::new(test_dir)) {
            remove_dir_all(test_dir).unwrap();
        }
        let ps = Arc::new(PageSystem::new(test_dir));
        let (_node_id, mut node) = Node::<u32, u32>::new(&ps.clone(), node_n, 0);
        let mut key_set = HashSet::new();
        let test_n = 50000;
        let mut rng = StdRng::seed_from_u64(0);
        let mut handler: Option<FlushHandler> = None;
        for i in 0..test_n {
            let op = rng.gen::<u8>() % 100;
            if op < 40 {
                let key = rng.gen::<u32>();
                if key_set.get(&key).is_none() {
                    if node.insert(&key, &i) {
                        key_set.insert(key);
                    }
                } else {
                    assert!(node.insert(&key, &i));
                }
            } else if op < 80 {
                if rng.gen() {
                    if key_set.len() > 0 {
                        let key = *key_set.iter().next().unwrap();
                        if let Some(kv) = node.get(&key) {
                            assert_eq!(kv.key, key);
                        }
                    }
                } else {
                    let key = loop {
                        let key = rng.gen::<u32>();
                        if key_set.get(&key).is_none() {
                            break key;
                        }
                    };
                    if let Some(kv) = node.get(&key) {
                        assert_ne!(kv.key, key);
                    }
                }
            } else if op < 98 {
                if rng.gen() {
                    if key_set.len() > 0 {
                        let key = *key_set.iter().next().unwrap();
                        assert!(key_set.remove(&key));
                        assert!(node.remove(&key));
                    }
                } else {
                    let key = loop {
                        let key = rng.gen::<u32>();
                        if key_set.get(&key).is_none() {
                            break key;
                        }
                    };
                    assert!(!node.remove(&key));
                }
            } else {
                if let Some(mut h) = handler {
                    h.join();
                }
                handler = Some(ps.flush());
            }
        }
        if let Some(mut h) = handler {
            h.join();
        }
    }

    #[test]
    fn test_btreenode_op() {
        let test_dir = "./test_dir/btree_node2";
        if Path::exists(Path::new(test_dir)) {
            remove_dir_all(test_dir).unwrap();
        }
        let node_n = 10;
        let ps = Arc::new(PageSystem::new(test_dir));
        let (node_id, mut node) = InnerNode::new(&ps.clone(), node_n, 0);
        let cap = node.capacity();
        for i in 0..cap as u32 {
            node.insert(&(2 * i), &0);
        }
        let (right_id, mut right) = InnerNode::new(&ps.clone(), node_n, 0);

        node.split_to(&mut right, None);
        ps.flush().join();

        let ps = Arc::new(unsafe { PageSystem::load(test_dir) });
        unsafe {
            let mut node = InnerNode::<u32>::load(&ps.clone(), node_n, node_id, 0);
            let mut right = InnerNode::<u32>::load(&ps, node_n, right_id, 0);
            node.merge_with(&mut right, None);

            node.change_key(&0, 1);
            assert_eq!(node.first().unwrap().key, 1);
        }
    }

    #[test]
    fn test_cursor() {
        let test_dir = "./test_dir/btree_node3";
        if Path::exists(Path::new(test_dir)) {
            remove_dir_all(test_dir).unwrap();
        }
        let node_n = 10;
        let ps = Arc::new(PageSystem::new(test_dir));
        let (_node_id, mut node) = DataNode::new(&ps, node_n, 0);
        let cap = node.capacity();
        for i in 0..cap {
            node.insert(&i, &0);
        }
        let mut cursor = NodeCursor::new(&node);
        let mut cnt = 0;
        loop {
            cursor.move_next();
            if cursor.is_null() {
                break;
            }
            assert_eq!(cnt, cursor.cur().key);
            cnt += 1;
        }
        loop {
            cursor.move_prev();
            if cursor.is_null() {
                break;
            }
            cnt -= 1;
            assert_eq!(cnt, cursor.cur().key);
        }
    }
}
