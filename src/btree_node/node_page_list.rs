use std::{
    mem::{align_of, size_of},
    ptr::{slice_from_raw_parts_mut, NonNull},
};

use memmap2::MmapMut;

use crate::{
    page_manager::{
        page::{PageId, PageRef},
        Page,
    },
    page_system::page_system::PageSystem,
    util::KV,
};

pub type BlockId = u16;

/// The node of list in page
pub struct Block<K, V> {
    pub kv: KV<K, V>,
    pub next: BlockId,
}

/// Meta of node page
struct NodePageMeta<T> {
    next: PageId,
    // the next `NodePage`'s id, if no `PageId::MAX`
    other: T,
}

struct NodePageRef<K, V, T> {
    meta: NonNull<NodePageMeta<T>>,
    blocks: NonNull<[Block<K, V>]>,
}

unsafe impl<K, V, T> Send for NodePageRef<K, V, T> {}

unsafe impl<K, V, T> Sync for NodePageRef<K, V, T> {}

impl<K, V, T> PageRef for NodePageRef<K, V, T> {
    fn load_from(mmap: &MmapMut) -> Self {
        let space = mmap.as_ref();
        let mut p = space.as_ptr() as usize;
        let p_end = p + space.len();

        p = p.div_ceil(align_of::<NodePageMeta<T>>()) * align_of::<NodePageMeta<T>>();
        let meta = NonNull::new(p as *mut NodePageMeta<T>).unwrap();
        p += size_of::<NodePageMeta<T>>();

        p = p.div_ceil(align_of::<Block<K, V>>()) * align_of::<Block<K, V>>();
        let capacity = (p_end - p) / size_of::<Block<K, V>>();
        assert!(capacity as BlockId <= BlockId::MAX); // so max id is BlockId::MAX - 1
        assert!(capacity > 0);
        let blocks =
            NonNull::new(slice_from_raw_parts_mut(p as *mut Block<K, V>, capacity)).unwrap();
        Self { meta, blocks }
    }
}

impl<K, V, T> NodePageRef<K, V, T> {
    const NULL_ID: BlockId = BlockId::MAX;

    #[inline]
    fn meta(&self) -> &NodePageMeta<T> {
        unsafe { self.meta.as_ref() }
    }

    #[inline]
    fn meta_mut(&mut self) -> &mut NodePageMeta<T> {
        unsafe { self.meta.as_mut() }
    }

    #[inline]
    fn blocks(&self) -> &[Block<K, V>] {
        unsafe { self.blocks.as_ref() }
    }

    #[inline]
    fn blocks_mut(&mut self) -> &mut [Block<K, V>] {
        unsafe { self.blocks.as_mut() }
    }
}

/// The extra meta of first node in nodelist.
/// unused_head point to an unused list to allocate block.
/// prev is the id of the first page id of prev node
pub struct ExtraMeta {
    pub head: BlockId,
    pub unused_head: BlockId,
    pub prev: PageId,
}

type FirstNodePage<K, V> = Page<NodePageRef<K, V, ExtraMeta>>;
type OtherNodePage<K, V> = Page<NodePageRef<K, V, ()>>;

/// consist of several NodePages and can manage blocks on these pages
pub struct NodePageList<K: Clone, V: Clone> {
    first_cap: BlockId,
    others_cap: Option<BlockId>,
    first: FirstNodePage<K, V>,
    others: Vec<OtherNodePage<K, V>>,
}

impl<K: Clone, V: Clone> NodePageList<K, V> {
    pub const NULL_ID: BlockId = BlockId::MAX;

    /// new a `NodePageList` with `node_n` NodePage. return the id of the first page and initialized `NodePageList`
    /// if `cap > 0` and `cap` less than the max capacity, set `cap` as capacity
    pub fn new(ps: &PageSystem, node_n: usize, cap: BlockId) -> (PageId, Self) {
        let mut pg: FirstNodePage<K, V> = ps.new_page();
        let mut pgs: Vec<OtherNodePage<K, V>> = Vec::new();
        let mut nexts = Vec::new();
        for _i in 1..node_n {
            pgs.push(ps.new_page());
            nexts.push(pgs.last().unwrap().page_id);
        }
        nexts.push(PageId::MAX);

        let mut first_cap = pg.read().blocks().len() as BlockId;
        let mut others_cap = pgs
            .first()
            .map(|first| first.read().blocks().len() as BlockId);
        if cap > 0 {
            assert!(cap <= first_cap && cap >= 3);
            first_cap = cap;
            if let Some(c) = others_cap.as_mut() {
                assert!(cap <= *c);
                *c = cap;
            }
        }
        {
            let mut page = pg.write();
            let meta = page.meta_mut();
            meta.other.prev = PageId::MAX;
            meta.other.unused_head = 0;
            page.meta_mut().other.head = Self::NULL_ID;
            page.meta_mut().next = nexts[0];
            // new an unused list with all blocks in these pages
            let blocks = page.blocks_mut();
            for i in 0..first_cap {
                blocks[i as usize].next = i + 1;
            }
        }
        let mut off = first_cap;
        for (i, pg) in pgs.iter_mut().enumerate() {
            let mut page = pg.write();
            page.meta_mut().next = nexts[i + 1];
            let blocks = page.blocks_mut();
            for j in 0..others_cap.unwrap() {
                blocks[j as usize].next = off + j + 1;
            }
            off += others_cap.unwrap();
        }
        // init next pointer of the last block
        if let Some(last) = pgs.last_mut() {
            last.write().blocks_mut()[others_cap.unwrap() as usize - 1].next = Self::NULL_ID;
        } else {
            pg.write().blocks_mut()[first_cap as usize - 1].next = Self::NULL_ID;
        };
        (
            pg.page_id,
            Self {
                first_cap,
                others_cap,
                first: pg,
                others: pgs,
            },
        )
    }

    pub unsafe fn load(ps: &PageSystem, node_n: usize, id: PageId, cap: BlockId) -> Self {
        let pg: FirstNodePage<K, V> = ps.load_page(id);
        let mut pgs: Vec<OtherNodePage<K, V>> = Vec::new();
        let mut next_id = pg.read().meta().next;
        for _i in 1..node_n {
            pgs.push(ps.load_page(next_id));
            next_id = pgs.last().unwrap().read().meta().next;
        }

        let mut first_cap = pg.read().blocks().len() as BlockId;
        let mut others_cap = pgs
            .first()
            .map(|first| first.read().blocks().len() as BlockId);
        if cap > 0 {
            assert!(cap <= first_cap && cap >= 3);
            first_cap = cap;
            if let Some(c) = others_cap.as_mut() {
                assert!(cap <= *c);
                *c = cap;
            }
        }
        Self {
            first_cap,
            others_cap,
            first: pg,
            others: pgs,
        }
    }

    /// delete all pages in the list
    pub fn delete(&self, ps: &PageSystem) {
        ps.delete_page(&self.first);
        for p in self.others.iter() {
            ps.delete_page(p)
        }
    }

    /// get reference of block in several pages by `id`
    #[inline]
    pub fn block(&self, mut id: BlockId) -> &Block<K, V> {
        if id < self.first_cap {
            return &self.first.read().blocks()[id as usize];
        }
        id -= self.first_cap;
        let idx = id / self.others_cap.unwrap();
        id = id % self.others_cap.unwrap();
        &self.others[idx as usize].read().blocks()[id as usize]
    }

    /// get mut reference of block in several pages by `id`
    #[inline]
    fn set_block_next(&mut self, mut id: BlockId, next: BlockId) {
        if id < self.first_cap {
            self.first.write().blocks_mut()[id as usize].next = next;
        } else {
            id -= self.first_cap;
            let idx = id / self.others_cap.unwrap();
            id = id % self.others_cap.unwrap();
            self.others[idx as usize].write().blocks_mut()[id as usize].next = next;
        };
    }

    #[inline]
    pub fn set_block_kv(&mut self, mut id: BlockId, key: &K, val: &V) {
        if id < self.first_cap {
            self.first.write().blocks_mut()[id as usize].kv = KV::new(key.clone(), val.clone());
        } else {
            id -= self.first_cap;
            let idx = id / self.others_cap.unwrap();
            id = id % self.others_cap.unwrap();
            self.others[idx as usize].write().blocks_mut()[id as usize].kv =
                KV::new(key.clone(), val.clone());
        };
    }

    /// get ref of the extra meta of first node
    #[inline]
    pub fn meta(&self) -> &ExtraMeta {
        &self.first.read().meta().other
    }

    #[inline]
    fn set_head(&mut self, head: BlockId) {
        self.first.write().meta_mut().other.head = head;
    }

    #[inline]
    fn set_unused_head(&mut self, unused_head: BlockId) {
        self.first.write().meta_mut().other.unused_head = unused_head;
    }

    /// alloc one block and return `None` if cannot alloc more
    pub fn alloc(&mut self) -> Option<BlockId> {
        let unused = self.meta().unused_head;
        if unused != Self::NULL_ID {
            let new_block = self.block(unused);
            let new_unused = new_block.next;
            self.set_unused_head(new_unused);
            Some(unused)
        } else {
            None
        }
    }

    /// free the block with block_id `id`. unsafe if `id` is outof range or is not in use
    pub fn free(&mut self, id: BlockId) {
        let old_unused = self.meta().unused_head;
        self.set_block_next(id, old_unused);
        self.set_unused_head(id);
    }

    /// insert after block `at` with block `id`. unsafe if at or id is out of range or not in use
    pub fn insert_after(&mut self, at: BlockId, id: BlockId) {
        if at != Self::NULL_ID {
            let prev = self.block(at);
            self.set_block_next(id, prev.next);
            self.set_block_next(at, id);
        } else {
            self.set_block_next(id, self.meta().head);
            self.set_head(id);
        }
    }

    /// remove the next block with. panic if next of at is null. unsafe if at is out of range or not in use. return remove block id
    pub unsafe fn remove_after(&mut self, at: BlockId) -> BlockId {
        if at != Self::NULL_ID {
            let del_id = self.block(at).next;
            assert_ne!(del_id, Self::NULL_ID);
            self.set_block_next(at, self.block(del_id).next);
            del_id
        } else {
            let head_id = self.meta().head;
            let del_id = self.block(head_id).next;
            self.set_head(del_id);
            head_id
        }
    }

    #[inline]
    pub fn capacity(&self) -> usize {
        (self.first_cap + self.others_cap.unwrap_or(0) * self.others.len() as BlockId) as usize
    }

    /// get the next page id of the last page
    /// this is the first page id of the next node
    #[inline]
    pub fn last_next(&self) -> PageId {
        self.others
            .last()
            .map_or(self.first.read().meta().next, |last| {
                last.read().meta().next
            })
    }

    /// set next node id
    #[inline]
    pub fn set_last_next(&mut self, next_id: PageId) {
        if let Some(l) = self.others.last_mut() {
            l.write().meta_mut().next = next_id;
        } else {
            self.first.write().meta_mut().next = next_id;
        }
    }

    /// id of the first page.
    /// this is the node id of current node
    #[inline]
    pub fn first_id(&self) -> PageId {
        self.first.page_id
    }

    pub fn first_prev(&self) -> PageId {
        self.first.read().meta().other.prev
    }

    pub fn set_first_prev(&mut self, prev_id: PageId) {
        self.first.write().meta_mut().other.prev = prev_id;
    }
}

impl<K: Clone, V: Clone> NodePageList<K, V> {
    pub fn check(&self) -> usize {
        let mut cnt = 0;
        let mut id = self.meta().head;
        while id != Self::NULL_ID {
            let block = self.block(id);
            cnt += 1;
            id = block.next;
        }
        let len = cnt as usize;
        id = self.meta().unused_head;
        while id != Self::NULL_ID {
            let block = self.block(id);
            cnt += 1;
            id = block.next;
        }
        assert_eq!(cnt, self.capacity());
        len
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, fs::remove_dir_all, path::Path};

    use rand::{prelude::StdRng, Rng, SeedableRng};

    use super::*;

    #[test]
    fn test_seq() {
        let test_dir = "./test_dir/node_page_list1";
        let node_n = 10;
        if Path::exists(Path::new(test_dir)) {
            remove_dir_all(test_dir).unwrap();
        }
        for i in 0..2 {
            let ps = if i == 0 {
                PageSystem::new(test_dir)
            } else {
                unsafe { PageSystem::load(test_dir) }
            };
            let (nodes_id, mut nodes) = NodePageList::<u32, u32>::new(&ps, node_n, 0);
            let cap = nodes.capacity();

            for j in 0..cap {
                let id = nodes.alloc().unwrap();
                nodes.insert_after(NodePageList::<u32, u32>::NULL_ID, id);
                if j % 1000 == 0 {
                    nodes.check();
                }
            }
            nodes.check();
            assert!(nodes.alloc().is_none());
            let mut nodes = unsafe { NodePageList::<u32, u32>::load(&ps, node_n, nodes_id, 0) };
            for j in 0..cap {
                unsafe {
                    let id = nodes.remove_after(NodePageList::<u32, u32>::NULL_ID);
                    nodes.free(id);
                }
                if j % 1000 == 0 {
                    nodes.check();
                }
            }
            nodes.check();
        }
    }

    #[test]
    fn test_random() {
        let test_dir = "./test_dir/node_page_list2";
        let node_n = 10;
        if Path::exists(Path::new(test_dir)) {
            remove_dir_all(test_dir).unwrap();
        }
        let ps = PageSystem::new(test_dir);
        let mut ids = Vec::new();
        let test_n = 50000;
        let mut rng = StdRng::seed_from_u64(0);
        let (_node_id, mut nodes) = NodePageList::<u32, u32>::new(&ps, node_n, 0);
        let mut id_set = HashSet::new();
        for _i in 0..test_n {
            let op = rng.gen::<u8>() % 5;
            if op < 3 {
                if let Some(id) = nodes.alloc() {
                    let at = loop {
                        let mut idx = rng.gen::<usize>() % (ids.len() + 1);
                        if idx == 0 {
                            break NodePageList::<u32, u32>::NULL_ID;
                        } else {
                            idx -= 1;
                            let at = ids[idx];
                            if id_set.get(&at).is_none() {
                                let last = ids.len() - 1;
                                ids.swap(idx, last);
                                ids.pop().unwrap();
                            } else {
                                break at;
                            }
                        }
                    };
                    // println!("insert after {} with {}", at, id);
                    nodes.insert_after(at, id);
                    ids.push(id);
                    id_set.insert(id);
                }
            } else {
                let mut flag = false;
                let at = loop {
                    if ids.is_empty() {
                        flag = true;
                        break 0;
                    }
                    let idx = rng.gen::<usize>() % ids.len();
                    let at = ids[idx];
                    if id_set.get(&at).is_none() {
                        let last = ids.len() - 1;
                        ids.swap(last, idx);
                        ids.pop().unwrap();
                    } else if nodes.block(at).next == NodePageList::<u32, u32>::NULL_ID {
                        break NodePageList::<u32, u32>::NULL_ID;
                    } else {
                        break at;
                    }
                };
                if flag {
                    continue;
                }
                let id = unsafe { nodes.remove_after(at) };
                // println!("remove after {} is {}", at, id);
                assert!(id_set.remove(&id));
                nodes.free(id);
            }
        }
    }
}
