use std::{ptr::NonNull, mem::swap, hash::Hash, collections::HashMap, fmt::Debug};

use rand::{prelude::StdRng, Rng, SeedableRng};

use crate::util::KV;

/// max height of `SkipList`
const MAX_LAYER: usize = 32;

struct SkipNode<K, V> {
    /// `next[0]` is the adjacent next node, next[i] is the ith layer.
    /// The second field is the distance between this node and next, min is 1.
    next: Vec<Option<(NonNull<SkipNode<K, V>>, usize)>>,
    /// adjacent prev node
    prev: Option<NonNull<SkipNode<K, V>>>,
    kv: KV<K, V>,
}

impl<K, V> SkipNode<K, V> {
    /// new a `SkipNode` with `kv`
    fn new_with(layer: usize, kv: KV<K, V>) -> Self {
        Self {
            next: vec![None; layer],
            prev: None,
            kv,
        }
    }
}

pub struct SkipList<K: PartialOrd, V: Clone> {
    /// first null node
    head: NonNull<SkipNode<K, V>>,
    /// last node. if skiplist is empty, point to null node
    tail: NonNull<SkipNode<K, V>>,
    max_layer: usize,
    len: usize,
    rng: StdRng,
}

unsafe impl<K: PartialOrd, V: Clone> Send for SkipList<K, V> {}

unsafe impl<K: PartialOrd, V: Clone> Sync for SkipList<K, V> {}

pub struct SkipListCursor<'a, K: PartialOrd, V: Clone> {
    list: &'a SkipList<K, V>,
    cur: NonNull<SkipNode<K, V>>,
    dist: usize,
}

impl<'a, K: PartialOrd, V: Clone> SkipListCursor<'a, K, V> {
    /// new a cursor that cur point to null node
    pub fn new(list: &'a SkipList<K, V>) -> Self {
        Self {
            list,
            cur: list.head,
            dist: 0,
        }
    }

    #[inline]
    unsafe fn new_at(list: &'a SkipList<K, V>, at: NonNull<SkipNode<K, V>>, dist: usize) -> Self {
        Self {
            list,
            cur: at,
            dist,
        }
    }

    /// return if cur is the null node in skiplist
    #[inline]
    pub fn is_null(&self) -> bool {
        self.cur == self.list.head
    }

    /// return cur index
    #[inline]
    pub fn index(&self) -> Option<usize> {
        if self.dist > 0 {
            Some(self.dist - 1)
        } else {
            None
        }
    }

    /// return reference of cur kv
    #[inline]
    pub fn cur(&self) -> &'a KV<K, V> {
        &unsafe { self.cur.as_ref() }.kv
    }

    /// move cur next.
    /// if cur is the last node, move cur to null node.
    #[inline]
    pub fn move_next(&mut self) {
        if let Some((next, _d)) = unsafe { self.cur.as_mut() }.next[0] {
            self.dist += 1;
            self.cur = next;
        } else {
            self.dist = 0;
            self.cur = self.list.head;
        }
    }

    /// move cur to prev.
    /// if cur is null node, move cur to the last node.
    #[inline]
    pub fn move_prev(&mut self) {
        if let Some(prev) = unsafe { self.cur.as_ref() }.prev {
            self.cur = prev;
            self.dist -= 1;
        } else {
            self.cur = self.list.tail;
            self.dist = self.list.len();
        }
    }

    /// set cur val
    #[inline]
    pub fn set_val(&mut self, val: V) {
        unsafe { self.cur.as_mut() }.kv.val = val;
    }
}

impl<K: PartialOrd, V: Clone> SkipList<K, V> {
    /// randomly produce a layer in [1, MAX_LAYER]. the high layer's possibility is half of the lower one
    #[inline]
    fn rand_layer_n(rng: &mut StdRng) -> usize {
        let mut res = 1;
        let mut r = rng.gen::<u32>();
        while res <= MAX_LAYER && (r & 1) != 0 {
            r >>= 1;
            res += 1;
        }
        res
    }

    pub fn new(kv: KV<K, V>) -> Self {
        let rng = StdRng::seed_from_u64(3);
        let head =
            NonNull::new(Box::into_raw(Box::new(SkipNode::new_with(MAX_LAYER, kv)))).unwrap();
        Self {
            head,
            tail: head,
            max_layer: 0,
            len: 0,
            rng,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Get the node that less or equal `key` and return the curor point to that node.
    /// If `key` if less than first element, return curosr point to null.
    pub fn get(&self, key: &K) -> SkipListCursor<K, V> {
        unsafe {
            let mut p = self.head;
            let mut layer = self.max_layer;
            let mut dist = 0; // dist from null node to p
            while layer > 0 {
                if let Some((next, d)) = p.as_ref().next[layer - 1] {
                    let k = &next.as_ref().kv.key;
                    if k <= key {
                        p = next;
                        dist += d;
                        if k == key {
                            break;
                        }
                    } else {
                        layer -= 1;
                    }
                } else {
                    layer -= 1;
                }
            }
            // index should be dist - 1 because null node is not taken into account
            SkipListCursor::new_at(self, p, dist)
        }
    }

    /// Insert `key` and `val` in proper positon.
    /// If `key` is already in skiplist, return cursor point to that node.
    /// Else, return the cursor point to the newly inserted node and old value.
    pub fn insert(&mut self, key: K, mut val: V) -> (SkipListCursor<K, V>, Option<V>) {
        unsafe {
            let mut p = self.head;
            let mut layer = self.max_layer; // layer of p
                                            // prev_vec[i].0 is in `i` layer the last less or equal node`
                                            // prev_vec[i].1 is that node's index + 1
            let mut prev_vec = [(NonNull::dangling(), 0); MAX_LAYER];
            let mut dist = 0; // node from null node to p
            while layer > 0 {
                if let Some((next, d)) = p.as_mut().next[layer - 1] {
                    let k = &next.as_ref().kv.key;
                    if k <= &key {
                        p = next;
                        dist += d;
                        if k == &key {
                            break;
                        }
                    } else {
                        prev_vec[layer - 1] = (p, dist);
                        layer -= 1;
                    }
                } else {
                    prev_vec[layer - 1] = (p, dist);
                    layer -= 1;
                }
            }
            // layer != 0, must break from loop. key already in skiplist, p point to that node
            if layer > 0 {
                swap(&mut p.as_mut().kv.val, &mut val);
                return (SkipListCursor::new_at(self, p, dist), Some(val));
            }
            // p is the last less or eqaul node
            dist += 1; // dist is new node's dist from null
            let layer = Self::rand_layer_n(&mut self.rng);
            let new_node = Box::leak(Box::new(SkipNode::new_with(layer, KV::new(key, val))));
            let new_p = NonNull::new(new_node).unwrap();
            // set prev of adjacent next node and new node
            new_node.prev = Some(p);
            if let Some((mut next, _d)) = p.as_ref().next[0] {
                next.as_mut().prev = Some(new_p);
            } else {
                self.tail = new_p;
            }
            // update max_layer and add prev_vec from front
            if layer > self.max_layer {
                for l in self.max_layer..layer {
                    // dist from null to itself is 0
                    prev_vec[l] = (self.head, 0);
                }
                self.max_layer = layer;
            }
            // set next of new_node and prev node.
            for l in 0..layer {
                // d is the dist from null to p's owner, d1 is the dist from p's owner to old next node
                let (mut p, d) = prev_vec[l];
                // set next_node's next
                new_node.next[l] = p.as_ref().next[l].map(|(next, d1)| (next, d + d1 + 1 - dist));
                // update prev's next
                p.as_mut().next[l] = Some((new_p, dist - d));
            }
            // set the prev node in layer l's next distance if next exists
            for l in layer..self.max_layer {
                let (mut p, _d) = prev_vec[l];
                if let Some((_n, d)) = &mut p.as_mut().next[l] {
                    *d += 1;
                } else {
                    break;
                }
            }
            self.len += 1;
            (SkipListCursor::new_at(self, new_p, dist), None)
        }
    }

    /// Remove the `key` from skiplist. return cursor point to the last node less than or equal to `key`. If `key` existed, return the prev value
    pub fn remove(&mut self, key: &K) -> (SkipListCursor<K, V>, Option<V>) {
        unsafe {
            let mut p = self.head;
            let mut layer = self.max_layer; // layer of p
            let mut prev_dist = Vec::new();
            let mut dist = 0;
            while layer > 0 {
                if let Some((next, d)) = &mut p.as_mut().next[layer - 1] {
                    let k = &next.as_ref().kv.key;
                    if k <= key {
                        dist += *d;
                        if k == key {
                            break;
                        }
                        p = *next;
                    } else {
                        prev_dist.push(d);
                        layer -= 1;
                    }
                } else {
                    layer -= 1;
                }
            }
            return if layer > 0 {
                // p is adjacent prev node of target node
                let (del_p, _d) = p.as_ref().next[layer - 1].unwrap();
                let del_node = del_p.as_ref();
                let del_val = del_node.kv.val.clone();
                while layer > 0 {
                    let (nextp, d) = p.as_ref().next[layer - 1].unwrap();
                    assert!(&nextp.as_ref().kv.key <= key);
                    if &nextp.as_ref().kv.key != key {
                        p = nextp;
                    } else {
                        p.as_mut().next[layer - 1] =
                            nextp.as_ref().next[layer - 1].map(|(nextp, d1)| (nextp, d1 + d - 1));
                        layer -= 1;
                    }
                }
                // set prev
                if let Some((mut nextp, _d)) = del_node.next[0] {
                    nextp.as_mut().prev = del_node.prev;
                } else {
                    self.tail = del_node.prev.unwrap();
                }
                drop(Box::from_raw(del_p.as_ptr()));
                for d in prev_dist {
                    *d -= 1;
                }
                // remove empty layer
                let mut l = self.max_layer;
                while l > 0 && self.head.as_ref().next[l - 1].is_none() {
                    l -= 1;
                }
                self.max_layer = l;
                self.len -= 1;
                (SkipListCursor::new_at(self, p, dist - 1), Some(del_val))
            } else {
                (SkipListCursor::new_at(self, p, dist), None)
            };
        }
    }

    #[inline]
    pub fn first(&self) -> Option<&KV<K, V>> {
        unsafe {
            self.head.as_ref().next[0].map(|(mut first, _d)| {
                let first = first.as_mut();
                &first.kv
            })
        }
    }

    #[inline]
    pub fn last(&self) -> Option<&KV<K, V>> {
        unsafe {
            (self.tail != self.head).then(|| {
                let tail = self.tail;
                &tail.as_ref().kv
            })
        }
    }

    pub fn get_by_idx(&self, idx: usize) -> Option<(&K, &mut V)> {
        unsafe {
            if idx >= self.len() {
                return None;
            }
            let mut p = self.head;
            let mut layer = self.max_layer;
            let mut dist = 0;
            loop {
                if let Some((mut next, d)) = p.as_ref().next[layer - 1] {
                    if d + dist < idx + 1 {
                        p = next;
                        dist += d;
                    } else if d + idx > idx + 1 {
                        layer -= 1;
                    } else {
                        let kv = &mut next.as_mut().kv;
                        return Some((&kv.key, &mut kv.val));
                    }
                } else {
                    layer -= 1;
                }
            }
        }
    }
}

impl<K: PartialOrd, V: Clone> Drop for SkipList<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut q = self.head.as_ref().next[0];
            while let Some((p, _d)) = q {
                q = p.as_ref().next[0];
                drop(Box::from_raw(p.as_ptr()));
            }
            drop(Box::from_raw(self.head.as_ptr()));
        }
    }
}

impl<K: PartialOrd + Debug, V: Clone> SkipList<K, V> {
    pub fn print(&self) {
        unsafe {
            let mut layer = self.max_layer;
            while layer > 0 {
                let mut next = self.head.as_ref().next[layer - 1];
                while let Some((p, d)) = next {
                    print!("({} {:?})", d, p.as_ref().kv.key);
                    next = p.as_ref().next[layer - 1];
                }
                println!();
                layer -= 1;
            }
        }
    }
}

impl<K: PartialOrd + Eq + Hash, V: Clone> SkipList<K, V> {
    pub fn check(&self) {
        unsafe {
            let mut map = HashMap::new();
            let mut p = self.head;
            let mut prev_k = None;
            let mut prev_p = Some(self.head);
            let mut idx = 0;
            // traverse the bottom layer
            while let Some((next, d)) = p.as_ref().next[0] {
                assert_eq!(d, 1); // bottom layer dist is 1
                let key = &next.as_ref().kv.key;

                if let Some(p) = prev_k {
                    assert!(p < key); // ascending order
                }
                prev_k = Some(key);

                if let Some(p) = prev_p {
                    assert_eq!(p, next.as_ref().prev.unwrap()); // check prev
                }
                prev_p = Some(next);

                map.insert(key, idx);
                idx += 1;
                p = next;
            }
            assert_eq!(self.tail, prev_p.unwrap());
            let mut p = self.head;
            let mut layer = self.max_layer;
            while layer > 0 {
                if let Some((next, d)) = p.as_ref().next[layer - 1] {
                    let d1 = *map.get(&next.as_ref().kv.key).unwrap();
                    let d2 = if p == self.head {
                        -1 // head index is -1
                    } else {
                        *map.get(&p.as_ref().kv.key).unwrap()
                    };
                    assert_eq!((d1 - d2) as usize, d); //check index
                    p = next;
                } else {
                    p = self.head;
                    layer -= 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashSet, mem::MaybeUninit};

    use rand::random;

    use super::*;

    #[test]
    fn test_random() {
        let mut skip = SkipList::new(KV::new(0, 1));
        let mut rng = StdRng::seed_from_u64(0);
        let mut keys = Vec::new();
        let max_key = 10000;
        {
            let mut keys_set = HashSet::new();
            for i in 0..max_key {
                let key = rng.gen::<u32>() % max_key + 1;
                if !keys_set.insert(key) {
                    continue;
                }
                let l1 = skip.len();
                // println!("insert {}", key);
                let (cursor, _v) = skip.insert(key, i);
                assert_eq!(cursor.index(), skip.get(&key).index());
                // skip.print();
                // println!();
                // skip.check();
                if skip.len() > l1 {
                    keys.push(key);
                }
            }
        }
        let mut i = 0;
        {
            let mut idx_map = HashMap::new();
            let mut keys1 = keys.clone();
            keys1.sort();
            for (i, k) in keys1.iter().enumerate() {
                idx_map.insert(*k, i);
            }
            for k in keys.iter() {
                let cursor = skip.get(k);
                assert_eq!(*idx_map.get(k).unwrap(), cursor.index().unwrap());
                i += 1;
            }
            for i in 0..skip.len() {
                let (k, _v) = skip.get_by_idx(i).unwrap();
                assert_eq!(&keys1[i], k);
            }
            assert!(skip.get_by_idx(skip.len).is_none());
        }
        i = 0;
        while i < keys.len() {
            let mut flag = true;
            let key = if rng.gen::<u32>() % 3 == 0 {
                flag = false;
                rng.gen::<u32>() % max_key
            } else {
                i += 1;
                keys[i - 1]
            };
            // println!("remove {}", key);
            let (cursor, _v) = skip.remove(&key);
            let prev_k = cursor.cur().key;
            let prev_idx = cursor.index();
            assert_eq!(skip.get(&prev_k).index(), prev_idx);
            // skip.print();
            // println!();
            // skip.check();
            let res = skip.get(&key);
            if !res.is_null() {
                if flag {
                    assert_ne!(res.cur().key, key);
                }
            }
        }
        assert_eq!(skip.max_layer, 0);
    }

    #[test]
    fn test_performance() {
        let mut skip = SkipList::<u32, u32>::new(unsafe { MaybeUninit::uninit().assume_init() });
        let mut keys = vec![];
        let cap = 10000;
        keys.reserve(cap);
        for i in 0..cap as u32 {
            let key = random::<u32>() % (u32::MAX - 1) + 1;
            let (_cursor, v) = skip.insert(key, i);
            if v.is_none() {
                keys.push(key);
            }
        }
        for _i in 0..100 {
            for k in keys.iter() {
                skip.get(k);
            }
        }
        for k in keys {
            skip.remove(&k);
        }
    }

    #[test]
    fn test_cursor() {
        let mut skip = SkipList::<u32, u32>::new(KV::new(0, 0));
        for i in 1..10001 {
            skip.insert(i, 0);
        }
        let mut cursor = SkipListCursor::new(&skip);
        let mut prev_k = None;
        let mut cnt = 0;
        loop {
            if let Some(idx) = cursor.index() {
                assert_eq!(idx + 1, cnt);
            }
            let cur = cursor.cur();
            if let Some(pk) = prev_k {
                assert!(pk < cur.key);
            }
            prev_k = Some(cur.key);
            cursor.move_next();
            if cursor.is_null() {
                break;
            }
            cnt += 1;
        }
        assert_eq!(cnt, skip.len());
        prev_k = None;
        loop {
            cursor.move_prev();
            if let Some(idx) = cursor.index() {
                assert_eq!(idx + 1, cnt);
            }
            let cur = cursor.cur();
            if let Some(pk) = prev_k {
                assert!(cur.key < pk);
            }
            prev_k = Some(cur.key);
            if cursor.is_null() {
                break;
            }
            cnt -= 1;
        }
        assert_eq!(cnt, 0);
    }
}
