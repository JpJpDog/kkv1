use std::{collections::HashMap, hash::Hash, mem::MaybeUninit, ptr::NonNull};

use crate::util::KV;

struct CacheNode<K, V> {
    kv: MaybeUninit<KV<K, V>>,
    next: Option<NonNull<CacheNode<K, V>>>,
    prev: NonNull<CacheNode<K, V>>,
}

struct CacheList<K, V> {
    head: NonNull<CacheNode<K, V>>,
    tail: NonNull<CacheNode<K, V>>,
    len: usize,
}

impl<K, V> CacheList<K, V> {
    fn new() -> Self {
        let head = Box::new(CacheNode {
            kv: MaybeUninit::uninit(),
            next: None,
            prev: NonNull::dangling(),
        });
        let head = NonNull::new(Box::into_raw(head)).unwrap();
        let tail = head;
        Self { head, tail, len: 0 }
    }

    unsafe fn move_head(&mut self, mut at: NonNull<CacheNode<K, V>>) {
        assert_ne!(at, self.head);
        let next = at.as_mut().next;
        let mut prev = at.as_mut().prev;
        prev.as_mut().next = next;
        if let Some(mut next) = next {
            next.as_mut().prev = prev;
        } else {
            self.tail = prev;
        }
        if let Some(mut old) = self.head.as_ref().next {
            old.as_mut().prev = at;
        }
        at.as_mut().next = self.head.as_ref().next;
        at.as_mut().prev = self.head;
        self.head.as_mut().next = Some(at);
    }

    unsafe fn push_head(&mut self, key: K, val: V) -> NonNull<CacheNode<K, V>> {
        let node = Box::new(CacheNode {
            kv: MaybeUninit::new(KV::new(key, val)),
            next: self.head.as_ref().next,
            prev: self.head,
        });
        let node = NonNull::new(Box::into_raw(node)).unwrap();
        if let Some(mut old) = self.head.as_ref().next {
            old.as_mut().prev = node;
        }
        self.head.as_mut().next = Some(node);
        self.len += 1;
        node
    }

    unsafe fn remove_at(&mut self, mut at: NonNull<CacheNode<K, V>>) {
        assert_ne!(at, self.head);
        if let Some(mut next) = at.as_ref().next {
            next.as_mut().prev = at.as_ref().prev;
        } else {
            self.tail = at.as_ref().prev;
        }
        at.as_mut().prev.as_mut().next = at.as_ref().next;
        self.len -= 1;
        drop(Box::from_raw(at.as_ptr()));
    }

    fn pop_tail(&mut self) -> Option<NonNull<CacheNode<K, V>>> {
        (self.tail != self.head).then(|| unsafe {
            let mut tail = self.tail;
            tail.as_mut().prev.as_mut().next = None;
            self.tail = tail.as_ref().prev;
            self.len -= 1;
            tail
        })
    }
}

impl<K, V> Drop for CacheList<K, V> {
    fn drop(&mut self) {
        unsafe {
            let mut p = self.head;
            let mut cnt = 0;
            loop {
                let q = p.as_ref().next;
                if p != self.head {
                    drop(Box::from_raw(p.as_ptr()));
                } else {
                    drop(Box::from_raw(p.as_ptr() as *mut u8));
                }
                cnt += 1;
                if q.is_none() {
                    break;
                }
                p = q.unwrap();
            }
            assert_eq!(self.len + 1, cnt);
        }
    }
}

/// LRU cache that need manually update
pub struct LRUCache<K: Eq + Hash + Clone, V: Clone> {
    map: HashMap<K, NonNull<CacheNode<K, V>>>,
    list: CacheList<K, V>,
    capacity: usize,
}

unsafe impl<K: Eq + Hash + Clone, V: Clone> Send for LRUCache<K, V> {}

unsafe impl<K: Eq + Hash + Clone, V: Clone> Sync for LRUCache<K, V> {}

impl<K: Eq + Hash + Clone, V: Clone> LRUCache<K, V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            map: HashMap::new(),
            list: CacheList::new(),
        }
    }

    pub fn put(&mut self, key: K, val: V) -> bool {
        unsafe {
            if self.map.len() >= self.capacity {
                if let Some(tail) = self.list.pop_tail() {
                    let tail = Box::from_raw(tail.as_ptr());
                    assert!(self.map.remove(&tail.kv.assume_init_ref().key).is_some());
                }
            }
            if let Some(node) = self.map.get_mut(&key) {
                node.as_mut().kv.assume_init_mut().val = val;
                self.list.move_head(*node);
                false
            } else {
                let node = self.list.push_head(key.clone(), val);
                self.map.insert(key, node);
                true
            }
        }
    }

    pub fn delete(&mut self, key: &K) -> bool {
        unsafe {
            if let Some(node) = self.map.remove(key) {
                self.list.remove_at(node);
                true
            } else {
                false
            }
        }
    }

    pub fn update(&mut self, keys: Vec<K>) {
        unsafe {
            for key in keys {
                if let Some(node) = self.map.get(&key) {
                    self.list.move_head(*node);
                }
            }
        }
    }

    pub fn get(&self, key: &K) -> Option<V> {
        self.map
            .get(key)
            .map(|node| unsafe { node.as_ref().kv.assume_init_ref().val.clone() })
    }
}

#[cfg(test)]
mod test {
    use std::{
        mem::swap,
        sync::{Arc, RwLock},
        thread::spawn,
    };

    use rand::{prelude::StdRng, random, Rng, SeedableRng};

    use super::*;

    #[test]
    fn test_lru() {
        let mut cache = LRUCache::new(3);
        cache.put(1, 1);
        cache.put(2, 2);
        cache.put(3, 3);
        cache.update(vec![1, 2]);
        cache.put(4, 4);
        assert!(cache.get(&3).is_none());
        cache.delete(&1);
        cache.put(5, 5);
        assert!(cache.get(&1).is_none());
        cache.put(6, 6);
        assert!(cache.get(&2).is_none());
    }

    #[test]
    fn test_random() {
        let capacity = 50;
        let mut cache = LRUCache::<u32, u32>::new(capacity);
        let test_n = 100000;
        let mut rng = StdRng::seed_from_u64(0);
        let mut keys = Vec::new();
        let mut updates = Vec::new();
        for i in 0..test_n {
            let op = rng.gen::<u8>() % 4;
            if op == 0 {
                if keys.len() > 0 {
                    let idx = rng.gen::<usize>() % keys.len();
                    let key = keys[idx];
                    if cache.get(&key).is_some() {
                        updates.push(key);
                    }
                }
            } else if op == 1 {
                if rng.gen() {
                    let len = keys.len();
                    if len > 0 {
                        let idx = rng.gen::<usize>() % len;
                        let key = keys[idx];
                        cache.put(key, i);
                    }
                } else {
                    let key = rng.gen();
                    cache.put(key, i);
                    keys.push(key);
                }
            } else if op == 2 {
                if rng.gen() {
                    let len = keys.len();
                    if len > 0 {
                        let idx = rng.gen::<usize>() % len;
                        keys.swap(idx, len - 1);
                        let key = keys.pop().unwrap();
                        cache.delete(&key);
                    }
                } else {
                    let key = rng.gen();
                    cache.delete(&key);
                }
            } else {
                let mut updates1 = Vec::new();
                swap(&mut updates, &mut updates1);
                cache.update(updates1);
            }
            assert_eq!(cache.map.len(), cache.list.len);
        }
    }

    #[test]
    fn test_multi_random() {
        let capacity = 5;
        let cache1 = Arc::new(RwLock::new(LRUCache::<u32, u32>::new(capacity)));
        let thread_n = 16;
        let mut handlers = Vec::new();
        let updates1 = Arc::new(RwLock::new(Vec::new()));
        let max_key = 1000;
        for _i in 0..thread_n {
            let cache = cache1.clone();
            let updates = updates1.clone();
            let h = spawn(move || {
                for _i in 0..10000 {
                    let op = random::<u32>() % 10;
                    let key = random::<u32>() % max_key;
                    if op < 7 {
                        if cache.read().unwrap().get(&key).is_some() {
                            updates.write().unwrap().push(key);
                        }
                    } else if op == 7 {
                        cache.write().unwrap().put(key, 0);
                    } else if op == 8 {
                        cache.write().unwrap().delete(&key);
                    } else {
                        let mut updates1 = Vec::new();
                        let mut updates_g = updates.write().unwrap();
                        swap(&mut updates1, &mut updates_g);
                        drop(updates_g);
                        cache.write().unwrap().update(updates1);
                    }
                }
            });
            handlers.push(h);
        }
        for h in handlers {
            h.join().unwrap()
        }
    }
}
