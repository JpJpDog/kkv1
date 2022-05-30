use core::panic;
use std::{
    fs::remove_dir_all,
    mem::swap,
    path::Path,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, RwLock,
    },
    thread::spawn,
};

use rand::{
    prelude::{SliceRandom, StdRng},
    random, Rng, SeedableRng,
};

use crate::page_manager::{p_manager::FHandler, FlushHandler};

use super::{BTree, DEFAULT_CRABBING_CONFIG};

#[test]
fn test_random_insert_s() {
    let test_dir = "./test_dir/btree1";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_CRABBING_CONFIG;
    config.node_cap = 10;
    let mut btree = BTree::<u32, u32>::new(test_dir, 0, config);
    let total_n = 1000;
    let max_key = 100000;
    let batch_size = 100;
    let batch_n = total_n / batch_size;
    let mut rng = StdRng::seed_from_u64(0);
    let mut handler: Option<FlushHandler> = None;
    for _j in 0..batch_n {
        let mut cnt = 0;
        while cnt < batch_size {
            let op = rng.gen::<u8>() % 10;
            let key = rng.gen::<u32>() % max_key + 1;
            if op < 8 {
                let mut val = 0;
                btree.get(&key, &mut val);
            } else {
                // println!("insert {}", key);
                btree.insert(&key, &cnt);
                // btree.store.dump();
                btree.store.check();
                cnt += 1;
            }
        }
        if let Some(mut h) = handler {
            h.join();
        }

        // // todo: add in update
        btree.update_cache();
        handler = Some(btree.flush());
    }
    if let Some(mut h) = handler {
        h.join();
    }
}

#[test]
fn test_random_insert_m() {
    let test_dir = "./test_dir/btree2";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_CRABBING_CONFIG;
    config.node_cap = 10;
    let btree = BTree::<u32, u32>::new(test_dir, 0, config);
    let btree1: Arc<RwLock<(BTree<u32, u32>, Option<FlushHandler>)>> =
        Arc::new(RwLock::new((btree, None)));
    let total_n = 1000;
    let max_key = 100000;
    let batch_size = 100;
    // let batch_n = total_n / batch_size;
    let thread_n = 2;
    let mut handlers = Vec::new();
    let cnt1 = Arc::new(AtomicU32::new(0));
    for _i in 0..thread_n {
        let btree = btree1.clone();
        let cnt = cnt1.clone();
        let h = spawn(move || loop {
            let op = random::<u32>() % 100;
            let key = random::<u32>() % max_key + 1;
            if op < 80 {
                let btree_g = btree.read().unwrap();
                let mut val = 0;
                btree_g.0.get(&key, &mut val);
                continue;
            }
            let old_n = cnt.fetch_add(1, Ordering::Relaxed);
            btree.read().unwrap().0.insert(&key, &0);
            if old_n % 10000 == 0 {
                println!("{}", old_n);
            }
            if old_n % batch_size == 0 {
                let mut btree_g = btree.write().unwrap();
                let mut h1 = None;
                swap(&mut h1, &mut btree_g.1);
                if let Some(mut h) = h1 {
                    h.join();
                }
                btree_g.1 = Some(btree_g.0.flush());
                btree_g.0.update_cache();
            }
            if old_n >= total_n {
                break;
            }
        });
        handlers.push(h);
    }
    for h in handlers {
        h.join().unwrap();
    }
    match Arc::try_unwrap(btree1) {
        Ok(a) => {
            let (_btree, h) = a.into_inner().unwrap();
            if let Some(mut h) = h {
                h.join();
            }
        }
        Err(_) => panic!(),
    }
}

#[test]
fn seq_insert_remove_s() {
    let test_dir = "./test_dir/btree3";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut rng = StdRng::seed_from_u64(0);
    let mut config = DEFAULT_CRABBING_CONFIG;
    config.node_cap = 10;
    let btree = BTree::<u32, u32>::new(test_dir, 0, config);
    let mut handler: Option<FlushHandler> = None;
    let total_n = 1000;
    let max_key = 100000;
    let batch_size = 100;
    let mut keys = Vec::new();
    for i in 0..total_n {
        let key = rng.gen::<u32>() % max_key;
        let val = 0;
        btree.insert(&key, &val);
        keys.push(key);
        if i % batch_size == 0 {
            if let Some(mut h) = handler {
                h.join();
            }
            handler = Some(btree.flush());
        }
    }
    // btree.store.dump();
    // println!();
    keys.shuffle(&mut rng);
    for (i, k) in keys.into_iter().enumerate() {
        let mut val = 0;
        // println!("remove {}", &k);
        btree.remove(&k);
        // btree.store.dump();
        // println!();
        assert!(!btree.get(&k, &mut val));
        if i % batch_size == 0 {
            if let Some(mut h) = handler {
                h.join();
            }
            handler = Some(btree.flush());
        }
    }
    if let Some(mut h) = handler {
        h.join();
    }
}
