use std::{
    fs::remove_dir_all,
    path::Path,
    ptr::NonNull,
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc, RwLock,
    },
    thread::spawn,
};

use crate::{
    crabbing::{BTree, FHandler, FlushHandler, DEFAULT_CRABBING_CONFIG},
    p2p_palm::{palm_msg::PALMReq, P2PPALMTree, DEFAULT_P2P_PALM_CONFIG},
    palm::{palm_msg::PALMCmd, PALMTree, DEFAULT_PALM_CONFIG},
};
use rand::{prelude::StdRng, random, SeedableRng};

#[test]
fn test1() {
    crabbing_random_insert(10000000, u32::MAX - 1, 16, true, 0, 80);
}

#[test]
fn test2() {
    palm_random_insert(10000000, u32::MAX - 1, 30, 300, 0, 80);
}

#[test]
fn test3() {
    p2p_random_insert(1000000, u32::MAX - 1, 20, 400, true, 80);
}

#[test]
fn test4() {
    crabbing_random_insert_remove(10000000, u32::MAX - 1, 16, false, 1000, 70, 10);
}

#[test]
fn test5() {
    crabbing_seq_insert(10000000, 16, true, 0, 80);
}

#[test]
fn test6() {
    palm_seq_insert(10000000, 8, 1000, 0, 80);
}

#[test]
fn test7() {
    p2p_seq_insert(10000000, 20, 500, true, 80);
}

fn crabbing_random_insert(
    test_n: usize,
    max_key: u32,
    thread_n: usize,
    optimistic: bool,
    flush_n: usize,
    read_percent: u32,
) {
    let test_dir = "./test_dir/pf_test1";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_CRABBING_CONFIG;
    config.optimistic = optimistic;
    let btree = Arc::new(BTree::<u32, u32>::new(test_dir, 0, config));
    let mut handlers = Vec::new();
    let guard: Arc<RwLock<Option<FlushHandler>>> = Arc::new(RwLock::new(None));
    let cnt1 = Arc::new(AtomicU32::new(0));
    for _i in 0..thread_n {
        let btree = btree.clone();
        let cnt = cnt1.clone();
        let guard = guard.clone();
        let h = spawn(move || loop {
            let op = random::<u32>() % 100;
            let key = random::<u32>() % max_key + 1;
            if op < read_percent {
                let mut val = 0;
                btree.get(&key, &mut val);
            } else {
                btree.insert(&key, &0);
            }
            let old_n = cnt.fetch_add(1, Ordering::SeqCst);
            if flush_n > 0 && old_n % flush_n as u32 == 0 {
                let mut guard_g = guard.write().unwrap();
                // if let Some(g) = guard_g.as_mut() {
                //     g.join();
                // }
                *guard_g = Some(btree.flush());
                btree.update_cache();
            }
            if old_n >= test_n as u32 {
                if let Some(g) = guard.write().unwrap().as_mut() {
                    g.join();
                }
                break;
            }
        });
        handlers.push(h);
    }
    for h in handlers {
        h.join().unwrap();
    }
}

fn palm_random_insert(
    test_n: usize,
    max_key: u32,
    thread_n: usize,
    batch_n: usize,
    flush_n: usize,
    read_percent: u32,
) {
    assert!(flush_n == 0 || flush_n % batch_n == 0);
    let test_dir = "./test_dir/pf_test2";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_PALM_CONFIG;
    config.thread_n = thread_n;
    let palm = PALMTree::<u32, u32>::new(test_dir, 0, config);
    let mut vals = vec![0; batch_n];
    let mut cnt = 1;
    // let handler = None;
    while cnt <= test_n {
        let mut cmds = Vec::new();
        for val in vals.iter_mut() {
            let key = random::<u32>() % max_key as u32 + 1;
            if random::<u32>() % 100 < read_percent {
                cmds.push(PALMCmd::Get {
                    key,
                    dest: NonNull::new(val).unwrap(),
                });
            } else {
                cmds.push(PALMCmd::Insert {
                    key,
                    val: NonNull::new(val).unwrap(),
                });
                // if cnt % 10000 == 0 {
                //     println!("{}", cnt);
                // }
            }
            cnt += 1;
        }
        let _results = palm.op(cmds);
        if flush_n > 0 && cnt as usize % flush_n == 0 {
            // if let Some(h) = handler {
            //     h.join().unwrap();
            // }
            // handler = Some(palm.flush().unwrap());
        }
    }
}

fn p2p_random_insert(
    test_n: usize,
    max_key: u32,
    thread_n: usize,
    batch_n: usize,
    flush: bool,
    read_percent: u32,
) {
    let test_dir = "./test_dir/pf_test3";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_P2P_PALM_CONFIG;
    config.thread_n = thread_n;
    let palm = P2PPALMTree::<u32, u32>::new(test_dir, 0, config);
    let mut vals = vec![0; batch_n];
    let mut cnt = 1;
    let mut handler: Option<FlushHandler> = None;
    while cnt <= test_n {
        let mut cmds = Vec::new();
        for val in vals.iter_mut() {
            let key = random::<u32>() % max_key as u32 + 1;
            if random::<u32>() % 100 < read_percent {
                cmds.push(PALMReq::Get {
                    key,
                    dest: NonNull::new(val).unwrap(),
                });
            } else {
                cmds.push(PALMReq::Insert {
                    key,
                    val: NonNull::new(val).unwrap(),
                });
                // if cnt % 10000 == 0 {
                //     println!("{}", cnt);
                // }
            }
            cnt += 1;
        }
        let _results = palm.op(cmds);
        if flush {
            // if let Some(mut h) = handler {
            //     h.join();
            // }
            handler = Some(palm.flush());
        }
    }
    if let Some(mut h) = handler {
        h.join();
    }
}

fn crabbing_random_insert_remove(
    test_n: usize,
    max_key: u32,
    thread_n: usize,
    optimistic: bool,
    flush_n: usize,
    read_percent: u32,
    del_percent: u32,
) {
    let test_dir = "./test_dir/pf_test4";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_CRABBING_CONFIG;
    config.optimistic = optimistic;
    let btree = Arc::new(BTree::<u32, u32>::new(test_dir, 0, config));
    let mut handlers = Vec::new();
    let cnt1 = Arc::new(AtomicU32::new(0));
    for _i in 0..thread_n {
        let btree = btree.clone();
        let cnt = cnt1.clone();
        let mut guard: Option<FlushHandler> = None;
        let h = spawn(move || loop {
            let op = random::<u32>() % 100;
            let key = random::<u32>() % max_key + 1;
            if op < read_percent {
                let mut val = 0;
                btree.get(&key, &mut val);
            } else if op < del_percent + read_percent {
                btree.remove(&key);
            } else {
                btree.insert(&key, &0);
            }
            let old_n = cnt.fetch_add(1, Ordering::SeqCst);
            if flush_n > 0 && old_n % flush_n as u32 == 0 {
                if let Some(mut g) = guard {
                    g.join();
                }
                guard = Some(btree.flush());
                btree.update_cache();
            }
            if old_n >= test_n as u32 {
                break;
            }
        });
        handlers.push(h);
    }
    for h in handlers {
        h.join().unwrap();
    }
}

fn crabbing_seq_insert(
    test_n: usize,
    thread_n: usize,
    optimistic: bool,
    flush_n: usize,
    read_percent: u32,
) {
    const VAL_SIZE: usize = 1;
    let test_dir = "./test_dir/pf_test5".to_string();
    if Path::exists(Path::new(&test_dir)) {
        remove_dir_all(&test_dir).unwrap();
    }
    let mut config = DEFAULT_CRABBING_CONFIG;
    config.optimistic = optimistic;
    let btree = Arc::new(BTree::<u32, Val<VAL_SIZE>>::new(&test_dir, 0, config));
    let key1 = Arc::new(AtomicU32::new(1));
    let mut handlers = Vec::new();
    for _i in 0..thread_n {
        let btree = btree.clone();
        let key = key1.clone();
        let mut guard: Option<FlushHandler> = None;
        let h = spawn(move || loop {
            let op = random::<u32>() % 100;
            let mut val = Val::default();
            if op < read_percent {
                let k = random::<u32>() % test_n as u32 + 1;
                btree.get(&k, &mut val);
                continue;
            }
            let k = key.fetch_add(1, Ordering::SeqCst);
            if k > test_n as u32 + 1 {
                if let Some(mut g) = guard {
                    g.join();
                }
                break;
            }
            btree.insert(&k, &val);
            if flush_n > 0 && k % flush_n as u32 == 0 {
                if let Some(mut g) = guard {
                    g.join();
                }
                guard = Some(btree.flush());
                btree.update_cache();
            }
        });
        handlers.push(h);
    }
    for h in handlers {
        h.join().unwrap();
    }
}

#[derive(Clone)]
struct Val<const N: usize> {
    val: [u32; N],
}

impl<const N: usize> Default for Val<N> {
    fn default() -> Self {
        Self { val: [0; N] }
    }
}

fn palm_seq_insert(
    test_n: usize,
    thread_n: usize,
    batch_n: usize,
    flush_n: usize,
    read_percent: u32,
) {
    const VAL_SIZE: usize = 1;
    let test_dir = "./test_dir/pf_test6";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_PALM_CONFIG;
    config.thread_n = thread_n;
    let mut palm = PALMTree::<u32, Val<VAL_SIZE>>::new(test_dir, 0, config);
    let mut vals = vec![Val::<VAL_SIZE>::default(); batch_n];
    let mut key = 0;
    let mut guard: Option<FlushHandler> = None;
    while key < test_n as u32 {
        let mut reqs = Vec::new();
        for val in vals.iter_mut() {
            if random::<u32>() % 100 < read_percent {
                let key = random();
                reqs.push(PALMCmd::Get {
                    key,
                    dest: NonNull::new(val).unwrap(),
                });
            } else {
                reqs.push(PALMCmd::Insert {
                    key,
                    val: NonNull::new(val).unwrap(),
                });
                key += 1;
                // if key % 10000 == 0 {
                //     println!("{}", key);
                // }
            }
        }
        let _results = palm.op(reqs);
        if flush_n > 0 && key as usize % flush_n == 0 {
            if let Some(mut g) = guard {
                g.join();
            }
            guard = Some(palm.flush());
        }
    }
    if let Some(mut g) = guard {
        g.join();
    }
}

fn p2p_seq_insert(test_n: usize, thread_n: usize, batch_n: usize, flush: bool, read_percent: u32) {
    const VAL_SIZE: usize = 1;
    let test_dir = "./test_dir/pf_test7";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_P2P_PALM_CONFIG;
    config.thread_n = thread_n;
    let palm = P2PPALMTree::<u32, Val<VAL_SIZE>>::new(test_dir, 0, config);
    let mut vals = vec![Val::default(); batch_n];
    let mut key = 0;
    let mut guard: Option<FlushHandler> = None;
    while key < test_n as u32 {
        let mut reqs = Vec::new();
        for val in vals.iter_mut() {
            if random::<u32>() % 100 < read_percent {
                let key = random();
                reqs.push(PALMReq::Get {
                    key,
                    dest: NonNull::new(val).unwrap(),
                });
            } else {
                reqs.push(PALMReq::Insert {
                    key,
                    val: NonNull::new(val).unwrap(),
                });
                key += 1;
                // if key % 10000 == 0 {
                //     println!("{}", key);
                // }
            }
        }
        let _results = palm.op(reqs);
        if flush {
            if let Some(mut g) = guard {
                g.join();
            }
            guard = Some(palm.flush());
        }
    }
    if let Some(mut g) = guard {
        g.join();
    }
}
