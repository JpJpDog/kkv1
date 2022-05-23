use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::cmp::min;
use std::collections::HashSet;
use std::fs::remove_dir_all;
use std::path::Path;
use std::ptr::NonNull;

use super::palm::{PALMTree, DEFAULT_PALM_CONFIG};
use super::palm_msg::PALMResult;
use crate::page_manager::p_manager::FHandler;
use crate::page_manager::FlushHandler;
use crate::palm::palm_msg::PALMCmd;

#[test]
fn test_random_insert() {
    let test_dir = "./test_dir/palm1";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_PALM_CONFIG;
    config.node_cap = 10;
    let mut palm = PALMTree::<u32, u32>::new(test_dir, 0, config);
    let mut rng = StdRng::seed_from_u64(0);
    let batch_n = 100;
    let test_n = 10000;
    let max_key = 100000;
    let mut vals = vec![0; batch_n];
    let mut cnt = 0;
    let mut handler: Option<FlushHandler> = None;
    while cnt < test_n {
        let mut cmds = Vec::new();
        for val in vals.iter_mut() {
            let key = rng.gen::<u32>() % max_key + 1;
            if rng.gen::<u32>() % 100 < 80 {
                cmds.push(PALMCmd::Get {
                    key,
                    dest: NonNull::new(val).unwrap(),
                });
            } else {
                cmds.push(PALMCmd::Insert {
                    key,
                    val: NonNull::new(val).unwrap(),
                });
                cnt += 1;
                if cnt % 10000 == 0 {
                    println!("{}", cnt);
                }
            }
        }
        let _results = palm.op(cmds);
        if let Some(mut h) = handler {
            h.join();
        }
        handler = Some(palm.flush());
    }
    if let Some(mut h) = handler {
        h.join();
    }
}

#[test]
fn test_persistence() {
    let test_dir = "./test_dir/palm2";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }
    let mut config = DEFAULT_PALM_CONFIG;
    config.node_cap = 10;
    let mut palm = PALMTree::<u32, u32>::new(test_dir, 0, config);
    let mut key_set = HashSet::new();
    let mut rng = StdRng::seed_from_u64(0);
    let test_n = 10000;
    while key_set.len() < test_n {
        key_set.insert(rng.gen::<u32>());
    }
    let batch_n = 1000;
    let mut val = vec![0; batch_n];
    let mut keys = Vec::new();
    key_set.into_iter().for_each(|e| keys.push(e));
    let mut off = 0;
    while off < keys.len() {
        let mut cmds = Vec::new();
        for i in off..min(off + batch_n, keys.len()) {
            cmds.push(PALMCmd::Insert {
                key: keys[i],
                val: NonNull::new(&mut val[i - off]).unwrap(),
            });
        }
        palm.op(cmds);
        off = min(off + batch_n, keys.len());
    }
    palm.flush().join();
    let palm = PALMTree::<u32, u32>::load(test_dir, DEFAULT_PALM_CONFIG);
    off = 0;
    while off < keys.len() {
        let mut cmds = Vec::new();
        for i in off..min(off + batch_n, keys.len()) {
            cmds.push(PALMCmd::Get {
                key: keys[i],
                dest: NonNull::new(&mut val[i - off]).unwrap(),
            });
        }
        let res = palm.op(cmds);
        for r in res {
            match r {
                PALMResult::Insert => assert!(false),
                PALMResult::Get(exist) => assert!(exist),
            }
        }
        off = min(off + batch_n, keys.len());
    }
}
