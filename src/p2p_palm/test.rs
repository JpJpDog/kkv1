use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::{collections::HashSet, fs::remove_dir_all, path::Path, ptr::NonNull};

use crate::crabbing::{FlushHandler, FHandler};
use crate::p2p_palm::palm_msg::{PALMReq, PALMRsp};

use super::{P2PPALMTree, DEFAULT_P2P_PALM_CONFIG};

#[test]
fn test1() {
    let test_dir = "./test_dir/p2p_palm1";
    if Path::exists(Path::new(test_dir)) {
        remove_dir_all(test_dir).unwrap();
    }

    let mut config = DEFAULT_P2P_PALM_CONFIG;
    config.node_cap = 5;
    config.thread_n = 20;
    let palm = P2PPALMTree::<u32, u32>::new(test_dir, 0, config);
    let mut rng = StdRng::seed_from_u64(0);
    let batch_n = 100;
    let test_n = 10000;
    let max_key = 100000;
    let mut vals = vec![0; batch_n];
    let mut cnt = 0;
    let mut handler: Option<FlushHandler> = None;
    let mut key_set = HashSet::new();
    while cnt < test_n {
        let mut reqs = Vec::new();
        for val in vals.iter_mut() {
            let key = rng.gen::<u32>() % max_key + 1;
            if rng.gen::<u32>() % 100 < 20 {
                reqs.push(PALMReq::Get {
                    key,
                    dest: NonNull::new(val).unwrap(),
                });
            } else {
                reqs.push(PALMReq::Insert {
                    key,
                    val: NonNull::new(val).unwrap(),
                });
                cnt += 1;
                // print!("{} ",key);
            }
        }
        // println!();

        let results = palm.op(reqs.clone());
        for (i, r) in results.into_iter().enumerate() {
            let key = reqs[i].key();
            match r {
                PALMRsp::Insert => {
                    key_set.insert(*key);
                }
                PALMRsp::Get(exist) => {
                    assert_eq!(key_set.get(&key).is_some(), exist);
                }
                PALMRsp::Remove => todo!(),
            }
        }
        if let Some(mut h) = handler {
            h.join();
        }
        handler = Some(palm.flush());
        // palm.store.dump();
        // palm.store.check();
    }

    if let Some(mut h) = handler {
        h.join();
    }
}
