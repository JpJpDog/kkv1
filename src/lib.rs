#![feature(int_roundings)]
#![feature(slice_as_chunks)]
#![feature(new_uninit)]
#![feature(async_closure)]
#![feature(generic_associated_types)]
#![feature(unzip_option)]

extern crate core;

mod btree_node;
mod btree_util;
mod crabbing;
mod page_manager;
mod page_system;
mod palm;
mod p2p_palm;
mod util;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
