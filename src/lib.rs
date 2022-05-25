#![feature(int_roundings)]
#![feature(slice_as_chunks)]
#![feature(new_uninit)]
#![feature(async_closure)]
#![feature(generic_associated_types)]

extern crate core;

mod btree_node;
mod btree_util;
mod crabbing;
mod page_manager;
mod page_system;
// mod palm;
mod util;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        let result = 2 + 2;
        assert_eq!(result, 4);
    }
}
