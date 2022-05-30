use std::sync::Arc;

use memmap2::MmapMut;

use super::page::{PageId, PageInner, PageRef};

pub trait FHandler {
    fn join(&mut self) -> bool;
}

pub trait PManager {
    type FlushHandler: FHandler;

    fn new(root_dir: &str) -> Self;

    unsafe fn load(root_dir: &str) -> Self;

    fn new_page<T: PageRef>(arc_self: Arc<Self>, page_id: PageId) -> PageInner<T, Self>
    where
        Self: Sized;

    unsafe fn load_page<T: PageRef>(arc_self: Arc<Self>, page_id: PageId) -> PageInner<T, Self>
    where
        Self: Sized;

    fn flush(&self) -> Self::FlushHandler;

    fn write_start(&self, page_id: PageId);

    fn write_ok(&self, page_id: PageId, mmap: Arc<MmapMut>);
}
