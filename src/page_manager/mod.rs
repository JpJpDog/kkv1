use self::{
    page::PageInner,
    page_manager_1::{FlushHandler1, PageManager1},
    page_manager_2::{FlushHandler2, PageManager2},
};

pub mod p_manager;
pub mod page;
mod page_manager_1;
mod page_manager_2;
mod persistencer;

#[cfg(test)]
mod test;

pub type Page<T> = PageInner<T, PageManager2>;
pub type FlushHandler = FlushHandler2;
pub type PageManager = PageManager2;
