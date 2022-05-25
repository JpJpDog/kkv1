use self::{
    page::PageInner,
    page_manager_2::{FlushHandler1, PageManager1},
};

pub mod p_manager;
pub mod page;
mod page_manager_2;
mod page_manager_1;
mod persistencer;

#[cfg(test)]
mod test;

pub type Page<T> = PageInner<T, PageManager1>;
pub type FlushHandler = FlushHandler1;
pub type PageManager = PageManager1;
