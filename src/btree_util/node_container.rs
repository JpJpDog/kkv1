use std::{
    cell::UnsafeCell,
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard},
};

use crate::btree_node::Node;

pub trait NodeContainer<K: PartialOrd + Clone, V: Clone>: Clone {
    type NodeReadGuard<'a>: Deref<Target = Node<K, V>>
    where
        Self: 'a;

    type NodeWriteGuard<'a>: DerefMut<Target = Node<K, V>>
    where
        Self: 'a;

    fn new(node: Node<K, V>) -> Self;

    fn read(&self) -> Self::NodeReadGuard<'_>;

    fn write(&mut self) -> Self::NodeWriteGuard<'_>;
}

#[derive(Clone)]
pub struct LockNodeContainer<K: PartialOrd + Clone, V: Clone> {
    inner: Arc<RwLock<Node<K, V>>>,
}

impl<K: PartialOrd + Clone, V: Clone> NodeContainer<K, V> for LockNodeContainer<K, V> {
    type NodeReadGuard<'a> = RwLockReadGuard<'a, Node<K, V>> where Self:'a;

    type NodeWriteGuard<'a> = RwLockWriteGuard<'a, Node<K, V>> where Self:'a;

    #[inline]
    fn new(node: Node<K, V>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(node)),
        }
    }

    #[inline]
    fn read(&self) -> Self::NodeReadGuard<'_> {
        self.inner.read().unwrap()
    }

    #[inline]
    fn write(&mut self) -> Self::NodeWriteGuard<'_> {
        self.inner.write().unwrap()
    }
}

#[derive(Clone)]
pub struct RawNodeContainer<K: PartialOrd + Clone, V: Clone> {
    inner: Arc<UnsafeCell<Node<K, V>>>,
}

impl<K: PartialOrd + Clone, V: Clone> NodeContainer<K, V> for RawNodeContainer<K, V> {
    type NodeReadGuard<'a> = &'a Node<K,V>
    where
        Self: 'a;

    type NodeWriteGuard<'a> = &'a mut Node<K,V>
    where
        Self: 'a;

    fn new(node: Node<K, V>) -> Self {
        Self {
            inner: Arc::new(UnsafeCell::new(node)),
        }
    }

    fn read(&self) -> Self::NodeReadGuard<'_> {
        unsafe { &*self.inner.get() }
    }

    fn write(&mut self) -> Self::NodeWriteGuard<'_> {
        unsafe { &mut *self.inner.get() }
    }
}

unsafe impl<K: PartialOrd + Clone, V: Clone> Send for RawNodeContainer<K, V> {}

unsafe impl<K: PartialOrd + Clone, V: Clone> Sync for RawNodeContainer<K, V> {}
