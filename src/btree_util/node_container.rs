use std::{ops::{Deref, DerefMut}, sync::{RwLock, Arc, RwLockReadGuard, RwLockWriteGuard}, cell::UnsafeCell};

use crate::btree_node::btree_node::Node;

pub trait NodeContainer<K: PartialOrd + Clone, V: Clone>: Clone {
    type NodeReadGuard<'a>: Deref<Target = Node<K, V>>
    where
        Self: 'a;

    type NodeWriteGuard<'a>: DerefMut<Target = Node<K, V>>
    where
        Self: 'a;

    fn new(node: Node<K, V>) -> Self;

    fn read<'a>(&'a self) -> Self::NodeReadGuard<'a>;

    fn write<'a>(&'a mut self) -> Self::NodeWriteGuard<'a>;
}

#[derive(Clone)]
struct LockNodeContainer<K: PartialOrd + Clone, V: Clone> {
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
    fn read<'a>(&'a self) -> Self::NodeReadGuard<'a> {
        self.inner.read().unwrap()
    }

    #[inline]
    fn write<'a>(&'a mut self) -> Self::NodeWriteGuard<'a> {
        self.inner.write().unwrap()
    }
}

#[derive(Clone)]
struct RawNodeContainer<K: PartialOrd + Clone, V: Clone> {
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

    fn read<'a>(&'a self) -> Self::NodeReadGuard<'a> {
        unsafe { &*self.inner.get() }
    }

    fn write<'a>(&'a mut self) -> Self::NodeWriteGuard<'a> {
        unsafe { &mut *self.inner.get() }
    }
}