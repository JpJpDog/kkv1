#[derive(Debug, Clone)]
pub struct KV<K, V> {
    pub key: K,
    pub val: V,
}

impl<K, V> KV<K, V> {
    #[inline]
    pub fn new(key: K, val: V) -> Self {
        Self { key, val }
    }
}

impl<K: Default, V: Default> Default for KV<K, V> {
    fn default() -> Self {
        Self {
            key: Default::default(),
            val: Default::default(),
        }
    }
}
