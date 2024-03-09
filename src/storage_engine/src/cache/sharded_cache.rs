use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
    marker::PhantomData,
    sync::Arc,
};

use super::Cache;

/// A sharded cache container by key hash
pub struct ShardedCache<K, V, C>
where
    C: Cache<K, V>,
    K: Sync + Send,
    V: Sync + Send + Clone,
{
    shards: Arc<Vec<C>>,
    _k: PhantomData<K>,
    _v: PhantomData<V>,
}

impl<K, V, C> ShardedCache<K, V, C>
where
    C: Cache<K, V>,
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + Clone,
{
    /// Create a new `ShardedCache` with given shards
    #[must_use] pub fn new(shards: Vec<C>) -> Self {
        Self {
            shards: Arc::new(shards),
            _k: PhantomData,
            _v: PhantomData,
        }
    }

    fn find_shard(&self, k: &K) -> usize {
        let mut s = DefaultHasher::new();
        let len = self.shards.len();
        k.hash(&mut s);
        usize::try_from(s.finish()).expect("truncate error") % len
    }
}

impl<K, V, C> Cache<K, V> for ShardedCache<K, V, C>
where
    C: Cache<K, V>,
    K: Sync + Send + Hash + Eq,
    V: Sync + Send + Clone,
{
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V> {
        let idx = self.find_shard(&key);
        self.shards[idx].insert(key, value, charge)
    }

    fn get(&self, key: &K) -> Option<V> {
        let idx = self.find_shard(key);
        self.shards[idx].get(key)
    }

    fn erase(&self, key: &K) {
        let idx = self.find_shard(key);
        self.shards[idx].erase(key);
    }

    fn total_charge(&self) -> usize {
        self.shards.iter().fold(0, |acc, s| acc + s.total_charge())
    }
}
