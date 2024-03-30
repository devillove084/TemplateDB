use std::{fmt::Debug, hash::Hash};

use moka::future::Cache;

use super::CacheAsync;

pub struct AsyncLRUCache<K: Eq + Hash, V> {
    inner: Cache<K, V>,
}

impl<K, V> AsyncLRUCache<K, V>
where
    K: Send + Sync + Eq + Hash + 'static,
    V: Send + Sync + Clone + 'static,
{
    pub fn new(cap: u64) -> Self {
        Self {
            inner: Cache::new(cap),
            // usage: AtomicUsize::new(0),
        }
    }
}

unsafe impl<K: Send + Eq + Hash + Copy, V: Send + Clone> Send for AsyncLRUCache<K, V> {}
unsafe impl<K: Sync + Eq + Hash + Copy, V: Sync + Clone> Sync for AsyncLRUCache<K, V> {}

#[async_trait::async_trait]
impl<K, V> CacheAsync<K, V> for AsyncLRUCache<K, V>
where
    K: Eq + Hash + Copy + Send + Sync + Debug + 'static,
    V: Send + Sync + Clone + Sync + Send + 'static,
{
    async fn insert(&self, key: K, value: V) -> Option<V> {
        self.inner.insert(key, value.clone()).await;
        Some(value)
    }

    async fn get(&self, key: &K) -> Option<V> {
        self.inner.get(key).await
    }

    async fn erase(&self, key: &K) -> Option<V> {
        self.inner.remove(key).await
    }

    async fn total_charge(&self) -> usize {
        self.inner.entry_count() as usize
    }
}

impl<K, V> AsyncLRUCache<K, V>
where
    K: Eq + Hash + Copy + Send + Sync + Debug + 'static,
    V: Send + Sync + Clone + Sync + Send + 'static,
{
    pub async fn insert_o(&self, key: K, value: V) -> Option<V> {
        self.insert(key, value.clone()).await;
        None
    }

    pub async fn get_o(&self, key: &K) -> Option<V> {
        self.inner.get(key).await
    }

    pub async fn erase_o(&self, key: &K) -> Option<V> {
        self.inner.remove(key).await
    }

    pub async fn total_charge_o(&self) -> usize {
        self.inner.entry_count() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::AsyncLRUCache;

    const CACHE_SIZE: u64 = 100;

    #[tokio::test]
    async fn test_hit_and_miss() {
        let cache = AsyncLRUCache::new(CACHE_SIZE);
        assert_eq!(None, cache.get_o(&100).await);
        cache.insert_o(100, 101).await;
        assert_eq!(Some(101), cache.get_o(&100).await);
        assert_eq!(None, cache.get_o(&200).await);
        assert_eq!(None, cache.get_o(&300).await);

        cache.insert_o(200, 201).await;
        assert_eq!(Some(101), cache.get_o(&100).await);
        assert_eq!(Some(201), cache.get_o(&200).await);
        assert_eq!(None, cache.get_o(&300).await);

        cache.insert_o(100, 102).await;
        assert_eq!(Some(102), cache.get_o(&100).await);
        assert_eq!(Some(201), cache.get_o(&200).await);
        assert_eq!(None, cache.get_o(&300).await);
    }

    // #[test]
    // fn test_erase() {
    //     let cache = CacheTest::new(CACHE_SIZE);
    //     cache.erase(200);

    //     cache.insert(100, 101);
    //     cache.insert(200, 201);
    //     cache.erase(100);

    //     assert_eq!(None, cache.get(100));
    //     assert_eq!(Some(201), cache.get(200));

    //     cache.erase(100);
    //     assert_eq!(None, cache.get(100));
    //     assert_eq!(Some(201), cache.get(200));
    // }

    // #[test]
    // fn test_entries_are_pinned() {
    //     let cache = CacheTest::new(CACHE_SIZE);
    //     cache.insert(100, 101);
    //     let v1 = cache.assert_get(100, 101);
    //     assert_eq!(v1, 101);
    //     cache.insert(100, 102);
    //     let v2 = cache.assert_get(100, 102);
    //     assert_eq!(v1, 101);
    //     assert_eq!(v2, 102);

    //     cache.erase(100);
    //     assert_eq!(v1, 101);
    //     assert_eq!(v2, 102);
    //     assert_eq!(None, cache.get(100));
    // }

    // #[test]
    // fn test_eviction_policy() {
    //     let cache = CacheTest::new(CACHE_SIZE);
    //     cache.insert(100, 101);
    //     cache.insert(200, 201);
    //     cache.insert(300, 301);

    //     // frequently used entry must be kept around
    //     for i in 0..(CACHE_SIZE + 100) as u32 {
    //         cache.insert(1000 + i, 2000 + i);
    //         assert_eq!(Some(2000 + i), cache.get(1000 + i));
    //         assert_eq!(Some(101), cache.get(100));
    //     }
    //     assert_eq!(Some(101), cache.get(100));
    //     assert_eq!(None, cache.get(200));
    //     assert_eq!(None, cache.get(300));
    // }

    // #[test]
    // fn test_use_exceeds_cache_size() {
    //     let cache = CacheTest::new(CACHE_SIZE);
    //     let extra = 100;
    //     let total = CACHE_SIZE + extra;
    //     // overfill the cache, keeping handles on all inserted entries
    //     for i in 0..total as u32 {
    //         cache.insert(1000 + i, 2000 + i)
    //     }

    //     // check that all the entries can be found in the cache
    //     for i in 0..total as u32 {
    //         if i < extra as u32 {
    //             assert_eq!(None, cache.get(1000 + i))
    //         } else {
    //             assert_eq!(Some(2000 + i), cache.get(1000 + i))
    //         }
    //     }
    // }

    // #[test]
    // fn test_heavy_entries() {
    //     let cache = CacheTest::new(CACHE_SIZE);
    //     let light = 1;
    //     let heavy = 10;
    //     let mut added = 0;
    //     let mut index = 0;
    //     while added < 2 * CACHE_SIZE {
    //         let weight = if index & 1 == 0 { light } else { heavy };
    //         cache.insert_with_charge(index, 1000 + index, weight);
    //         added += weight;
    //         index += 1;
    //     }
    //     let mut cache_weight = 0;
    //     for i in 0..index {
    //         let weight = if index & 1 == 0 { light } else { heavy };
    //         if let Some(val) = cache.get(i) {
    //             cache_weight += weight;
    //             assert_eq!(1000 + i, val);
    //         }
    //     }
    //     assert!(cache_weight < CACHE_SIZE);
    // }

    // #[test]
    // fn test_zero_size_cache() {
    //     let cache = CacheTest::new(0);
    //     cache.insert(100, 101);
    //     assert_eq!(None, cache.get(100));
    // }
}
