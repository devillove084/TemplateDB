pub mod bloom_filter_cache;
pub mod lru_cache;
pub mod new_lru_cache;
pub mod sharded_cache;
pub mod table_cache;

/// A `Cache` is an interface that maps keys to values.
/// It has internal synchronization and may be safely accessed concurrently from
/// multiple threads.
/// It may automatically evict entries to make room for new entries.
/// Values have a specified charge against the cache capacity.
/// For example, a cache where the values are variable length strings, may use the
/// length of the string as the charge for the string.
///
/// A builtin cache implementation with a least-recently-used eviction
/// policy is provided.
/// Clients may use their own implementations if
/// they want something more sophisticated (like scan-resistance, a
/// custom eviction policy, variable cache sizing, etc.)
pub trait CacheSync<K, V>: Sync + Send
where
    K: Sync + Send,
    V: Sync + Send + Clone,
{
    /// Insert a mapping from key->value into the cache and assign it
    /// the specified charge against the total cache capacity.
    fn insert(&self, key: K, value: V, charge: usize) -> Option<V>;

    /// If the cache has no mapping for `key`, returns `None`.
    fn get(&self, key: &K) -> Option<V>;

    /// If the cache contains entry for key, erase it.
    fn erase(&self, key: &K);

    /// Return an estimate of the combined charges of all elements stored in the
    /// cache.
    fn total_charge(&self) -> usize;
}

#[async_trait::async_trait]
pub trait CacheAsync<K, V>: Sync + Send
where
    K: Sync + Send,
    V: Sync + Send + Clone,
{
    /// Insert a mapping from key->value into the cache and assign it
    /// the specified charge against the total cache capacity.
    async fn insert(&self, key: K, value: V) -> Option<V>;

    /// If the cache has no mapping for `key`, returns `None`.
    async fn get(&self, key: &K) -> Option<V>;

    /// If the cache contains entry for key, erase it.
    async fn erase(&self, key: &K) -> Option<V>;

    /// Return an estimate of the combined charges of all elements stored in the
    /// cache.
    async fn total_charge(&self) -> usize;
}

/// `FilterPolicy` is an algorithm for probabilistically encoding a set of keys.
/// The canonical implementation is a Bloom filter.
///
/// Every `FilterPolicy` has a name. This names the algorithm itself, not any one
/// particular instance. Aspects specific to a particular instance, such as the
/// set of keys or any other parameters, will be encoded in the byte filter
/// returned by `new_filter_writer`.
///
/// The name may be written to files on disk, along with the filter data. To use
/// these filters, the `FilterPolicy` name at the time of writing must equal the
/// name at the time of reading. If they do not match, the filters will be
/// ignored, which will not affect correctness but may affect performance.
pub trait FilterPolicy: Send + Sync {
    /// Return the name of this policy.  Note that if the filter encoding
    /// changes in an incompatible way, the name returned by this method
    /// must be changed.  Otherwise, old incompatible filters may be
    /// passed to methods of this type.
    fn name(&self) -> &str;

    /// `MayContain` returns whether the encoded filter may contain given key.
    /// False positives are possible, where it returns true for keys not in the
    /// original set.
    fn may_contain(&self, filter: &[u8], key: &[u8]) -> bool;

    /// Creates a filter based on given keys
    // TODO: use another type instead of &[Vec<u8>]
    fn create_filter(&self, keys: &[Vec<u8>]) -> Vec<u8>;
}

#[cfg(test)]
mod tests {
    use std::{
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        thread,
    };

    use tests::sharded_cache::ShardedCache;

    // use lru::*;
    use super::*;
    use crate::cache::lru_cache::LRUCache;

    fn new_test_lru_shards(n: usize) -> Vec<LRUCache<String, String>> {
        (0..n).fold(vec![], |mut acc, _| {
            acc.push(LRUCache::new(1 << 20));
            acc
        })
    }

    #[test]
    fn test_concurrent_insert() {
        let cache = Arc::new(ShardedCache::new(new_test_lru_shards(8)));
        let n = 4; // use 4 thread
        let repeated = 10;
        let mut hs = vec![];
        let kv: Arc<Mutex<Vec<(String, String)>>> = Arc::new(Mutex::new(vec![]));
        let total_size = Arc::new(AtomicU64::new(0));
        for i in 0..n {
            let cache = cache.clone();
            let kv = kv.clone();
            let total_size = total_size.clone();
            let h = thread::spawn(move || {
                for x in 1..=repeated {
                    let k = i.to_string().repeat(x);
                    let v = k.clone();
                    {
                        let mut kv = kv.lock().unwrap();
                        (*kv).push((k.clone(), v.clone()));
                    }
                    total_size.fetch_add(x as u64, Ordering::SeqCst);
                    assert_eq!(cache.insert(k, v, x), None);
                }
            });
            hs.push(h);
        }
        for h in hs {
            h.join().unwrap();
        }
        assert_eq!(
            total_size.load(Ordering::Relaxed) as usize,
            cache.total_charge()
        );
        for (k, v) in kv.lock().unwrap().clone() {
            assert_eq!(cache.get(&k), Some(v));
        }
    }
}
