use std::collections::BTreeMap;

use tokio::sync::Mutex;

use crate::codec::{self, Timestamp, Value};

pub struct Memtable {
    inner: Mutex<Inner>,
}

struct Inner {
    map: BTreeMap<Vec<u8>, Value>,
    size: usize,
    last_ts: Timestamp,
}

impl Memtable {
    pub fn new(ts: Timestamp) -> Self {
        let inner = Inner {
            map: BTreeMap::new(),
            size: 0,
            last_ts: ts,
        };
        Memtable {
            inner: Mutex::new(inner),
        }
    }

    pub async fn get(&self, key: &[u8]) -> Option<Value> {
        let inner = self.inner.lock().await;
        inner.map.get(key).cloned()
    }

    pub async fn iter(&self) -> BTreeMap<Vec<u8>, Value> {
        let inner = self.inner.lock().await;
        inner.map.clone()
    }

    pub async fn insert(&self, ts: Timestamp, key: Vec<u8>, value: Value) {
        let mut inner = self.inner.lock().await;
        inner.size += codec::record_size(&key, &value);
        assert!(ts > inner.last_ts);
        inner.last_ts = ts;
        inner.map.insert(key, value);
    }

    pub async fn approximate_size(&self) -> usize {
        self.inner.lock().await.size
    }

    pub async fn last_update_timestamp(&self) -> Timestamp {
        self.inner.lock().await.last_ts
    }
}
