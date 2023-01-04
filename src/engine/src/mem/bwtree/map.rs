// Copyright 2022 The template Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::{path::PathBuf, sync::Arc};

use crossbeam::epoch::pin;

use super::{
    data::{Key, Value},
    env::Env,
    error::Result,
    stats::Stats,
    tree::{Iter, Tree},
    util::{Options, Sequencer},
};

#[derive(Clone)]
pub struct Map<E: Env> {
    raw: RawMap<E>,
    lsn: Arc<Sequencer>,
}

impl<E: Env> Map<E> {
    pub async fn open(env: E, root: PathBuf, opts: Options) -> Result<Self> {
        let raw = RawMap::open(env, root, opts).await?;
        raw.set_min_lsn(u64::MAX);
        let lsn = Arc::new(Sequencer::new(0));
        Ok(Self { raw, lsn })
    }

    /// Gets the value corresponding to `key` and calls `func` with it.
    pub fn get<F>(&self, key: &[u8], func: F) -> Result<()>
    where
        F: FnMut(Option<&[u8]>),
    {
        self.raw.get(key, u64::MAX, func)
    }

    pub fn iter(&self) -> Iter<E> {
        self.raw.iter()
    }

    /// Puts the key-value pair into this map.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let lsn = self.lsn.inc();
        self.raw.put(key, lsn, value)
    }

    /// Deletes the key-value pair from this map.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let lsn = self.lsn.inc();
        self.raw.delete(key, lsn)
    }

    /// Returns statistics of this map.
    pub fn stats(&self) -> Stats {
        self.raw.stats()
    }
}

#[derive(Clone)]
pub struct RawMap<E: Env> {
    tree: Arc<Tree<E>>,
}

impl<E: Env> RawMap<E> {
    pub async fn open(env: E, root: PathBuf, opts: Options) -> Result<Self> {
        let tree = Tree::open(env, root, opts).await?;
        Ok(Self {
            tree: Arc::new(tree),
        })
    }

    /// Finds the value corresponding to `key` and calls `func` with it.
    pub fn get<F>(&self, key: &[u8], lsn: u64, f: F) -> Result<()>
    where
        F: FnMut(Option<&[u8]>),
    {
        let guard = &pin();
        let key = Key::new(key, lsn);
        self.tree.get(key, guard).map(f)
    }

    pub fn iter(&self) -> Iter<E> {
        Iter::new(self.tree.clone())
    }

    /// Puts the key-value pair into this map.
    pub fn put(&self, key: &[u8], lsn: u64, value: &[u8]) -> Result<()> {
        let guard = &pin();
        let key = Key::new(key, lsn);
        let value = Value::Put(value);
        self.tree.insert(key, value, guard)
    }

    /// Deletes the key-value pair into this map.
    pub fn delete(&self, key: &[u8], lsn: u64) -> Result<()> {
        let guard = &pin();
        let key = Key::new(key, lsn);
        let value = Value::Delete;
        self.tree.insert(key, value, guard)
    }

    /// Returns statistics of this map.
    pub fn stats(&self) -> Stats {
        self.tree.stats()
    }

    /// Returns the minimal valid LSN for reads.
    pub fn min_lsn(&self) -> u64 {
        self.tree.min_lsn()
    }

    /// Updates the minimal valid LSN for reads.
    ///
    /// Entries with smaller LSNs will be dropped later.
    pub fn set_min_lsn(&self, lsn: u64) {
        self.tree.set_min_lsn(lsn);
    }
}

// #[cfg(test)]
// mod test {
//     use std::path::PathBuf;
//     extern crate test;

//     use test::Bencher;

//     use super::Map;
//     use crate::bwtree::{env::Env, util::Options};

//     fn get<E: Env>(map: &Map<E>, k: u64) {
//         let buf = k.to_be_bytes();
//         let key = buf.as_slice();
//         map.get(key, |_| {}).unwrap();
//     }

//     fn put<E: Env>(map: &Map<E>, k: u64) {
//         let buf = k.to_be_bytes();
//         let key = buf.as_slice();
//         map.put(key, key).unwrap();
//     }
//     const NUM_KEYS: u64 = 10_000_000;

//     async fn build_bw_map<E: Env>(env: E, root: PathBuf, opts: Options) -> Map<E> {
//         match Map::open(env, root, opts).await {
//             Ok(m) => return m,
//             Err(e) => panic!("Error in build map {}", e),
//         }
//     }

//     fn set_up<E: Env>(map: Map<E>) {
//         for i in 0..NUM_KEYS {
//             //map.put(i, i);
//             put(&map, i)
//         }
//     }

//     struct env {}

//     #[bench]
//     #[tokio::bench]
//     async fn run(b: &mut Bencher) {
//         let map = build_bw_map(env, root, opts).await;
//         b.iter(|| {});
//     }
// }
