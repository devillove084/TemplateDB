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

use std::sync::atomic::{AtomicU64, Ordering};

use super::{
    env::Env, pagecache::PageCache, pagestore::store::PageStore, pagetable::PageTable,
    stats::AtomicStats, util::Options,
};

struct MinLsn(AtomicU64);

impl MinLsn {
    fn new() -> Self {
        Self(AtomicU64::new(0))
    }

    fn get(&self) -> u64 {
        self.0.load(Ordering::Relaxed)
    }

    fn set(&self, lsn: u64) {
        let mut min = self.0.load(Ordering::Relaxed);
        while min < lsn {
            match self
                .0
                .compare_exchange(min, lsn, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(new) => min = new,
            }
        }
    }
}

pub struct Tree<E: Env> {
    opts: Options,
    table: PageTable,
    cache: PageCache,
    store: PageStore<E>,
    stats: AtomicStats,
    min_lsn: MinLsn,
}
