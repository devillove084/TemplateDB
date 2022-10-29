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

use crate::bwtree::util::Counter;

#[derive(Default, Debug)]
pub struct Stats {
    pub cache_size: u64,
    pub succeeded: OpStats,
    pub conflicted: OpStats,
}

#[derive(Default, Debug)]
pub struct OpStats {
    pub num_gets: u64,
    pub num_inserts: u64,
    pub num_data_splits: u64,
    pub num_data_consolidates: u64,
    pub num_index_splits: u64,
    pub num_index_consolidates: u64,
}

#[derive(Default)]
pub struct AtomicStats {
    pub succeeded: AtomicOpStats,
    pub conflicted: AtomicOpStats,
}

#[derive(Default)]
pub struct AtomicOpStats {
    pub num_gets: Counter,
    pub num_inserts: Counter,
    pub num_data_splits: Counter,
    pub num_data_consolidates: Counter,
    pub num_index_splits: Counter,
    pub num_index_consolidates: Counter,
}

impl AtomicOpStats {
    pub fn snapshot(&self) -> OpStats {
        OpStats {
            num_gets: self.num_gets.get(),
            num_inserts: self.num_inserts.get(),
            num_data_splits: self.num_data_splits.get(),
            num_data_consolidates: self.num_data_consolidates.get(),
            num_index_splits: self.num_index_splits.get(),
            num_index_consolidates: self.num_index_consolidates.get(),
        }
    }
}
