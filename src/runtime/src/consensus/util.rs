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

use super::id::{ProcessId, ShardId};

/// Returns an iterator with all process identifiers in this shard in a system
/// with `n` processes.
pub fn process_ids(shard_id: ShardId, n: usize) -> impl Iterator<Item = ProcessId> {
    // compute process identifiers, making sure ids are non-zero
    let shift = n * shard_id as usize;
    (1..=n).map(move |id| (id + shift) as ProcessId)
}

pub fn all_process_ids(shard_count: usize, n: usize) -> impl Iterator<Item = (ProcessId, ShardId)> {
    (0..shard_count).flat_map(move |shard_id| {
        let shard_id = shard_id as ShardId;
        process_ids(shard_id, n).map(move |process_id| (process_id, shard_id))
    })
}
