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

use std::collections::HashMap;

use super::region::Region;

#[allow(dead_code)]
pub struct BuildEnv {
    /// mapping from region A to a mapping from region B to the latency between
    /// A and B
    /// Region A -> (RegionB -> latency)
    latencies: HashMap<Region, HashMap<Region, u64>>,

    /// mapping from each region to the regions sorted by distance,
    /// put u64 on the top of tuple, means we sort by latency.
    /// Region A -> (All Regions(by sorted))
    sorted_distance: HashMap<Region, Vec<(u64, Region)>>,
}
