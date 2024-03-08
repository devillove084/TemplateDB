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
