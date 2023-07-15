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

#[allow(dead_code)]
pub(crate) struct StreamConfig {
    /// The interval of periodic barrier
    barrier_interval_ms: u32,

    /// The maximum number of barriers in-flight the compute node
    in_flight_barrier_nums: u32,

    /// There will be a checkpoint for every n barriers
    checkpoint_frequency: usize,

    /// Whether to enable the minimal scheduling strategy, that is, only schedule the streaming
    /// fragment on one parallel unit per compute node.
    minimal_scheduling: bool,

    /// The thread number of the streaming actor runtime in the compute node. The default value is
    /// decided by `tokio`.
    actor_runtime_worker_threads_num: Option<usize>,

    dev_config: DeveloperConfig,
}

impl Default for StreamConfig {
    fn default() -> Self {
        todo!()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct DeveloperConfig {
    /// The size of the channel used for output to exchange/shuffle.
    pub batch_output_channel_size: usize,

    /// The size of a chunk produced by `RowSeqScanExecutor`
    pub batch_chunk_size: usize,

    /// Set to true to enable per-executor row count metrics. This will produce a lot of timeseries
    /// and might affect the prometheus performance. If you only need actor input and output
    /// rows data, see `stream_actor_in_record_cnt` and `stream_actor_out_record_cnt` instead.
    pub stream_enable_executor_row_count: bool,

    /// Whether to use a managed lru cache (evict by epoch)
    pub stream_enable_managed_cache: bool,

    /// The capacity of the chunks in the channel that connects between `ConnectorSource` and
    /// `SourceExecutor`.
    pub stream_connector_message_buffer_size: usize,

    /// Limit number of cached entries (one per group key).
    pub unsafe_stream_hash_agg_cache_size: usize,

    /// Limit number of the cached entries (one per join key) on each side.
    pub unsafe_stream_join_cache_size: usize,

    /// Limit number of the cached entries in an extreme aggregation call.
    pub unsafe_stream_extreme_cache_size: usize,

    /// The maximum size of the chunk produced by executor at a time.
    pub stream_chunk_size: usize,

    /// The initial permits that a channel holds, i.e., the maximum row count can be buffered in
    /// the channel.
    pub stream_exchange_initial_permits: usize,

    /// The permits that are batched to add back, for reducing the backward `AddPermits` messages
    /// in remote exchange.
    pub stream_exchange_batched_permits: usize,
}
