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

use std::{collections::HashMap, fmt, fmt::Debug, time::Duration};

use serde::{Deserialize, Serialize};

use super::{
    action::Action,
    command::ConsensusCommand,
    commit::CommittedAndExecuted,
    id::{Dot, ProcessId, ShardId},
    message::{MessageTrait, PeriodicTrait},
    options::ConsensusConfig,
    time::SysTime,
};
use crate::consensus::executor::ExecutorTrait;

#[derive(Clone, Copy, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub enum ProtocolMetricsKind {
    /// fast paths of all commands
    FastPath,
    /// slow paths of all commands
    SlowPath,
    /// fast paths of read only commands
    FastPathReads,
    /// slow paths of read only commands
    SlowPathReads,
    Stable,
    CommitLatency,
    WaitConditionDelay,
    CommittedDepsLen,
    CommandKeyCount,
}

impl Debug for ProtocolMetricsKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProtocolMetricsKind::FastPath => write!(f, "fast_path"),
            ProtocolMetricsKind::SlowPath => write!(f, "slow_path"),
            ProtocolMetricsKind::FastPathReads => write!(f, "fast_path_reads"),
            ProtocolMetricsKind::SlowPathReads => write!(f, "slow_path_reads"),
            ProtocolMetricsKind::Stable => write!(f, "stable"),
            ProtocolMetricsKind::CommitLatency => {
                write!(f, "commit_latency")
            }
            ProtocolMetricsKind::WaitConditionDelay => {
                write!(f, "wait_condition_delay")
            }
            ProtocolMetricsKind::CommittedDepsLen => {
                write!(f, "committed_deps_len")
            }
            ProtocolMetricsKind::CommandKeyCount => {
                write!(f, "command_key_count")
            }
        }
    }
}

#[async_trait::async_trait]
pub(crate) trait ConsensusProtocol {
    type Message: MessageTrait;
    type PeriodicEvent: PeriodicTrait;
    type Executor: ExecutorTrait;
    /// Returns a new instance of the protocol and a list of periodic events.
    async fn new(
        process_id: ProcessId,
        shard_id: ShardId,
        config: ConsensusConfig,
    ) -> (Box<Self>, Vec<(Self::PeriodicEvent, Duration)>);

    async fn id(&self) -> ProcessId;

    async fn shard_id(&self) -> ShardId;

    async fn discover(
        &mut self,
        processes: Vec<(ProcessId, ShardId)>,
    ) -> (bool, HashMap<ShardId, ProcessId>);

    async fn submit(&mut self, dot: Option<Dot>, cmd: ConsensusCommand, time: &dyn SysTime);

    async fn handle(
        &mut self,
        from: ProcessId,
        from_shard_id: ShardId,
        msg: Self::Message,
        time: &dyn SysTime,
    );

    async fn handle_event(&mut self, event: Self::PeriodicEvent, time: &dyn SysTime);

    async fn handle_executed(
        &mut self,
        _committed_and_executed: CommittedAndExecuted,
        _time: &dyn SysTime,
    ) {
        // protocols interested in handling this type of notifications at the
        // worker `GC_WORKER_INDEX` (see fantoch::run::prelude) should overwrite
        // this
    }

    #[must_use]
    async fn to_processes(&mut self) -> Option<Action<Self>>;

    // #[must_use]
    // fn to_processes_iter(&mut self) -> ToProcessesIter<'_, Self> {
    //     ToProcessesIter { process: self }
    // }

    // #[must_use]
    // fn to_executors(&mut self) -> Option<<Self::Executor as Executor>::ExecutionInfo>;

    // #[must_use]
    // fn to_executors_iter(&mut self) -> ToExecutorsIter<'_, Self> {
    //     ToExecutorsIter { process: self }
    // }

    fn parallel() -> bool;

    fn leaderless() -> bool;

    // fn metrics(&self) -> &ProtocolMetrics;
}
