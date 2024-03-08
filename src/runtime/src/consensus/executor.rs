use std::fmt::Debug;

use serde::{de::DeserializeOwned, Serialize};

use crate::consensus::message::MessageIndexTrait;

#[allow(dead_code)]
pub(crate) trait ExecutionInfoTrait:
    Debug + Clone + Serialize + DeserializeOwned + Send + Sync + MessageIndexTrait
{
}

impl<T> ExecutionInfoTrait for T where
    T: Debug + Clone + Serialize + DeserializeOwned + Send + Sync + MessageIndexTrait
{
}

#[allow(dead_code)]
#[async_trait::async_trait]
pub(crate) trait ExecutorTrait: Clone {
    // TODO why is Send needed?
    type ExecutionInfo: ExecutionInfoTrait; // TODO why is Sync needed??

    // async fn new(process_id: ProcessId, shard_id: ShardId, config: Config) -> Self;

    // fn set_executor_index(&mut self, _index: usize) {
    //     // executors interested in the index should overwrite this
    // }

    // fn cleanup(&mut self, _time: &dyn SysTime) {
    //     // executors interested in a periodic cleanup should overwrite this
    // }

    // fn monitor_pending(&mut self, _time: &dyn SysTime) {
    //     // executors interested in a periodic check of pending commands should
    //     // overwrite this
    // }

    // fn handle(&mut self, infos: Self::ExecutionInfo, time: &dyn SysTime);

    // #[must_use]
    // fn to_clients(&mut self) -> Option<ExecutorResult>;

    // #[must_use]
    // fn to_clients_iter(&mut self) -> ToClientsIter<'_, Self> {
    //     ToClientsIter { executor: self }
    // }

    // #[must_use]
    // fn to_executors(&mut self) -> Option<(ShardId, Self::ExecutionInfo)> {
    //     // non-genuine protocols should overwrite this
    //     None
    // }

    // #[must_use]
    // fn to_executors_iter(&mut self) -> ToExecutorsIter<'_, Self> {
    //     ToExecutorsIter { executor: self }
    // }

    // #[must_use]
    // fn executed(
    //     &mut self,
    //     _time: &dyn SysTime,
    // ) -> Option<CommittedAndExecuted> { // protocols that are interested in notifying the worker
    //   // `GC_WORKER_INDEX` (see fantoch::run::prelude) with these executed // notifications
    //   should overwrite this None
    // }

    // fn parallel() -> bool;

    // fn metrics(&self) -> &ExecutorMetrics;

    // fn monitor(&self) -> Option<ExecutionOrderMonitor>;
}
