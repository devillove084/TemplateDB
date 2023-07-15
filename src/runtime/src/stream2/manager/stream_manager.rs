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

use std::{
    collections::HashMap,
    sync::{atomic::AtomicU64, Arc},
};

use engine::storage_impl::ContainerStoreImpl;
use tokio::{runtime::Runtime, sync::Mutex, task::JoinHandle};

use super::{context::SharedContext, scheduler::ActorSchedulerImpl};
use crate::{actor::StreamActor, stream2::config::StreamConfig};
#[allow(dead_code)]
type RuntimeID = u64;
#[allow(dead_code)]
type ActorID = u32;
#[allow(dead_code)]
type ActorHandle = JoinHandle<()>;

#[allow(dead_code)]
pub(crate) struct StreamManagerCore {
    runtimes: &'static Runtime,

    handles: HashMap<ActorID, ActorHandle>,

    context: Arc<SharedContext>,

    actors: HashMap<ActorID, StreamActor>,

    actor_monitor_tasks: HashMap<ActorID, ActorHandle>,

    state_store: ContainerStoreImpl,

    // stream_metrics: Arc<StreamingMetrics>,
    config: StreamConfig,

    // TODO: Management for actors lifetime.
    // stack_trace_manager: Option<StacktraceManager<ActorID>>,
    watermark_epoch: Option<Arc<AtomicU64>>,
}

#[allow(dead_code)]
pub(crate) struct StreamManager {
    cores: HashMap<RuntimeID, Mutex<StreamManagerCore>>,
    state_store: ContainerStoreImpl,
    context: Arc<SharedContext>,
    scheduler: Arc<ActorSchedulerImpl>,
    // streaming_metrics: Arc<StreamingMetrics>,
}
