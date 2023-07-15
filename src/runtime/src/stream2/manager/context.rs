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
pub(crate) struct SharedContext {
    // / Stores the senders and receivers for later `Processor`'s usage.
    // /
    // / Each actor has several senders and several receivers. Senders and receivers are created
    // / during `update_actors` and stored in a channel map. Upon `build_actors`, all these channels
    // / will be taken out and built into the executors and outputs.
    // / One sender or one receiver can be uniquely determined by the upstream and downstream actor
    // / id.
    // /
    // / There are three cases when we need local channels to pass around messages:
    // / 1. pass `Message` between two local actors
    // / 2. The RPC client at the downstream actor forwards received `Message` to one channel in
    // / `ReceiverExecutor` or `MergerExecutor`.
    // / 3. The RPC `Output` at the upstream actor forwards received `Message` to
    // / `ExchangeServiceImpl`.
    // /
    // / The channel serves as a buffer because `ExchangeServiceImpl`
    // / is on the server-side and we will also introduce backpressure.

    // channel_map: Mutex<HashMap<UpDownActorIds, ConsumableChannelPair>>,

    // /// Stores all actor information.
    // actor_infos: RwLock<HashMap<ActorId, ActorInfo>>,

    // /// Stores the local address.
    // ///
    // /// It is used to test whether an actor is local or not,
    // /// thus determining whether we should setup local channel only or remote rpc connection
    // /// between two actors/actors.
    // addr: HostAddr,

    // /// The pool of compute clients.
    // // TODO: currently the client pool won't be cleared. Should remove compute clients when
    // // disconnected.
    // // compute_client_pool: ComputeClientPool,
    // barrier_manager: Arc<Mutex<BarrierManager>>,

    // config: StreamConfig,
}
