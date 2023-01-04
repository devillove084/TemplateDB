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
use std::{marker::PhantomData, sync::Arc};

use super::{
    api::Api,
    kvengine::{engine_impl::Engine, kvformat::KvFormat},
    lockmanager::manager::LockManager,
    mvcc::{conmgr::ConcurrencyManager, readpool::ReadPoolHandle},
    resource::{quota::QuotaLimiter, tag::ResourceTagFactory},
    timemgr::causal::CausalTsProviderImpl,
    txn::scheduler::TxnScheduler,
};

/// [`Storage`](Storage) implements transactional KV APIs and raw KV APIs on a
/// given [`Engine`]. An [`Engine`] provides low level KV functionality.
/// [`Engine`] has multiple implementations. When a TemplateKV server is running, a
/// [`RaftKv`](crate::server::raftkv::RaftKv) will be the underlying [`Engine`]
/// of [`Storage`]. The other two types of engines are for test purpose.
///
/// [`Storage`] is reference counted and cloning [`Storage`] will just increase
/// the reference counter. Storage resources (i.e. threads, engine) will be
/// released when all references are dropped.
///
/// Notice that read and write methods may not be performed over full data in
/// most cases, i.e. when underlying engine is
/// [`RaftKv`](crate::server::raftkv::RaftKv), which limits data access in the
/// range of a single region according to specified `ctx` parameter. However,
/// [`unsafe_destroy_range`](crate::server::gc_worker::GcTask::
/// UnsafeDestroyRange) is the only exception. It's always performed on the
/// whole TemplateKV.
///
/// Operations of [`Storage`](Storage) can be divided into two types: MVCC
/// operations and raw operations. MVCC operations uses MVCC keys, which usually
/// consist of several physical keys in different CFs. In default CF and write
/// CF, the key will be memcomparable-encoded and append the timestamp to it, so
/// that multiple versions can be saved at the same time. Raw operations use raw
/// keys, which are saved directly to the engine without memcomparable- encoding
/// and appending timestamp.
#[allow(dead_code)]
pub struct Storage<E: Engine, L: LockManager, F: KvFormat> {
    engine: E,

    sched: TxnScheduler<E, L>,

    /// The thread pool used to run most read operations.
    read_pool: ReadPoolHandle,

    concurrency_manager: ConcurrencyManager,

    // Fields below are storage configurations.
    max_key_size: usize,

    resource_tag_factory: ResourceTagFactory,

    api_version: Api,

    causal_ts_provider: Option<Arc<CausalTsProviderImpl>>,

    quota_limiter: Arc<QuotaLimiter>,

    _phantom: PhantomData<F>,
}
