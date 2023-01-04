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

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::stream::common::Sequence;

pub enum Opt<T> {
    Mutate(T),
    AsIs,
}

/// The meta data of a record in kv
#[derive(Serialize, Deserialize, Debug, Default, Clone, Eq, PartialEq)]
pub struct KVMeta {
    /// expiration time in second since 1970
    pub expire_at: Option<u64>,
}

pub struct MutateKV {
    pub key: Bytes,

    pub seq: Sequence,

    pub value: Opt<Vec<u8>>,

    pub value_meta: Option<KVMeta>,
}

pub struct MutateKVReq {}

#[async_trait::async_trait]
pub trait KVApi {}
