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

pub mod core;
pub mod engine;
pub mod group;
pub mod node;
pub mod policy;
pub mod reader;
pub mod store;
pub mod stream;
pub mod tenant;

type TonicResult<T> = std::result::Result<T, tonic::Status>;
pub use runtime::stream::{
    error::{Error, Result},
    Entry, Sequence,
};

pub use self::{
    engine::Engine,
    stream::{EpochState, Role, Stream},
    tenant::Tenant,
};
