// Copyright 2021 The arrowkv Authors.
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

use std::fmt::Debug;

use crate::{async_trait, Result};

/// A generic timestamp to order events.
pub trait Timestamp: Eq + Ord + PartialEq + PartialOrd + Clone + Debug + Send + 'static {}

impl Timestamp for u64 {}

#[derive(Clone, Debug, PartialEq)]
pub struct Event<T> {
    pub ts: T,
    pub data: Vec<u8>,
}

#[async_trait]
pub trait StreamReader<T> {
    async fn seek(&mut self, ts: T) -> Result<()>;

    async fn next(&mut self) -> Result<Option<Event<T>>>;
}

#[async_trait]
pub trait StreamWriter<T> {
    /// Appends an event.
    async fn append(&mut self, event: Event<T>) -> Result<()>;

    /// Releases events up to a timestamp (exclusive).
    async fn release(&mut self, ts: T) -> Result<()>;
}
