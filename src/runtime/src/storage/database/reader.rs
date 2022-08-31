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

use std::collections::VecDeque;

use futures::Stream;

use super::streamdb::StreamFlow;
use crate::{Entry, ReadResponse};

pub struct SegmentReader {
    required_epoch: u32,
    next_index: u32,
    limit: usize,
    finished: bool,
    require_acked: bool,
    cached_entries: VecDeque<(u32, Entry)>,
    stream: StreamFlow,
}

impl SegmentReader {
    pub fn new(
        required_epoch: u32,
        next_index: u32,
        limit: usize,
        require_acked: bool,
        stream: StreamFlow,
    ) -> Self {
        SegmentReader {
            required_epoch,
            next_index,
            limit,
            finished: false,
            require_acked,
            cached_entries: VecDeque::new(),
            stream,
        }
    }

    pub fn take_cached_entry(&mut self) -> Option<ReadResponse> {
        todo!()
    }
}

impl Stream for SegmentReader {
    type Item = std::result::Result<ReadResponse, tonic::Status>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        todo!()
    }
}
