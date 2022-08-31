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
    collections::{HashMap, VecDeque},
    path::Path,
    sync::Arc,
    task::Context,
};

use tokio::sync::Mutex;

use super::{
    dboption::DBOption,
    pipeline::{PipelinedWriter, WriterOwner},
    reader::SegmentReader,
    tributary::PartialStream,
    version::{Version, VersionSet},
};
use crate::{
    manifest::StreamMeta,
    storage::log::manager::{LogEngine, LogFileManager},
    stream::{error::Result, types::Sequence},
    Entry,
};

pub struct StreamDB {
    log: LogEngine,
    version_set: VersionSet,
    core: Arc<Mutex<StreamDBCore>>,
}

struct StreamDBCore {
    streams: HashMap<u64, StreamFlow>,
}

pub struct StreamFlow {
    stream_id: u64,
    core: Arc<Mutex<StreamCore>>,
}

pub(crate) struct StreamCore {
    strorage: PartialStream<LogFileManager>,
    writer: PipelinedWriter,
}

impl StreamDB {
    pub fn open<P: AsRef<Path>>(base_dir: P, opt: DBOption) -> Result<StreamDB> {
        todo!()
    }

    pub fn recover<P: AsRef<Path>>(base_dir: P, opt: Arc<DBOption>) -> Result<StreamDB> {
        todo!()
    }

    pub fn create<P: AsRef<Path>>(base_dir: P) -> Result<()> {
        todo!()
    }

    pub async fn write(
        &self,
        stream_id: u64,
        segment_epoch: u32,
        writer_epoch: u32,
        acked_seq: Sequence,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> Result<(u32, u32)> {
        todo!()
    }

    pub fn read(
        &self,
        stream_id: u64,
        segment_epoch: u32,
        start_index: u32,
        limit: usize,
        require_acked: bool,
    ) -> Result<SegmentReader> {
        todo!()
    }

    pub async fn seal(&self, stream_id: u64, segment_epoch: u32, writer_epoch: u32) -> Result<u32> {
        todo!()
    }

    pub async fn truncate(&self, stream_id: u64, keep_seq: Sequence) -> Result<()> {
        todo!()
    }

    fn must_get_stream(&self, stream_id: u64) -> StreamFlow {
        todo!()
    }

    fn might_get_stream(&self, stream_id: u64) -> Result<StreamFlow> {
        todo!()
    }

    async fn advance_grace_peiod_of_version_set(&self) {
        todo!()
    }
}

impl StreamFlow {
    pub fn new(
        stream_id: u64,
        storage: PartialStream<LogFileManager>,
        log_engine: LogEngine,
    ) -> Self {
        todo!()
    }

    pub fn new_empty(stream_id: u64, version: Version, log_engine: LogEngine) -> Self {
        todo!()
    }

    async fn write(
        &self,
        segment_epoch: u32,
        writer_epoch: u32,
        acked_seq: Sequence,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> Result<(u32, u32)> {
        todo!()
    }

    async fn seal(&self, segment_epoch: u32, writer_epoch: u32) -> Result<u32> {
        todo!()
    }

    async fn stream_meta(&self, keep_seq: Sequence) -> Result<StreamMeta> {
        todo!()
    }

    pub fn poll_entries(
        &self,
        cx: &mut Context<'_>,
        required_epoch: u32,
        start_index: u32,
        limit: usize,
        require_acked: bool,
    ) -> Result<Option<VecDeque<(u32, Entry)>>> {
        todo!()
    }
}

impl WriterOwner for StreamCore {
    fn borrow_pipelined_writer_mut(
        &mut self,
    ) -> (&mut PartialStream<LogFileManager>, &mut PipelinedWriter) {
        (&mut self.strorage, &mut self.writer)
    }
}
