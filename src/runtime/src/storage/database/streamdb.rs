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
    ops::DerefMut,
    path::Path,
    sync::Arc,
    task::Context,
};

use tokio::sync::Mutex;

use super::{
    dblayout::analyze_db_layout,
    dboption::DBOption,
    pipeline::{PipelinedWriter, WriterOwner},
    reader::SegmentReader,
    tributary::PartialStream,
    version::{Version, VersionSet},
};
use crate::{
    manifest::StreamMeta,
    storage::{
        log::manager::{LogEngine, LogFileManager},
        util::{current, recover_log_engine},
    },
    stream::{
        error::{Error, Result},
        types::Sequence,
    },
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

#[derive(Clone)]
pub struct StreamFlow {
    stream_id: u64,
    core: Arc<Mutex<StreamCore>>,
}

pub(crate) struct StreamCore {
    storage: PartialStream<LogFileManager>,
    writer: PipelinedWriter,
}

impl StreamDB {
    pub async fn open<P: AsRef<Path>>(base_dir: P, opt: DBOption) -> Result<StreamDB> {
        std::fs::create_dir_all(&base_dir)?;
        let opt = Arc::new(opt);

        // TODO(luhuanbing): add file block
        if !current(&base_dir).try_exists()? {
            if !opt.create_if_missing {
                return Err(Error::NotFound(format!(
                    "stream database {}",
                    base_dir.as_ref().display()
                )));
            }
            Self::create(&base_dir).await?;
        }
        Self::recover(base_dir, opt).await
    }

    pub async fn recover<P: AsRef<Path>>(base_dir: P, opt: Arc<DBOption>) -> Result<StreamDB> {
        let version_set = VersionSet::recover(&base_dir).unwrap();
        let mut db_layout =
            analyze_db_layout(&base_dir, version_set.manifest_number().await).await?;
        version_set
            .set_next_file_number(db_layout.max_file_number + 1)
            .await;
        let (log_engine, streams) =
            recover_log_engine(&base_dir, opt, version_set.current().await, &mut db_layout).await?;
        let streams = streams
            .into_iter()
            .map(|(stream_id, part_stream)| {
                (
                    stream_id,
                    StreamFlow::new(stream_id, part_stream, log_engine.clone()),
                )
            })
            .collect();
        Ok(StreamDB {
            log: log_engine,
            version_set,
            core: Arc::new(Mutex::new(StreamDBCore { streams })),
        })
    }

    pub async fn create<P: AsRef<Path>>(base_dir: P) -> Result<()> {
        VersionSet::create(base_dir).await
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
        self.must_get_stream(stream_id)
            .await
            .write(segment_epoch, writer_epoch, acked_seq, first_index, entries)
            .await
    }

    pub async fn get_segment_reader(
        &self,
        stream_id: u64,
        segment_epoch: u32,
        start_index: u32,
        limit: usize,
        require_acked: bool,
    ) -> Result<SegmentReader> {
        Ok(SegmentReader::new(
            segment_epoch,
            start_index,
            limit,
            require_acked,
            self.might_get_stream(stream_id).await?,
        ))
    }

    pub async fn seal(&self, stream_id: u64, segment_epoch: u32, writer_epoch: u32) -> Result<u32> {
        self.must_get_stream(stream_id)
            .await
            .seal(segment_epoch, writer_epoch)
            .await
    }

    pub async fn truncate(&self, stream_id: u64, keep_seq: Sequence) -> Result<()> {
        let stream_meta = self
            .must_get_stream(stream_id)
            .await
            .stream_meta(keep_seq)
            .await?;
        if u64::from(keep_seq) > stream_meta.acked_seq {
            return Err(Error::InvalidArgument(format!(
                "truncate un-acked entries, acked seq {}, keep seq {}",
                stream_meta.acked_seq, keep_seq
            )));
        }

        self.version_set.truncate_stream(stream_meta).await?;
        self.advance_grace_peiod_of_version_set().await;
        Ok(())
    }

    async fn must_get_stream(&self, stream_id: u64) -> StreamFlow {
        let mut core = self.core.lock().await;
        let core = core.deref_mut();
        let cur_version = self.version_set.current().await;

        core.streams
            .entry(stream_id)
            .or_insert_with(|| {
                // TODO(luhuanbing): acquire version set lock in db's lock
                StreamFlow::new_empty(stream_id, cur_version, self.log.clone())
            })
            .clone()
    }

    async fn might_get_stream(&self, stream_id: u64) -> Result<StreamFlow> {
        let core = self.core.lock().await;
        match core.streams.get(&stream_id) {
            Some(s) => Ok(s.clone()),
            None => Err(Error::NotFound(format!("stream {}", stream_id))),
        }
    }

    async fn advance_grace_peiod_of_version_set(&self) {
        let db = self.to_owned();
        let streams = {
            let core = db.core.lock().await;
            core.streams.keys().cloned().collect::<Vec<_>>()
        };
        for stream_id in streams {
            if let Ok(stream) = db.might_get_stream(stream_id).await {
                let mut core = stream.core.lock().await;
                core.storage.refresh_versions();
            }
        }
    }
}

impl StreamFlow {
    pub fn new(
        stream_id: u64,
        storage: PartialStream<LogFileManager>,
        log_engine: LogEngine,
    ) -> Self {
        let writer = PipelinedWriter::new(stream_id, log_engine);
        StreamFlow {
            stream_id,
            core: Arc::new(Mutex::new(StreamCore { storage, writer })),
        }
    }

    pub fn new_empty(stream_id: u64, version: Version, log_engine: LogEngine) -> Self {
        let storage = PartialStream::new(
            version.stream_version(stream_id),
            log_engine.log_file_manager(),
        );
        Self::new(stream_id, storage, log_engine)
    }

    async fn write(
        &self,
        segment_epoch: u32,
        writer_epoch: u32,
        acked_seq: Sequence,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> Result<(u32, u32)> {
        let (index, acked_index, waiter) = {
            let num_entries = entries.len() as u32;
            let mut core = self.core.lock().await;
            let txn =
                core.storage
                    .write(writer_epoch, segment_epoch, acked_seq, first_index, entries);
            let continously_index = core
                .storage
                .continuous_index(segment_epoch, first_index..(first_index + num_entries));
            let acked_index = core.storage.acked_index(segment_epoch);
            (
                continously_index,
                acked_index,
                core.writer.submit(self.core.clone(), txn).await,
            )
        };

        Ok((index, acked_index))
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
        (&mut self.storage, &mut self.writer)
    }
}
