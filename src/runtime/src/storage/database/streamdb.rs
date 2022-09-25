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
    sync::{Arc, Mutex},
    task::Context,
};

use super::{
    dblayout::analyze_db_layout,
    dboption::DBOption,
    pipeline::{PipelinedWriter, WriterOwner},
    reader::SegmentReader,
    tributary::PartialStream,
    version::{Version, VersionSet},
};
use crate::{
    manifest::{ReplicaMeta, StreamMeta},
    storage::{
        log::manager::{LogEngine, LogFileManager},
        util::{current, recover_log_engine, remove_obsoleted_files},
    },
    stream::{
        error::{Error, Result},
        Entry, Sequence,
    },
};

struct StreamDBCore {
    streams: HashMap<u64, StreamFlow>,
}

#[derive(Clone)]
pub struct StreamDB {
    log_engine: LogEngine,
    version_set: VersionSet,
    core: Arc<Mutex<StreamDBCore>>,
}

#[derive(Clone)]
pub struct StreamFlow {
    stream_id: u64,
    core: Arc<Mutex<StreamCore>>,
}

struct StreamCore {
    storage: PartialStream<LogFileManager>,
    writer: PipelinedWriter,
}

impl StreamDB {
    pub fn open<P: AsRef<Path>>(base_dir: P, opt: DBOption) -> Result<StreamDB> {
        std::fs::create_dir_all(&base_dir)?;
        let opt = Arc::new(opt);

        // TODO(luhuanbing) add file lock.
        if !current(&base_dir).try_exists()? {
            if !opt.create_if_missing {
                return Err(Error::NotFound(format!(
                    "stream database {}",
                    base_dir.as_ref().display()
                )));
            }

            // Create new DB instance then recover it.
            Self::create(&base_dir)?;
        }

        Self::recover(base_dir, opt)
    }

    fn recover<P: AsRef<Path>>(base_dir: P, opt: Arc<DBOption>) -> Result<StreamDB> {
        let version_set = VersionSet::recover(&base_dir).unwrap();
        let mut db_layout = analyze_db_layout(&base_dir, version_set.manifest_number())?;
        version_set.set_next_file_number(db_layout.max_file_number + 1);
        let (log_engine, streams) =
            recover_log_engine(&base_dir, opt, version_set.current(), &mut db_layout)?;
        remove_obsoleted_files(db_layout);
        let streams = streams
            .into_iter()
            .map(|(stream_id, partial_stream)| {
                (
                    stream_id,
                    StreamFlow::new(stream_id, partial_stream, log_engine.clone()),
                )
            })
            .collect();

        Ok(StreamDB {
            log_engine,
            version_set,
            core: Arc::new(Mutex::new(StreamDBCore { streams })),
        })
    }

    #[inline(always)]
    fn create<P: AsRef<Path>>(base_dir: P) -> Result<()> {
        VersionSet::create(base_dir)
    }

    #[inline]
    pub async fn write(
        &self,
        stream_id: u64,
        seg_epoch: u32,
        writer_epoch: u32,
        acked_seq: Sequence,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> Result<(u32, u32)> {
        self.must_get_stream(stream_id)
            .write(seg_epoch, writer_epoch, acked_seq, first_index, entries)
            .await
    }

    #[inline]
    pub fn read(
        &self,
        stream_id: u64,
        seg_epoch: u32,
        start_index: u32,
        limit: usize,
        require_acked: bool,
    ) -> Result<SegmentReader> {
        Ok(SegmentReader::new(
            seg_epoch,
            start_index,
            limit,
            require_acked,
            self.might_get_stream(stream_id)?,
        ))
    }

    #[inline]
    pub async fn seal(&self, stream_id: u64, seg_epoch: u32, writer_epoch: u32) -> Result<u32> {
        self.must_get_stream(stream_id)
            .seal(seg_epoch, writer_epoch)
            .await
    }

    pub async fn truncate(&self, stream_id: u64, keep_seq: Sequence) -> Result<()> {
        let stream_meta = self
            .must_get_stream(stream_id)
            .stream_meta(keep_seq)
            .await?;

        if u64::from(keep_seq) > stream_meta.acked_seq {
            return Err(Error::InvalidArgument(format!(
                "truncate un-acked entries, acked seq {}, keep seq {}",
                stream_meta.acked_seq, keep_seq
            )));
        }

        self.version_set.truncate_stream(stream_meta).await?;

        self.advance_grace_period_of_version_set().await;

        Ok(())
    }

    #[inline(always)]
    fn must_get_stream(&self, stream_id: u64) -> StreamFlow {
        let mut core = self.core.lock().unwrap();
        let core = core.deref_mut();
        core.streams
            .entry(stream_id)
            .or_insert_with(|| {
                // FIXME(luhuanbing) acquire version set lock in db's lock.
                StreamFlow::new_empty(
                    stream_id,
                    self.version_set.current(),
                    self.log_engine.clone(),
                )
            })
            .clone()
    }

    #[inline(always)]
    fn might_get_stream(&self, stream_id: u64) -> Result<StreamFlow> {
        let core = self.core.lock().unwrap();
        match core.streams.get(&stream_id) {
            Some(s) => Ok(s.clone()),
            None => Err(Error::NotFound(format!("stream {}", stream_id))),
        }
    }

    async fn advance_grace_period_of_version_set(&self) {
        let db = self.clone();
        tokio::spawn(async move {
            let streams = {
                let core = db.core.lock().unwrap();
                core.streams.keys().cloned().collect::<Vec<_>>()
            };

            for stream_id in streams {
                if let Ok(stream) = db.might_get_stream(stream_id) {
                    let mut core = stream.core.lock().unwrap();
                    core.storage.refresh_versions();
                }
                tokio::task::yield_now().await;
            }
        });
    }
}

impl StreamFlow {
    fn new(stream_id: u64, storage: PartialStream<LogFileManager>, log_engine: LogEngine) -> Self {
        let writer = PipelinedWriter::new(stream_id, log_engine);
        StreamFlow {
            stream_id,
            core: Arc::new(Mutex::new(StreamCore { storage, writer })),
        }
    }

    fn new_empty(stream_id: u64, version: Version, log_engine: LogEngine) -> Self {
        let storage = PartialStream::new(
            version.stream_version(stream_id),
            log_engine.log_file_manager(),
        );
        Self::new(stream_id, storage, log_engine)
    }

    async fn write(
        &self,
        seg_epoch: u32,
        writer_epoch: u32,
        acked_seq: Sequence,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> Result<(u32, u32)> {
        let (index, acked_index, waiter) = {
            let num_entries = entries.len() as u32;
            let mut core = self.core.lock().unwrap();
            let txn = core
                .storage
                .write(seg_epoch, writer_epoch, acked_seq, first_index, entries);
            let continuously_index = core
                .storage
                .continuous_index(seg_epoch, first_index..(first_index + num_entries));
            let acked_index = core.storage.acked_index(seg_epoch);
            (
                continuously_index,
                acked_index,
                core.writer.submit(self.core.clone(), txn),
            )
        };

        waiter.await?;
        Ok((index, acked_index))
    }

    async fn seal(&self, seg_epoch: u32, writer_epoch: u32) -> Result<u32> {
        let (acked_index, waiter) = {
            let mut core = self.core.lock().unwrap();
            let txn = core.storage.seal(seg_epoch, writer_epoch);
            let acked_index = core.storage.acked_index(seg_epoch);
            (acked_index, core.writer.submit(self.core.clone(), txn))
        };

        waiter.await?;

        Ok(acked_index)
    }

    async fn stream_meta(&self, keep_seq: Sequence) -> Result<StreamMeta> {
        // Read the memory state and wait until all previous txn are committed.
        let (acked_seq, sealed_table, waiter) = {
            let mut core = self.core.lock().unwrap();
            let acked_seq = core.storage.acked_seq();
            let sealed_table = core.storage.sealed_epoches();
            (
                acked_seq,
                sealed_table,
                core.writer.submit_txn(self.core.clone(), None),
            )
        };
        waiter.await?;

        Ok(StreamMeta {
            stream_id: self.stream_id,
            acked_seq: acked_seq.into(),
            initial_seq: keep_seq.into(),
            replicas: sealed_table
                .into_iter()
                .map(|(epoch, promised)| ReplicaMeta {
                    epoch,
                    promised_epoch: Some(promised),
                    set_files: Vec::default(),
                })
                .collect(),
        })
    }

    /// Poll entries from start_index, if the entries aren't ready for
    /// reading, a [`None`] is returned, and a [`std::task::Waker`] is taken.
    pub fn poll_entries(
        &self,
        cx: &mut Context<'_>,
        required_epoch: u32,
        start_index: u32,
        limit: usize,
        require_acked: bool,
    ) -> Result<Option<VecDeque<(u32, Entry)>>> {
        let mut core = self.core.lock().unwrap();
        if let Some(entries_container) =
            core.storage
                .scan_entries(required_epoch, start_index, limit, require_acked)?
        {
            Ok(Some(entries_container))
        } else {
            core.writer.register_reading_waiter(cx.waker().clone());
            Ok(None)
        }
    }
}

impl WriterOwner for StreamCore {
    fn borrow_pipelined_writer_mut(
        &mut self,
    ) -> (&mut PartialStream<LogFileManager>, &mut PipelinedWriter) {
        (&mut self.storage, &mut self.writer)
    }
}
