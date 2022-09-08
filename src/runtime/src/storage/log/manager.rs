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
    collections::{HashMap, HashSet, VecDeque},
    fs::File,
    path::{Path, PathBuf},
    sync::Arc,
};

use futures::channel::oneshot;
use tokio::{sync::Mutex, task::JoinHandle};

use crate::{
    storage::database::dboption::DBOption,
    stream::{
        channel::Channel,
        error::{IOKindResult, IOResult, Result},
    },
    Record,
};

#[async_trait::async_trait]
pub trait ReleaseReferringLogFile {
    /// All entries in the corresponding log file are acked or over written, so
    /// release the reference of the log file.
    async fn release(&self, stream_id: u64, log_number: u64);
}

struct LogFileManagerInner {
    next_log_number: u64,
    recycled_log_files: VecDeque<u64>,
    /// log_number => {stream_id}
    refer_streams: HashMap<u64, HashSet<u64>>,
}

#[derive(Clone)]
pub struct LogFileManager {
    opt: Arc<DBOption>,
    base_dir: PathBuf,
    inner: Arc<Mutex<LogFileManagerInner>>,
}

impl LogFileManager {
    pub fn new<P: AsRef<Path>>(base_dir: P, next_log_number: u64, opt: Arc<DBOption>) -> Self {
        LogFileManager {
            opt,
            base_dir: base_dir.as_ref().to_path_buf(),
            inner: Arc::new(Mutex::new(LogFileManagerInner {
                next_log_number,
                recycled_log_files: VecDeque::new(),
                refer_streams: HashMap::new(),
            })),
        }
    }

    pub fn recycle_all(&self, log_numbers: Vec<u64>) {
        todo!()
    }

    pub fn allocate_file(&self) -> IOResult<(u64, File)> {
        todo!()
    }

    pub fn delegate(&self, log_number: u64, refer_streams: HashSet<u64>) {
        todo!()
    }

    pub fn option(&self) -> Arc<DBOption> {
        todo!()
    }
}

#[async_trait::async_trait]
impl ReleaseReferringLogFile for LogFileManager {
    async fn release(&self, stream_id: u64, log_number: u64) {
        let mut inner = self.inner.lock().await;
        if let Some(stream_set) = inner.refer_streams.get_mut(&log_number) {
            stream_set.remove(&stream_id);
            if stream_set.is_empty() {
                inner.refer_streams.remove(&log_number);
                // TODO(luhunabing): submit background task, then add log number into recycled log
                // files.
            }
        }
    }
}

#[derive(Clone)]
pub struct LogEngine {
    channel: Channel,
    log_file_manager: LogFileManager,
    core: Arc<Mutex<LogEngineCore>>,
}

impl LogEngine {
    pub fn recover<P: AsRef<Path>, F>(
        base_dir: P,
        mut log_numbers: Vec<u64>,
        log_file_mgr: LogFileManager,
        reader: &mut F,
    ) -> Result<LogEngine> {
        todo!()
    }

    pub fn log_file_manager(&self) -> LogFileManager {
        self.log_file_manager.clone()
    }

    pub fn add_record(&self, record: Record) -> oneshot::Receiver<IOKindResult<u64>> {
        self.channel.append(record)
    }
}

struct LogEngineCore {
    work_handle: Option<JoinHandle<()>>,
}
