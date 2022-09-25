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
    fs::{File, OpenOptions},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    thread::JoinHandle,
};

use futures::channel::oneshot;

use super::{logwoker::LogWorker, logwriter::LogWriter};
use crate::{
    storage::{
        database::dboption::DBOption,
        fs::FileExt,
        util::{log, recover_log_file, temp},
    },
    stream::{
        channel::Channel,
        error::{IOKindResult, IOResult, Result},
    },
    Record,
};

pub trait ReleaseReferringLogFile {
    /// All entries in the corresponding log file are acked or over written, so
    /// release the reference of the log file.
    fn release(&self, stream_id: u64, log_number: u64);
}

#[derive(Clone)]
pub struct LogFileManager {
    opt: Arc<DBOption>,
    base_dir: PathBuf,
    inner: Arc<Mutex<LogFileManagerInner>>,
}

struct LogFileManagerInner {
    next_log_number: u64,
    recycled_log_files: VecDeque<u64>,
    /// log_number => { stream_id }.
    refer_streams: HashMap<u64, HashSet<u64>>,
}

impl LogFileManager {
    pub fn new<P: AsRef<Path>>(base_dir: P, next_log_number: u64, opt: Arc<DBOption>) -> Self {
        LogFileManager {
            opt,
            base_dir: base_dir.as_ref().to_path_buf(),
            inner: Arc::new(Mutex::new(LogFileManagerInner {
                recycled_log_files: VecDeque::new(),
                next_log_number,
                refer_streams: HashMap::new(),
            })),
        }
    }

    pub fn recycle_all(&self, log_numbers: Vec<u64>) {
        let mut inner = self.inner.lock().unwrap();
        inner.recycled_log_files.extend(log_numbers.into_iter());
    }

    pub fn allocate_file(&self) -> IOResult<(u64, File)> {
        let (log_number, prev_log_number) = {
            let mut inner = self.inner.lock().unwrap();
            let log_number = inner.next_log_number;
            inner.next_log_number += 1;
            (log_number, inner.recycled_log_files.pop_front())
        };

        let log_file_name = log(&self.base_dir, log_number);
        let prev_file_name = if let Some(prev_log_number) = prev_log_number {
            log(&self.base_dir, prev_log_number)
        } else {
            let tmp = temp(&self.base_dir, log_number);
            let mut file = OpenOptions::new().write(true).create(true).open(&tmp)?;
            file.preallocate(self.opt.log.log_file_size)?;
            tmp
        };
        std::fs::rename(prev_file_name, &log_file_name)?;
        let file = OpenOptions::new()
            .write(true)
            .truncate(false)
            .open(log_file_name)?;

        // See `man 2 fsync`:
        //
        // Calling fsync() does not necessarily ensure that the entry in the directory
        // containing the file has also reached disk.  For that an explicit fsync() on a
        // file descriptor for the directory is also needed.
        File::open(&self.base_dir)?.sync_all()?;

        Ok((log_number, file))
    }

    /// A log file is filled, delegate lifecycle to LogFileManager with the
    /// reference of streams.
    pub fn delegate(&self, log_number: u64, refer_streams: HashSet<u64>) {
        let mut inner = self.inner.lock().unwrap();
        assert!(
            inner
                .refer_streams
                .insert(log_number, refer_streams)
                .is_none(),
            "each file only allow to delegate once"
        );
    }

    pub fn option(&self) -> Arc<DBOption> {
        self.opt.clone()
    }
}

impl ReleaseReferringLogFile for LogFileManager {
    fn release(&self, stream_id: u64, log_number: u64) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(stream_set) = inner.refer_streams.get_mut(&log_number) {
            stream_set.remove(&stream_id);
            if stream_set.is_empty() {
                inner.refer_streams.remove(&log_number);
                // TODO(walter) submit background task, then add log number into
                // recycled log files.
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
    ) -> Result<LogEngine>
    where
        F: FnMut(u64, Record) -> Result<()>,
    {
        let mut last_file_info = None;
        log_numbers.sort_unstable();
        for log_number in log_numbers {
            let (next_record_offset, refer_streams) =
                recover_log_file(&base_dir, log_number, reader)?;
            last_file_info = Some((log_number, next_record_offset));
            log_file_mgr.delegate(log_number, refer_streams);
        }

        let mut writer = None;
        let opt = log_file_mgr.option();
        if let Some((log_number, initial_offset)) = last_file_info {
            if initial_offset < opt.log.log_file_size as u64 {
                let file = File::options()
                    .write(true)
                    .open(log(&base_dir, log_number))?;
                writer = Some(LogWriter::new(
                    file,
                    log_number,
                    initial_offset as usize,
                    opt.log.log_file_size,
                )?);
            }
        }

        let channel = Channel::new();
        let mut log_worker = LogWorker::new(channel.clone(), writer, log_file_mgr.clone())?;
        let worker_handle = std::thread::spawn(move || log_worker.run());

        Ok(LogEngine {
            channel,
            log_file_manager: log_file_mgr,
            core: Arc::new(Mutex::new(LogEngineCore {
                worker_handle: Some(worker_handle),
            })),
        })
    }

    #[inline(always)]
    pub fn log_file_manager(&self) -> LogFileManager {
        self.log_file_manager.clone()
    }

    pub fn add_record(&self, record: Record) -> oneshot::Receiver<IOKindResult<u64>> {
        self.channel.append(record)
    }
}

impl Drop for LogEngine {
    fn drop(&mut self) {
        let mut core = self.core.lock().unwrap();
        self.channel.shutdown();
        if let Some(handle) = core.worker_handle.take() {
            handle.join().unwrap_or_default();
        }
    }
}

struct LogEngineCore {
    worker_handle: Option<JoinHandle<()>>,
}
