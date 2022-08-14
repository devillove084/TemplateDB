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
    path::PathBuf,
    sync::Arc,
};

use tokio::sync::Mutex;

use crate::storage::database::dboption::DBOption;

pub(crate) trait ReleaseReferringLogFile {
    /// All entries in the corresponding log file are acked or over written, so
    /// release the reference of the log file.
    fn release(&self, stream_id: u64, log_number: u64);
}

struct LogFileManagerInner {
    next_log_number: u64,
    recycled_log_files: VecDeque<u64>,
    /// log_number => {stream_id}
    refer_streams: HashMap<u64, HashSet<u64>>,
}

pub struct LogFileManager {
    opt: Arc<DBOption>,
    base_dir: PathBuf,
    inner: Arc<Mutex<LogFileManagerInner>>,
}
