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

use std::{collections::HashMap, sync::Arc};

use tokio::sync::Mutex;

use super::{pipeline::PipelinedWriter, tributary::PartialStream, version::VersionSet};
use crate::storage::log::manager::{LogEngine, LogFileManager};

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
