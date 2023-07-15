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
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
};

use super::{define::SourceImpl, format::SourceFormat};
use crate::stream2::container::catalog::{
    desc::{SourceColumnDesc, SourceRowDesc},
    id::ContainerID,
};

type WeakSourceDescRef = Weak<SourceDesc>;
pub(crate) type ContainerSourceManagerRef = Arc<ContainerSourceManager>;

#[allow(dead_code)]
pub(crate) struct SourceDesc {
    source: SourceImpl,
    format: SourceFormat,
    columns: Option<Vec<SourceColumnDesc>>,
    // metrics: Arc<SourceMetrics>,
    rows: Option<Vec<SourceRowDesc>>,
    column_id_index: Option<usize>,
    row_id_index: Option<usize>,
    // pk_column_ids: Vec<i32>,
}

#[allow(dead_code)]
pub(crate) struct ContainerSourceManager {
    sources: Mutex<HashMap<ContainerID, WeakSourceDescRef>>,

    // metrics: Arc<SourceMetrics>,
    connector_message_buffer_size: usize,
}
