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

use super::{
    error::{Error, Result},
    streams::StreamInfo,
};
use crate::{StreamDesc, TenantDesc};

pub struct TenantInner {
    desc: TenantDesc,
    next_id: u64,
    streams: HashMap<u64, StreamInfo>,
}

#[derive(Clone)]
pub struct Tenant {
    inner: Arc<Mutex<TenantInner>>,
}

impl Tenant {
    pub fn new(desc: TenantDesc) -> Self {
        let next_id = desc.id;
        let inner = TenantInner {
            desc,
            next_id,
            streams: HashMap::new(),
        };
        Self {
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn desc(&self) -> TenantDesc {
        self.inner.lock().await.desc.clone()
    }

    pub async fn stream_desc(&self, name: &str) -> Result<StreamDesc> {
        let inner = self.inner.lock().await;
        inner
            .streams
            .values()
            .find(|info| info.stream_name == name)
            .map(StreamInfo::stream_desc)
            .ok_or_else(|| Error::NotFound(format!("stream {}", name)))
    }

    pub async fn stream_descs(&self) -> Result<Vec<StreamDesc>> {
        let inner = self.inner.lock().await;
        let descs = inner
            .streams
            .values()
            .map(StreamInfo::stream_desc)
            .collect();
        Ok(descs)
    }

    pub async fn stream(&self, stream_id: u64) -> Result<StreamInfo> {
        let inner = self.inner.lock().await;
        inner
            .streams
            .get(&stream_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("stream_id {}", stream_id)))
    }

    pub async fn create_stream(&self, mut desc: StreamDesc) -> Result<StreamDesc> {
        let mut inner = self.inner.lock().await;
        if inner
            .streams
            .values()
            .any(|info| info.stream_name == desc.name)
        {
            return Err(Error::AlreadyExists(format!("stream {}", desc.name)));
        }

        desc.id = inner.next_id;
        inner.next_id += 1;
        desc.parent_id = inner.desc.id;
        inner.streams.insert(
            desc.id,
            StreamInfo::new(desc.parent_id, desc.id, desc.name.clone()),
        );
        Ok(desc)
    }
}
