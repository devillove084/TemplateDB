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

use std::{collections::HashMap, sync::Arc, time::Duration};

// use tokio::sync::Mutex;
use parking_lot::Mutex;

use super::{
    error::{Error, Result},
    tenant::Tenant,
};
use crate::TenantDesc;

#[derive(Debug, Clone)]
pub struct Config {
    /// How many tick before an observer's lease is timeout
    ///
    /// Default: 3
    pub heartbeat_timeout_tick: u64,

    /// Obeserver heartbeat intervals in ms.
    ///
    /// Default: 500ms
    pub heartbeat_interval: u64,
}

impl Config {
    pub fn heartbeat_timeout(&self) -> Duration {
        Duration::from_millis(self.heartbeat_interval * self.heartbeat_timeout_tick)
    }
}

impl Default for Config {
    fn default() -> Self {
        Config {
            heartbeat_interval: 500,
            heartbeat_timeout_tick: 3,
        }
    }
}

pub struct NodeInner {
    next_id: u32,
    tenants: HashMap<String, Tenant>,
}

/// This node may will be master
pub struct Node {
    pub config: Config,

    pub stores: Vec<String>,

    inner: Arc<Mutex<NodeInner>>,
}

impl Node {
    pub fn new(config: Config, stores: Vec<String>) -> Self {
        let inner = NodeInner {
            next_id: 1,
            tenants: HashMap::new(),
        };

        Self {
            config,
            stores,
            inner: Arc::new(Mutex::new(inner)),
        }
    }

    pub async fn tenant(&self, name: &str) -> Result<Tenant> {
        let inner = self.inner.lock();
        inner
            .tenants
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("tenant {}", name)))
    }

    pub async fn tenants(&self) -> Result<Vec<Tenant>> {
        let inner = self.inner.lock();
        let tenants = inner.tenants.values().cloned().collect();
        Ok(tenants)
    }

    pub async fn create_tenant(&self, mut desc: TenantDesc) -> Result<TenantDesc> {
        let mut inner = self.inner.lock();
        if inner.tenants.contains_key(&desc.name) {
            return Err(Error::AlreadyExists(format!("tenant {}", desc.name)));
        }

        desc.id = (inner.next_id as u64) << 32;
        inner.next_id += 1;
        let db = Tenant::new(desc.clone());
        inner.tenants.insert(desc.name.clone(), db);
        Ok(desc)
    }
}
