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

use std::path::PathBuf;

use tokio::fs;

use super::{list::DirLister, tenant::FileSystemTenant};
use crate::{
    error::{Error, Result},
    file::store_trait::{Lister, Store, Tenant},
};

pub struct FileSystemStore {
    path: PathBuf,
}

impl FileSystemStore {
    pub async fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        fs::create_dir_all(&path).await?;
        Ok(Self { path })
    }
}

#[async_trait::async_trait]
impl Store for FileSystemStore {
    fn tenant(&self, name: &str) -> Box<dyn Tenant> {
        Box::new(FileSystemTenant::new(self.path.join(name)))
    }

    async fn list_tenants(&self) -> Result<Box<dyn Lister<Item = String>>> {
        let dir = fs::read_dir(&self.path).await?;
        Ok(Box::new(DirLister::new(dir)))
    }

    async fn create_tenant(&self, name: &str) -> Result<Box<dyn Tenant>> {
        let path = self.path.join(name);
        if path.exists() {
            return Err(Error::AlreadyExists(format!("tenant {}", name)));
        }
        fs::create_dir_all(&path).await?;
        Ok(self.tenant(name))
    }

    async fn delete_tenant(&self, name: &str) -> Result<()> {
        let path = self.path.join(name);
        fs::remove_dir_all(&path).await?;
        Ok(())
    }
}
