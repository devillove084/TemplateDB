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
    io::Result,
    path::{Path, PathBuf},
};

use super::manifestfs::ManifestFileWriter;
use crate::bwtree::env::Env;

pub struct FilePath {
    root: PathBuf,
}

impl FilePath {
    pub fn new(root: PathBuf) -> Self {
        return Self { root };
    }

    pub fn root(&self) -> &Path {
        self.root.as_path()
    }

    pub fn current_file(&self) -> PathBuf {
        self.root.as_path().join("CURRENT")
    }

    pub fn lock_file(&self) -> PathBuf {
        self.root.as_path().join("LOCK")
    }

    pub fn manifest_file(&self, number: u64) -> PathBuf {
        let m = format!("MANIFEST_NUMVER-{}", number);
        self.root.as_path().join(m)
    }
}

pub struct FileStore<E: Env> {
    e: E,
    dir: FilePath,
    writer: ManifestFileWriter<E::SequentialFile>,
}

impl<E: Env> FileStore<E> {
    pub async fn open(env: E, root: PathBuf) -> Result<Self> {
        let dir = FilePath::new(root);
        let this = if !env.path_exist(dir.current_file()).await? {
            Self::init(env, dir).await?
        } else {
            Self::recover(env, dir).await?
        };
        Ok(this)
    }

    pub async fn init(env: E, root: FilePath) -> Result<Self> {
        env.make_dir(root.root()).await?;
        env.lock_file(root.lock_file()).await?;
        todo!()
    }

    pub async fn recover(env: E, root: FilePath) -> Result<Self> {
        env.lock_file(root.lock_file()).await?;
        todo!()
    }

    pub async fn close(env: E, root: FilePath) -> Result<()> {
        env.unlock_file(root.lock_file()).await?;
        todo!()
    }
}
