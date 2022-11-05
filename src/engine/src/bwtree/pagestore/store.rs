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

use super::pagefs::FileStore;
use crate::bwtree::{env::Env, error::Result, util::Options};

#[derive(Debug, Clone, Copy)]
pub struct PageInfo {
    pub ver: u64,
    pub len: u8,
    pub is_leaf: bool,
}

pub struct PageStore<E: Env> {
    fs: FileStore<E>,
    opts: Options,
}

impl<E: Env> PageStore<E> {
    pub async fn open(env: E, root: PathBuf, opts: Options) -> Result<Self> {
        let fs = FileStore::open(env, root).await?;
        Ok(Self { fs, opts })
    }
}
