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
    future::Future,
    io::Result,
    path::Path,
    pin::Pin,
    task::{Context, Poll},
};

use async_trait::async_trait;

use super::ioutil::{PositionalRead, PositionalWrite, SequentialRead, SequentialWrite};

pub trait ReadDir {
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context) -> Poll<std::io::Result<Option<String>>>;
}

pub trait SequentialFile: SequentialRead + SequentialWrite {}

pub trait PositionalFile: PositionalRead + PositionalWrite {}

#[allow(dead_code)]
pub struct OpenOption {
    read: bool,
    write: bool,
}

#[async_trait]
pub trait Env {
    type ReadDir: ReadDir;
    type SequentialFile: SequentialFile;
    type PositionalFile: PositionalFile;

    fn spawn_background<F: Future<Output = ()> + Send + 'static>(&self, f: F) -> Result<()>;

    async fn path_exist<P: AsRef<Path>>(&self, path: P) -> Result<bool>;

    async fn make_dir<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    async fn read_dir<P: AsRef<Path>>(&self, path: P) -> Result<Self::ReadDir>;

    async fn lock_file<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    async fn unlock_file<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    async fn open_sequential_file<P: AsRef<Path>>(
        &self,
        path: P,
        opts: OpenOption,
    ) -> Result<Self::SequentialFile>;

    async fn open_positional_file<P: AsRef<Path>>(
        &self,
        p: Path,
        opts: OpenOption,
    ) -> Result<Self::PositionalFile>;
}
