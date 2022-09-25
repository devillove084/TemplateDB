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

use std::{os::unix::fs::FileExt, path::PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::list::FileLister;
use crate::{
    error::Result,
    file::store_trait::{Bucket, FileDesc, Lister, RandomRead, SequentialRead, SequentialWrite},
};

pub struct FileSystemBucket {
    path: PathBuf,
}

impl FileSystemBucket {
    pub(crate) fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

#[async_trait::async_trait]
impl Bucket for FileSystemBucket {
    async fn list_files(&self) -> Result<Box<dyn Lister<Item = FileDesc>>> {
        let dir = tokio::fs::read_dir(&self.path).await?;
        Ok(Box::new(FileLister::new(dir)))
    }

    async fn new_random_reader(&self, name: &str) -> Result<Box<dyn RandomRead>> {
        let path = self.path.join(name);
        let file = tokio::fs::File::open(&path).await?;
        Ok(Box::new(FileSystemRandomReader {
            file: file.into_std().await,
        }))
    }

    async fn new_sequential_reader(&self, name: &str) -> Result<Box<dyn SequentialRead>> {
        let path = self.path.join(name);
        let file = tokio::fs::File::open(&path).await?;
        Ok(Box::new(FileSystemSequentialReader { file }))
    }

    async fn new_sequential_writer(&self, name: &str) -> Result<Box<dyn SequentialWrite>> {
        let path = self.path.join(name);
        let file = tokio::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .await?;
        Ok(Box::new(FileSystemSequentialWriter { file }))
    }
}

pub struct FileSystemRandomReader {
    file: std::fs::File,
}

#[async_trait::async_trait]
impl RandomRead for FileSystemRandomReader {
    async fn read_at(&self, buf: &mut [u8], offset: usize) -> Result<usize> {
        let size = self.file.read_at(buf, offset as u64)?;
        Ok(size)
    }

    async fn read_exact_at(&self, buf: &mut [u8], offset: usize) -> Result<()> {
        self.file.read_exact_at(buf, offset as u64)?;
        Ok(())
    }
}

pub struct FileSystemSequentialReader {
    file: tokio::fs::File,
}

#[async_trait::async_trait]
impl SequentialRead for FileSystemSequentialReader {
    async fn read(&mut self, buf: &mut [u8]) -> Result<usize> {
        let size = self.file.read(buf).await?;
        Ok(size)
    }

    async fn read_exact(&mut self, buf: &mut [u8]) -> Result<()> {
        self.file.read_exact(buf).await?;
        Ok(())
    }
}

pub struct FileSystemSequentialWriter {
    file: tokio::fs::File,
}

#[async_trait::async_trait]
impl SequentialWrite for FileSystemSequentialWriter {
    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.file.write_all(buf).await?;
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        self.file.sync_all().await?;
        Ok(())
    }
}
