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
    pin::Pin,
    task::{Context, Poll},
};

use tokio::fs::File;

use crate::bwtree::error::Result;

pub trait SequentialRead {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>>;
}

pub trait SequentialWrite {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: Context,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>>;
}

pub trait SequentialWriteExt: SequentialWrite {
    fn write_all(&self, buf: &[u8]) -> WriteAll {
        WriteAll {}
    }
}

pub struct WriteAll {}

impl<T: SequentialWrite + ?Sized> SequentialWriteExt for T {}

impl Future for WriteAll {
    type Output = std::io::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

pub trait PositionalRead {
    fn poll_read_at(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
        offset: u64,
    ) -> Poll<std::io::Result<usize>>;
}

pub trait PositionalReadExt: PositionalRead {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> ReadAt {
        ReadAt {}
    }

    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> ReadExactAt {
        ReadExactAt {}
    }
}

impl<T: PositionalRead + ?Sized> PositionalReadExt for T {}

pub struct ReadAt {}

impl Future for ReadAt {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

pub trait PositionalWrite {
    fn poll_write_at(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
        offset: u64,
    ) -> Poll<Result<usize>>;
}

pub struct ReadExactAt {}

impl Future for ReadExactAt {
    type Output = std::io::Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        todo!()
    }
}

pub struct SequentialFile {
    file: File,
}

impl SequentialRead for SequentialFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        todo!()
        //Poll::Ready(self.file.read(buf))
    }
}

impl SequentialWrite for SequentialFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: Context,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        todo!()
    }
}

pub struct PositionalFile {
    file: File,
}

impl PositionalRead for PositionalFile {
    fn poll_read_at(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &mut [u8],
        offset: u64,
    ) -> Poll<std::io::Result<usize>> {
        todo!()
    }
}

impl PositionalWrite for PositionalFile {
    fn poll_write_at(
        self: Pin<&mut Self>,
        cx: &mut Context,
        buf: &[u8],
        offset: u64,
    ) -> Poll<Result<usize>> {
        todo!()
    }
}
