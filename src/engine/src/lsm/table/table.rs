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

use std::sync::Arc;

use bytes::{Buf, BufMut};

use super::{
    block::{BlockBuilder, BlockHandle, BlockIter, ENCODED_SIZE},
    format::Key,
};
use crate::{
    error::{Error, Result},
    file::store_trait::{RandomRead, SequentialWrite},
};

#[derive(Default)]
pub struct TableDesc {
    pub table_size: usize,
    pub lower_bound: Vec<u8>,
    pub upper_bound: Vec<u8>,
}

pub struct TableBuilderOptions {
    pub block_size: usize,
}

impl Default for TableBuilderOptions {
    fn default() -> Self {
        Self { block_size: 8192 }
    }
}

pub struct TableBuilder {
    writer: FileWriter,
    options: TableBuilderOptions,
    lower_bound: Vec<u8>,
    upper_bound: Vec<u8>,
    data_block_builder: BlockBuilder,
    index_block_builder: BlockBuilder,
}

#[allow(dead_code)]
impl TableBuilder {
    pub fn new(writer: SequentialWriter, options: TableBuilderOptions) -> Self {
        Self {
            writer: FileWriter::new(writer),
            options,
            lower_bound: Vec::new(),
            upper_bound: Vec::new(),
            data_block_builder: BlockBuilder::default(),
            index_block_builder: BlockBuilder::default(),
        }
    }

    pub async fn add(&mut self, key: Key<'_>, value: &[u8]) -> Result<()> {
        if self.lower_bound.is_empty() {
            self.lower_bound = key.to_owned();
        }
        self.upper_bound = key.to_owned();
        self.data_block_builder.add(key.as_slice(), value);
        if self.data_block_builder.encoded_size() >= self.options.block_size as usize {
            self.finish_data_block().await?;
        }
        Ok(())
    }

    pub fn estimated_size(&self) -> usize {
        self.writer.offset()
            + self.data_block_builder.encoded_size()
            + self.index_block_builder.encoded_size()
    }

    pub async fn finish(mut self) -> Result<TableDesc> {
        self.finish_data_block().await?;
        self.finish_index_block().await?;
        self.writer.finish().await?;
        Ok(TableDesc {
            table_size: self.writer.offset(),
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
        })
    }

    async fn finish_data_block(&mut self) -> Result<()> {
        if self.data_block_builder.num_entries() > 0 {
            let block = self.data_block_builder.finish();
            let handle = self.writer.write_block(block).await?;
            self.data_block_builder.reset();
            let index_value = handle.encode_to_vec();
            self.index_block_builder
                .add(&self.upper_bound, &index_value);
        }
        Ok(())
    }

    async fn finish_index_block(&mut self) -> Result<()> {
        if self.index_block_builder.num_entries() > 0 {
            let block = self.index_block_builder.finish();
            let handle = self.writer.write_block(block).await?;
            self.index_block_builder.reset();
            let footer = TableFooter {
                index_handle: handle,
            };
            self.writer.write_footer(&footer).await?;
        }
        Ok(())
    }
}

type SequentialWriter = Box<dyn SequentialWrite>;

struct FileWriter {
    writer: SequentialWriter,
    offset: usize,
}

impl FileWriter {
    fn new(writer: SequentialWriter) -> Self {
        Self { writer, offset: 0 }
    }

    fn offset(&self) -> usize {
        self.offset
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        self.writer.write(buf).await?;
        self.offset += buf.len();
        Ok(())
    }

    async fn write_block(&mut self, block: &[u8]) -> Result<BlockHandle> {
        let handle = BlockHandle {
            offset: self.offset,
            length: block.len(),
        };
        self.write(block).await?;
        Ok(handle)
    }

    async fn write_footer(&mut self, footer: &TableFooter) -> Result<()> {
        let buf = footer.encode_to_vec();
        self.write(&buf).await
    }

    async fn finish(&mut self) -> Result<()> {
        self.writer.finish().await
    }
}

pub struct TableFooter {
    pub index_handle: BlockHandle,
}

//pub const ENCODED_SIZE: usize = ENCODED_SIZE;

impl TableFooter {
    pub fn encode_to<B: BufMut>(&self, buf: &mut B) {
        self.index_handle.encode_to(buf);
    }

    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ENCODED_SIZE);
        self.encode_to(&mut buf);
        buf
    }

    pub fn decode_from<B: Buf>(buf: &mut B) -> Result<Self> {
        if buf.remaining() >= ENCODED_SIZE {
            let index_handle = BlockHandle::decode_from(buf);
            Ok(Self { index_handle })
        } else {
            Err(Error::corrupted("table footer is too small"))
        }
    }
}

#[allow(dead_code)]
pub struct TableReader {
    reader: FileReader,
    index_block: Arc<[u8]>,
}

impl TableReader {
    pub async fn open(reader: RandomReader, table_size: usize) -> Result<Self> {
        let reader = FileReader::new(reader);
        let footer = reader.read_footer(table_size).await?;
        let index_block = reader.read_block(&footer.index_handle).await?;
        Ok(Self {
            reader,
            index_block,
        })
    }

    pub fn iter(&self) -> TableIter {
        let index_iter = BlockIter::new(self.index_block.clone());
        TableIter::new(self.reader.clone(), index_iter)
    }
}

pub struct TableIter {
    reader: FileReader,
    index_iter: BlockIter,
    block_iter: Option<BlockIter>,
}

impl TableIter {
    fn new(reader: FileReader, index_iter: BlockIter) -> Self {
        Self {
            reader,
            index_iter,
            block_iter: None,
        }
    }

    pub fn key(&self) -> Key<'_> {
        debug_assert!(self.valid());
        self.block_iter.as_ref().unwrap().key()
    }

    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid());
        self.block_iter.as_ref().unwrap().value()
    }

    pub fn valid(&self) -> bool {
        self.block_iter
            .as_ref()
            .map(|e| e.valid())
            .unwrap_or_default()
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.index_iter.seek_to_first();
        self.block_iter = if self.index_iter.valid() {
            let mut iter = self.read_block_iter().await?;
            iter.seek_to_first();
            Some(iter)
        } else {
            None
        };
        Ok(())
    }

    pub async fn seek(&mut self, key: Key<'_>) -> Result<()> {
        self.index_iter.seek(key);
        self.block_iter = if self.index_iter.valid() {
            let mut iter = self.read_block_iter().await?;
            iter.seek(key);
            Some(iter)
        } else {
            None
        };
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        if let Some(mut block_iter) = self.block_iter.take() {
            block_iter.next();
            if block_iter.valid() {
                self.block_iter = Some(block_iter);
            } else {
                self.index_iter.next();
                if self.index_iter.valid() {
                    let mut iter = self.read_block_iter().await?;
                    iter.seek_to_first();
                    self.block_iter = Some(iter);
                } else {
                    self.block_iter = None;
                }
            }
        }
        Ok(())
    }

    async fn read_block_iter(&mut self) -> Result<BlockIter> {
        let mut index_value = self.index_iter.value();
        let handle = BlockHandle::decode_from(&mut index_value);
        let block = self.reader.read_block(&handle).await?;
        Ok(BlockIter::new(block))
    }
}

type RandomReader = Arc<dyn RandomRead>;

#[derive(Clone)]
struct FileReader {
    reader: RandomReader,
}

impl FileReader {
    fn new(reader: RandomReader) -> Self {
        Self { reader }
    }

    async fn read_block(&self, handle: &BlockHandle) -> Result<Arc<[u8]>> {
        let mut buf = vec![0u8; handle.length as usize];
        self.reader.read_exact_at(&mut buf, handle.offset).await?;
        Ok(buf.into())
    }

    async fn read_footer(&self, table_size: usize) -> Result<TableFooter> {
        let mut buf = [0; ENCODED_SIZE];
        let offset = table_size - buf.len();
        self.reader.read_exact_at(&mut buf, offset).await?;
        TableFooter::decode_from(&mut buf.as_slice())
    }
}
