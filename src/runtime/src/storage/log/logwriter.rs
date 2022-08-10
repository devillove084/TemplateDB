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

use std::fs::File;

use crate::stream::error::{IOResult, Result};

pub struct LogWriter {
    log_number: u64,
    /// The number of block this file already used (exclusive the last partial)
    num_block: usize,

    /// The offset of first avail byte in a block
    block_offset: usize,

    /// The maximum allowed bytes of this file
    max_file_size: usize,

    synced_offset: usize,
}

impl LogWriter {
    pub fn new(mut file: File, initial_offset: usize, max_file_size: usize) -> IOResult<LogWriter> {
        todo!()
    }

    pub fn add_record(&mut self, content: &[u8]) -> Result<()> {
        todo!()
    }

    pub fn fill_entire_avail_space(&mut self) -> Result<()> {
        todo!()
    }

    pub fn flush(&mut self) -> Result<()> {
        todo!()
    }

    pub fn log_number(&self) -> u64 {
        todo!()
    }

    pub fn avail_space(&self) -> usize {
        todo!()
    }

    pub fn block_avail_space(&self) -> usize {
        todo!()
    }

    fn switch_block(&mut self, sync_data: bool) -> Result<()> {
        todo!()
    }
}

impl Drop for LogWriter {
    fn drop(&mut self) {
        // Align the last block, so that reader would recognize the old records.
        if self.block_offset > 0 {
            if let Err(err) = self.switch_block(false) {
                return;
            }
        }
        if let Err(err) = self.flush() {
            // TODO: Tracing the error
        }
    }
}
