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
    convert::TryInto,
    fs::File,
    io::{ErrorKind, Read, Seek, SeekFrom},
};

use super::format::{
    MAX_BLOCK_SIZE, PAGE_SIZE, RECORD_BAD_CRC32, RECORD_BAD_LENGTH, RECORD_EMPTY, RECORD_FULL,
    RECORD_HEAD, RECORD_HEADER_SIZE, RECORD_MID, RECORD_PAGE_ALIGN,
};
use crate::{
    storage::log::format::RECORD_ZERO,
    stream::error::{Error, Result},
};

pub struct LogReader {
    log_number: u64,
    file: File,
    checksum: bool,
    eof: bool,
    consumed_bytes: usize,
    next_read_offset: usize,
    next_record_offset: usize,
    buf_start: usize,
    buf_size: usize,
    buffer: Box<[u8; MAX_BLOCK_SIZE]>,
}

impl LogReader {
    pub fn new(mut file: File, log_number: u64, checksum: bool) -> Result<LogReader> {
        file.seek(SeekFrom::Start(0))?;
        Ok(LogReader {
            log_number,
            file,
            checksum,
            eof: false,
            consumed_bytes: 0,
            next_read_offset: 0,
            next_record_offset: 0,
            buf_start: 0,
            buf_size: 0,
            buffer: Box::new([0u8; MAX_BLOCK_SIZE]),
        })
    }

    pub fn next_record_offset(&self) -> usize {
        self.next_record_offset
    }

    pub fn read_record(&mut self) -> Result<Option<Vec<u8>>> {
        let mut in_frgmented_record = false;
        let mut content = vec![];
        loop {
            let (kind, record) = match self.read_physical_record()? {
                Some((k, r)) => (k, r),
                None => return Ok(None),
            };

            match kind {
                RECORD_FULL => {
                    if in_frgmented_record {
                        return Err(Error::Corruption(
                            "partial record without end(1)".to_string(),
                        ));
                    }
                    self.next_record_offset = self.consumed_bytes + self.buf_start;
                    return Ok(Some(record));
                }
                RECORD_HEAD => {
                    if in_frgmented_record {
                        return Err(Error::Corruption(
                            "partial record without end(2)".to_string(),
                        ));
                    }
                    in_frgmented_record = true;
                    content.extend_from_slice(&record);
                }
                RECORD_MID => {
                    if !in_frgmented_record {
                        return Err(Error::Corruption(
                            "missing header of fragmented record(1)".to_string(),
                        ));
                    }
                    content.extend_from_slice(&record)
                }
                RECOED_TAIL => {
                    if !in_frgmented_record {
                        return Err(Error::Corruption(
                            "missing header of fragmented record(2)".to_string(),
                        ));
                    }
                    content.extend_from_slice(&record);
                    self.next_record_offset = self.consumed_bytes + self.buf_start;
                    return Ok(Some(content));
                }
                RECORD_BAD_LENGTH | RECORD_BAD_CRC32 => {
                    return Err(Error::Corruption(format!(
                        "fragmented {}, type {}",
                        in_frgmented_record, kind
                    )))
                }
                _ => {
                    return Err(crate::stream::error::Error::Corruption(format!(
                        "unknown record type {}",
                        kind
                    )));
                }
            }
        }
    }

    fn read_physical_record(&mut self) -> Result<Option<(u8, Vec<u8>)>> {
        loop {
            if self.buf_size < RECORD_HEADER_SIZE {
                if !self.eof {
                    // Skip the trailing padding, if buf_size is not zero.
                    self.read_block()?;
                    continue;
                } else {
                    // We have meet a truncated header in end of file.
                    self.buf_size = 0;
                    return Ok(None);
                }
            }
            let mut buf = &self.buffer[self.buf_start..];
            let kind = buf[0];
            if kind == RECORD_EMPTY {
                return Ok(None);
            }

            if kind == RECORD_PAGE_ALIGN {
                let next_page_boundary = (self.buf_start + PAGE_SIZE) & !(PAGE_SIZE - 1);
                if self.buf_start + self.buf_size < next_page_boundary {
                    debug_assert!(self.eof);
                    return Ok(None);
                }
                self.buf_size -= next_page_boundary - self.buf_start;
                self.buf_start = next_page_boundary;
                continue;
            }

            debug_assert!(RECORD_HEAD <= kind && kind <= RECORD_ZERO);
            let log_number = buf[1];
            assert!(log_number <= self.log_number as u8);
            if log_number < self.log_number as u8 {
                // this is a recycled log file, now all records are consumed.
                return Ok(None);
            }
            let size = u16::from_le_bytes(buf[2..4].try_into().unwrap()) as usize;
            let crc32 = u32::from_le_bytes(buf[4..8].try_into().unwrap());

            buf = &buf[RECORD_HEADER_SIZE..];
            if size + RECORD_HEADER_SIZE > self.buf_size {
                if !self.eof {
                    return Ok(Some((RECORD_BAD_LENGTH, vec![])));
                } else {
                    return Ok(None);
                }
            }
            let content = &buf[..size];
            self.buf_start += size + RECORD_HEADER_SIZE;
            self.buf_size -= size + RECORD_HEADER_SIZE;
            if kind == RECORD_ZERO {
                continue;
            }
            if self.checksum && crc32fast::hash(content) != crc32 {
                return Ok(Some((RECORD_BAD_CRC32, vec![])));
            }
            return Ok(Some((kind, content.to_owned())));
        }
    }

    fn read_block(&mut self) -> Result<()> {
        self.buf_start = 0;
        self.buf_size = 0;

        while self.buf_size < MAX_BLOCK_SIZE {
            let read_size = match self.file.read(&mut self.buffer[self.buf_size..]) {
                Ok(size) => size,
                Err(err) => {
                    if err.kind() == ErrorKind::Interrupted {
                        continue;
                    }
                    return Err(err.into());
                }
            };
            if read_size == 0 {
                self.eof = true;
                break;
            }
            self.buf_size += read_size;
        }
        self.consumed_bytes = self.next_read_offset;
        self.next_record_offset += self.buf_size;
        Ok(())
    }
}
