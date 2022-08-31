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

use crate::{
    stream::types::{Entry, Sequence},
    Record,
};

pub enum TxnContext {
    Write {
        segment_epoch: u32,
        first_index: u32,
        acked_seq: Sequence,
        prev_acked_seq: Sequence,
        entries: Vec<Entry>,
    },
    Sealed {
        segment_epoch: u32,
        writer_epoch: u32,
        prev_epoch: Option<u32>,
    },
}

pub fn convert_to_txn_context(record: &Record) -> (u64, TxnContext) {
    if let Some(writer_epoch) = &record.writer_epoch {
        (
            record.stream_id,
            TxnContext::Sealed {
                segment_epoch: record.epoch,
                writer_epoch: *writer_epoch,
                prev_epoch: None,
            },
        )
    } else {
        (
            record.stream_id,
            TxnContext::Write {
                segment_epoch: record.epoch,
                first_index: record.first_index(),
                acked_seq: record.acked_seq().into(),
                prev_acked_seq: Sequence::new(0, 0),
                entries: record.entries.iter().cloned().map(Into::into).collect(),
            },
        )
    }
}
