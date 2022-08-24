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
    collections::{BTreeMap, HashMap, VecDeque},
    io::ErrorKind,
    mem::{swap, replace}, ops::Range,
};

use log::warn;

use super::{txn::TxnContext, version::StreamVersion, memtable::MemtableIter};
use crate::{
    storage::log::manager::ReleaseReferringLogFile,
    stream::{error::{IOKindResult, Result}, types::Sequence},
    Entry,
};

pub(crate) struct PartialStream<R> {
    stream_id: u64,
    version: StreamVersion,
    /// segment epoch => writer epoch
    sealed: HashMap<u32, u32>,
    /// log file number => memtable, there ensure that entries in
    /// stabled_tables are not overlapped, and no empty table exists.
    stabled_tables: HashMap<u64, BTreeMap<Sequence, Entry>>,
    active_table: Option<(u64, BTreeMap<Sequence, Entry>)>,

    /// all previous entries (exclusive) are not accessable
    initial_seq: Sequence,
    /// all previous entries (inclusive) are acked.
    acked_seq: Sequence,
    log_file_releaser: R,
}

impl<R: ReleaseReferringLogFile> PartialStream<R> {
    pub fn new(version: StreamVersion, log_file_releaser: R) -> Self {
        let stream_id = version.stream_id;
        let stream_meta = &version.stream_meta;
        let acked_seq = stream_meta.acked_seq.into();
        let initial_seq = stream_meta.initial_seq.into();
        let sealed = stream_meta
            .replicas
            .iter()
            .filter(|r| r.promised_epoch.is_some())
            .map(|r| (r.epoch, r.promised_epoch.unwrap()))
            .collect::<HashMap<_, _>>();

        PartialStream {
            stream_id,
            version,
            sealed,
            stabled_tables: HashMap::new(),
            active_table: None,
            initial_seq,
            acked_seq,
            log_file_releaser,
        }
    }

    pub fn acked_seq(&self) -> Sequence {
        self.acked_seq
    }

    pub fn sealed_epoches(&self) -> HashMap<u32, u32> {
        self.sealed.clone()
    }

    fn truncate_entries(&mut self) {
        let initial_seq = self.initial_seq;
        for mem_table in self.stabled_tables.values_mut() {
            let mut left = mem_table.split_off(&initial_seq);
            swap(&mut left, mem_table);
        }
        let recycled_logs = self
            .stabled_tables
            .drain_filter(|_, memtable| memtable.is_empty())
            .map(|v| v.0)
            .collect::<Vec<_>>();
        // TODO: make release async
        for ln in recycled_logs {
            self.log_file_releaser.release(self.stream_id, ln);
        }
    }

    pub fn refresh_versions(&mut self) {
        if !self.version.try_applt_edits() {
            return;
        }

        // Might update local initial seq, and release useless entries
        let actual_initial_seq = self.version.stream_meta.initial_seq.into();
        if self.initial_seq < actual_initial_seq {
            self.initial_seq = actual_initial_seq;
            self.truncate_entries();
        }
    }

    fn reject_staled(&mut self, segment_epoch: u32, writer_epoch: u32) -> IOKindResult<()> {
        if segment_epoch < self.initial_seq.epoch {
            warn!(
                "stream {} seg {} reject staled request, initial epoch is {}",
                self.stream_id, segment_epoch, self.initial_seq.epoch
            );
            return Err(ErrorKind::Other);
        }

        if let Some(sealed_epoch) = self.sealed.get(&segment_epoch) {
            if writer_epoch < *sealed_epoch {
                warn!("stream {} seg {} reject staled request, writer epoch is {}, sealed epoch is {}", self.stream_id, segment_epoch, writer_epoch, sealed_epoch);
                return Err(ErrorKind::Other);
            }
        }
        Ok(())
    }

    pub fn write(
        &mut self,
        writer_epoch: u32,
        segment_epoch: u32,
        acked_seq: Sequence,
        first_index: u32,
        entries: Vec<Entry>,
    ) -> IOKindResult<Option<TxnContext>> {
        self.refresh_versions();
        self.reject_staled(segment_epoch, writer_epoch)?;

        if entries.is_empty() && self.acked_seq >= acked_seq {
            return Ok(None);
        }

        let prev_acked_seq = self.acked_seq;
        self.acked_seq = self.acked_seq.max(acked_seq);
        Ok(Some(TxnContext::Write {
            segment_epoch,
            first_index,
            acked_seq,
            prev_acked_seq,
            entries,
        }))
    }

    pub fn seal(
        &mut self,
        segment_epoch: u32,
        writer_epoch: u32,
    ) -> IOKindResult<Option<TxnContext>> {
        self.refresh_versions();
        self.reject_staled(segment_epoch, writer_epoch)?;

        let prev_epoch = self.sealed.get(&segment_epoch).cloned();
        if prev_epoch.map(|e| e == writer_epoch).unwrap_or_default() {
            Ok(None)
        } else {
            self.sealed.insert(segment_epoch, writer_epoch);
            Ok(Some(TxnContext::Sealed {
                segment_epoch,
                writer_epoch,
                prev_epoch,
            }))
        }
    }

    fn commit_write_txn(
        &mut self,
        log_number: u64,
        segment_epoch: u32,
        first_index: u32,
        entries: Vec<Entry>,
    ) {
        let mut delta_table = BTreeMap::new();
        for (offset, entry) in entries.into_iter().enumerate() {
            let seq = Sequence::new(segment_epoch, first_index + offset as u32);
            delta_table.insert(seq, entry);
        }

        if self.active_table.is_none() {
            self.active_table = Some((log_number, delta_table));
            return ;
        }

        let (active_log_number, active_mem_table) = self.active_table.as_mut().unwrap();
        if *active_log_number == log_number {
            active_mem_table.append(&mut delta_table);
            return ;
        }

        // switch memtable because log file is switched
        for mem_table in self.stabled_tables.values_mut() {
            mem_table.drain_filter(|seq, _| active_mem_table.contains_key(seq));
        }

        for (log_number, _) in self.stabled_tables.drain_filter(|_, mem_table| mem_table.is_empty()) {
            self.log_file_releaser.release(self.stream_id, log_number);
        }

        self.stabled_tables.insert(*active_log_number, replace(active_mem_table, delta_table));
        *active_log_number = log_number;
    }

    pub fn commit(&mut self, log_number: u64, txn: TxnContext) {
        match txn {
            TxnContext::Write {
                segment_epoch,
                first_index,
                acked_seq,
                entries,
                ..
            } => {
                self.commit_write_txn(log_number, segment_epoch, first_index, entries);
                if self.acked_seq < acked_seq {
                    self.acked_seq = acked_seq;
                }
            }
            TxnContext::Sealed {
                segment_epoch,
                writer_epoch,
                ..
            } => {
                if !self
                    .sealed
                    .get(&segment_epoch)
                    .map(|e| writer_epoch < *e)
                    .unwrap_or_default()
                {
                    self.sealed.insert(segment_epoch, writer_epoch);
                }
            }
        }
    }

    pub fn rollback(&mut self, txn: TxnContext) {
        match txn {
            TxnContext::Write { prev_acked_seq, .. } => {
                self.acked_seq = prev_acked_seq;
            },
            TxnContext::Sealed { segment_epoch, prev_epoch, .. } => {
                if let Some(prev_epoch) = prev_epoch {
                    self.sealed.insert(segment_epoch, prev_epoch);
                } else {
                    self.sealed.remove(&segment_epoch);
                }
            },
        }
    }

    pub fn acked_index(&self, epoch: u32) -> u32 {
        match self.acked_seq.epoch.cmp(&epoch) {
            std::cmp::Ordering::Less => 0,
            std::cmp::Ordering::Equal => self.acked_seq.index,
            std::cmp::Ordering::Greater => {
                // found the maximum entry of the corresponding epoch
                todo!()
            },
        }
    }

    fn seek(&self, start_seq: Sequence) -> MemtableIter {
        let mut iters = self.stabled_tables.iter().map(|(_, m)| m.range(start_seq..).peekable()).collect::<Vec<_>>();
        if let Some((_, m)) = &self.active_table {
            iters.push(m.range(start_seq..).peekable());
        }
        MemtableIter::new(start_seq, iters)
    }

    pub fn continuous_index(&self, epoch: u32, hole: Range<u32>) -> u32 {
        let mut index = if hole.start == 1 {
            debug_assert!(hole.end > 0);
            hole.end - 1
        } else {
            0
        };

        let iter = self.seek(Sequence::new(epoch, 0));
        for (seq, _) in iter {
            if seq.epoch != epoch || seq.index != index + 1 {
                break;
            }
            index += 1;
            if hole.start == index + 1 {
                index = hole.end - 1;
            }
        }
        index
    }

    pub fn scan_entries(&self, segment_epoch: u32, first_index: u32, limit: usize, require_acked: bool) -> Result<Option<VecDeque<(u32, Entry)>>> {
        todo!()
    }
}
