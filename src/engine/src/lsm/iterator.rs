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
    cmp::{Ordering, Reverse},
    collections::{BTreeSet, BinaryHeap},
    ops::Bound::*,
    sync::Arc,
};

use super::{
    table::{
        format::{Key, Timestamp},
        table::{TableIter, TableReader},
    },
    version::{FileMetadata, OrdByUpperBound},
};
use crate::{error::Result, file::store_trait::Store as FileStore};

pub struct LevelIter {
    tenant: String,
    bucket: String,
    local_store: Arc<dyn FileStore>,

    manifest_file_iter: ManifestIter,
    current_file: Option<FileMetadata>,
    current_iter: Option<TableIter>,

    init: bool,
}

impl LevelIter {
    pub async fn new(
        tenant: &str,
        bucket: &str,
        manifest_file_iter: ManifestIter,
        local_store: Arc<dyn FileStore>,
    ) -> Result<Self> {
        let mut iter = Self {
            tenant: tenant.to_owned(),
            bucket: bucket.to_owned(),
            manifest_file_iter,
            current_file: None,
            current_iter: None,
            local_store,
            init: false,
        };
        iter.seek_to_first().await?;
        Ok(iter)
    }

    pub fn key(&self) -> Option<Key<'_>> {
        debug_assert!(self.valid());
        self.current_iter.as_ref().map(|e| e.key())
    }

    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid());
        self.current_iter.as_ref().unwrap().value()
    }

    pub fn valid(&self) -> bool {
        !self.init
            || (self.current_file.is_some()
                && self.current_iter.is_some()
                && self.current_iter.as_ref().unwrap().valid())
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        self.manifest_file_iter.seek_to_first();
        if !self.manifest_file_iter.valid() {
            return Ok(());
        }
        self.current_file = Some(self.manifest_file_iter.value());
        let file_metadata = self.current_file.as_ref().unwrap();
        self.current_iter = Some(
            self.open_table_iter(&file_metadata.name, file_metadata.file_size as usize)
                .await?,
        );
        self.current_iter.as_mut().unwrap().seek_to_first().await?;
        if !self.init {
            self.init = true;
        }
        Ok(())
    }

    pub async fn seek(&mut self, target: Key<'_>) -> Result<()> {
        self.manifest_file_iter.seek(target);
        if !self.manifest_file_iter.valid() {
            return Ok(());
        }
        self.set_current_tbl_iter().await?;
        self.current_iter.as_mut().unwrap().seek(target).await?;
        if !self.init {
            self.init = true;
        }
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        if !self.init {
            self.manifest_file_iter.seek_to_first();
            if !self.manifest_file_iter.valid() {
                return Ok(());
            }
            self.set_current_tbl_iter().await?;
            self.current_iter.as_mut().unwrap().seek_to_first().await?;
        } else if !self.current_iter.as_ref().unwrap().valid() {
            self.manifest_file_iter.next();
            if !self.manifest_file_iter.valid() {
                return Ok(());
            }
            self.set_current_tbl_iter().await?;
            self.current_iter.as_mut().unwrap().seek_to_first().await?;
        } else {
            self.current_iter.as_mut().unwrap().next().await?;
        }
        Ok(())
    }

    async fn set_current_tbl_iter(&mut self) -> Result<()> {
        self.current_file = Some(self.manifest_file_iter.value());
        let f = self.current_file.as_ref().unwrap();
        self.current_iter = Some(self.open_table_iter(&f.name, f.file_size as usize).await?);
        Ok(())
    }

    async fn open_table_iter(&self, file: &str, file_size: usize) -> Result<TableIter> {
        let r = self
            .local_store
            .tenant(&self.tenant)
            .bucket(&self.bucket)
            .new_random_reader(file)
            .await?;
        let tr = TableReader::open(r.into(), file_size).await?;
        Ok(tr.iter())
    }
}

pub struct ManifestIter {
    files: BTreeSet<OrdByUpperBound>,
    current: Option<OrdByUpperBound>,
    init: bool,
}

impl ManifestIter {
    pub fn new(files: BTreeSet<OrdByUpperBound>) -> Self {
        let mut last_upper_bound = None;
        for f in &files {
            if let Some(last) = last_upper_bound {
                assert!(f.lower_bound > last)
            }
            last_upper_bound = Some(f.upper_bound.to_owned());
        }
        Self {
            files,
            current: None,
            init: false,
        }
    }

    pub fn key(&self) -> Key<'_> {
        debug_assert!(self.valid());
        self.current.as_ref().unwrap().upper_bound.as_slice().into()
    }

    pub fn value(&self) -> FileMetadata {
        debug_assert!(self.valid());
        self.current.as_ref().cloned().unwrap().0
    }

    pub fn valid(&self) -> bool {
        !self.init || self.current.is_some()
    }

    pub fn seek_to_first(&mut self) {
        self.current = self.files.first().cloned();
        if !self.init {
            self.init = true;
        }
    }

    pub fn seek(&mut self, target: Key<'_>) {
        self.current = self
            .files
            .range((
                Included(OrdByUpperBound(FileMetadata {
                    upper_bound: target.to_owned(),
                    ..Default::default()
                })),
                Unbounded,
            ))
            .next()
            .cloned();
        if !self.init {
            self.init = true;
        }
    }

    pub fn next(&mut self) {
        if !self.init {
            self.seek_to_first();
            self.init = true
        }
        self.current = self
            .files
            .range((
                Excluded(OrdByUpperBound(FileMetadata {
                    upper_bound: self.current.as_ref().unwrap().upper_bound.to_owned(),
                    ..Default::default()
                })),
                Unbounded,
            ))
            .next()
            .cloned()
    }
}

pub struct MergingIterator {
    pub levels: Vec<LevelIter>,
    pub iter_heap: BinaryHeap<Reverse<HeapItem>>,

    pub init: bool,
    pub snapshot: Option<Timestamp>,
}

impl MergingIterator {
    pub fn new(levels: Vec<LevelIter>, snapshot: Option<u64>) -> Self {
        let iter_heap = BinaryHeap::with_capacity(levels.len());
        Self {
            levels,
            iter_heap,
            init: false,
            snapshot,
        }
    }

    pub fn key(&self) -> Key<'_> {
        debug_assert!(self.valid());
        let (k, _) = self.current_entry().unwrap();
        k
    }

    pub fn value(&self) -> &[u8] {
        debug_assert!(self.valid());
        let (_, v) = self.current_entry().unwrap();
        v
    }

    pub fn valid(&self) -> bool {
        if !self.init {
            return true;
        }
        if let Some(iter) = self.iter_heap.peek() {
            return self.levels[iter.0.index].valid();
        }
        false
    }

    pub async fn seek_to_first(&mut self) -> Result<()> {
        for l in self.levels.iter_mut() {
            l.seek_to_first().await?;
        }
        self.init_heap();
        if !self.init {
            self.init = true
        }
        self.seek_to_next_visible().await?;
        Ok(())
    }

    pub async fn seek(&mut self, target: Key<'_>) -> Result<()> {
        for l in self.levels.iter_mut() {
            l.seek(target).await?;
        }
        self.init_heap();
        if !self.init {
            self.init = true
        }
        self.seek_to_next_visible().await?;
        Ok(())
    }

    pub async fn next(&mut self) -> Result<()> {
        if !self.init {
            self.seek_to_first().await?;
            return Ok(());
        }

        self.next_entry().await?;
        self.seek_to_next_visible().await?;
        Ok(())
    }

    fn init_heap(&mut self) {
        self.iter_heap.clear();
        for (index, iter) in self.levels.iter().enumerate() {
            if !iter.valid() {
                continue;
            }
            if let Some(key) = iter.key() {
                self.iter_heap.push(Reverse(HeapItem {
                    index,
                    key: key.to_owned(),
                }));
            }
        }
    }

    async fn next_entry(&mut self) -> Result<()> {
        let prev_smallest = self.iter_heap.peek();
        if prev_smallest.is_none() {
            return Ok(());
        }

        let prev_level_iter = &mut self.levels[prev_smallest.unwrap().0.index];
        prev_level_iter.next().await?;

        if prev_level_iter.valid() {
            if !prev_level_iter.valid() {
                print!("")
            }
            self.iter_heap.peek_mut().unwrap().0.key = prev_level_iter.key().unwrap().to_owned();
            let item = self.iter_heap.pop().unwrap();
            self.iter_heap.push(item)
        } else {
            self.iter_heap.pop();
        }
        Ok(())
    }

    async fn seek_to_next_visible(&mut self) -> Result<()> {
        while !self.iter_heap.is_empty() {
            let level = &self.iter_heap.peek().unwrap().0;
            let key = Key::from(level.key.as_slice());

            if self.check_key_visible(key) {
                return Ok(());
            }
            self.next_entry().await?;
        }
        Ok(())
    }

    fn check_key_visible(&self, key: Key<'_>) -> bool {
        if let Some(snapshot) = self.snapshot {
            return key.ts() <= snapshot;
        }
        true
    }

    fn current_entry(&self) -> Option<(Key<'_>, &[u8])> {
        if self.iter_heap.is_empty() {
            return None;
        }
        if let Some(item) = self.iter_heap.peek() {
            let l = &self.levels[item.0.index];
            let key = l.key()?;
            return Some((key, l.value()));
        }
        None
    }
}

#[derive(Eq)]
pub struct HeapItem {
    index: usize,
    key: Vec<u8>,
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        Key::from(self.key.as_slice()).cmp(&Key::from(other.key.as_slice()))
    }
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
