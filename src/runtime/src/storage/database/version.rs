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
    collections::{HashMap, HashSet},
    io::{Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::Arc,
};

use prost::Message;
use tokio::sync::Mutex;

use crate::{
    manifest::{RecycleLog, StreamMeta},
    storage::{
        log::logwriter::LogWriter,
        util::{
            create_new_manifest, parse_current_file, parse_file_name, recover_manifest,
            AtomicArcPtr,
        },
    },
    stream::error::{Error, Result},
};

pub const MIN_AVAIL_LOG_NUMBER: u64 = 1;
pub const MAX_DESCRIPTOR_FILE_SIZE: usize = 4 * 1024 * 1024;

// TODO: Maybe linkedlist is not the best choice!
#[derive(Clone, Default)]
pub struct VersionEdit {
    raw_edit: crate::manifest::VersionEdit,
    next_edit: AtomicArcPtr<VersionEdit>,
}

impl VersionEdit {
    fn encode_to_vec(&self) -> Vec<u8> {
        self.raw_edit.encode_to_vec()
    }
}

#[derive(Clone)]
pub struct LogNumberRecord {
    /// The min useful log number. All log files with
    /// small log small would be released safety.
    ///
    /// DEFAULT: [`MIN_AVAIL_LOG_NUMBER`]
    pub min_log_number: u64,

    pub recycled_log_number: HashSet<u64>,
}

impl Default for LogNumberRecord {
    fn default() -> Self {
        LogNumberRecord {
            min_log_number: MIN_AVAIL_LOG_NUMBER,
            recycled_log_number: HashSet::default(),
        }
    }
}

impl LogNumberRecord {
    pub fn is_log_recycled(&self, log_number: u64) -> bool {
        log_number < self.min_log_number || self.recycled_log_number.contains(&log_number)
    }
}

pub struct StreamVersion {
    pub stream_id: u64,
    pub log_num_record: LogNumberRecord,
    pub stream_meta: StreamMeta,
    next_edit: AtomicArcPtr<VersionEdit>,
}

impl StreamVersion {
    pub fn new(stream_id: u64) -> Self {
        let stream_meta = StreamMeta {
            stream_id,
            acked_seq: 0,
            initial_seq: 0,
            replicas: vec![],
        };
        StreamVersion {
            stream_id,
            stream_meta,
            log_num_record: LogNumberRecord::default(),
            next_edit: AtomicArcPtr::default(),
        }
    }

    pub fn is_log_recycled(&self, log_number: u64) -> bool {
        self.log_num_record.is_log_recycled(log_number)
    }

    pub fn try_applt_edits(&mut self) -> bool {
        todo!()
        //VersionBuilder::try_applt_edits_about_stream(self)
    }
}

#[derive(Clone, Default)]
pub struct Version {
    pub log_number_record: LogNumberRecord,
    pub streams: HashMap<u64, crate::manifest::StreamMeta>,
    next_edit: AtomicArcPtr<VersionEdit>,
}

impl Version {
    pub fn stream_version(&self, stream_id: u64) -> StreamVersion {
        let stream_meta = if let Some(stream_meta) = self.streams.get(&stream_id).cloned() {
            stream_meta
        } else {
            StreamMeta {
                stream_id,
                acked_seq: 0,
                initial_seq: 0,
                replicas: vec![],
            }
        };

        StreamVersion {
            stream_id,
            log_num_record: self.log_number_record.clone(),
            stream_meta,
            next_edit: self.next_edit.clone(),
        }
    }

    pub fn is_log_recycled(&self, log_number: u64) -> bool {
        self.log_number_record.is_log_recycled(log_number)
    }

    pub fn try_applt_edits(&mut self) -> bool {
        todo!()
    }

    fn install_edit(&mut self, mut edit: Box<VersionEdit>) {
        loop {
            self.try_applt_edits();
            match self.next_edit.compare_store(edit) {
                Ok(()) => return,
                Err(e) => edit = e,
            }
        }
    }

    fn snapshot(&self) -> crate::manifest::VersionEdit {
        crate::manifest::VersionEdit {
            streams: self.streams.values().cloned().collect(),
            min_log_number: if self.log_number_record.min_log_number == MIN_AVAIL_LOG_NUMBER {
                None
            } else {
                Some(self.log_number_record.min_log_number)
            },
            recycled_logs: self
                .log_number_record
                .recycled_log_number
                .iter()
                .map(|ln| RecycleLog {
                    log_number: *ln,
                    updated_streams: Default::default(),
                })
                .collect(),
        }
    }
}

struct VersionSetCore {
    base_dir: PathBuf,
    writer: LogWriter,

    // Recover from the maximum file
    next_file_number: u64,
    manifest_number: u64,
    version: Version,
}

impl VersionSetCore {
    // TODO: Make these func be async!!!!
    fn log_and_apply(&mut self, version_edit: Box<VersionEdit>) -> Result<()> {
        let content = version_edit.encode_to_vec();
        if self.writer.avail_space() < content.len() {
            self.writer.fill_entire_avail_space()?;
            self.writer.flush()?;
            self.writer =
                create_new_manifest(&self.base_dir, &self.version, self.next_file_number)?;
            self.next_file_number += 1;
        }

        self.writer.add_record(&content)?;
        self.writer.flush()?;
        self.version.install_edit(version_edit);

        Ok(())
    }
}

pub(crate) struct VersionSet {
    core: Arc<Mutex<VersionSetCore>>,
}

impl VersionSet {
    pub fn create<P: AsRef<Path>>(base_dir: P) -> Result<()> {
        let manifest_number = 1;
        let version = Version::default();
        create_new_manifest(base_dir, &version, manifest_number)?;
        Ok(())
    }

    pub fn recover<P: AsRef<Path>>(base_dir: P) -> Result<VersionSet> {
        // TODO: Can i unwrap directly here?
        let manifest = parse_current_file(&base_dir).unwrap();
        let manifest_number = match parse_file_name(&manifest).unwrap() {
            crate::storage::util::FileType::Manifest(num) => num,
            _ => return Err(Error::Corruption("Invalid MANIFEST file name".to_owned())),
        };

        let (initial_offset, version) = recover_manifest(&manifest).unwrap();
        let mut file = std::fs::File::options()
            .write(true)
            .open(&manifest)
            .unwrap();
        file.seek(SeekFrom::Start(initial_offset as u64)).unwrap();
        let writer = LogWriter::new(file, 0, MAX_DESCRIPTOR_FILE_SIZE).unwrap();
        Ok(VersionSet {
            core: Arc::new(Mutex::new(VersionSetCore {
                base_dir: base_dir.as_ref().to_owned(),
                writer,
                next_file_number: 0,
                manifest_number,
                version,
            })),
        })
    }

    pub async fn manifest_number(&self) -> u64 {
        self.core.lock().await.manifest_number
    }

    pub async fn current(&self) -> Version {
        self.core.lock().await.version.clone()
    }

    pub async fn set_next_file_number(&self, file_number: u64) {
        let mut core = self.core.lock().await;
        debug_assert!(core.next_file_number < file_number);
        core.next_file_number = file_number;
    }

    pub async fn truncate_stream(&self, stream_meta: StreamMeta) -> Result<()> {
        let mut core = self.core.lock().await;
        // Ensure there no any edit would be added before this one is finished.
        core.version.try_applt_edits();

        let stream_id = stream_meta.stream_id;
        if let Some(former) = core.version.streams.get(&stream_id) {
            match former.initial_seq.cmp(&stream_meta.initial_seq) {
                std::cmp::Ordering::Greater => {
                    return Err(Error::Staled(format!(
                        "stream {} has been truncated to {}",
                        stream_id, former.initial_seq
                    )));
                }
                std::cmp::Ordering::Equal => return Ok(()),
                _ => {}
            }
            // TODO: merge stream meta
        }

        let version_edit = Box::new(VersionEdit {
            raw_edit: crate::manifest::VersionEdit {
                streams: vec![stream_meta],
                recycled_logs: Default::default(),
                min_log_number: Default::default(),
            },
            next_edit: Default::default(),
        });
        core.log_and_apply(version_edit)
    }
}

pub(crate) struct VersionBuilder {
    version: Version,
}

impl VersionBuilder {
    pub fn apply(&mut self, edit: crate::manifest::VersionEdit) {
        todo!()
    }

    pub fn finalize(self) -> Version {
        todo!()
    }

    pub fn try_applt_edits(version: &mut Version) -> bool {
        todo!()
    }

    pub fn try_apply_edits_on_stream(version: &mut StreamVersion) -> bool {
        todo!()
    }

    fn apply_edit(version: &mut Version, edit: &crate::manifest::VersionEdit) {
        todo!()
    }

    fn apply_edits_on_stream(version: &mut StreamVersion, edit: &crate::manifest::VersionEdit) {
        todo!()
    }

    fn advance_min_log_number(record: &mut LogNumberRecord, edit: &crate::manifest::VersionEdit) {
        todo!()
    }

    fn merge_stream(
        stream_meta: &mut crate::manifest::StreamMeta,
        update: &mut crate::manifest::StreamMeta,
    ) {
        todo!()
    }
}
