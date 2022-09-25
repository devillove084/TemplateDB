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
    ffi::OsStr,
    fs::File,
    os::unix::prelude::OsStrExt,
    path::{Path, PathBuf},
    sync::{atomic::AtomicPtr, Arc},
};

use log::error;
use prost::Message;

use super::{
    database::{
        dblayout::DBLayout,
        dboption::DBOption,
        tributary::PartialStream,
        txn::{convert_to_txn_context, TxnContext},
        version::{Version, VersionBuilder, MAX_DESCRIPTOR_FILE_SIZE},
    },
    fs::FileExt,
    log::{
        logreader::LogReader,
        logwriter::LogWriter,
        manager::{LogEngine, LogFileManager},
    },
};
use crate::{
    stream::error::{Error, Result},
    Record, RecordGroup,
};

/// For multi thread share one atomic ptr, and
/// simplfy the compare code.
#[repr(transparent)]
pub struct AtomicArcPtr<T>(Arc<AtomicPtr<T>>);

impl<T> AtomicArcPtr<T> {
    pub fn new(t: Box<T>) -> Self {
        AtomicArcPtr(Arc::new(AtomicPtr::new(Box::leak(t))))
    }

    pub fn try_deref(&self) -> Option<&T> {
        unsafe { self.0.load(std::sync::atomic::Ordering::Acquire).as_ref() }
    }

    pub fn compare_store(&self, t: Box<T>) -> std::result::Result<(), Box<T>> {
        self.0
            .compare_exchange(
                std::ptr::null_mut(),
                Box::leak(t),
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::Relaxed,
            )
            .map(|_| ())
            .map_err(|raw_ptr| unsafe { Box::from_raw(raw_ptr) })
    }
}

impl<T> From<Box<T>> for AtomicArcPtr<T> {
    fn from(box_ptr: Box<T>) -> Self {
        AtomicArcPtr::new(box_ptr)
    }
}

impl<T> Default for AtomicArcPtr<T> {
    fn default() -> Self {
        AtomicArcPtr(Arc::new(AtomicPtr::default()))
    }
}

impl<T> Clone for AtomicArcPtr<T> {
    fn clone(&self) -> Self {
        AtomicArcPtr(self.0.clone())
    }
}

impl<T> Drop for AtomicArcPtr<T> {
    fn drop(&mut self) {
        let arc = std::mem::replace(&mut self.0, Arc::new(AtomicPtr::new(std::ptr::null_mut())));
        if let Ok(atomic_ptr) = Arc::<AtomicPtr<T>>::try_unwrap(arc) {
            let ptr = atomic_ptr.into_inner();
            if !ptr.is_null() {
                unsafe {
                    // FIXME(luhuanbing): ?
                    drop(Box::from_raw(ptr));
                }
            }
        }
    }
}

pub enum FileType {
    Unknown,
    Current,
    Manifest(u64),
    Log(u64),
    Temp,
}

pub fn current<P: AsRef<Path>>(base_dir: P) -> PathBuf {
    base_dir.as_ref().join("CURRENT")
}

pub fn manifest(file_number: u64) -> String {
    format!("MANIFEST-{:06}", file_number)
}

#[allow(dead_code)]
pub fn descriptor<P: AsRef<Path>>(base_dir: P, file_number: u64) -> PathBuf {
    base_dir.as_ref().join(&manifest(file_number))
}

pub fn log<P: AsRef<Path>>(base_dir: P, file_number: u64) -> PathBuf {
    let name = format!("{:09}.log", file_number);
    base_dir.as_ref().join(&name)
}

pub fn temp<P: AsRef<Path>>(base_dir: P, file_number: u64) -> PathBuf {
    let name = format!("{:09}.tmp", file_number);
    base_dir.as_ref().join(&name)
}

pub fn parse_file_name<P: AsRef<Path>>(path: P) -> Result<FileType> {
    let path = path.as_ref();
    if !path.is_file() {
        return Err(Error::InvalidArgument("target isn't a file".to_string()));
    }

    let name = path.file_name().and_then(|s| s.to_str()).unwrap();
    if name == "CURRENT" {
        Ok(FileType::Current)
    } else if name.starts_with("MANIFEST-") {
        let rest = name.strip_prefix("MANIFEST-").unwrap();
        match rest.parse::<u64>() {
            Ok(file_number) => Ok(FileType::Manifest(file_number)),
            Err(_) => Ok(FileType::Unknown),
        }
    } else if name.ends_with(".log") {
        let rest = name.strip_suffix(".log").unwrap();
        match rest.parse::<u64>() {
            Ok(file_number) => Ok(FileType::Log(file_number)),
            Err(_) => Ok(FileType::Unknown),
        }
    } else if name.ends_with(".tmp") {
        Ok(FileType::Temp)
    } else {
        Ok(FileType::Unknown)
    }
}

pub fn write_snapshot(writer: &mut LogWriter, version: &Version) -> Result<()> {
    let snapshot = version.snapshot();
    let content = snapshot.encode_to_vec();
    writer.add_record(content.as_slice())?;
    writer.flush()?;
    Ok(())
}

pub fn create_new_manifest<P: AsRef<Path>>(
    base_dir: P,
    version: &Version,
    manifest_number: u64,
) -> Result<LogWriter> {
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .open(&descriptor(&base_dir, manifest_number))?;
    file.preallocate(MAX_DESCRIPTOR_FILE_SIZE)?;
    let mut writer = LogWriter::new(file, 0, 0, MAX_DESCRIPTOR_FILE_SIZE)?;
    write_snapshot(&mut writer, version)?;
    switch_current_file(&base_dir, manifest_number)?;

    Ok(writer)
}

/// Read and parse CURRENT file, return the path of corresponding MANIFEST file.
pub fn parse_current_file<P: AsRef<Path>>(base_dir: P) -> Result<PathBuf> {
    let content = match std::fs::read_to_string(current(&base_dir)) {
        Ok(content) => content,
        Err(err) => {
            error!("read CURRENT file: {:?}", err);
            return Err(err.into());
        }
    };

    let content = match content.as_bytes().strip_suffix(&[b'\n']) {
        Some(content) => content,
        None => {
            return Err(Error::Corruption(
                "CURRENT file does not end with newline".to_owned(),
            ));
        }
    };

    Ok(base_dir
        .as_ref()
        .join(Path::new(OsStr::from_bytes(content))))
}

// Update CURRENT file and point to the corresponding MANIFEST file.
pub fn switch_current_file<P: AsRef<Path>>(base_dir: P, manifest_number: u64) -> Result<()> {
    let tmp = temp(&base_dir, manifest_number);
    let content = format!("{}\n", manifest(manifest_number));
    std::fs::write(&tmp, content)?;
    std::fs::rename(&tmp, current(&base_dir))?;
    std::fs::File::open(&base_dir)?.sync_all()?;
    Ok(())
}

/// Read and apply version edits, returns the finalized version and the next
/// record offset.
pub(crate) fn recover_manifest<P: AsRef<Path>>(manifest: P) -> Result<(usize, Version)> {
    let file = std::fs::File::open(manifest)?;
    let mut reader = LogReader::new(file, 0, true)?;
    let mut builder = VersionBuilder::default();
    // FIXME(walter) handle partial write or corruption?
    while let Some(content) = reader.read_record()? {
        let edit = crate::manifest::VersionEdit::decode(content.as_slice())
            .expect("corrupted version edit");
        builder.apply(edit);
    }
    let version = builder.finalize();
    Ok((reader.next_record_offset(), version))
}

pub(crate) fn recover_log_engine<P: AsRef<Path>>(
    base_dir: P,
    opt: Arc<DBOption>,
    version: Version,
    db_layout: &mut DBLayout,
) -> Result<(LogEngine, HashMap<u64, PartialStream<LogFileManager>>)> {
    let log_file_mgr = LogFileManager::new(&base_dir, db_layout.max_file_number + 1, opt);
    log_file_mgr.recycle_all(
        version
            .log_number_record
            .recycled_log_numbers
            .iter()
            .cloned()
            .collect(),
    );

    let mut streams: HashMap<u64, PartialStream<_>> = HashMap::new();
    for stream_id in version.streams.keys() {
        streams.insert(
            *stream_id,
            PartialStream::new(version.stream_version(*stream_id), log_file_mgr.clone()),
        );
    }
    let mut applier = |log_number, record| {
        let (stream_id, txn) = convert_to_txn_context(&record);
        let stream = streams.entry(stream_id).or_insert_with(|| {
            PartialStream::new(version.stream_version(stream_id), log_file_mgr.clone())
        });
        stream.commit(log_number, txn);
        Ok(())
    };
    let log_engine = LogEngine::recover(
        base_dir,
        db_layout.log_numbers.clone(),
        log_file_mgr.clone(),
        &mut applier,
    )?;
    Ok((log_engine, streams))
}

pub fn convert_to_record(stream_id: u64, txn: &TxnContext) -> Record {
    match txn {
        TxnContext::Write {
            segment_epoch,
            first_index,
            acked_seq,
            entries,
            ..
        } => Record {
            stream_id,
            epoch: *segment_epoch,
            writer_epoch: None,
            acked_seq: Some((*acked_seq).into()),
            first_index: Some(*first_index),
            entries: entries.iter().cloned().map(Into::into).collect(),
        },
        TxnContext::Sealed {
            segment_epoch,
            writer_epoch,
            ..
        } => Record {
            stream_id,
            epoch: *segment_epoch,
            writer_epoch: Some(*writer_epoch),
            acked_seq: None,
            first_index: None,
            entries: vec![],
        },
    }
}

/// Recovers log file, returns the next record offset and the referenced
/// streams of the specifies log file.
pub fn recover_log_file<P: AsRef<Path>, F>(
    base_dir: P,
    log_number: u64,
    callback: &mut F,
) -> Result<(u64, HashSet<u64>)>
where
    F: FnMut(u64, Record) -> Result<()>,
{
    let file = File::open(log(base_dir, log_number))?;
    let mut reader = LogReader::new(file, log_number, true)?;
    let mut refer_streams = HashSet::new();
    while let Some(record) = reader.read_record()? {
        let record_group: RecordGroup = Message::decode(record.as_slice())?;
        for record in record_group.records {
            let stream_id = record.stream_id;
            callback(log_number, record)?;
            refer_streams.insert(stream_id);
        }
    }
    Ok((reader.next_record_offset() as u64, refer_streams))
}

pub fn remove_obsoleted_files(db_layout: DBLayout) {
    for name in db_layout.obsoleted_files {
        if let Err(err) = std::fs::remove_file(&name) {
            log::warn!("remove obsoleted file {:?}: {}", name, err);
        } else {
            log::info!("obsoleted file {:?} is removed", name);
        }
    }
}
