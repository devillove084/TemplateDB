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
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{atomic::AtomicPtr, Arc},
};

use super::{
    database::{
        dblayout::DBLayout,
        dboption::DBOption,
        tributary::PartialStream,
        txn::{convert_to_txn_context, TxnContext},
        version::Version,
    },
    log::{
        logwriter::LogWriter,
        manager::{LogEngine, LogFileManager},
    },
};
use crate::{
    stream::error::{Error, Result},
    Record,
};

/// For multi thread share one atomic ptr, and
/// simplfy the compare code.
#[repr(transparent)]
#[derive(Clone)]
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

/// Some log and file writer helper funcs.

pub fn write_snapshot(writer: &mut LogWriter, version: &Version) -> Result<()> {
    todo!()
}

pub fn create_new_manifest<P: AsRef<Path>>(
    base_dir: P,
    version: &Version,
    manifest_number: u64,
) -> Result<LogWriter> {
    todo!()
}

pub fn parse_current_file<P: AsRef<Path>>(base_dir: P) -> Result<PathBuf> {
    todo!()
}

pub fn recover_manifest<P: AsRef<Path>>(manifest: P) -> Result<(usize, Version)> {
    todo!()
}

pub fn switch_current_file<P: AsRef<Path>>(base_dir: P, manifest_number: u64) -> Result<()> {
    todo!()
}

pub async fn recover_log_engine<P: AsRef<Path>>(
    base_dir: P,
    opt: Arc<DBOption>,
    version: Version,
    db_layout: &mut DBLayout,
) -> Result<(LogEngine, HashMap<u64, PartialStream<LogFileManager>>)> {
    let log_file_mgr = LogFileManager::new(&base_dir, db_layout.max_file_number + 1, opt);
    log_file_mgr.recycle_all(
        version
            .log_number_record
            .recycled_log_number
            .iter()
            .cloned()
            .collect(),
    );

    let mut streams = HashMap::new();
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
        Ok::<(), Error>(()) // TODO(luhuanbing): Is that Ok?
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
