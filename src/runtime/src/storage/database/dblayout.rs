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

use std::{collections::HashMap, ffi::OsString, fs::read_dir, path::Path, sync::Arc};

use super::{
    dboption::DBOption, tributary::PartialStream, txn::convert_to_txn_context, version::Version,
};
use crate::{
    storage::{
        log::manager::{LogEngine, LogFileManager},
        util::parse_file_name,
    },
    stream::error::{Error, Result},
};

pub struct DBLayout {
    pub max_file_number: u64,
    pub log_numbers: Vec<u64>,
    obsoleted_files: Vec<OsString>,
}

pub async fn analyze_db_layout<P: AsRef<Path>>(
    base_dir: P,
    manifest_file_number: u64,
) -> Result<DBLayout> {
    let mut max_file_number = 0;
    let mut log_numbers = vec![];
    let mut obsoleted_files = vec![];
    for dir_entry in read_dir(&base_dir)? {
        let dir_entry = dir_entry?;
        let path = dir_entry.path();
        if !path.is_file() {
            continue;
        }

        match parse_file_name(&path)? {
            crate::storage::util::FileType::Unknown => {}
            crate::storage::util::FileType::Current => continue,
            crate::storage::util::FileType::Temp => {
                obsoleted_files.push(path.file_name().unwrap().to_owned())
            }
            crate::storage::util::FileType::Manifest(num) => {
                max_file_number = manifest_file_number.max(num);
                if num != manifest_file_number {
                    obsoleted_files.push(path.file_name().unwrap().to_owned());
                }
            }
            crate::storage::util::FileType::Log(num) => {
                max_file_number = max_file_number.max(num);
                log_numbers.push(num);
            }
        }
    }
    Ok(DBLayout {
        max_file_number,
        log_numbers,
        obsoleted_files,
    })
}

async fn recover_log_engine<P: AsRef<Path>>(
    base_dir: P,
    opt: Arc<DBOption>,
    version: Version,
    db_layout: &mut DBLayout,
) -> Result<(LogEngine, HashMap<u64, PartialStream<LogFileManager>>)> {
    let log_file_mgr = LogFileManager::new(&base_dir, db_layout.max_file_number + 1, opt);
    log_file_mgr
        .recycle_all(
            version
                .log_number_record
                .recycled_log_number
                .iter()
                .cloned()
                .collect(),
        )
        .await;

    let mut streams = HashMap::new();
    for stream_id in version.streams.keys() {
        streams.insert(
            *stream_id,
            PartialStream::new(version.stream_version(*stream_id), log_file_mgr.clone()),
        );
    }

    let mut applier = |ln, record| {
        let (stream_id, txn) = convert_to_txn_context(&record);
        let stream = streams.entry(stream_id).or_insert_with(|| {
            PartialStream::new(version.stream_version(stream_id), log_file_mgr.clone())
        });
        stream.commit(ln, txn);
        Ok::<(), Error>(())
    };

    let log_engine = LogEngine::recover(
        base_dir,
        db_layout.log_numbers.clone(),
        log_file_mgr.clone(),
        &mut applier,
    )
    .await?;
    Ok((log_engine, streams))
}
