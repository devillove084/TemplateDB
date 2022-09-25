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

use std::{ffi::OsString, path::Path};

use crate::{
    storage::util::{parse_file_name, FileType},
    stream::error::Result,
};

pub struct DBLayout {
    pub max_file_number: u64,
    pub log_numbers: Vec<u64>,
    pub obsoleted_files: Vec<OsString>,
}

pub fn analyze_db_layout<P: AsRef<Path>>(
    base_dir: P,
    manifest_file_number: u64,
) -> Result<DBLayout> {
    let mut max_file_number: u64 = 0;
    let mut log_numbers = vec![];
    let mut obsoleted_files = vec![];
    for dir_entry in std::fs::read_dir(&base_dir)? {
        let dir_entry = dir_entry?;
        let path = dir_entry.path();
        if !path.is_file() {
            continue;
        }
        match parse_file_name(&path)? {
            FileType::Current => continue,
            FileType::Unknown => {}
            FileType::Temp => obsoleted_files.push(path.file_name().unwrap().to_owned()),
            FileType::Manifest(number) => {
                max_file_number = max_file_number.max(number);
                if number != manifest_file_number {
                    obsoleted_files.push(path.file_name().unwrap().to_owned());
                }
            }
            FileType::Log(number) => {
                max_file_number = max_file_number.max(number);
                log_numbers.push(number);
            }
        }
    }
    Ok(DBLayout {
        max_file_number,
        log_numbers,
        obsoleted_files,
    })
}
