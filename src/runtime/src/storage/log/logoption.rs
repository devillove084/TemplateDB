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

pub const LOG_FILE_SIZE: usize = 512 * 1024 * 1024;
pub const MAX_LOG_FILES: usize = 16;

pub struct LogOption {
    /// Sync data before response
    ///
    /// DEFAULT: true
    pub sync_data: bool,

    /// The number of bytes per log file, it must equals to exp of 2.
    ///
    /// DEFAULT: `LOG_FILE_SIZE`
    pub log_file_size: usize,

    /// The maximum number of log files.
    ///
    /// DEFAULT: MAX_LOG_FILES
    pub max_log_files: usize,
}
