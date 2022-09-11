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

use std::io::Error;

pub trait FileExt {
    fn sync_range(&mut self, offset: usize, len: usize) -> Result<(), Error>;
    fn preallocate(&mut self, len: usize) -> Result<(), Error>;
}

impl FileExt for std::fs::File {
    fn sync_range(&mut self, offset: usize, len: usize) -> Result<(), Error> {
        #[cfg(target_os = "linux")]
        unsafe {
            use std::os::unix::io::AsRawFd;

            let retval = libc::sync_file_range(
                self.as_raw_fd(),
                offset as i64,
                len as i64,
                libc::SYNC_FILE_RANGE_WRITE,
            );
            if retval == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }

        #[cfg(not(target_os = "linux"))]
        {
            let _ = offset;
            let _ = len;
        }
        Ok(())
    }

    fn preallocate(&mut self, len: usize) -> Result<(), Error> {
        #[cfg(target_os = "linux")]
        unsafe {
            use std::os::unix::io::AsRawFd;
            let retval = libc::fallocate(self.as_raw_fd(), 0, 0, len as i64);
            if retval == -1 {
                return Err(std::io::Error::last_os_error());
            }
        }

        #[cfg(target_os = "macos")]
        unsafe {
            use std::os::unix::io::AsRawFd;
            let retval = libc::ftruncate(self.as_raw_fd(), len as i64);
            if retval != 0 {
                return Err(std::io::Error::from_raw_os_error(retval));
            }
        }
        Ok(())
    }
}
