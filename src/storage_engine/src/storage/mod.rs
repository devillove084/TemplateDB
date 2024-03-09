pub mod file;
pub mod mem;

use std::{
    io,
    io::SeekFrom,
    path::{Path, PathBuf},
};

use crate::error::{TemplateKVError, TemplateResult};

/// `Storage` is a namespace for files.
///
/// The names are filepath names: they may be / separated or \ separated,
/// depending on the underlying operating system.
///
/// `Storage` should be thread safe
pub trait Storage: Send + Sync {
    type F: File + 'static;
    /// Create a file if it does not exist and truncates exist one.
    fn create<P: AsRef<Path>>(&self, name: P) -> TemplateResult<Self::F>;

    /// Open a file for writing and reading
    fn open<P: AsRef<Path>>(&self, name: P) -> TemplateResult<Self::F>;

    /// Delete the named file
    fn remove<P: AsRef<Path>>(&self, name: P) -> TemplateResult<()>;

    /// Removes a directory at this path. If `recursively`, removes all its contents.
    fn remove_dir<P: AsRef<Path>>(&self, dir: P, recursively: bool) -> TemplateResult<()>;

    /// Returns true iff the named file exists.
    fn exists<P: AsRef<Path>>(&self, name: P) -> bool;

    /// Rename a file or directory to a new name, replacing the original file if
    /// `new` already exists.
    fn rename<P: AsRef<Path>>(&self, old: P, new: P) -> TemplateResult<()>;

    /// Recursively create a directory and all of its parent components if they
    /// are missing.
    fn mkdir_all<P: AsRef<Path>>(&self, dir: P) -> TemplateResult<()>;

    /// Returns a list of the full-path to each file in given directory
    fn list<P: AsRef<Path>>(&self, dir: P) -> TemplateResult<Vec<PathBuf>>;
}

/// A file abstraction for IO operations
pub trait File: Send + Sync {
    fn write(&mut self, buf: &[u8]) -> TemplateResult<usize>;
    fn flush(&mut self) -> TemplateResult<()>;
    fn close(&mut self) -> TemplateResult<()>;
    fn seek(&mut self, pos: SeekFrom) -> TemplateResult<u64>;
    fn read(&mut self, buf: &mut [u8]) -> TemplateResult<usize>;
    fn read_all(&mut self, buf: &mut Vec<u8>) -> TemplateResult<usize>;
    fn len(&self) -> TemplateResult<u64>;
    fn is_empty(&self) -> bool {
        if let Ok(length) = self.len() {
            return length == 0;
        }
        // Err is considered as empty
        false
    }
    /// Locks the file for exclusive usage, blocking if the file is currently
    /// locked.
    fn lock(&self) -> TemplateResult<()>;
    fn unlock(&self) -> TemplateResult<()>;

    /// Reads bytes from an offset in this source into a buffer, returning how
    /// many bytes were read.
    ///
    /// This function may yield fewer bytes than the size of `buf`, if it was
    /// interrupted or hit the "EOF".
    ///
    /// See [`Read::read()`](https://doc.rust-lang.org/std/io/trait.Read.html#tymethod.read)
    /// for details.
    fn read_at(&self, buf: &mut [u8], offset: u64) -> TemplateResult<usize>;

    /// Reads the exact number of bytes required to fill `buf` from an `offset`.
    ///
    /// Errors if the "EOF" is encountered before filling the buffer.
    ///
    /// See [`Read::read_exact()`](https://doc.rust-lang.org/std/io/trait.Read.html#method.read_exact)
    /// for details.
    fn read_exact_at(&self, mut buf: &mut [u8], mut offset: u64) -> TemplateResult<()> {
        while !buf.is_empty() {
            match self.read_at(buf, offset) {
                Ok(0) => break,
                Ok(n) => {
                    let tmp = buf;
                    buf = &mut tmp[n..];
                    offset += n as u64;
                }
                Err(e) => match e {
                    TemplateKVError::IO(err) => {
                        if err.kind() != io::ErrorKind::Interrupted {
                            return Err(TemplateKVError::IO(err));
                        }
                    }
                    _ => return Err(e),
                },
            }
        }
        if !buf.is_empty() {
            let e = io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer");
            Err(TemplateKVError::IO(e))
        } else {
            Ok(())
        }
    }
}

/// Write given `data` into underlying `env` file and flush file iff `should_sync` is true
pub fn do_write_string_to_file<S: Storage, P: AsRef<Path>>(
    env: &S,
    data: String,
    file_name: P,
    should_sync: bool,
) -> TemplateResult<()> {
    let mut file = env.create(&file_name)?;
    file.write(data.as_bytes())?;
    if should_sync {
        file.flush()?;
    }
    if file.close().is_err() {
        env.remove(&file_name)?;
    }
    Ok(())
}
