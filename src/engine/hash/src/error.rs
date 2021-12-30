use journal::Error as JournalError;
use kernel::Error as KernelError;
use storage::Error as StorageError;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error("corrupted: {0}")]
    Corrupted(String),
    #[error(transparent)]
    Kernel(#[from] KernelError),
    #[error(transparent)]
    Journal(#[from] JournalError),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

impl Error {
    pub fn corrupted<E: ToString>(err: E) -> Self {
        Self::Corrupted(err.to_string())
    }
}

pub type Result<T> = std::result::Result<T, Error>;
