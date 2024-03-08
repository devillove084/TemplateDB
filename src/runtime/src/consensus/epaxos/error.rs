use std::io;

use thiserror::Error;

#[derive(Debug, Error)]
pub enum CommitError {}

#[derive(Debug, Error)]
pub enum ExecuteError {
    #[error("invalid command {0} ")]
    InvalidCommand(String),
    #[error("meet io related error")]
    IOError(#[from] io::Error),
}

#[derive(Debug, Error)]
pub enum RpcError {
    #[error("meet io related error")]
    IOError(#[from] io::Error),
}
