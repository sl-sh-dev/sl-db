//! Contains an error that extend the base CommitError with new errors from the tokio commits.

use sldb_core::error::flush::FlushError;
use sldb_core::error::insert::InsertError;
use std::error::Error;
use std::{fmt, io};

/// Error from async commit().
#[derive(Debug)]
pub enum CommitError {
    /// An error flushing any cached data.
    Flush(FlushError),
    /// An io error occured syncing the data file.
    DataFileSync(io::Error),
    /// An io error occured syncing the index file.
    IndexFileSync(io::Error),
    /// If a previous insert() failed return the error.
    PreviousInsertFailed(InsertError),
    /// The channel to send commands to the insert db thread has closed (this is fatal for the DB).
    SendChannelClosed,
    /// Failed to receive a response, this indicates a failed DB in an unknown state.
    ReceiveFailed,
    /// DB is opened read-only.
    ReadOnly,
}

impl Error for CommitError {}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::Flush(e) => write!(f, "flush: {}", e),
            Self::DataFileSync(io_err) => write!(f, "data sync: {}", io_err),
            Self::IndexFileSync(io_err) => write!(f, "index sync: {}", io_err),
            Self::PreviousInsertFailed(err) => write!(f, "insert failed: {}", err),
            Self::SendChannelClosed => write!(f, "send channel closed"),
            Self::ReceiveFailed => write!(f, "receive failed"),
            Self::ReadOnly => write!(f, "receive failed"),
        }
    }
}

impl From<sldb_core::error::CommitError> for CommitError {
    fn from(err: sldb_core::error::CommitError) -> Self {
        match err {
            sldb_core::error::CommitError::Flush(e) => Self::Flush(e),
            sldb_core::error::CommitError::DataFileSync(e) => Self::DataFileSync(e),
            sldb_core::error::CommitError::IndexFileSync(e) => Self::IndexFileSync(e),
            sldb_core::error::CommitError::ReadOnly => Self::ReadOnly,
        }
    }
}
