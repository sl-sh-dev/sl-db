//! Implements the error for an SLDB.

pub mod deserialize;
pub mod flush;
pub mod insert;
pub mod serialize;
pub mod source;

use crate::error::deserialize::DeserializeError;
use crate::error::flush::FlushError;
use std::error::Error;
use std::fmt;
use std::io;

/// Error from read_key().
#[derive(Debug)]
pub enum ReadKeyError {
    /// An IO error trying to read a key.
    IO(io::Error),
    /// Error deserializing the key's data from the data file.
    DeserializeKey(DeserializeError),
}

impl Error for ReadKeyError {}

impl fmt::Display for ReadKeyError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::IO(io_err) => write!(f, "io: {}", io_err),
            Self::DeserializeKey(msg) => write!(f, "deserialize key: {}", msg),
        }
    }
}

impl From<io::Error> for ReadKeyError {
    fn from(io_err: io::Error) -> Self {
        Self::IO(io_err)
    }
}

impl From<DeserializeError> for ReadKeyError {
    fn from(err: DeserializeError) -> Self {
        Self::DeserializeKey(err)
    }
}

/// Error from commit().
#[derive(Debug)]
pub enum CommitError {
    /// An error flushing any cached data.
    Flush(FlushError),
    /// An io error occured syncing the data file.
    DataFileSync(io::Error),
    /// An io error occured syncing the index file.
    IndexFileSync(io::Error),
}

impl Error for CommitError {}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::Flush(e) => write!(f, "flush: {}", e),
            Self::DataFileSync(io_err) => write!(f, "data sync: {}", io_err),
            Self::IndexFileSync(io_err) => write!(f, "index sync: {}", io_err),
        }
    }
}

/// Error on loading a file (inder or data) header.
#[derive(Debug)]
pub enum LoadHeaderError {
    /// The type string for the header was invalid- corrupted or incorrect file type.
    InvalidType,
    /// An underlying IO error while loading the header
    IO(io::Error),
}

impl Error for LoadHeaderError {}

impl fmt::Display for LoadHeaderError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::InvalidType => write!(f, "invalid type id"),
            Self::IO(e) => write!(f, "io: {}", e),
        }
    }
}

impl From<io::Error> for LoadHeaderError {
    fn from(io_err: io::Error) -> Self {
        Self::IO(io_err)
    }
}

/// Error on opening a DB.
#[derive(Debug)]
pub enum OpenError {
    /// Error opening the data file.
    DataFileOpen(LoadHeaderError),
    /// Error opening the index file.
    IndexFileOpen(LoadHeaderError),
    /// An seeking in the data file (this should be hard to get).
    Seek(io::Error),
}

impl Error for OpenError {}

impl fmt::Display for OpenError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::DataFileOpen(e) => write!(f, "data open failed: {}", e),
            Self::IndexFileOpen(e) => write!(f, "index open failed: {}", e),
            Self::Seek(e) => write!(f, "seek: {}", e),
        }
    }
}

/// Error on reading a DB record.
#[derive(Debug)]
pub enum FetchError {
    /// Found an overflow bucket instead of a record in the data file.
    UnexpectedOverflowBucket,
    /// Failed to deserialize the key.
    DeserializeKey(DeserializeError),
    /// Failed to deserialize the value.
    DeserializeValue(DeserializeError),
    /// An seeking in the data file (this should be hard to get).
    IO(io::Error),
    /// Requested item was not found.
    NotFound,
}

impl Error for FetchError {}

impl fmt::Display for FetchError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::UnexpectedOverflowBucket => write!(f, "unexpected overflow bucket"),
            Self::DeserializeKey(e) => write!(f, "deserialize key: {}", e),
            Self::DeserializeValue(e) => write!(f, "deserialize value: {}", e),
            Self::IO(e) => write!(f, "io: {}", e),
            Self::NotFound => write!(f, "not found"),
        }
    }
}

impl From<io::Error> for FetchError {
    fn from(io_err: io::Error) -> Self {
        Self::IO(io_err)
    }
}
