//! Contains the error type for the flush() function.

use crate::error::ReadKeyError;
use std::error::Error;
use std::fmt;
use std::io;

/// Error from read_key().
#[derive(Debug)]
pub enum FlushError {
    /// Error writing to the data file.
    WriteData(io::Error),
    /// Error expanding buckets.
    ExpandBuckets(ReadKeyError),
    /// Error saving a bucket.
    SaveToBucket(ReadKeyError),
    /// Error writing the index header back to the index file.
    IndexHeader(io::Error),
    /// Error writing bucket(s) to the index file.
    WriteIndexData(io::Error),
    /// Database opened read-only.
    ReadOnly,
}

impl Error for FlushError {}

impl fmt::Display for FlushError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::WriteData(e) => write!(f, "write data: {e}"),
            Self::ExpandBuckets(e) => write!(f, "expand buckets: {e}"),
            Self::SaveToBucket(e) => write!(f, "save bucket: {e}"),
            Self::IndexHeader(e) => write!(f, "write index header: {e}"),
            Self::WriteIndexData(e) => write!(f, "write index data: {e}"),
            Self::ReadOnly => write!(f, "read only"),
        }
    }
}
