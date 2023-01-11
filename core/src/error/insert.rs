//! Contains the error for the insert() function.

use crate::error::flush::FlushError;
use crate::error::serialize::SerializeError;
use crate::error::ReadKeyError;
use std::error::Error;
use std::fmt;

/// Custom error type for Inserts.
#[derive(Debug)]
pub enum InsertError {
    /// Key is already in the DB and duplicates are not allowed.
    DuplicateKey,
    /// Error serializing the key to store in DB.
    SerializeKey(SerializeError),
    /// Error serializing the value to store in DB.
    SerializeValue(SerializeError),
    /// Invalid key length for a fixed sized key.
    InvalidKeyLength,
    /// Error on flush.
    Flush(FlushError),
    /// Error accessing key.
    KeyError(ReadKeyError),
    /// Database opened read-only.
    ReadOnly,
    /// Crc32 check failed on an index access.
    IndexCrcError,
    /// Error writing an index overflow bucket.
    IndexOverflow,
}

impl Error for InsertError {}

impl fmt::Display for InsertError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::DuplicateKey => write!(f, "key is already in database"),
            Self::SerializeKey(e) => write!(f, "key serialization: {}", e),
            Self::SerializeValue(e) => write!(f, "value serialization: {}", e),
            Self::InvalidKeyLength => write!(f, "invalid key length"),
            Self::Flush(e) => write!(f, "flush: {}", e),
            Self::KeyError(e) => write!(f, "key access: {}", e),
            Self::ReadOnly => write!(f, "read only"),
            Self::IndexCrcError => write!(f, "index crc32 failed"),
            Self::IndexOverflow => write!(f, "index overflow failed"),
        }
    }
}

impl From<FlushError> for InsertError {
    fn from(err: FlushError) -> Self {
        Self::Flush(err)
    }
}

impl From<ReadKeyError> for InsertError {
    fn from(err: ReadKeyError) -> Self {
        Self::KeyError(err)
    }
}
