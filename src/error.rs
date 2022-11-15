//! Implements the error for an SLDB.

use std::error::Error;
use std::fmt;
use std::io;

/// Custom error type for SLDB.
#[derive(Debug)]
pub enum DBError {
    /// Underlying IO error.
    IO(io::Error),
    /// Item was not found in the DB.
    NotFound,
    /// Error deserializing a key or value with a message (note serialize is expected to be infallible).
    Deserialize(String),
    /// A provided key did not serialize to the required bytes.
    InvalidKeyLength,
    /// A data file contained an invalid header (maybe not a data file?).
    InvalidDataHeader,
    /// A data file was invalid.
    InvalidDataFile,
    /// A hash index file contained an invalid header (maybe not a hash index file?).
    InvalidIndexHeader,
    /// A hash index file was invalid.
    InvalidIndexFile,
    /// Indicates an overflow bucket was found in data when a record was expected.
    UnexpectedOverflowBucket,
    /// Tried to insert the same key twice.
    DuplicateInsert,
}

impl Error for DBError {}

impl fmt::Display for DBError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::IO(io_err) => write!(f, "IO Error: {}", io_err),
            Self::NotFound => write!(f, "Not Found"),
            Self::Deserialize(msg) => write!(f, "Deserialize Errror: {}", msg),
            Self::InvalidKeyLength => write!(f, "Invalid Key Length"),
            Self::InvalidDataHeader => write!(f, "Invalid Data File Header"),
            Self::InvalidDataFile => write!(f, "Invalid Data File"),
            Self::InvalidIndexHeader => write!(f, "Invalid Index File Header"),
            Self::InvalidIndexFile => write!(f, "Invalid Index File"),
            Self::UnexpectedOverflowBucket => write!(f, "Unexpected Overflow Bucket"),
            Self::DuplicateInsert => write!(f, "Key already in the database"),
        }
    }
}

impl From<io::Error> for DBError {
    fn from(io_err: io::Error) -> Self {
        DBError::IO(io_err)
    }
}

/// Type to wrap a Result<T, DBError>.
pub type DBResult<T> = Result<T, DBError>;
