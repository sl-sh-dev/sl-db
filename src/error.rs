use std::error::Error;
use std::fmt;
use std::io;

#[derive(Debug)]
pub enum DBError {
    IO(io::Error),
    NotFound,
    Deserialize(String),
    InvalidKeyLength,
    InvalidDataHeader,
    InvalidDataFile,
    InvalidIndexHeader,
    InvalidIndexFile,
    UnexpectedOverflowBucket,
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
        }
    }
}

impl From<io::Error> for DBError {
    fn from(io_err: io::Error) -> Self {
        DBError::IO(io_err)
    }
}

impl DBError {
    pub fn not_found() -> Self {
        Self::NotFound
    }

    pub fn deserialize(msg: String) -> Self {
        Self::Deserialize(msg)
    }
}

pub type DBResult<T> = Result<T, DBError>;
