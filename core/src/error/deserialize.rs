//! Define the deserialization error.

use crate::error::source::SourceError;
use std::error::Error;
use std::fmt;

/// Error type for deserialiazation.
/// Contains a message and a GenericOptError that may wrap an underlying error.
#[derive(Debug)]
pub struct DeserializeError {
    message: String,
    source: SourceError,
}
impl Error for DeserializeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        if self.source.is_none() {
            None
        } else {
            Some(&self.source)
        }
    }
}
impl fmt::Display for DeserializeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl DeserializeError {
    /// Create a new DeserializeError with a message and optional source error.
    pub fn new(message: String, source: Option<Box<dyn Error + Send + Sync>>) -> Self {
        Self {
            message,
            source: SourceError::new_opt_error(source),
        }
    }
}
