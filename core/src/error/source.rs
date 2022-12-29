//! Define an error sutable for use as a generic source for other errors.

use std::error::Error;
use std::fmt;

/// Can be None or a Boxed dynamic Error.
/// Implements Error so it can be used as a source for other errors.
#[derive(Debug)]
pub struct SourceError(Option<Box<dyn Error + Send + Sync>>);
impl Error for SourceError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.0 {
            None => None,
            Some(e) => e.source(),
        }
    }
}
impl fmt::Display for SourceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self.0 {
            None => write!(f, "no error set"),
            Some(e) => write!(f, "{}", e),
        }
    }
}
impl SourceError {
    /// Create a new empty (Contains None) GenericOptError.
    pub fn new_empty() -> Self {
        Self(None)
    }

    /// Create a new GenericOptError that contains err.
    pub fn new_error(err: Box<dyn Error + Send + Sync>) -> Self {
        Self(Some(err))
    }

    /// Create a new GenericOptError that contains err.
    pub fn new_opt_error(err: Option<Box<dyn Error + Send + Sync>>) -> Self {
        match err {
            None => Self(None),
            Some(err) => Self::new_error(err),
        }
    }

    /// Return an Optional reference to the wrapped Error.
    pub fn error(&self) -> Option<&(dyn Error + Send + Sync)> {
        self.0.as_ref().map(|e| e.as_ref())
    }

    /// Does this not contain an error.
    pub fn is_none(&self) -> bool {
        self.0.is_none()
    }

    /// Return true if the inner type is the same as T.
    pub fn is<T: Error + 'static>(&self) -> bool {
        self.0.as_ref().map(|e| e.is::<T>()).unwrap_or(false)
    }

    /// Returns some reference to the inner value if it is of type T, or None if it isn’t.
    pub fn downcast_ref<T: Error + 'static>(&self) -> Option<&T> {
        self.0
            .as_ref()
            .map(|e| e.downcast_ref::<T>())
            .unwrap_or(None)
    }

    /// Returns some mutable reference to the inner value if it is of type T, or None if it isn’t.
    pub fn downcast_mut<T: Error + 'static>(&mut self) -> Option<&mut T> {
        self.0
            .as_mut()
            .map(|e| e.downcast_mut::<T>())
            .unwrap_or(None)
    }
}
impl<'a> From<&'a SourceError> for Option<&'a Box<dyn Error + Send + Sync>> {
    fn from(err: &SourceError) -> Option<&Box<dyn Error + Send + Sync>> {
        err.0.as_ref()
    }
}
impl From<SourceError> for Option<Box<dyn Error + Send + Sync>> {
    fn from(err: SourceError) -> Option<Box<dyn Error + Send + Sync>> {
        err.0
    }
}
impl From<Box<dyn Error + Send + Sync>> for SourceError {
    fn from(err: Box<dyn Error + Send + Sync>) -> Self {
        Self::new_error(err)
    }
}
