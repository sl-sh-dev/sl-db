#![deny(missing_docs)]

//! Crate to implement a simple key/value data store.
//! It uses an append only (all appended data is immutable) data file with a second file containing
//! an on-disk hash for key lookup.
//! It is possible to iterate the data without the key and and a new index can also be built from
//! just an intact data file.

pub mod db;
pub mod db_config;
pub mod db_raw_iter;
pub mod error;
pub mod fxhasher;
