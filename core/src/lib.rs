#![deny(missing_docs)]

//! Crate to implement a simple key/value data store Content Addressable Store).
//! This provides efficient lookups by key but no sorting.
//! It uses an append only (all appended data is immutable) data file with an index.
//! The index consists of two files, a mutable file containing hash buckets and an immutable
//! append only bucket overflow file.  With correct parameters the overflow file should rarely be
//! used but allows for efficient "overflowing" of a hash bucket.
//! Hashing uses the the linear hashing algorithm (https://en.wikipedia.org/wiki/Linear_hashing).
//! The default hasher is FxHasher from the Rust compiler.  Note that it requires a stable hasher so
//! the default Rust hasher is NOT appropriate (using it would make your index invalid when reopened
//! since the Rust hasher is not stable to provide DOS protection).  Otherwise you can use your own
//! hasher just like HashMap.
//!
//! It uses CRC32 checksums to verify all data (See https://github.com/srijs/rust-crc32fast).
//!
//! It is possible to iterate the data without the index and a new index can also be built from
//! just an intact data file.

pub(crate) mod crc;
pub mod db;
pub mod db_bytes;
pub mod db_config;
pub mod db_files;
pub mod db_key;
pub mod db_raw_iter;
pub mod error;
pub mod fxhasher;
