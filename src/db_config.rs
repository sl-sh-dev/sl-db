//! Define the configuration used to create a SLDB.

use crate::db::{Db, DbBytes, DbKey};
use crate::error::DBResult;
use std::fmt::Debug;
use std::hash::BuildHasher;
use std::path::PathBuf;

/// Configuration for a database.
pub struct DbConfig {
    pub(crate) dir: PathBuf,
    pub(crate) base_name: PathBuf,
    pub(crate) appnum: u64,
    pub(crate) initial_buckets: u32,
    pub(crate) bucket_elements: u16,
    pub(crate) load_factor: f32,
    pub(crate) read_only: bool,
    pub(crate) allow_bucket_expansion: bool, // don't allow more buckets- for testing lots of overflows...
    pub(crate) allow_duplicate_inserts: bool,
    pub(crate) cache_writes: bool,
}

impl DbConfig {
    /// Create a new config.
    pub fn new<P: Into<PathBuf>>(dir: P, base_name: P, appnum: u64) -> Self {
        Self {
            dir: dir.into(),
            base_name: base_name.into(),
            appnum,
            initial_buckets: 1,
            bucket_elements: 255,
            load_factor: 0.5,
            read_only: false,
            allow_bucket_expansion: true,
            allow_duplicate_inserts: false,
            cache_writes: true,
        }
    }

    /// Set the directory that contains the DB files.
    pub fn set_dir<P: Into<PathBuf>>(mut self, dir: P) -> Self {
        self.dir = dir.into();
        self
    }

    /// Set the base name for the DB files in dir.
    pub fn set_base_name<P: Into<PathBuf>>(mut self, base_name: P) -> Self {
        self.base_name = base_name.into();
        self
    }

    /// Consumes the config and builds a Db.
    pub fn build<K, V, const KSIZE: u16, S>(self) -> DBResult<Db<K, V, KSIZE, S>>
    where
        K: DbKey<KSIZE> + DbBytes<K>,
        V: Debug + DbBytes<V>,
        S: BuildHasher + Default,
    {
        Db::open(self)
    }
}
