//! Define the configuration used to create a SLDB.

use crate::db::data_header::BUCKET_ELEMENT_SIZE;
use crate::db::DbCore;
use crate::db_bytes::DbBytes;
use crate::db_files::DbFiles;
use crate::db_key::DbKey;
use crate::error::OpenError;
use std::fmt::Debug;
use std::hash::BuildHasher;
use std::path::PathBuf;

/// Configuration for a database.
#[derive(Clone, Debug)]
pub struct DbConfig {
    pub(crate) files: DbFiles,
    pub(crate) appnum: u64,
    pub(crate) initial_buckets: u32,
    pub(crate) bucket_elements: u16,
    pub(crate) bucket_size: u16,
    pub(crate) load_factor: f32,
    pub(crate) write: bool,
    pub(crate) create: bool,
    pub(crate) truncate: bool,
    pub(crate) allow_bucket_expansion: bool, // don't allow more buckets- for testing lots of overflows...
    pub(crate) allow_duplicate_inserts: bool,
    pub(crate) cache_writes: bool,
    pub(crate) auto_flush: bool,
    pub(crate) read_buffer_size: u32,
    pub(crate) write_buffer_size: u32,
    pub(crate) bucket_cache_size: u32,
}

impl DbConfig {
    /// Create a new config.  Uses the supplied files for name/storage.
    pub fn with_files(files: DbFiles, appnum: u64) -> Self {
        let initial_buckets = 128;
        let bucket_size = 512; //12 + (BUCKET_ELEMENT_SIZE as u16 * bucket_elements);
        let bucket_elements = (bucket_size - 12) / BUCKET_ELEMENT_SIZE as u16;
        Self {
            files,
            appnum,
            initial_buckets,
            bucket_elements,
            bucket_size,
            load_factor: 0.5,
            write: true,
            create: false,
            truncate: false,
            allow_bucket_expansion: true,
            allow_duplicate_inserts: false,
            cache_writes: true,
            auto_flush: true,
            read_buffer_size: 8 * 1024,          // 8kb default.
            write_buffer_size: 8 * 1024,         // 8kb default.
            bucket_cache_size: 32 * 1024 * 1024, // 32mb default.
        }
    }

    /// Create a new config.  Data file will be stored in data_dir and index files in index_dir with
    /// name and appnum.  Allows the data and index to be stored on separate devices and still
    /// support renames.
    pub fn with_data_index_path<S, P, Q>(data_dir: P, index_dir: Q, name: S, appnum: u64) -> Self
    where
        S: Into<String>,
        P: Into<PathBuf>,
        Q: Into<PathBuf>,
    {
        Self::with_files(DbFiles::with_data_index(data_dir, index_dir, name), appnum)
    }

    /// Create a new config.  Stored in dir with name and appnum.
    pub fn with_data_path<S, P>(dir: P, name: S, appnum: u64) -> Self
    where
        S: Into<String>,
        P: Into<PathBuf>,
    {
        Self::with_files(DbFiles::with_data(dir, name), appnum)
    }

    /// Returns a reference to the file names for this DB.
    pub fn files(&self) -> &DbFiles {
        &self.files
    }

    /// Returns a mutable reference to the file names for this DB.
    pub fn files_mut(&mut self) -> &mut DbFiles {
        &mut self.files
    }

    /// Set the config files to files- do this before it is used or files will be ignored.
    pub fn set_files(mut self, files: DbFiles) -> Self {
        self.files = files;
        self
    }

    /// Replace the config files with files.
    pub fn replace_files(&mut self, files: DbFiles) {
        self.files = files;
    }

    /// Change DB name.
    /// This will change reported file paths (if not using explicit files) so don't do it without
    /// moving files.
    pub fn set_name<Q: Into<String>>(&mut self, name: Q) {
        self.files.set_name(name);
    }

    /// Open the database as read-only.
    pub fn read_only(mut self) -> Self {
        self.write = false;
        self
    }

    /// If the database does not exist then create it, otherwise open existing.
    /// File must be writable in order to create it if missing (option ignored if read-only).
    pub fn create(mut self) -> Self {
        self.create = true;
        self
    }

    /// Do NOT cache writes.
    pub fn no_write_cache(mut self) -> Self {
        self.cache_writes = false;
        self
    }

    /// Do NOT auto flush records.
    /// Note that disabling auto flush will use the write cache even if no_write_cache() is called.
    pub fn no_auto_flush(mut self) -> Self {
        self.auto_flush = false;
        self
    }

    /// Return the auto flush state.
    pub fn auto_flush(&self) -> bool {
        self.auto_flush
    }

    /// If the database exists then truncate it on open, requires write mode (option ignored if read-only).
    /// This will rebuild the database with new parameters instead of using the old parameters.
    pub fn truncate(mut self) -> Self {
        self.truncate = true;
        self
    }

    /// Allow duplicate keys to be inserted.  The earlier value will become unavailable when this is done.
    pub fn allow_duplicate_inserts(mut self) -> Self {
        self.allow_duplicate_inserts = true;
        self
    }

    /// Set the bucket size to size, will adjust bucket_elements to the largest size that will fit.
    /// Calling this will overwrite values set by set_bucket_elements, and vice-versa.
    /// Panics if size is less than 8 + BUCKET_ELEMENT_SIZE.
    pub fn set_bucket_size(mut self, size: u16) -> Self {
        if size < 12 + BUCKET_ELEMENT_SIZE as u16 {
            panic!(
                "Invalid bucket size, must be at least {}",
                12 + BUCKET_ELEMENT_SIZE
            );
        }
        self.bucket_size = size;
        self.bucket_elements = (size - 12) / BUCKET_ELEMENT_SIZE as u16;
        self
    }

    /// Set the size of the bucket cache.
    pub fn set_bucket_cache_size(mut self, cache_bytes: u32) -> Self {
        self.bucket_cache_size = cache_bytes;
        self
    }

    /// Sets the elements in each bucket to bucket_elements.  Will set bucket_size to
    /// 8 + (BUCKET_ELEMENT_SIZE as u16 * bucket_elements).
    /// Calling this will overwrite values set by set_bucket_size, and vice-versa.
    pub fn set_bucket_elements(mut self, bucket_elements: u16) -> Self {
        self.bucket_elements = bucket_elements;
        self.bucket_size = 12 + (BUCKET_ELEMENT_SIZE as u16 * bucket_elements);
        self
    }

    /// Set the initial buckets in the index file.  Initial capacity will by this * bucket_elements.
    pub fn set_initial_buckets(mut self, buckets: u32) -> Self {
        self.initial_buckets = buckets;
        self
    }

    /// Set the load factor, this will determine the extra bucket capacity to maintain.
    pub fn set_load_factor(mut self, load_factor: f32) -> Self {
        self.load_factor = load_factor;
        self
    }

    /// Consumes the config and builds a Db.
    pub fn build<K, V, const KSIZE: u16, S>(self) -> Result<DbCore<K, V, KSIZE, S>, OpenError>
    where
        K: DbKey<KSIZE> + DbBytes<K>,
        V: Debug + DbBytes<V>,
        S: BuildHasher + Default,
    {
        DbCore::open(self)
    }
}
