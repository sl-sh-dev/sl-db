//! Main module for the SLDB core.  This implements the core sync single threaded access to the DB.

use crate::crc::add_crc32;
use crate::db::data_header::DataHeader;
use crate::db::hdx_index::HdxIndex;
use crate::db_bytes::DbBytes;
use crate::db_config::DbConfig;
use crate::db_files::{DbFiles, RenameError};
use crate::db_key::DbKey;
use crate::db_raw_iter::DbRawIter;
use crate::error::flush::FlushError;
use crate::error::insert::InsertError;
use crate::error::ReadKeyError;
use crate::error::{CommitError, FetchError, LoadHeaderError, OpenError};
use crate::fxhasher::FxHasher;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, BuildHasherDefault, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::{fs, io};

mod bucket_iter;
mod core_io_traits;
pub mod data_header;
pub mod hdx_index;
pub mod odx_header;

/// An instance of a DB.
/// Will consist of a data file (.dat), hash index (.hdx) and hash bucket overflow file (.odx).
pub struct DbCore<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    inner: DbInner<K, V, KSIZE, S>,
}

impl<K, V, const KSIZE: u16, S> DbCore<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Open a new or reopen an existing database.
    pub fn open(config: DbConfig) -> Result<Self, OpenError> {
        Ok(Self {
            inner: DbInner::open(config)?,
        })
    }

    /// Open a new or reopen an existing database.
    /// If a problem is detected try a reindex to recover if possible.
    pub fn open_with_recover(config: DbConfig) -> Result<Self, OpenError> {
        Ok(Self {
            inner: DbInner::open_with_recover(config)?,
        })
    }

    /// Will destroy the existing index for DB and rebuild it based on the data file.
    /// This will also verify the integrity of the data file.  If only the final record is corrupt
    /// then the file will be truncated to leave a valid DB.  Other corrupt records will be ignored
    /// (they will be garbage in the data file but won't be indexed).
    pub fn reindex(config: DbConfig) -> Result<Self, OpenError> {
        Ok(Self {
            inner: DbInner::reindex(config)?,
        })
    }

    /// Close and destroy the DB (remove all it's files).
    /// If it can not remove a file it will silently ignore this.
    pub fn destroy(self) {
        self.inner.destroy();
    }

    /// Rename the database to new_name.
    /// This will return an error if using explicit filenames instead or directory based DbFiles.
    pub fn rename<Q: Into<String>>(&mut self, new_name: Q) -> Result<(), RenameError> {
        self.inner.rename(new_name)
    }

    /// Returns a reference to the file names for this DB.
    pub fn files(&self) -> &DbFiles {
        self.inner.files()
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub fn fetch(&mut self, key: &K) -> Result<V, FetchError> {
        self.inner.fetch(key)
    }

    /// True if the database contains key.
    pub fn contains_key(&mut self, key: &K) -> Result<bool, ReadKeyError> {
        self.inner.contains_key(key)
    }

    /// If in read-only mode refresh the index header data from on-disk.
    /// Useful if the DB is also opened for writing.
    pub fn refresh_index(&mut self) {
        self.inner.refresh_index()
    }

    /// Insert a new key/value pair in Db.
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    /// For the erros IndexCrcError, IndexOverflow, WriteDataError or KeyError the DB will move to a
    /// failed state and become read only.  These errors all indicate serious underlying issues that
    /// can not be trivially fixed, a reopen/repair might help.
    pub fn insert(&mut self, key: K, value: &V) -> Result<(), InsertError> {
        self.inner.insert(key, value)
    }

    /// Return the number of records in Db.
    pub fn len(&self) -> usize {
        self.inner.len()
    }

    /// Is the DB empty?
    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    /// Return the DB version.
    pub fn version(&self) -> u16 {
        self.inner.version()
    }

    /// Return the DB application number (set at creation).
    pub fn appnum(&self) -> u64 {
        self.inner.appnum()
    }

    /// Return the DB uid (generated at creation).
    pub fn uid(&self) -> u64 {
        self.inner.uid()
    }

    /// Return the DB index salt (generated at creation).
    /// Can be used with the pepper to test the hasher.
    pub fn salt(&self) -> u64 {
        self.inner.salt()
    }

    /// Return the DB pepper (generated at creation from the salt with Hasher).
    /// Can be used with the salt to test the hasher.
    pub fn pepper(&self) -> u64 {
        self.inner.pepper()
    }

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    /// Note this is an expensive call (syncing to disk is not cheap).
    pub fn commit(&mut self) -> Result<(), CommitError> {
        self.inner.commit()
    }

    /// Flush any in memory caches to file.
    /// Note this is only a flush not a commit, it does not do a sync on the files.
    pub fn flush(&mut self) -> Result<(), FlushError> {
        self.inner.flush()
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    /// This iterator will not see any data in the write cache.
    pub fn raw_iter(&self) -> Result<DbRawIter<K, V, KSIZE>, LoadHeaderError> {
        self.inner.raw_iter()
    }
}

/// An instance of a DB.
/// Will consist of a data file (.dat), hash index (.hdx) and hash bucket overflow file (.odx).
/// This is synchronous and single threaded.  It is intended to keep the algorithms clearer and
/// to be wrapped for async or multi-threaded synchronous use.
/// This is the private inner type, this protects the io (Read, Write, Sync) traits from external use).
struct DbInner<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    header: DataHeader,
    data_file: File,
    data_file_end: u64,
    hasher: S,
    key_buffer: Vec<u8>,
    value_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    read_buffer: Vec<u8>,
    read_buffer_start: usize,
    read_buffer_len: usize,
    hdx_index: HdxIndex<K, KSIZE>,
    seek_pos: u64,
    config: DbConfig,
    failed: bool,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K, V, const KSIZE: u16, S> Drop for DbInner<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    fn drop(&mut self) {
        if self.config.write {
            let _ = self.commit();
        }
    }
}

impl<K, V, const KSIZE: u16, S> DbInner<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Open a new or reopen an existing database.
    pub fn open(config: DbConfig) -> Result<Self, OpenError> {
        Self::open_internal(config, true, false)
    }

    /// Open a new or reopen an existing database.
    /// If a problem is detected try a reindex to recover if possible.
    pub fn open_with_recover(config: DbConfig) -> Result<Self, OpenError> {
        Self::open_internal(config, true, true)
    }

    /// Open a new or reopen an existing database.
    /// ATakes a flag (require_hdx_check) that will skip the clean shutdown check.
    /// If auto_recover is true and a there is a non-data file issue then try to reindex to recover.
    fn open_internal(
        config: DbConfig,
        require_hdx_check: bool,
        auto_recover: bool,
    ) -> Result<Self, OpenError> {
        if config.create {
            // Best effort to create the dir if asked to create the DB.
            let _ = fs::create_dir_all(config.files.data_dir());
            let _ = fs::create_dir_all(config.files.hdx_dir());
            let _ = fs::create_dir_all(config.files.odx_dir());
        }
        let hasher = S::default();
        let (mut data_file, header) =
            Self::open_data_file(&config).map_err(OpenError::DataFileOpen)?;
        let data_file_end = data_file.seek(SeekFrom::End(0)).map_err(OpenError::Seek)?;
        let hdx_index = match HdxIndex::open_hdx_file(&header, config.clone(), &hasher) {
            Ok(index) => index,
            Err(err) => {
                return if auto_recover {
                    Self::reindex(config)
                } else {
                    Err(OpenError::IndexFileOpen(err))
                };
            }
        };
        let write_buffer = if config.write {
            Vec::with_capacity(config.write_buffer_size as usize)
        } else {
            // If opening read only wont need capacity.
            Vec::new()
        };
        let mut read_buffer = vec![0; config.read_buffer_size as usize];
        // Prime the read buffer so we don't have to check if it is empty, etc.
        let mut read_buffer_len = 0_usize;
        if data_file_end > 0 {
            data_file.rewind().map_err(OpenError::Seek)?;
            if data_file_end < config.read_buffer_size as u64 {
                data_file
                    .read_exact(&mut read_buffer[..data_file_end as usize])
                    .map_err(OpenError::DataReadError)?;
                read_buffer_len = data_file_end as usize;
            } else {
                data_file
                    .read_exact(&mut read_buffer[..])
                    .map_err(OpenError::DataReadError)?;
                read_buffer_len = config.read_buffer_size as usize;
            }
        }
        if require_hdx_check && data_file_end != hdx_index.header().data_file_length() {
            if auto_recover {
                Self::reindex(config)
            } else {
                Err(OpenError::InvalidShutdown)
            }
        } else {
            Ok(Self {
                header,
                data_file,
                data_file_end,
                hasher,
                hdx_index,
                key_buffer: Vec::new(),
                value_buffer: Vec::new(),
                write_buffer,
                read_buffer,
                read_buffer_start: 0,
                read_buffer_len,
                seek_pos: 0,
                config,
                failed: false,
                _key: PhantomData,
                _value: PhantomData,
            })
        }
    }

    /// Will destroy the existing index for DB and rebuild it based on the data file.
    /// This will also verify the integrity of the data file.  If only the final record is corrupt
    /// then the file will be truncated to leave a valid DB.  Other corrupt records will be ignored
    /// (they will be garbage in the data file but won't be indexed).
    pub fn reindex(config: DbConfig) -> Result<Self, OpenError> {
        let config = config.create();
        let _ = fs::remove_file(&config.files.hdx_path());
        let _ = fs::remove_file(&config.files.odx_path());

        let mut db = Self::open_internal(config, false, false)?;

        let mut iter = db.raw_iter().map_err(OpenError::DataFileOpen)?;
        let mut record_pos = iter.position().map_err(OpenError::Seek)?;
        let mut prev_record_pos = record_pos;
        let mut last_err = false;
        while let Some(rec) = iter.next() {
            last_err = false;
            if let Ok((key, _value)) = rec {
                let hash = db.hash(&key);
                if let Err(err) = db.expand_buckets() {
                    return Err(OpenError::RebuildIndex(err));
                }
                if let Err(err) = db.save_to_bucket(&key, hash, record_pos) {
                    return Err(OpenError::RebuildIndex(err));
                }
                db.hdx_index.inc_values();
            } else {
                last_err = true;
            }
            prev_record_pos = record_pos;
            record_pos = iter.position().map_err(OpenError::Seek)?;
        }
        if last_err {
            // If the final record was corrupt then truncate to remove it.
            db.data_file
                .set_len(prev_record_pos)
                .map_err(OpenError::Seek)?;
            db.data_file_end = prev_record_pos;
            let config = db.config.clone();
            // Need to drop and reopen after a truncate or the write position will be past the end.
            drop(db);
            db = Self::open(config)?;
        }
        Ok(db)
    }

    /// Close and destroy the DB (remove all it's files).
    /// If it can not remove a file it will silently ignore this.
    pub fn destroy(self) {
        let files = self.config.files.clone();
        drop(self);
        files.delete();
    }

    /// Rename the database to name.
    /// This will return an error if using explicit filenames instead or directory based DbFiles.
    pub fn rename<Q: Into<String>>(&mut self, name: Q) -> Result<(), RenameError> {
        self.config.files.rename(name)
    }

    /// Returns a reference to the file names for this DB.
    pub fn files(&self) -> &DbFiles {
        &self.config.files
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub fn fetch(&mut self, key: &K) -> Result<V, FetchError> {
        let hash = self.hash(key);
        let bucket = self.hdx_index.hash_to_bucket(hash);

        let mut iter = unsafe { self.hdx_index.bucket_iter(bucket) };
        for (rec_hash, rec_pos) in &mut iter {
            // rec_pos > 0 handles degenerate case of a 0 hash.
            if hash == rec_hash && rec_pos > 0 {
                let (rkey, val) = self.read_record(rec_pos)?;
                if &rkey == key {
                    return Ok(val);
                }
            }
        }
        if iter.crc_failure() {
            Err(FetchError::CrcFailed)
        } else {
            Err(FetchError::NotFound)
        }
    }

    /// True if the database contains key.
    pub fn contains_key(&mut self, key: &K) -> Result<bool, ReadKeyError> {
        let hash = self.hash(key);
        let bucket = self.hdx_index.hash_to_bucket(hash);
        let mut iter = unsafe { self.hdx_index.bucket_iter(bucket) };
        for (rec_hash, rec_pos) in &mut iter {
            // rec_pos > 0 handles degenerate case of a 0 hash.
            if hash == rec_hash && rec_pos > 0 {
                let rkey = self.read_key(rec_pos)?;
                if &rkey == key {
                    return Ok(true);
                }
            }
        }
        if iter.crc_failure() {
            Err(ReadKeyError::CrcFailed)
        } else {
            Ok(false)
        }
    }

    /// If in read-only mode refresh the index header data from on-disk.
    /// Useful if the DB is also opened for writing.
    pub fn refresh_index(&mut self) {
        if !self.config.write {
            self.hdx_index.reload_header();
            self.data_file_end = self
                .data_file
                .seek(SeekFrom::End(0))
                .unwrap_or(self.data_file_end);
        }
    }

    /// Do the actual insert so the public function can rollback easily on an error.
    fn insert_inner(&mut self, key: K, value: &V) -> Result<(), InsertError> {
        let record_pos = self.data_file_end + self.write_buffer.len() as u64;
        let hash = self.hash(&key);
        let mut crc32_hasher = crc32fast::Hasher::new();

        // Try to serialize the key and value and error out before saving anything if that fails.
        key.serialize(&mut self.key_buffer)
            .map_err(InsertError::SerializeKey)?;
        value
            .serialize(&mut self.value_buffer)
            .map_err(InsertError::SerializeValue)?;

        // Go ahead and expand the index if needed and return an error before we save anything.
        self.expand_buckets()?;

        // If we have a variable sized key write it's size otherwise no need.
        if K::is_variable_key_size() {
            let key_size = (self.key_buffer.len() as u16).to_le_bytes();
            // We have to write the key size when variable.
            self.write_all(&key_size)?;
            crc32_hasher.update(&key_size);
        } else if K::KEY_SIZE as usize != self.key_buffer.len() {
            return Err(InsertError::InvalidKeyLength);
        }
        // Once we have written to write_buffer, it needs to be rolled back before returning an error.
        // Space for the value length.
        let value_size = (self.value_buffer.len() as u32).to_le_bytes();
        self.write_all(&value_size)?;
        crc32_hasher.update(&value_size);

        {
            // Turn self into a reference to a Write trait.
            // Need this to write buffers owned by self.
            let writer: &mut dyn Write = unsafe {
                (self as *mut dyn Write)
                    .as_mut()
                    .expect("this can't be null")
            };
            // Write the key to the buffer.
            writer.write_all(&self.key_buffer)?;
            crc32_hasher.update(&self.key_buffer);

            // Save current pos, then jump back to the value size and write that then finally write
            // the value into the saved position.
            writer.write_all(&self.value_buffer)?;
            crc32_hasher.update(&self.value_buffer);
        }
        let crc32 = crc32_hasher.finalize();
        self.write_all(&crc32.to_le_bytes())?;

        // Save the key to the index, do this now so the data file will not have been written if the
        // index update fails.
        self.save_to_bucket(&key, hash, record_pos)?;

        // Since the inserted data will still be "available" even after an error after this point go
        // ahead and increment the values.
        self.hdx_index.inc_values();
        Ok(())
    }

    /// Insert a new key/value pair in Db.
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    /// For the erros IndexCrcError, IndexOverflow, WriteDataError or KeyError the DB will move to a
    /// failed state and become read only.  These errors all indicate serious underlying issues that
    /// can not be trivially fixed, a reopen/repair might help.
    pub fn insert(&mut self, key: K, value: &V) -> Result<(), InsertError> {
        if !self.config.write || self.failed {
            return Err(InsertError::ReadOnly);
        }
        let result = self.insert_inner(key, value);
        if let Err(err) = &result {
            match err {
                // These errors all indicate a failed DB that can no longer be inserted too.
                InsertError::IndexCrcError | InsertError::IndexOverflow => self.failed = true,
                InsertError::WriteDataError(_io_err) => self.failed = true,
                InsertError::KeyError(_key_err) => self.failed = true,
                // These errors do not indicate a failed DB.
                InsertError::DuplicateKey
                | InsertError::SerializeKey(_)
                | InsertError::SerializeValue(_)
                | InsertError::InvalidKeyLength
                | InsertError::ReadOnly => {}
            }
        }
        result
    }

    /// Return the number of records in Db.
    pub fn len(&self) -> usize {
        self.hdx_index.values() as usize
    }

    /// Is the DB empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return the DB version.
    pub fn version(&self) -> u16 {
        self.header.version()
    }

    /// Return the DB application number (set at creation).
    pub fn appnum(&self) -> u64 {
        self.header.appnum()
    }

    /// Return the DB uid (generated at creation).
    pub fn uid(&self) -> u64 {
        self.header.uid()
    }

    /// Return the DB index salt (generated at creation).
    /// Can be used with the pepper to test the hasher.
    pub fn salt(&self) -> u64 {
        self.hdx_index.header().salt()
    }

    /// Return the DB pepper (generated at creation from the salt with Hasher).
    /// Can be used with the salt to test the hasher.
    pub fn pepper(&self) -> u64 {
        self.hdx_index.header().pepper()
    }

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    /// Note this is a very expensive call (syncing to disk is not cheap).
    pub fn commit(&mut self) -> Result<(), CommitError> {
        if !self.config.write || self.failed {
            return Err(CommitError::ReadOnly);
        }
        self.flush().map_err(CommitError::Flush)?;
        self.data_file
            .sync_all()
            .map_err(CommitError::DataFileSync)?;
        self.hdx_index.sync()?;
        Ok(())
    }

    /// Flush any in memory caches to file.
    /// Note this is only a flush not a commit, it does not do a sync on the files.
    pub fn flush(&mut self) -> Result<(), FlushError> {
        if !self.config.write || self.failed {
            return Err(FlushError::ReadOnly);
        }
        Write::flush(self).map_err(FlushError::WriteData)?;
        self.hdx_index
            .save_bucket_cache()
            .map_err(FlushError::WriteIndexData)?;
        self.hdx_index.set_data_file_length(self.data_file_end);
        self.hdx_index
            .write_header()
            .map_err(FlushError::IndexHeader)?;
        Ok(())
    }

    fn open_data_file(config: &DbConfig) -> Result<(File, DataHeader), LoadHeaderError> {
        if config.truncate && config.write {
            // truncate is incompatible with append so truncate then open for append.
            OpenOptions::new()
                .write(true)
                .create(config.create)
                .truncate(true)
                .open(config.files.data_path())?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .append(config.write)
            .create(config.create && config.write)
            .open(config.files.data_path())?;
        file.seek(SeekFrom::End(0))?;
        let file_end = file.stream_position()?;

        let header = if file_end == 0 {
            let header = DataHeader::new(config);
            header.write_header(&mut file)?;
            header
        } else {
            let header = DataHeader::load_header(&mut file)?;
            if header.version() != 0 {
                return Err(LoadHeaderError::InvalidVersion);
            }
            if header.appnum() != config.appnum {
                return Err(LoadHeaderError::InvalidAppNum);
            }
            header
        };
        Ok((file, header))
    }

    /// Return the u64 hash of key.
    fn hash(&self, key: &K) -> u64 {
        let mut hasher = self.hasher.build_hasher();
        key.hash(&mut hasher);
        hasher.finish()
    }

    /// Add buckets to expand capacity.
    /// Capacity is number of elements per bucket * number of buckets.
    /// If current length >= capacity * load factor then split buckets until this is not true.
    fn expand_buckets(&mut self) -> Result<(), InsertError> {
        let unsafe_db: &mut DbInner<K, V, KSIZE, S> = unsafe {
            (self as *mut DbInner<K, V, KSIZE, S>)
                .as_mut()
                .expect("this can't be null")
        };
        self.hdx_index
            .expand_buckets(&mut |pos| unsafe_db.read_key(pos))
    }

    /// Save the (hash, position) tuple to the bucket.  Handles overflow records.
    fn save_to_bucket(&mut self, key: &K, hash: u64, record_pos: u64) -> Result<(), InsertError> {
        // Make a self with a new lifetime so we can pass in the read_key closure.
        // read_key should not mutate hdx_index so this should be fine.
        let unsafe_db: &mut DbInner<K, V, KSIZE, S> = unsafe {
            (self as *mut DbInner<K, V, KSIZE, S>)
                .as_mut()
                .expect("this can't be null")
        };
        self.hdx_index
            .save_to_bucket(key, hash, record_pos, &mut |pos| unsafe_db.read_key(pos))
    }

    /// Read the record at position.
    /// Returns the (key, value) tuple
    /// Will produce an error for IO or or for a failed CRC32 integrity check.
    fn read_record(&mut self, position: u64) -> Result<(K, V), FetchError> {
        // Turn self into a reference to a Read trait with it's own lifetime.
        // This is so we can call read_exact with a buffer owned by self.  There will not be any
        // overlap doing this so should be safe, restricting the reference to the Read trait to help
        // enforce this.
        let reader: &mut dyn Read = unsafe {
            (self as *mut dyn Read)
                .as_mut()
                .expect("this can't be null")
        };
        self.seek(SeekFrom::Start(position))?;
        let mut crc32_hasher = crc32fast::Hasher::new();
        let key_size = if K::is_variable_key_size() {
            let mut key_size = [0_u8; 2];
            reader.read_exact(&mut key_size)?;
            crc32_hasher.update(&key_size);
            u16::from_le_bytes(key_size)
        } else {
            K::KEY_SIZE
        } as usize;

        let mut val_size_buf = [0_u8; 4];
        reader.read_exact(&mut val_size_buf)?;
        crc32_hasher.update(&val_size_buf);
        let val_size = u32::from_le_bytes(val_size_buf);
        self.key_buffer.resize(key_size, 0);
        reader.read_exact(&mut self.key_buffer[..])?;
        crc32_hasher.update(&self.key_buffer);
        let key = K::deserialize(&self.key_buffer[..]).map_err(FetchError::DeserializeKey)?;
        self.value_buffer.resize(val_size as usize, 0);
        reader.read_exact(&mut self.value_buffer[..])?;
        crc32_hasher.update(&self.value_buffer);
        let calc_crc32 = crc32_hasher.finalize();
        let val = V::deserialize(&self.value_buffer[..]).map_err(FetchError::DeserializeValue)?;
        let mut buf_u32 = [0_u8; 4];
        reader.read_exact(&mut buf_u32)?;
        let read_crc32 = u32::from_le_bytes(buf_u32);
        if calc_crc32 != read_crc32 {
            return Err(FetchError::CrcFailed);
        }
        Ok((key, val))
    }

    /// Read the key for the record at position.
    /// The position needs to be valid, attempting to read an invalid area will
    /// produce an error or invalid key.  This is a truncated read and DOES NOT do a CRC32 check.
    fn read_key(&mut self, position: u64) -> Result<K, ReadKeyError> {
        // Turn self into a reference to a Read trait with it's own lifetime.
        // This is so we can call read_exact with a buffer owned by self.  There will not be any
        // overlap doing this so should be safe, restricting the reference to the Read trait to help
        // enforce this.
        let reader: &mut dyn Read = unsafe {
            (self as *mut dyn Read)
                .as_mut()
                .expect("this can't be null")
        };

        self.seek(SeekFrom::Start(position))?;
        let key_size = if K::is_variable_key_size() {
            let mut key_size = [0_u8; 2];
            self.read_exact(&mut key_size)?;
            let key_size = u16::from_le_bytes(key_size);
            key_size as usize
        } else {
            K::KEY_SIZE as usize
        };
        self.key_buffer.resize(key_size, 0);
        self.seek(SeekFrom::Current(4))?;
        reader.read_exact(&mut self.key_buffer[..])?;
        // Skip the value size and read the key.
        let key = K::deserialize(&self.key_buffer[..])?;

        Ok(key)
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    /// This iterator will not see any data in the write cache.
    pub fn raw_iter(&self) -> Result<DbRawIter<K, V, KSIZE>, LoadHeaderError> {
        let dat_file = { self.data_file.try_clone()? };
        DbRawIter::with_file(dat_file)
    }

    /// Copy bytes form the read buffer into buf.  This expects seek_pos to be within the
    /// read_buffer (will panic if called incorrectly).
    fn copy_read_buffer(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut size = buf.len();
        let read_depth = self.seek_pos as usize - self.read_buffer_start;
        if read_depth + size > self.read_buffer_len {
            size = self.read_buffer_len - read_depth;
        }
        buf[..size].copy_from_slice(&self.read_buffer[read_depth..read_depth + size]);
        self.seek_pos += size as u64;
        if size == 0 {
            panic!("Invalid call to from_read_buffer, size: {}, read buffer index: {}, seek pos: {}, read buffer start: {}",
                   size, read_depth, self.seek_pos, self.read_buffer_start);
        }
        Ok(size)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::err_info;
    //use crate::error::source::SourceError;
    //use std::io::ErrorKind;
    use crate::error::deserialize::DeserializeError;
    use crate::error::serialize::SerializeError;
    use std::time;

    #[derive(Hash, PartialEq, Eq, Copy, Clone, Debug)]
    struct Key([u8; 32]);

    impl DbKey<32> for Key {}
    impl DbBytes<Key> for Key {
        fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
            buffer.resize(32, 0);
            buffer.copy_from_slice(&self.0);
            Ok(())
        }

        fn deserialize(buffer: &[u8]) -> Result<Key, DeserializeError> {
            let mut key = [0_u8; 32];
            key.copy_from_slice(buffer);
            Ok(Key(key))
        }
    }

    type TestDb = DbCore<Key, String, 32>;

    #[test]
    fn test_one() {
        {
            let mut db: TestDb = DbConfig::with_data_path("db_tests", "xxx1", 2)
                .create()
                .truncate()
                .build()
                .unwrap();
            let key = Key([1_u8; 32]);
            db.insert(key, &"Value One".to_string()).unwrap();
            let key = Key([2_u8; 32]);
            db.insert(key, &"Value Two".to_string()).unwrap();
            let key = Key([3_u8; 32]);
            db.insert(key, &"Value Three".to_string()).unwrap();
            let key = Key([4_u8; 32]);
            db.insert(key, &"Value Four".to_string()).unwrap();
            let key = Key([5_u8; 32]);
            db.insert(key, &"Value Five".to_string()).unwrap();

            let v = db.fetch(&key).unwrap();
            assert_eq!(v, "Value Five");
            let key = Key([1_u8; 32]);
            let v = db.fetch(&key).unwrap();
            assert_eq!(v, "Value One");
            let key = Key([3_u8; 32]);
            let v = db.fetch(&key).unwrap();
            assert_eq!(v, "Value Three");
            let key = Key([2_u8; 32]);
            let v = db.fetch(&key).unwrap();
            assert_eq!(v, "Value Two");
            let key = Key([4_u8; 32]);
            let v = db.fetch(&key).unwrap();
            assert_eq!(v, "Value Four");

            db.flush().unwrap();
            let mut iter = db.raw_iter().unwrap().map(|r| r.unwrap());
            let key = Key([1_u8; 32]);
            assert_eq!(iter.next().unwrap(), (key, "Value One".to_string()));
            let key = Key([2_u8; 32]);
            assert_eq!(iter.next().unwrap(), (key, "Value Two".to_string()));
            let key = Key([3_u8; 32]);
            assert_eq!(iter.next().unwrap(), (key, "Value Three".to_string()));
            let key = Key([4_u8; 32]);
            assert_eq!(iter.next().unwrap(), (key, "Value Four".to_string()));
            let key = Key([5_u8; 32]);
            assert_eq!(iter.next().unwrap(), (key, "Value Five".to_string()));
            assert!(iter.next().is_none());
            assert_eq!(db.len(), 5);
        }
        let mut db: TestDb = DbConfig::with_data_path("db_tests", "xxx1", 2)
            .build()
            .unwrap();
        let key = Key([6_u8; 32]);
        db.insert(key, &"Value One2".to_string()).unwrap();
        let key = Key([7_u8; 32]);
        db.insert(key, &"Value Two2".to_string()).unwrap();
        let key = Key([8_u8; 32]);
        db.insert(key, &"Value Three2".to_string()).unwrap();
        db.commit().unwrap();
        let key = Key([6_u8; 32]);
        let v = db.fetch(&key).unwrap();
        assert_eq!(v, "Value One2");
        let key = Key([7_u8; 32]);
        let v = db.fetch(&key).unwrap();
        assert_eq!(v, "Value Two2");
        let key = Key([8_u8; 32]);
        let v = db.fetch(&key).unwrap();
        assert_eq!(v, "Value Three2");
        drop(db);
        let mut db: TestDb = DbConfig::with_data_path("db_tests", "xxx1", 2)
            .build()
            .unwrap();
        let key = Key([6_u8; 32]);
        let v = db.fetch(&key).unwrap();
        assert_eq!(v, "Value One2");
        let key = Key([7_u8; 32]);
        let v = db.fetch(&key).unwrap();
        assert_eq!(v, "Value Two2");
        let key = Key([8_u8; 32]);
        let v = db.fetch(&key).unwrap();
        assert_eq!(v, "Value Three2");
        drop(db);

        let mut iter = DbRawIter::open("db_tests", "xxx1")
            .unwrap()
            .map(|r| r.unwrap());
        let key = Key([1_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value One".to_string()));
        let key = Key([2_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Two".to_string()));
        let key = Key([3_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Three".to_string()));
        let key = Key([4_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Four".to_string()));
        let key = Key([5_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Five".to_string()));
        let key = Key([6_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value One2".to_string()));
        let key = Key([7_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Two2".to_string()));
        let key = Key([8_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Three2".to_string()));

        let db: TestDb = DbConfig::with_data_path("db_tests", "xxx1", 2)
            .build()
            .unwrap();
        let mut iter = db.raw_iter().unwrap().map(|r| r.unwrap());
        let key = Key([1_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value One".to_string()));
        let key = Key([2_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Two".to_string()));
        let key = Key([3_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Three".to_string()));
        let key = Key([4_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Four".to_string()));
        let key = Key([5_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Five".to_string()));
        let key = Key([6_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value One2".to_string()));
        let key = Key([7_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Two2".to_string()));
        let key = Key([8_u8; 32]);
        assert_eq!(iter.next().unwrap(), (key, "Value Three2".to_string()));
        db.destroy();
    }

    #[test]
    fn test_vec_val() {
        let mut db: DbCore<u64, Vec<u8>, 8> = DbConfig::with_data_path("db_tests", "xxx_vec", 1)
            .create()
            .truncate()
            .no_auto_flush()
            //.set_bucket_elements(25)
            //.set_load_factor(0.6)
            .build()
            .unwrap();
        let val = vec![0_u8; 512];
        let max = 1_000_000;
        let start = time::Instant::now();
        for i in 0_u64..max {
            db.insert(i, &val).unwrap();
            if i % 100_000 == 0 {
                db.commit().unwrap();
            }
        }
        println!("XXXX insert time {}", start.elapsed().as_secs_f64());
        assert_eq!(db.len(), max as usize);

        let start = time::Instant::now();
        for i in 0..max {
            let item = db.fetch(&i);
            assert!(item.is_ok(), "Failed on item {i}, {item:?}");
            assert_eq!(&item.unwrap(), &val);
        }
        println!(
            "XXXX fetch (pre commit) time {}",
            start.elapsed().as_secs_f64()
        );

        let start = time::Instant::now();
        db.commit().unwrap();
        println!("XXXX commit time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        //let vals: Vec<String> = db.raw_iter().unwrap().map(|(_k, v)| v).collect();
        let vals: Vec<Vec<u8>> = db.raw_iter().unwrap().map(|r| r.unwrap().1).collect();
        assert_eq!(vals.len(), max as usize);
        for (_i, v) in vals.iter().enumerate() {
            assert_eq!(v, &val);
        }
        println!("XXXX iter time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        for i in 0..max {
            let item = db.fetch(&i);
            assert!(item.is_ok(), "Failed on item {i}");
            assert_eq!(&item.unwrap(), &val);
        }
        println!("XXXX fetch time {}", start.elapsed().as_secs_f64());
    }

    #[test]
    fn test_reindex() {
        let max = 10_000;
        let val = vec![0_u8; 512];
        {
            let mut db: DbCore<u64, Vec<u8>, 8> =
                DbConfig::with_data_path("db_tests", "xxx_reindex", 1)
                    .create()
                    .truncate()
                    .build()
                    .unwrap();
            for i in 0_u64..max {
                db.insert(i, &val).unwrap();
            }
            assert_eq!(db.len(), max as usize);

            for i in 0..max {
                let item = db.fetch(&i);
                assert!(item.is_ok(), "Failed on item {i}, {item:?}");
                assert_eq!(&item.unwrap(), &val);
            }

            db.commit().unwrap();
            for i in 0..max {
                let item = db.fetch(&i);
                assert!(item.is_ok(), "Failed on item {i}");
                assert_eq!(&item.unwrap(), &val);
            }
        }
        let config = DbConfig::with_data_path("db_tests", "xxx_reindex", 1);
        {
            let mut db = DbCore::<u64, Vec<u8>, 8>::reindex(config.clone()).unwrap();
            assert_eq!(db.len(), max as usize);
            for i in 0..max {
                let item = db.fetch(&i);
                assert!(item.is_ok(), "Failed on item {i}/{item:?}");
                assert_eq!(&item.unwrap(), &val);
            }
        }
        {
            let mut data_file = OpenOptions::new()
                .write(true)
                .open(&config.files.data_path())
                .unwrap();
            let data_len = data_file.seek(SeekFrom::End(0)).unwrap();
            data_file.set_len(data_len - 16).unwrap(); // Truncate the file making the last record corrupt.
        }
        let db = DbCore::<u64, Vec<u8>, 8>::open_with_recover(config).unwrap();
        assert_eq!(db.len(), max as usize - 1);
        let vals: Vec<Vec<u8>> = db.raw_iter().unwrap().map(|r| r.unwrap().1).collect();
        // Make sure the last record was removed (corrupted).
        assert_eq!(vals.len(), max as usize - 1);
        for (_i, v) in vals.iter().enumerate() {
            assert_eq!(v, &val);
        }
        db.destroy();
    }

    #[test]
    fn test_destroy() {
        let max = 1_000;
        let val = vec![0_u8; 512];
        {
            let mut db: DbCore<u64, Vec<u8>, 8> =
                DbConfig::with_data_path("db_tests", "xxx_destroy", 1)
                    .create()
                    .truncate()
                    .build()
                    .unwrap();
            for i in 0_u64..max {
                db.insert(i, &val).unwrap();
            }
            assert_eq!(db.len(), max as usize);

            for i in 0..max {
                let item = db.fetch(&i);
                assert!(item.is_ok(), "Failed on item {i}, {item:?}");
                assert_eq!(&item.unwrap(), &val);
            }

            db.commit().unwrap();
            for i in 0..max {
                let item = db.fetch(&i);
                assert!(item.is_ok(), "Failed on item {i}");
                assert_eq!(&item.unwrap(), &val);
            }
        }
        let config = DbConfig::with_data_path("db_tests", "xxx_destroy", 1);
        {
            let mut db = DbCore::<u64, Vec<u8>, 8>::open(config.clone().create()).unwrap();
            assert_eq!(db.len(), max as usize);
            for i in 0..max {
                let item = db.fetch(&i);
                assert!(item.is_ok(), "Failed on item {i}/{item:?}");
                assert_eq!(&item.unwrap(), &val);
            }
            db.destroy();
        }
        let db = DbCore::<u64, Vec<u8>, 8>::open(config.create()).unwrap();
        assert_eq!(db.len(), 0);
        assert_eq!(db.raw_iter().unwrap().count(), 0);
        db.destroy();
    }

    #[test]
    fn test_rename() {
        let max = 1_000;
        let val = vec![0_u8; 512];
        {
            let mut db: DbCore<u64, Vec<u8>, 8> =
                DbConfig::with_data_path("db_tests", "xxx_rename", 1)
                    .create()
                    .truncate()
                    .build()
                    .unwrap();
            for i in 0_u64..max {
                db.insert(i, &val).unwrap();
            }
            assert_eq!(db.len(), max as usize);

            for i in 0..max {
                let item = db.fetch(&i);
                assert!(item.is_ok(), "Failed on item {i}, {item:?}");
                assert_eq!(&item.unwrap(), &val);
            }

            db.commit().unwrap();
            for i in 0..max {
                let item = db.fetch(&i);
                assert!(item.is_ok(), "Failed on item {i}");
                assert_eq!(&item.unwrap(), &val);
            }
            db.rename("xxx_rename2").unwrap();
        }
        {
            let config = DbConfig::with_data_path("db_tests", "xxx_rename3", 1);
            DbCore::<u64, Vec<u8>, 8>::open(config.create()).unwrap();
        }
        let config = DbConfig::with_data_path("db_tests", "xxx_rename2", 1);
        let mut db = DbCore::<u64, Vec<u8>, 8>::open(config.create()).unwrap();
        assert_eq!(db.len(), max as usize);
        for i in 0..max {
            let item = db.fetch(&i);
            assert!(item.is_ok(), "Failed on item {i}/{item:?}");
            assert_eq!(&item.unwrap(), &val);
        }
        assert!(matches!(
            db.rename("xxx_rename3").unwrap_err(),
            RenameError::FilesExist
        ));
        db.destroy();
        {
            // Clean up db files.
            let config = DbConfig::with_data_path("db_tests", "xxx_rename3", 1);
            let db = DbCore::<u64, Vec<u8>, 8>::open(config.create()).unwrap();
            db.destroy();
        }
    }

    #[test]
    fn test_50k() {
        /*let e: Box<dyn std::error::Error + Send + Sync> =
            std::io::Error::new(ErrorKind::Other, "XXX".to_string()).into();
        let e: SourceError = e.into();
        assert!(e.is::<std::io::Error>());
        if let Some(e) = e.downcast_ref::<std::io::Error>() {
            println!("XXXX {:?}", e);
        }
        println!("{}", err_info!());*/
        let mut db: DbCore<u64, String, 8> = DbConfig::with_data_path("db_tests", "xxx50k", 10)
            .create()
            .truncate()
            .no_auto_flush()
            .set_bucket_cache_size(0) //32 * 1024 * 1024)
            .set_bucket_size(512)
            //.set_bucket_elements(25)
            //.set_load_factor(0.6)
            .build()
            .unwrap();
        println!("XXXX version: {}", db.version());
        println!("XXXX appnum: {}", db.appnum());
        println!("XXXX uid: {}", db.uid());
        println!("XXXX salt: {}", db.salt());
        println!("XXXX pepper: {}", db.pepper());
        assert!(!db.contains_key(&0).unwrap());
        assert!(!db.contains_key(&10).unwrap());
        assert!(!db.contains_key(&35_000).unwrap());
        assert!(!db.contains_key(&49_000).unwrap());
        assert!(!db.contains_key(&50_000).unwrap());
        let max = 1_000_000;
        let start = time::Instant::now();
        for i in 0_u64..max {
            db.insert(i, &format!("Value {i}")).unwrap();
            if i % 100_000 == 0 {
                db.commit().unwrap();
            }
        }
        println!("XXXX insert time {}", start.elapsed().as_secs_f64());
        assert_eq!(db.len(), max as usize);
        assert!(db.contains_key(&0).unwrap());
        assert!(db.contains_key(&10).unwrap());
        assert!(db.contains_key(&35_000).unwrap());
        assert!(db.contains_key(&49_000).unwrap());
        assert!(!db.contains_key(&max).unwrap());

        let start = time::Instant::now();
        for i in 0..max {
            let item = db.fetch(&(i));
            assert!(item.is_ok(), "Failed on item {i}, {item:?}");
            assert_eq!(&item.unwrap(), &format!("Value {i}"));
        }
        println!(
            "XXXX fetch (pre commit) time {}",
            start.elapsed().as_secs_f64()
        );

        let start = time::Instant::now();
        db.commit().unwrap();
        println!("XXXX commit time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        //let vals: Vec<String> = db.raw_iter().unwrap().map(|(_k, v)| v).collect();
        let vals: Vec<String> = db.raw_iter().unwrap().map(|r| r.unwrap().1).collect();
        assert_eq!(vals.len(), max as usize);
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(v, &format!("Value {i}"));
        }
        println!("XXXX iter time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        //assert_eq!(&db.fetch(&35_000).unwrap(), "Value 35000");
        for i in 0..max {
            let item = db.fetch(&i);
            assert!(item.is_ok(), "Failed on item {i}");
            assert_eq!(&item.unwrap(), &format!("Value {i}"));
        }
        println!("XXXX fetch time {}", start.elapsed().as_secs_f64());

        let start = time::Instant::now();
        //assert_eq!(&db.fetch(&35_000).unwrap(), "Value 35000");
        for i in 1..=max {
            //if i % 10_000 == 0 {println!("XXXXX fetching {}", max - i as u64); }
            let item = db.fetch(&(max - i));
            assert!(item.is_ok(), "Failed on item {}", max - i);
            assert_eq!(&item.unwrap(), &format!("Value {}", max - i));
        }
        println!("XXXX fetch (REV) time {}", start.elapsed().as_secs_f64());

        let start = time::Instant::now();
        //assert_eq!(&db.fetch(&35_000).unwrap(), "Value 35000");
        let max_val = max - 1;
        for i in 0..(max / 2) {
            //if i % 10_000 == 0 {println!("XXXXX fetching {}", max - i as u64); }
            let item = db.fetch(&(i));
            assert!(item.is_ok(), "Failed on item {i}");
            assert_eq!(&item.unwrap(), &format!("Value {i}"));

            let item = db.fetch(&(max_val - i));
            assert!(item.is_ok(), "Failed on item {}", max_val - i);
            assert_eq!(&item.unwrap(), &format!("Value {}", max_val - i));
        }
        println!("XXXX fetch (MIX) time {}", start.elapsed().as_secs_f64());
    }

    #[test]
    fn test_x50k_str() {
        let mut db: DbCore<String, String, 0> =
            DbConfig::with_data_path("db_tests", "xxx50k_str", 1)
                .create()
                .truncate()
                .build()
                .unwrap();
        for i in 0..50_000 {
            db.insert(format!("key {i}"), &format!("Value {i}"))
                .unwrap();
        }
        assert_eq!(db.len(), 50_000);
        db.flush().unwrap();
        let vals: Vec<String> = db.raw_iter().unwrap().map(|r| r.unwrap().1).collect();
        assert_eq!(vals.len(), 50_000);
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(v, &format!("Value {i}"));
        }
        assert_eq!(&db.fetch(&"key 35000".to_string()).unwrap(), "Value 35000");
        for i in 0..50_000 {
            assert_eq!(
                &db.fetch(&format!("key {i}"))
                    .unwrap_or_else(|e| panic!("Failed to read item {i}, {e}")),
                &format!("Value {i}")
            );
        }
        db.destroy();
    }

    #[test]
    fn test_duplicates() {
        {
            let mut db: DbCore<u64, u64, 8> = DbConfig::with_data_path("db_tests", "xxxDupTest", 1)
                .create()
                .truncate()
                .build()
                .unwrap();
            db.insert(1, &1).unwrap();
            db.insert(2, &2).unwrap();
            db.insert(3, &3).unwrap();
            db.insert(4, &4).unwrap();
            db.insert(5, &5).unwrap();
            let r1 = db.insert(1, &10);
            assert!(matches!(r1.unwrap_err(), InsertError::DuplicateKey));
            let r2 = db.insert(3, &10);
            assert!(matches!(r2.unwrap_err(), InsertError::DuplicateKey));
            let r3 = db.insert(5, &10);
            assert!(matches!(r3.unwrap_err(), InsertError::DuplicateKey));
            db.insert(6, &6).unwrap();
            assert_eq!(1, db.fetch(&1).unwrap());
            assert_eq!(2, db.fetch(&2).unwrap());
            assert_eq!(3, db.fetch(&3).unwrap());
            assert_eq!(4, db.fetch(&4).unwrap());
            assert_eq!(5, db.fetch(&5).unwrap());
        }
        let mut db: DbCore<u64, u64, 8> = DbConfig::with_data_path("db_tests", "xxxDupTest", 1)
            .allow_duplicate_inserts()
            .build()
            .unwrap();
        assert_eq!(1, db.fetch(&1).unwrap());
        assert_eq!(2, db.fetch(&2).unwrap());
        assert_eq!(3, db.fetch(&3).unwrap());
        assert_eq!(4, db.fetch(&4).unwrap());
        assert_eq!(5, db.fetch(&5).unwrap());
        db.insert(1, &10).unwrap();
        db.insert(3, &30).unwrap();
        db.insert(5, &50).unwrap();
        assert_eq!(10, db.fetch(&1).unwrap());
        assert_eq!(2, db.fetch(&2).unwrap());
        assert_eq!(30, db.fetch(&3).unwrap());
        assert_eq!(4, db.fetch(&4).unwrap());
        assert_eq!(50, db.fetch(&5).unwrap());
        db.destroy();
    }
}
