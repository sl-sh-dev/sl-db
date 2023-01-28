//! Main module for the SLDB core.  This implements the core sync single threaded access to the DB.

use crate::db::data_header::{DataHeader, BUCKET_ELEMENT_SIZE};
use crate::db::hdx_header::{HdxHeader, HDX_HEADER_SIZE};
use crate::db::odx_header::OdxHeader;
use crate::db_config::{DbConfig, DbFiles};
use crate::db_raw_iter::DbRawIter;
use crate::error::flush::FlushError;
use crate::error::insert::InsertError;
use crate::error::serialize::SerializeError;
use crate::error::ReadKeyError;
use crate::error::{
    deserialize::DeserializeError, CommitError, FetchError, LoadHeaderError, OpenError,
};
use crate::fxhasher::{FxHashMap, FxHasher};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::{fs, io};

pub mod data_header;
pub mod hdx_header;
pub mod odx_header;

/// Required trait for a key.  Note that setting KSIZE to 0 indicates a variable sized key.
pub trait DbKey<const KSIZE: u16>: Eq + Hash + Debug {
    /// Defines the key size for fixed size keys (will be 0 for variable sized keys).
    const KEY_SIZE: u16 = KSIZE;

    /// True if this DB has a fixed size key.
    #[inline(always)]
    fn is_fixed_key_size() -> bool {
        Self::KEY_SIZE > 0
    }

    /// True if this DB has a variable size key.
    /// Variable sized keys require key sizes to be saved in the DB.
    #[inline(always)]
    fn is_variable_key_size() -> bool {
        Self::KEY_SIZE == 0
    }
}

/// Trait that all key and values types must implement to convert to from bytes for the DB.
pub trait DbBytes<T> {
    /// Serialize the type into buffer.
    /// Buffer is expected to contain exactly the serialized type and nothing more.
    /// Implementations can make no assumptions about the state of the buffer passed in.
    /// Resizing the buffer is expected (why it is a Vec not a slice) and may already have sufficient
    /// capacity.
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError>;

    /// Deserialize a byte slice back into the type or error out.
    fn deserialize(buffer: &[u8]) -> Result<T, DeserializeError>;
}

/// An instance of a DB.
/// Will consist of a data file (.dat), hash index (.hdx) and hash bucket overflow file (.odx).
/// This is synchronous and single threaded.  It is intended to keep the algorithms clearer and
/// to be wrapped for async or multi-threaded synchronous use.
pub struct DbCore<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    header: DataHeader,
    data_file: File,
    data_file_end: u64,
    hdx_file: File,
    odx_file: File,
    hasher: S,
    key_buffer: Vec<u8>,
    value_buffer: Vec<u8>,
    write_buffer: Vec<u8>,
    read_buffer: Vec<u8>,
    read_buffer_start: usize,
    read_buffer_len: usize,
    bucket_cache: FxHashMap<u64, Vec<u8>>,
    hdx_header: HdxHeader,
    modulus: u32,
    load_factor: f32,
    capacity: u64,
    seek_pos: u64,
    config: DbConfig,
    failed: bool,
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

/// Macro to get an iterator over all the entries in a bucket (including overflow buckets).
/// For internal use only!  The unsafe should be fine as long as the iterators are used internally
/// in the single threaded/sync module.
macro_rules! bucket_iter {
    ($db:expr, $bucket:expr) => {{
        // Break the db lifetime away so we can get a usable bucket iter.
        // The db reference that the iter holds is used as a Read/Seek object and since the data
        // file is append only and this code is single threaded this should be perfectly safe.
        let unsafe_db: &mut DbCore<K, V, KSIZE, S> = unsafe {
            ($db as *mut DbCore<K, V, KSIZE, S>)
                .as_mut()
                .expect("this can't be null")
        };
        let mut buffer = vec![0; $db.hdx_header.bucket_size() as usize];
        if let Some(bucket) = $db.bucket_cache.get(&$bucket) {
            buffer.copy_from_slice(&bucket);
            // These cached buffers may be missing their crc32 so add it now.
            add_crc32(&mut buffer);
        } else {
            let bucket_size = $db.hdx_header.bucket_size() as usize;
            let bucket_pos: u64 = (HDX_HEADER_SIZE + ($bucket as usize * bucket_size)) as u64;
            if let Ok(_) = $db.hdx_file.seek(SeekFrom::Start(bucket_pos)) {
                let _ = $db.hdx_file.read_exact(&mut buffer);
            }
        }
        BucketIter::new(
            &mut unsafe_db.odx_file,
            buffer,
            $db.hdx_header.bucket_elements(),
        )
    }};
}

impl<K, V, const KSIZE: u16, S> Drop for DbCore<K, V, KSIZE, S>
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

impl<K, V, const KSIZE: u16, S> DbCore<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Open a new or reopen an existing database.
    pub fn open(config: DbConfig) -> Result<Self, OpenError> {
        if config.create {
            // Best effort to create the dir if asked to create the DB.
            let _ = fs::create_dir_all(&config.files.dir);
        }
        let hasher = S::default();
        let (mut data_file, header) =
            Self::open_data_file(&config).map_err(OpenError::DataFileOpen)?;
        let data_file_end = data_file.seek(SeekFrom::End(0)).map_err(OpenError::Seek)?;
        let (hdx_file, hdx_header) =
            Self::open_hdx_file(&header, &config, &hasher).map_err(OpenError::IndexFileOpen)?;
        // Don't want buckets and modulus to be the same, so +1
        let modulus = (hdx_header.buckets() + 1).next_power_of_two();
        let (write_buffer, bucket_cache) = if config.write {
            let mut bucket_cache = FxHashMap::default();
            bucket_cache.reserve(500);
            (
                Vec::with_capacity(config.write_buffer_size as usize),
                bucket_cache,
            )
        } else {
            // If opening read only wont need capacity.
            (Vec::new(), FxHashMap::default())
        };
        let mut read_buffer = vec![0; config.read_buffer_size as usize];
        // Prime the read buffer so we don't have to check if it is empty, etc.
        let mut read_buffer_len = 0_usize;
        if data_file_end > 0 {
            data_file
                .seek(SeekFrom::Start(0))
                .map_err(OpenError::Seek)?;
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
        let (odx_file, _odx_header) =
            Self::open_odx_file(&hdx_header, &config).map_err(OpenError::IndexFileOpen)?;
        // TODO, crosscheck all the headers and make sure everything checks out.
        Ok(Self {
            header,
            data_file,
            data_file_end,
            hdx_file,
            odx_file,
            hasher,
            hdx_header,
            modulus,
            load_factor: hdx_header.load_factor(),
            capacity: hdx_header.buckets() as u64 * hdx_header.bucket_elements() as u64,
            key_buffer: Vec::new(),
            value_buffer: Vec::new(),
            write_buffer,
            read_buffer,
            read_buffer_start: 0,
            read_buffer_len,
            bucket_cache,
            seek_pos: 0,
            config,
            failed: false,
            _key: PhantomData,
            _value: PhantomData,
        })
    }

    /// Will destroy the existing index for DB and rebuild it based on the data file.
    /// This will also verify the integrity of the data file.  If only the final record is corrupt
    /// then the file will be truncated to leave a valid DB.  Other corrupt records will be ignored
    /// (they will be garbage in the data file but won't be indexed).
    pub fn reindex(config: DbConfig) -> Result<Self, OpenError> {
        let config = config.create();
        let _ = fs::remove_file(&config.files.hdx_file);
        let _ = fs::remove_file(&config.files.odx_file);

        let mut db = Self::open(config)?;

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
                db.hdx_header.inc_values();
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
        let _ = fs::remove_file(&files.data_file);
        let _ = fs::remove_file(&files.hdx_file);
        let _ = fs::remove_file(&files.odx_file);
    }

    /// Returns a reference to the file names for this DB.
    pub fn files(&self) -> &DbFiles {
        &self.config.files
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub fn fetch(&mut self, key: &K) -> Result<V, FetchError> {
        let hash = self.hash(key);
        let bucket = self.get_bucket(hash);

        let mut iter = bucket_iter!(self, bucket);
        for (rec_hash, rec_pos) in &mut iter {
            // rec_pos > 0 handles degenerate case of a 0 hash.
            if hash == rec_hash && rec_pos > 0 {
                let (rkey, val) = self.read_record(rec_pos)?;
                if &rkey == key {
                    return Ok(val);
                }
            }
        }
        if iter.crc_failure {
            Err(FetchError::CrcFailed)
        } else {
            Err(FetchError::NotFound)
        }
    }

    /// True if the database contains key.
    pub fn contains_key(&mut self, key: &K) -> Result<bool, ReadKeyError> {
        let hash = self.hash(key);
        let bucket = self.get_bucket(hash);
        let mut iter = bucket_iter!(self, bucket);
        for (rec_hash, rec_pos) in &mut iter {
            // rec_pos > 0 handles degenerate case of a 0 hash.
            if hash == rec_hash && rec_pos > 0 {
                let rkey = self.read_key(rec_pos)?;
                if &rkey == key {
                    return Ok(true);
                }
            }
        }
        if iter.crc_failure {
            Err(ReadKeyError::CrcFailed)
        } else {
            Ok(false)
        }
    }

    /// If in read-only mode refresh the index header data from on-disk.
    /// Useful if the DB is also opened for writing.
    pub fn refresh_index(&mut self) {
        if !self.config.write {
            if let Ok(header) = HdxHeader::load_header(&mut self.hdx_file) {
                self.hdx_header = header;
                // Don't want buckets and modulus to be the same, so +1
                self.modulus = (self.hdx_header.buckets() + 1).next_power_of_two();
            }
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
        self.hdx_header.inc_values();
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
        self.hdx_header.values() as usize
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
        self.hdx_header.salt()
    }

    /// Return the DB pepper (generated at creation from the salt with Hasher).
    /// Can be used with the salt to test the hasher.
    pub fn pepper(&self) -> u64 {
        self.hdx_header.pepper()
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
        self.odx_file
            .sync_all()
            .map_err(CommitError::IndexFileSync)?;
        self.hdx_file
            .sync_all()
            .map_err(CommitError::IndexFileSync)?;
        Ok(())
    }

    /// Flush any in memory caches to file.
    /// Note this is only a flush not a commit, it does not do a sync on the files.
    pub fn flush(&mut self) -> Result<(), FlushError> {
        if !self.config.write || self.failed {
            return Err(FlushError::ReadOnly);
        }
        Write::flush(self).map_err(FlushError::WriteData)?;
        self.save_bucket_cache()
            .map_err(FlushError::WriteIndexData)?;
        self.hdx_file
            .seek(SeekFrom::Start(0))
            .map_err(FlushError::IndexHeader)?;
        self.hdx_header
            .write_header(&mut self.hdx_file)
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
                .open(&config.files.data_file)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .append(config.write)
            .create(config.create && config.write)
            .open(&config.files.data_file)?;
        file.seek(SeekFrom::End(0))?;
        let file_end = file.seek(SeekFrom::Current(0))?;

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

    fn open_hdx_file(
        data_header: &DataHeader,
        config: &DbConfig,
        hasher: &S,
    ) -> Result<(File, HdxHeader), LoadHeaderError> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(config.write)
            .create(config.create && config.write)
            .truncate(config.truncate && config.write)
            .open(&config.files.hdx_file)?;
        file.seek(SeekFrom::End(0))?;
        let file_end = file.seek(SeekFrom::Current(0))?;

        let header = if file_end == 0 {
            let mut fx_hasher = FxHasher::default();
            fx_hasher.write_u64(data_header.uid());
            let salt = fx_hasher.finish();
            let mut hasher = hasher.build_hasher();
            salt.hash(&mut hasher);
            let pepper = hasher.finish();
            let header = HdxHeader::from_data_header(data_header, config, salt, pepper);
            header.write_header(&mut file)?;
            let bucket_size = header.bucket_size() as usize;
            let mut buffer = vec![0_u8; bucket_size];
            add_crc32(&mut buffer);
            for _ in 0..header.buckets() {
                file.write_all(&buffer)?;
            }
            header
        } else {
            let header = HdxHeader::load_header(&mut file)?;
            // Basic validation of the odx header.
            if header.version() != data_header.version() {
                return Err(LoadHeaderError::InvalidIndexVersion);
            }
            if header.appnum() != data_header.appnum() {
                return Err(LoadHeaderError::InvalidIndexAppNum);
            }
            if header.uid() != data_header.uid() {
                return Err(LoadHeaderError::InvalidIndexUID);
            }
            // Check the salt/pepper.  This will make sure you are using the same hasher and it seems
            // to be stable (not the default Rust hasher for instance) since changing the hasher would
            // invalidate the index.
            let mut hasher = hasher.build_hasher();
            header.salt().hash(&mut hasher);
            if header.pepper() != hasher.finish() {
                return Err(LoadHeaderError::InvalidHasher);
            }
            header
        };
        Ok((file, header))
    }

    fn open_odx_file(
        hdx_header: &HdxHeader,
        config: &DbConfig,
    ) -> Result<(File, OdxHeader), LoadHeaderError> {
        if config.truncate && config.write {
            // truncate is incompatible with append so truncate then open for append.
            OpenOptions::new()
                .write(true)
                .create(config.create)
                .truncate(true)
                .open(&config.files.odx_file)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .append(config.write)
            .create(config.create && config.write)
            .open(&config.files.odx_file)?;
        file.seek(SeekFrom::End(0))?;
        let file_end = file.seek(SeekFrom::Current(0))?;

        let header = if file_end == 0 {
            let header = OdxHeader::from_hdx_header(hdx_header);
            header.write_header(&mut file)?;
            header
        } else {
            let header = OdxHeader::load_header(&mut file)?;
            // Basic validation of the odx header.
            if header.version() != hdx_header.version() {
                return Err(LoadHeaderError::InvalidIndexVersion);
            }
            if header.appnum() != hdx_header.appnum() {
                return Err(LoadHeaderError::InvalidIndexAppNum);
            }
            if header.uid() != hdx_header.uid() {
                return Err(LoadHeaderError::InvalidIndexUID);
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

    /// Return the bucket that will contain hash (if hash is available).
    fn get_bucket(&self, hash: u64) -> u64 {
        let modulus = self.modulus as u64;
        let bucket = hash % modulus;
        if bucket >= self.hdx_header.buckets() as u64 {
            bucket - modulus / 2
        } else {
            bucket
        }
    }

    /// Add one new bucket to the hash index.
    /// Buckets are split "in order" determined by the current modulus not based on how full any
    /// bucket is.
    fn split_one_bucket(&mut self) -> Result<(), InsertError> {
        let old_modulus = self.modulus;
        // This is the bucket that is being split.
        let split_bucket = (self.hdx_header.buckets() - (old_modulus / 2)) as u64;
        self.hdx_header.inc_buckets();
        // This is the newly created bucket that the items in split_bucket will possibly be moved into.
        let new_bucket = self.hdx_header.buckets() as u64 - 1;
        // Don't want buckets and modulus to be the same, so +1
        self.modulus = (self.hdx_header.buckets() + 1).next_power_of_two();

        let bucket_size = self.hdx_header.bucket_size() as usize;
        let mut buffer = vec![0; bucket_size];
        let mut buffer2 = vec![0; bucket_size];

        let mut iter = bucket_iter!(self, split_bucket);
        for (rec_hash, rec_pos) in &mut iter {
            if rec_pos > 0 {
                let bucket = self.get_bucket(rec_hash);
                if bucket != split_bucket && bucket != new_bucket {
                    panic!(
                        "got bucket {}, expected {} or {}, mod {}",
                        bucket,
                        split_bucket,
                        self.hdx_header.buckets() - 1,
                        self.modulus
                    );
                }
                if bucket == split_bucket {
                    self.save_to_bucket_buffer(None, rec_hash, rec_pos, &mut buffer)
                        .map_err(|_| InsertError::IndexOverflow)?;
                } else {
                    self.save_to_bucket_buffer(None, rec_hash, rec_pos, &mut buffer2)
                        .map_err(|_| InsertError::IndexOverflow)?;
                }
            }
        }
        if iter.crc_failure {
            return Err(InsertError::IndexCrcError);
        }
        self.bucket_cache.insert(split_bucket, buffer);
        self.bucket_cache.insert(new_bucket, buffer2);
        Ok(())
    }

    /// Add buckets to expand capacity.
    /// Capacity is number of elements per bucket * number of buckets.
    /// If current length >= capacity * load factor then split buckets until this is not true.
    fn expand_buckets(&mut self) -> Result<(), InsertError> {
        if self.config.allow_bucket_expansion {
            while self.len() >= (self.capacity as f32 * self.load_factor) as usize {
                self.split_one_bucket()?;
                self.capacity =
                    self.hdx_header.buckets() as u64 * self.hdx_header.bucket_elements() as u64;
            }
        }
        Ok(())
    }

    /// Save the (hash, position) tuple to the bucket.  Handles overflow records.
    fn save_to_bucket(&mut self, key: &K, hash: u64, record_pos: u64) -> Result<(), InsertError> {
        let bucket = self.get_bucket(hash);
        let mut buffer = if let Some(buf) = self.bucket_cache.remove(&bucket) {
            // Get the bucket from the bucket cache.
            buf
        } else {
            // Read the bucket from the index and verify (crc32) it.
            let bucket_size = self.hdx_header.bucket_size() as usize;
            let mut buffer = vec![0; bucket_size];
            let bucket_pos: u64 = (HDX_HEADER_SIZE + (bucket as usize * bucket_size)) as u64;
            {
                self.hdx_file
                    .seek(SeekFrom::Start(bucket_pos))
                    .map_err(|e| InsertError::KeyError(e.into()))?;
                self.hdx_file
                    .read_exact(&mut buffer)
                    .map_err(|e| InsertError::KeyError(e.into()))?;
                if !check_crc(&buffer) {
                    return Err(InsertError::KeyError(ReadKeyError::CrcFailed));
                }
            }
            buffer
        };

        let result = self.save_to_bucket_buffer(Some(key), hash, record_pos, &mut buffer);
        // Need to make sure the bucket goes into the cache even on error.
        self.bucket_cache.insert(bucket, buffer);
        result
    }

    /// Flush (save) the hash bucket cache to disk.
    fn save_bucket_cache(&mut self) -> Result<(), io::Error> {
        let bucket_size = self.hdx_header.bucket_size() as usize;
        for (bucket, mut buffer) in self.bucket_cache.drain() {
            let bucket_pos: u64 = (HDX_HEADER_SIZE + (bucket as usize * bucket_size)) as u64;
            add_crc32(&mut buffer);
            // Seeking and writing past the file end seems to extend the file correctly.
            self.hdx_file.seek(SeekFrom::Start(bucket_pos))?;
            self.hdx_file.write_all(&buffer)?;
        }
        Ok(())
    }

    /// Save the (hash, position, record_size) tuple to the bucket.  Handles overflow records.
    /// If this produces and Error then buffer will contain the same data.
    fn save_to_bucket_buffer(
        &mut self,
        key: Option<&K>,
        hash: u64,
        record_pos: u64,
        buffer: &mut [u8],
    ) -> Result<(), InsertError> {
        let mut pos = 8; // Skip the overflow file pos.
        for i in 0..self.hdx_header.bucket_elements() as u64 {
            let mut buf64 = [0_u8; 8];
            buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
            let rec_hash = u64::from_le_bytes(buf64);
            pos += 8;
            buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
            let rec_pos = u64::from_le_bytes(buf64);
            pos += 8;
            // Test rec_pos == 0 to handle degenerate case of a hash of 0.
            // Find an empty element should indicate no more elements (so the key check below is ok).
            if rec_hash == 0 && rec_pos == 0 {
                // Seek to the element we found that was empty and write the hash and position into it.
                let mut pos = 8 + (i as usize * BUCKET_ELEMENT_SIZE);
                buffer[pos..pos + 8].copy_from_slice(&hash.to_le_bytes());
                pos += 8;
                buffer[pos..pos + 8].copy_from_slice(&record_pos.to_le_bytes());
                return Ok(());
            }
            if rec_hash == hash {
                if let Some(key) = key {
                    if let Ok(rkey) = self.read_key(rec_pos) {
                        if &rkey == key {
                            if self.config.allow_duplicate_inserts {
                                // Overwrite the old element with the new in the index. This will leave
                                // garbage in the data file but lookups will work and be consistent.
                                let mut pos = 8 + (i as usize * BUCKET_ELEMENT_SIZE);
                                buffer[pos..pos + 8].copy_from_slice(&hash.to_le_bytes());
                                pos += 8;
                                buffer[pos..pos + 8].copy_from_slice(&record_pos.to_le_bytes());
                                return Ok(());
                            } else {
                                // Don't allow duplicates so error out (caller should roll back insert).
                                return Err(InsertError::DuplicateKey);
                            }
                        }
                    }
                }
            }
        }
        // false, save bucket as an overflow record and add to the fresh bucket.
        let overflow_pos = self
            .odx_file
            .seek(SeekFrom::End(0))
            .map_err(|e| InsertError::KeyError(e.into()))?;
        add_crc32(buffer);
        // Write the old buffer into the data file as an overflow record.
        self.odx_file
            .write_all(buffer)
            .map_err(|e| InsertError::KeyError(e.into()))?;
        // clear buffer and reset to 0.
        buffer.fill(0);
        // Copy the position of the overflow record into the first u64.
        buffer[0..8].copy_from_slice(&overflow_pos.to_le_bytes());
        // First element will be the hash and position being saved (rest of new bucket is empty).
        buffer[8..16].copy_from_slice(&hash.to_le_bytes());
        buffer[16..24].copy_from_slice(&record_pos.to_le_bytes());
        Ok(())
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
        self.key_buffer.resize(key_size as usize, 0);
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

impl<K, V, const KSIZE: u16, S> Read for DbCore<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Read for the DbInner.  This allows other code to not worry about whether data is read from
    /// the file, write buffer or the read buffer.  The file and write buffer will not have overlapping records
    /// so this will not read across them in one call.  This will not happen on a proper DB although
    /// the Read contract should handle this fine.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.seek_pos >= self.data_file_end {
            let write_pos = (self.seek_pos - self.data_file_end) as usize;
            if write_pos < self.write_buffer.len() {
                let mut size = buf.len();
                if write_pos + size > self.write_buffer.len() {
                    size = self.write_buffer.len() - write_pos;
                }
                buf[..size].copy_from_slice(&self.write_buffer[write_pos..write_pos + size]);
                self.seek_pos += size as u64;
                Ok(size)
            } else {
                Ok(0)
            }
        } else if self.seek_pos >= self.read_buffer_start as u64
            && self.seek_pos < (self.read_buffer_start + self.read_buffer_len) as u64
        {
            self.copy_read_buffer(buf)
        } else {
            let mut seek_pos = self.seek_pos;
            let mut end = self.data_file_end - seek_pos;
            if end < self.config.read_buffer_size as u64 {
                // If remaining bytes are less then the buffer pull back seek_pos to fill the buffer.
                seek_pos = if self.data_file_end > self.config.read_buffer_size as u64 {
                    self.data_file_end - self.config.read_buffer_size as u64
                } else {
                    0
                };
            } else {
                // Put the seek position in the mid point of the read buffer.  This might help increase
                // buffer hits or might do nothing or hurt depending on fetch patterns.
                seek_pos = if seek_pos > (self.config.read_buffer_size / 2) as u64 {
                    seek_pos - (self.config.read_buffer_size / 2) as u64
                } else {
                    0
                };
            }
            end = self.data_file_end - seek_pos;
            if end > 0 {
                self.data_file.seek(SeekFrom::Start(seek_pos))?;
                if end < self.config.read_buffer_size as u64 {
                    self.data_file
                        .read_exact(&mut self.read_buffer[..end as usize])?;
                    self.read_buffer_len = end as usize;
                } else {
                    self.data_file.read_exact(&mut self.read_buffer[..])?;
                    self.read_buffer_len = self.config.read_buffer_size as usize;
                }
                self.read_buffer_start = seek_pos as usize;
                self.copy_read_buffer(buf)
            } else {
                Ok(0)
            }
        }
    }
}

impl<K, V, const KSIZE: u16, S> Seek for DbCore<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Seek on the DbInner treating the file and write cache as one byte array.
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(pos) => self.seek_pos = pos,
            SeekFrom::End(pos) => {
                let end = (self.data_file_end + self.write_buffer.len() as u64) as i64 + pos;
                if end >= 0 {
                    self.seek_pos = end as u64;
                } else {
                    self.seek_pos = 0;
                }
            }
            SeekFrom::Current(pos) => {
                let end = self.seek_pos as i64 + pos;
                if end >= 0 {
                    self.seek_pos = end as u64;
                } else {
                    self.seek_pos = 0;
                }
            }
        }
        Ok(self.seek_pos)
    }
}

impl<K, V, const KSIZE: u16, S> Write for DbCore<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.write_buffer.len() >= self.config.write_buffer_size as usize {
            self.data_file.write_all(&self.write_buffer)?;
            self.data_file_end += self.write_buffer.len() as u64;
            self.write_buffer.clear();
        }
        let write_buffer_len = self.write_buffer.len();
        let write_capacity = self.config.write_buffer_size as usize - write_buffer_len;
        let buf_len = buf.len();
        if write_capacity > buf_len {
            self.write_buffer.write_all(buf)?;
            Ok(buf_len)
        } else {
            self.write_buffer.write_all(&buf[..write_capacity])?;
            self.data_file.write_all(&self.write_buffer)?;
            self.data_file_end += self.write_buffer.len() as u64;
            self.write_buffer.clear();
            Ok(write_capacity)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.data_file.write_all(&self.write_buffer)?;
        self.data_file_end += self.write_buffer.len() as u64;
        self.write_buffer.clear();
        Ok(())
    }
}

/// Check buffers crc32.  The last 4 bytes of the buffer are the CRC32 code and rest of the buffer
/// is checked against that.
fn check_crc(buffer: &[u8]) -> bool {
    let len = buffer.len();
    if len < 5 {
        return false;
    }
    let mut crc32_hasher = crc32fast::Hasher::new();
    crc32_hasher.update(&buffer[..(len - 4)]);
    let calc_crc32 = crc32_hasher.finalize();
    let mut buf32 = [0_u8; 4];
    buf32.copy_from_slice(&buffer[(len - 4)..]);
    let read_crc32 = u32::from_le_bytes(buf32);
    calc_crc32 == read_crc32
}

/// Add a crc32 code to buffer.  The last four bytes of buffer are overwritten by the crc32 code of
/// the rest of the buffer.
pub(crate) fn add_crc32(buffer: &mut [u8]) {
    let len = buffer.len();
    if len < 4 {
        return;
    }
    let mut crc32_hasher = crc32fast::Hasher::new();
    crc32_hasher.update(&buffer[..(len - 4)]);
    let crc32 = crc32_hasher.finalize();
    buffer[len - 4..].copy_from_slice(&crc32.to_le_bytes());
}

/// Iterates over the (hash, record_position) values contained in a bucket.
struct BucketIter<'src, R: Read + Seek> {
    odx_file: &'src mut R,
    buffer: Vec<u8>,
    bucket_pos: usize,
    overflow_pos: u64,
    elements: u16,
    crc_failure: bool,
}

impl<'src, R: Read + Seek> BucketIter<'src, R> {
    fn new(odx_file: &'src mut R, buffer: Vec<u8>, elements: u16) -> Self {
        let mut buf = [0_u8; 8]; // buffer for converting to u64s (needs an array)
        buf.copy_from_slice(&buffer[0..8]);
        let overflow_pos = u64::from_le_bytes(buf);
        let crc_failure = !check_crc(&buffer);
        Self {
            odx_file,
            buffer,
            bucket_pos: 0,
            overflow_pos,
            elements,
            crc_failure,
        }
    }
}

impl<'src, R: Read + Seek> Iterator for &mut BucketIter<'src, R> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.crc_failure {
            return None;
        }
        // For reading u64 values, needs an array.
        let mut buf64 = [0_u8; 8];
        loop {
            if self.bucket_pos < self.elements as usize {
                let mut pos = 8 + (self.bucket_pos * BUCKET_ELEMENT_SIZE);
                buf64.copy_from_slice(&self.buffer[pos..(pos + 8)]);
                let hash = u64::from_le_bytes(buf64);
                pos += 8;
                buf64.copy_from_slice(&self.buffer[pos..(pos + 8)]);
                let rec_pos = u64::from_le_bytes(buf64);
                if hash == 0 && rec_pos == 0 {
                    self.bucket_pos += 1;
                } else {
                    self.bucket_pos += 1;
                    return Some((hash, rec_pos));
                }
            } else if self.overflow_pos > 0 {
                // We have an overflow bucket to search as well.
                self.odx_file
                    .seek(SeekFrom::Start(self.overflow_pos))
                    .ok()?;
                self.odx_file.read_exact(&mut self.buffer[..]).ok()?;
                if !check_crc(&self.buffer) {
                    self.crc_failure = true;
                    return None;
                }
                self.bucket_pos = 0;
                buf64.copy_from_slice(&self.buffer[0..8]);
                self.overflow_pos = u64::from_le_bytes(buf64);
            } else {
                return None;
            }
        }
    }
}

impl DbKey<0> for String {}
impl DbBytes<String> for String {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
        let bytes = self.as_bytes();
        buffer.resize(bytes.len(), 0);
        buffer.copy_from_slice(bytes);
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<String, DeserializeError> {
        Ok(String::from_utf8_lossy(buffer).to_string())
    }
}

impl DbKey<8> for u64 {}
impl DbBytes<u64> for u64 {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
        buffer.resize(8, 0);
        buffer.copy_from_slice(&self.to_le_bytes());
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<u64, DeserializeError> {
        let mut buf = [0_u8; 8];
        buf.copy_from_slice(buffer);
        Ok(Self::from_le_bytes(buf))
    }
}

/// Allow raw bytes to be used as a key.
impl DbKey<0> for Vec<u8> {}
/// Allow raw bytes to be used as a value.
impl DbBytes<Vec<u8>> for Vec<u8> {
    fn serialize(&self, buffer: &mut Vec<u8>) -> Result<(), SerializeError> {
        buffer.resize(self.len(), 0);
        buffer.copy_from_slice(self);
        Ok(())
    }

    fn deserialize(buffer: &[u8]) -> Result<Vec<u8>, DeserializeError> {
        let mut v = vec![0_u8; buffer.len()];
        v.copy_from_slice(buffer);
        Ok(v)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    //use crate::err_info;
    //use crate::error::source::SourceError;
    //use std::io::ErrorKind;
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
            let mut db: TestDb = DbConfig::new("db_tests", "xxx1", 2)
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
        let mut db: TestDb = DbConfig::new("db_tests", "xxx1", 2).build().unwrap();
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
        let mut db: TestDb = DbConfig::new("db_tests", "xxx1", 2).build().unwrap();
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

        let db: TestDb = DbConfig::new("db_tests", "xxx1", 2).build().unwrap();
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
    }

    #[test]
    fn test_vec_val() {
        let mut db: DbCore<u64, Vec<u8>, 8> = DbConfig::new("db_tests", "xxx_vec", 1)
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
            let item = db.fetch(&(i as u64));
            assert!(item.is_ok(), "Failed on item {}, {:?}", i, item);
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
            let item = db.fetch(&(i as u64));
            assert!(item.is_ok(), "Failed on item {}", i);
            assert_eq!(&item.unwrap(), &val);
        }
        println!("XXXX fetch time {}", start.elapsed().as_secs_f64());
    }

    #[test]
    fn test_reindex() {
        let max = 10_000;
        let val = vec![0_u8; 512];
        {
            let mut db: DbCore<u64, Vec<u8>, 8> = DbConfig::new("db_tests", "xxx_reindex", 1)
                .create()
                .truncate()
                .no_auto_flush()
                //.set_bucket_elements(25)
                //.set_load_factor(0.6)
                .build()
                .unwrap();
            let start = time::Instant::now();
            for i in 0_u64..max {
                db.insert(i, &val).unwrap();
            }
            println!("XXXX insert time {}", start.elapsed().as_secs_f64());
            assert_eq!(db.len(), max as usize);

            let start = time::Instant::now();
            for i in 0..max {
                let item = db.fetch(&(i as u64));
                assert!(item.is_ok(), "Failed on item {}, {:?}", i, item);
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
                let item = db.fetch(&(i as u64));
                assert!(item.is_ok(), "Failed on item {}", i);
                assert_eq!(&item.unwrap(), &val);
            }
            println!("XXXX fetch time {}", start.elapsed().as_secs_f64());
        }
        let config = DbConfig::new("db_tests", "xxx_reindex", 1);
        {
            let mut db = DbCore::<u64, Vec<u8>, 8>::reindex(config.clone()).unwrap();
            assert_eq!(db.len(), max as usize);
            let start = time::Instant::now();
            for i in 0..max {
                let item = db.fetch(&(i as u64));
                assert!(item.is_ok(), "Failed on item {}/{:?}", i, item);
                assert_eq!(&item.unwrap(), &val);
            }
            println!(
                "XXXX fetch (reindex) time {}",
                start.elapsed().as_secs_f64()
            );
        }
        {
            let mut data_file = OpenOptions::new()
                .write(true)
                .open(&config.files.data_file)
                .unwrap();
            let data_len = data_file.seek(SeekFrom::End(0)).unwrap();
            println!("XXXX data len {}", data_len);
            data_file.set_len(data_len - 16).unwrap(); // Truncate the file making the last record corrupt.
        }
        let db = DbCore::<u64, Vec<u8>, 8>::reindex(config).unwrap();
        assert_eq!(db.len(), max as usize - 1);
        let vals: Vec<Vec<u8>> = db.raw_iter().unwrap().map(|r| r.unwrap().1).collect();
        // Make sure the last record was removed (corrupted).
        assert_eq!(vals.len(), max as usize - 1);
        for (_i, v) in vals.iter().enumerate() {
            assert_eq!(v, &val);
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
        let mut db: DbCore<u64, String, 8> = DbConfig::new("db_tests", "xxx50k", 10)
            .create()
            .truncate()
            .no_auto_flush()
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
            db.insert(i, &format!("Value {}", i)).unwrap();
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
            let item = db.fetch(&(i as u64));
            assert!(item.is_ok(), "Failed on item {}, {:?}", i, item);
            assert_eq!(&item.unwrap(), &format!("Value {}", i));
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
            assert_eq!(v, &format!("Value {}", i));
        }
        println!("XXXX iter time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        //assert_eq!(&db.fetch(&35_000).unwrap(), "Value 35000");
        for i in 0..max {
            let item = db.fetch(&(i as u64));
            assert!(item.is_ok(), "Failed on item {}", i);
            assert_eq!(&item.unwrap(), &format!("Value {}", i));
        }
        println!("XXXX fetch time {}", start.elapsed().as_secs_f64());

        let start = time::Instant::now();
        //assert_eq!(&db.fetch(&35_000).unwrap(), "Value 35000");
        for i in 1..=max {
            //if i % 10_000 == 0 {println!("XXXXX fetching {}", max - i as u64); }
            let item = db.fetch(&(max - i as u64));
            assert!(item.is_ok(), "Failed on item {}", max - i);
            assert_eq!(&item.unwrap(), &format!("Value {}", max - i));
        }
        println!("XXXX fetch (REV) time {}", start.elapsed().as_secs_f64());

        let start = time::Instant::now();
        //assert_eq!(&db.fetch(&35_000).unwrap(), "Value 35000");
        let max_val = max - 1;
        for i in 0..(max / 2) {
            //if i % 10_000 == 0 {println!("XXXXX fetching {}", max - i as u64); }
            let item = db.fetch(&(i as u64));
            assert!(item.is_ok(), "Failed on item {}", i);
            assert_eq!(&item.unwrap(), &format!("Value {}", i));

            let item = db.fetch(&(max_val - i as u64));
            assert!(item.is_ok(), "Failed on item {}", max_val - i);
            assert_eq!(&item.unwrap(), &format!("Value {}", max_val - i));
        }
        println!("XXXX fetch (MIX) time {}", start.elapsed().as_secs_f64());
    }

    #[test]
    fn test_x50k_str() {
        let mut db: DbCore<String, String, 0> = DbConfig::new("db_tests", "xxx50k_str", 1)
            .create()
            .truncate()
            .build()
            .unwrap();
        for i in 0..50_000 {
            db.insert(format!("key {i}"), &format!("Value {}", i))
                .unwrap();
        }
        assert_eq!(db.len(), 50_000);
        db.flush().unwrap();
        let vals: Vec<String> = db.raw_iter().unwrap().map(|r| r.unwrap().1).collect();
        assert_eq!(vals.len(), 50_000);
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(v, &format!("Value {}", i));
        }
        assert_eq!(&db.fetch(&"key 35000".to_string()).unwrap(), "Value 35000");
        for i in 0..50_000 {
            assert_eq!(
                &db.fetch(&format!("key {i}"))
                    .unwrap_or_else(|e| panic!("Failed to read item {}, {}", i, e)),
                &format!("Value {}", i)
            );
        }
    }

    #[test]
    fn test_duplicates() {
        {
            let mut db: DbCore<u64, u64, 8> = DbConfig::new("db_tests", "xxxDupTest", 1)
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
        let mut db: DbCore<u64, u64, 8> = DbConfig::new("db_tests", "xxxDupTest", 1)
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
    }
}
