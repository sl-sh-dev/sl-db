//! Main module for the SLDB core.  This implements the core sync single threaded access to the DB.

use crate::db::byte_trans::ByteTrans;
use crate::db::data_header::{DataHeader, BUCKET_ELEMENT_SIZE};
use crate::db::hdx_header::HdxHeader;
use crate::db_raw_iter::DbRawIter;
use crate::error::DBError;
use crate::error::DBResult;
use crate::fxhasher::{FxHashMap, FxHasher};
use std::cell::{Cell, RefCell};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::Path;

pub mod byte_trans;
pub mod data_header;
pub mod hdx_header;

/// Use a 16Mb buffer for inserts.
/// TODO- make this a setting instead.
const INSERT_BUFFER_SIZE: usize = 16 * 1024 * 1024;

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
    fn serialize(&self, buffer: &mut Vec<u8>);

    /// Deserialize a byte slice back into the type or error out.
    fn deserialize(buffer: &[u8]) -> DBResult<T>;
}

/// An instance of a DB.
/// Will consist of a data file and hash index with optional log for commit recovery.
pub struct Db<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    inner: RefCell<DbInner<K, V, KSIZE, S>>,
}

impl<K, V, const KSIZE: u16, S> Db<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Open a new or reopen an existing database.
    pub fn open<P: AsRef<Path>>(dir: P, base_name: P) -> DBResult<Self> {
        let inner = DbInner::open(dir, base_name)?;
        Ok(Self {
            inner: RefCell::new(inner),
        })
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub fn fetch(&self, key: &K) -> DBResult<V> {
        self.inner.borrow_mut().fetch(key)
    }

    /// True if the database contains key.
    pub fn contains_key(&self, key: &K) -> DBResult<bool> {
        self.inner.borrow_mut().contains_key(key)
    }

    /// Insert a new key/value pair in Db.
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    pub fn insert(&self, key: K, value: &V) -> DBResult<()> {
        self.inner.borrow_mut().insert(key, value)
    }

    /// Return the number of records in Db.
    pub fn len(&self) -> usize {
        self.inner.borrow_mut().len()
    }

    /// Is the DB empty?
    pub fn is_empty(&self) -> bool {
        self.inner.borrow_mut().is_empty()
    }

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    pub fn commit(&self) -> DBResult<()> {
        self.inner.borrow_mut().commit()
    }

    /// Flush any in memory caches to file.
    /// Note this is only a flush not a commit, it does not do a sync on the files.
    pub fn flush(&self) -> DBResult<()> {
        self.inner.borrow_mut().flush()
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    pub fn raw_iter(&self) -> DBResult<DbRawIter<K, V, KSIZE, S>> {
        self.inner.borrow().raw_iter()
    }
}

/// An instance of a DB.
/// Will consist of a data file and hash index with optional log for commit recovery.
struct DbInner<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    _header: DataHeader,
    data_file: File,
    data_file_end: u64,
    hdx_file: File,
    hasher: S,
    // Keep a buffer around to share and avoid allocations.
    // It is a Cell for interior mutability so we can use avoid borrow errors.
    scratch_buffer: Cell<Vec<u8>>,
    write_buffer: Vec<u8>,
    bucket_elements_cache: Vec<(u64, u64, u32)>,
    index_cache: FxHashMap<K, (u64, u64, u32)>,
    hdx_header: HdxHeader,
    modulus: u32,
    load_factor: f32,
    capacity: u64,
    seek_pos: u64,
    allow_bucket_expansion: bool, // don't allow more buckets- for testing lots of overflows...
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
        let _ = self.commit();
    }
}

impl<K, V, const KSIZE: u16, S> DbInner<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Open a new or reopen an existing database.
    pub fn open<P: AsRef<Path>>(dir: P, base_name: P) -> DBResult<Self> {
        let data_name = dir.as_ref().join(base_name.as_ref()).with_extension("dat");
        let hdx_name = dir.as_ref().join(base_name.as_ref()).with_extension("hdx");
        let (mut data_file, header) = Self::open_data_file(&data_name)?;
        data_file.seek(SeekFrom::End(0))?;
        let data_file_end = data_file.seek(SeekFrom::Current(0))?;
        let (hdx_file, hdx_header) = Self::open_hdx_file(&hdx_name, &header)?;
        // Don't want buckets and modulus to be the same, so +1
        let modulus = (hdx_header.buckets() + 1).next_power_of_two();
        // Make a 16mb write buffer.
        let write_buffer = Vec::with_capacity(1024); //INSERT_BUFFER_SIZE + 1024);
        let mut index_cache = FxHashMap::default();
        index_cache.reserve(50000);
        Ok(Self {
            _header: header,
            data_file,
            data_file_end,
            hdx_file,
            hasher: S::default(),
            hdx_header,
            modulus,
            load_factor: hdx_header.load_factor(),
            capacity: hdx_header.buckets() as u64 * hdx_header.bucket_elements() as u64,
            scratch_buffer: Cell::new(Vec::new()),
            write_buffer,
            bucket_elements_cache: Vec::with_capacity(header.bucket_elements() as usize),
            index_cache, //: FxHashMap::with_capacity(1000),//default(),
            seek_pos: 0,
            allow_bucket_expansion: true,
            _key: PhantomData,
            _value: PhantomData,
        })
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub fn fetch(&mut self, key: &K) -> DBResult<V> {
        if let Some((_hash, rec_pos, rec_size)) = self.index_cache.get(key) {
            // Try the index cache.
            let (_, val) = self.read_record(*rec_pos, *rec_size as usize)?;
            return Ok(val);
        } else {
            // Then check the on disk index.
            let hash = self.hash(key);
            let bucket = self.get_bucket(hash) as usize;
            self.load_bucket_elements_cache(bucket)?;
            for i in 0..self.bucket_elements_cache.len() {
                let (rec_hash, rec_pos, rec_size) = self.bucket_elements_cache[i];
                // rec_pos > 0 handles degenerate case of a 0 hash.
                if hash == rec_hash && rec_pos > 0 {
                    let (rkey, val) = self.read_record(rec_pos, rec_size as usize)?;
                    if &rkey == key {
                        return Ok(val);
                    }
                }
            }
        }
        Err(DBError::NotFound)
    }

    /// True if the database contains key.
    pub fn contains_key(&mut self, key: &K) -> DBResult<bool> {
        if self.index_cache.contains_key(key) {
            // Try the index cache.
            return Ok(true);
        } else {
            // Then check the on disk index.
            let hash = self.hash(key);
            let bucket = self.get_bucket(hash) as usize;
            self.load_bucket_elements_cache(bucket)?;
            for i in 0..self.bucket_elements_cache.len() {
                let (rec_hash, rec_pos, _rec_size) = self.bucket_elements_cache[i];
                //for (rec_hash, rec_pos, _rec_size) in &self.bucket_elements_cache {
                // rec_pos > 0 handles degenerate case of a 0 hash.
                if hash == rec_hash && rec_pos > 0 {
                    let rkey = self.read_key(rec_pos)?;
                    if &rkey == key {
                        return Ok(true);
                    }
                }
            }
        }
        Ok(false)
    }

    /// Insert a new key/value pair in Db.
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    pub fn insert(&mut self, key: K, value: &V) -> DBResult<()> {
        // XXXSLS- checking keys is expensive.
        if self.contains_key(&key)? {
            return Err(DBError::DuplicateInsert);
        }
        {
            let mut buffer = self.scratch_buffer.take();
            //self.write_buffer.clear();
            let start_write_buffer_len = self.write_buffer.len();
            let record_pos = self.data_file_end + self.write_buffer.len() as u64;
            let hash = self.hash(&key);
            key.serialize(&mut buffer);
            // If we have a variable sized key write it's size otherwise no need.
            if K::is_variable_key_size() {
                // We have to write the key size when variable.
                self.write_buffer
                    .write_all(&(buffer.len() as u16).to_ne_bytes())?;
            } else if K::KEY_SIZE as usize != buffer.len() {
                return Err(DBError::InvalidKeyLength);
            }
            // Space for the value length.
            let val_size_pos = self.write_buffer.len();
            self.write_buffer.write_all(&[0_u8; 4])?;

            // Write the key to the buffer.
            self.write_buffer.write_all(&buffer)?;

            value.serialize(&mut buffer);

            // Save current pos, then jump back to the value size and write that then finally write
            // the value into the saved position.
            self.write_buffer[val_size_pos..val_size_pos + 4]
                .copy_from_slice(&(buffer.len() as u32).to_ne_bytes());
            self.write_buffer.write_all(&buffer)?;

            // Write the buffer to the actual file (avoid excess IO syscalls).
            //file.write_all(&self.write_buffer)?;
            //*data_file_end += self.write_buffer.len() as u64;
            // An early return on error could cause a new buffer to be created, should not be a big deal.
            self.scratch_buffer.set(buffer);
            self.index_cache.insert(
                key,
                (
                    hash,
                    record_pos,
                    (self.write_buffer.len() - start_write_buffer_len) as u32,
                ),
            );
            self.hdx_header.inc_values();
        }
        if self.index_cache.len() > 200 || self.write_buffer.len() >= INSERT_BUFFER_SIZE {
            //if self.write_buffer.len() >= INSERT_BUFFER_SIZE {
            self.flush()?;
        }
        Ok(())
    }

    /// Return the number of records in Db.
    pub fn len(&self) -> usize {
        self.hdx_header.values() as usize
    }

    /// Is the DB empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    pub fn commit(&mut self) -> DBResult<()> {
        self.flush()?;
        self.data_file.sync_all()?;
        self.hdx_file.sync_all()?;
        Ok(())
    }

    /// Flush any in memory caches to file.
    /// Note this is only a flush not a commit, it does not do a sync on the files.
    pub fn flush(&mut self) -> DBResult<()> {
        {
            self.data_file.write_all(&self.write_buffer)?;
            self.data_file_end += self.write_buffer.len() as u64;
            self.write_buffer.clear();
        }
        self.expand_buckets()?;
        // Do this so we can call save_to_bucket while iterating the index_cache.
        // This should be safe since save_to_bucket does not touch the index_cache.
        // Maybe put index_cache in a Cell to avoid this unsafe?
        let unsafe_db: &mut DbInner<K, V, KSIZE, S> =
            unsafe { (self as *mut DbInner<K, V, KSIZE, S>).as_mut().unwrap() };
        for (key, (hash, pos, size)) in &self.index_cache {
            unsafe_db.save_to_bucket(key, *hash, (*pos, *size))?;
        }
        self.index_cache.clear();
        self.hdx_file.seek(SeekFrom::Start(0))?;
        self.hdx_file.write_all(self.hdx_header.as_ref())?;
        Ok(())
    }

    fn open_data_file(data_name: &Path) -> DBResult<(File, DataHeader)> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .append(true)
            .create(true)
            .open(data_name)?;
        file.seek(SeekFrom::End(0))?;
        let file_end = file.seek(SeekFrom::Current(0))?;

        let header = if file_end == 0 {
            let header = DataHeader::default();
            header.write_header(&mut file)?;
            header
        } else if file_end >= DataHeader::SIZE as u64 {
            DataHeader::load_header(&mut file)?
        } else {
            return Err(DBError::InvalidDataFile);
        };
        Ok((file, header))
    }

    fn open_hdx_file(data_name: &Path, data_header: &DataHeader) -> DBResult<(File, HdxHeader)> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_name)?;
        file.seek(SeekFrom::End(0))?;
        let file_end = file.seek(SeekFrom::Current(0))?;

        let header = if file_end == 0 {
            let header = HdxHeader::from_data_header(data_header);
            header.write_header(&mut file)?;
            let mut buffer: Vec<u8> = Vec::new();
            buffer.resize(header.buckets() as usize * header.bucket_size() as usize, 0);
            file.write_all(&buffer)?;
            header
        } else if file_end >= HdxHeader::SIZE as u64 {
            HdxHeader::load_header(&mut file)?
        } else {
            return Err(DBError::InvalidIndexFile);
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

    /// Add one new bucket hash index.
    fn split_one_bucket(&mut self) -> DBResult<()> {
        let old_modulus = self.modulus;
        let split_bucket = (self.hdx_header.buckets() - (old_modulus / 2)) as usize;
        self.hdx_header.inc_buckets();
        // Don't want buckets and modulus to be the same, so +1
        self.modulus = (self.hdx_header.buckets() + 1).next_power_of_two();
        // Grab the iter before we clear the bucket in the block below.
        self.load_bucket_elements_cache(split_bucket)?;

        let mut buffer = self.scratch_buffer.take();
        buffer.clear();
        buffer.resize(self.hdx_header.bucket_size() as usize, 0);
        let mut buffer2 = vec![0; self.hdx_header.bucket_size() as usize];

        for i in 0..self.bucket_elements_cache.len() {
            let (rec_hash, rec_pos, rec_size) = self.bucket_elements_cache[i];
            if rec_pos > 0 {
                let bucket = self.get_bucket(rec_hash);
                if bucket != split_bucket as u64 && bucket != self.hdx_header.buckets() as u64 - 1 {
                    panic!(
                        "got bucket {}, expected {} or {}, mod {}",
                        bucket,
                        split_bucket,
                        self.hdx_header.buckets() - 1,
                        self.modulus
                    );
                }
                if bucket as usize == split_bucket {
                    self.save_to_bucket_buffer(None, rec_hash, (rec_pos, rec_size), &mut buffer)?;
                } else {
                    self.save_to_bucket_buffer(None, rec_hash, (rec_pos, rec_size), &mut buffer2)?;
                }
            }
        }

        // Overwrite the existing bucket with a new blank bucket and add a new empty bucket to the end.
        let bucket_pos: u64 = (HdxHeader::SIZE
            + (split_bucket as usize * self.hdx_header.bucket_size() as usize))
            as u64;
        self.hdx_file.seek(SeekFrom::Start(bucket_pos))?;
        self.hdx_file.write_all(&buffer)?;
        self.hdx_file.seek(SeekFrom::End(0))?;
        self.hdx_file.write_all(&buffer2)?;
        self.scratch_buffer.set(buffer);

        Ok(())
    }

    /// Add buckets to expand capacity.
    fn expand_buckets(&mut self) -> DBResult<()> {
        if self.allow_bucket_expansion {
            while self.len() >= (self.capacity as f32 * self.load_factor) as usize {
                // arbitrarily add 10 buckets when we need more capacity.
                //for _ in 0..10 {
                self.split_one_bucket()?;
                //}
                self.capacity =
                    self.hdx_header.buckets() as u64 * self.hdx_header.bucket_elements() as u64;
            }
        }
        Ok(())
    }

    /// Save the (hash, position) tuple to the bucket.  Handles overflow records.
    fn save_to_bucket(&mut self, key: &K, hash: u64, pos_size: (u64, u32)) -> DBResult<()> {
        let bucket = self.get_bucket(hash);
        let mut buffer = self.scratch_buffer.take();
        let bucket_pos: u64 =
            (HdxHeader::SIZE + (bucket as usize * self.hdx_header.bucket_size() as usize)) as u64;
        {
            self.hdx_file.seek(SeekFrom::Start(bucket_pos))?;
            buffer.resize(self.hdx_header.bucket_size() as usize, 0);
            self.hdx_file.read_exact(&mut buffer)?;
        }

        let res = self.save_to_bucket_buffer(Some(key), hash, pos_size, &mut buffer);
        if res.is_ok() {
            self.hdx_file.seek(SeekFrom::Start(bucket_pos))?;
            self.hdx_file.write_all(&buffer)?;
        }
        self.scratch_buffer.set(buffer);
        res
    }

    /// Save the (hash, position, record_size) tuple to the bucket.  Handles overflow records.
    fn save_to_bucket_buffer(
        &mut self,
        key: Option<&K>,
        hash: u64,
        pos_size: (u64, u32),
        buffer: &mut [u8],
    ) -> DBResult<()> {
        let (record_pos, record_size) = pos_size;
        let mut pos = 8; // Skip the overflow file pos.
        for i in 0..self.hdx_header.bucket_elements() as u64 {
            let mut buf64 = [0_u8; 8];
            buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
            let rec_hash = u64::from_ne_bytes(buf64);
            pos += 8;
            buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
            let rec_pos = u64::from_ne_bytes(buf64);
            // Skip over size
            pos += 12;
            // Test rec_pos == 0 to handle degenerate case of a hash of 0.
            // Find an empty element should indicate no more elements (so the key check below is ok).
            if rec_hash == 0 && rec_pos == 0 {
                // Seek to the element we found that was empty and write the hash and position into it.
                let mut pos = 8 + (i as usize * BUCKET_ELEMENT_SIZE);
                buffer[pos..pos + 8].copy_from_slice(&hash.to_ne_bytes());
                pos += 8;
                buffer[pos..pos + 8].copy_from_slice(&record_pos.to_ne_bytes());
                pos += 8;
                buffer[pos..pos + 4].copy_from_slice(&record_size.to_ne_bytes());
                return Ok(());
            }
            if rec_hash == hash {
                if let Some(key) = key {
                    let rkey = self.read_key(rec_pos)?;
                    if &rkey == key {
                        // Overwrite the old element with the new in the index. This will leave
                        // garbage in the data file but lookups will work and be consistent.
                        let mut pos = 8 + (i as usize * BUCKET_ELEMENT_SIZE);
                        buffer[pos..pos + 8].copy_from_slice(&hash.to_ne_bytes());
                        pos += 8;
                        buffer[pos..pos + 8].copy_from_slice(&record_pos.to_ne_bytes());
                        pos += 8;
                        buffer[pos..pos + 4].copy_from_slice(&record_size.to_ne_bytes());
                        return Ok(());
                    }
                }
            }
        }
        // XXXSLS- should check if buffer has room.
        // Overflow, save bucket as an overflow record and add to the fresh bucket.
        // Need this allow, clippy can not tell the difference in 0_u16 and 0_u32 apparently.
        #[allow(clippy::if_same_then_else)]
        if K::is_variable_key_size() {
            // Write a 0 key size to indicate this is an overflow bucket not a data record.
            self.write_buffer.write_all(&0_u16.to_ne_bytes())?;
        } else {
            // Write a 0 value size to indicate this is an overflow bucket not a data record.
            self.write_buffer.write_all(&0_u32.to_ne_bytes())?;
        }
        let overflow_pos = self.data_file_end + self.write_buffer.len() as u64;
        // Write the old buffer into the data file as an overflow record.
        self.write_buffer.write_all(buffer)?;
        // clear buffer and reset to 0.
        buffer.fill(0);
        // Copy the position of the overflow record into the first u64.
        buffer[0..8].copy_from_slice(&overflow_pos.to_ne_bytes());
        // First element will be the hash and position being saved (rest of new bucket is empty).
        buffer[8..16].copy_from_slice(&hash.to_ne_bytes());
        buffer[16..24].copy_from_slice(&record_pos.to_ne_bytes());
        buffer[24..28].copy_from_slice(&record_size.to_ne_bytes());
        Ok(())
    }

    /// Read the record at position.
    /// Returns the (key, value) tuple
    /// Will produce an error for IO or if the record at position is actually an overflow bucket not
    /// a data record.
    fn read_record_raw(
        &mut self,
        position: u64,
        size: usize,
        buffer: &mut Vec<u8>,
    ) -> DBResult<(K, V)> {
        buffer.resize(size, 0);
        // Move the rea cursor to position.  Note the data file is opened in append mode so write
        // cursor is always EOF.
        self.seek(SeekFrom::Start(position))?;
        self.read_exact(&mut buffer[..])?;
        let mut pos = 0;
        let key_size = if K::is_variable_key_size() {
            let mut key_size = [0_u8; 2];
            key_size.copy_from_slice(&buffer[0..2]);
            pos += 2;
            let key_size = u16::from_ne_bytes(key_size);
            if key_size == 0 {
                // Overflow bucket, can not read as data so error.
                return Err(DBError::UnexpectedOverflowBucket);
            }
            key_size
        } else {
            K::KEY_SIZE
        } as usize;

        let mut val_size_buf = [0_u8; 4];
        val_size_buf.copy_from_slice(&buffer[pos..pos + 4]);
        pos += 4;
        let val_size = u32::from_ne_bytes(val_size_buf) as usize;
        if K::is_fixed_key_size() && val_size == 0 {
            // No key size so overflow indicated by a 0 value size.
            return Err(DBError::UnexpectedOverflowBucket);
        }
        let key = K::deserialize(&buffer[pos..pos + key_size])?;
        pos += key_size;
        let val = V::deserialize(&buffer[pos..pos + val_size])?;
        // An early return on error could cause a new buffer to be created, should not be a big deal.
        Ok((key, val))
    }

    /// Read the record at position.
    /// Returns the (key, value) tuple.
    /// Will produce an error for IO or if the record at position is actually an overflow bucket not
    /// a data record.
    fn read_record(&mut self, position: u64, size: usize) -> DBResult<(K, V)> {
        let mut buffer = self.scratch_buffer.take();
        let result = self.read_record_raw(position, size, &mut buffer);
        self.scratch_buffer.set(buffer);
        result
    }

    /// Read the key for the record at position.
    /// The position needs to be valid, attempting to read an overflow bucket or other invalid area will
    /// produce an error or invalid key.
    fn read_key(&mut self, position: u64) -> DBResult<K> {
        let mut buffer = self.scratch_buffer.take();

        self.seek(SeekFrom::Start(position))?;
        let key_size = if K::is_variable_key_size() {
            let mut key_size = [0_u8; 2];
            self.read_exact(&mut key_size)?;
            let key_size = u16::from_ne_bytes(key_size);
            key_size as usize
        } else {
            K::KEY_SIZE as usize
        };
        buffer.resize(key_size + 4, 0);
        self.read_exact(&mut buffer[..])?;
        // Skip the value size and read the key.
        let key = K::deserialize(&buffer[4..])?;

        self.scratch_buffer.set(buffer);
        Ok(key)
    }

    fn bucket_iter(&mut self, bucket: usize) -> DBResult<BucketIter<DbInner<K, V, KSIZE, S>>> {
        let mut buffer = Vec::with_capacity(self.hdx_header.bucket_size() as usize);
        {
            let bucket_size = self.hdx_header.bucket_size() as usize;
            let bucket_pos: u64 = (HdxHeader::SIZE + (bucket * bucket_size)) as u64;
            self.hdx_file.seek(SeekFrom::Start(bucket_pos))?;
            buffer.resize(bucket_size, 0);
            self.hdx_file.read_exact(&mut buffer)?;
        }
        BucketIter::new(self, buffer, self.hdx_header.bucket_elements())
    }

    /// Loads self.bucket_elements_cache with all the bucket elements for bucket.
    /// Using bucket_iter directly causes lots of borrowing issues since it needs a mutable lifetime.
    fn load_bucket_elements_cache(&mut self, bucket: usize) -> DBResult<()> {
        self.bucket_elements_cache.clear();
        // Break the db lifetime away so we can get the bucket iter and use it to extend the
        // bucket_element_cache.  These two pieces do not interact and this saves unneeded collects.
        let unsafe_db: &mut DbInner<K, V, KSIZE, S> =
            unsafe { (self as *mut DbInner<K, V, KSIZE, S>).as_mut().unwrap() };
        self.bucket_elements_cache
            .extend(unsafe_db.bucket_iter(bucket)?);
        Ok(())
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    pub fn raw_iter(&self) -> DBResult<DbRawIter<K, V, KSIZE, S>> {
        let dat_file = { self.data_file.try_clone()? };
        DbRawIter::new(dat_file)
    }
}

impl<K, V, const KSIZE: u16, S> Read for DbInner<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
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
        } else {
            // Move the read cursor to position.  Note the data file is opened in append mode so write
            // cursor is always EOF.
            self.data_file.seek(SeekFrom::Start(self.seek_pos))?;
            let size = self.data_file.read(buf)?;
            self.seek_pos += size as u64;
            Ok(size)
        }
    }
}

impl<K, V, const KSIZE: u16, S> Seek for DbInner<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
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

/// Iterates over the (hash, record_position) values contained in a bucket.
struct BucketIter<'src, R: Read + Seek> {
    dat_file: &'src mut R,
    buffer: Vec<u8>,
    bucket_pos: usize,
    overflow_pos: u64,
    elements: u16,
}

impl<'src, R: Read + Seek> BucketIter<'src, R> {
    fn new(dat_file: &'src mut R, buffer: Vec<u8>, elements: u16) -> DBResult<Self> {
        let mut buf = [0_u8; 8]; // buffer for converting to u64s (needs an array)
        buf.copy_from_slice(&buffer[0..8]);
        let overflow_pos = u64::from_ne_bytes(buf);
        Ok(Self {
            dat_file,
            buffer,
            bucket_pos: 0,
            overflow_pos,
            elements,
        })
    }
}

impl<'src, R: Read + Seek> Iterator for BucketIter<'src, R> {
    type Item = (u64, u64, u32);

    fn next(&mut self) -> Option<Self::Item> {
        // For reading u64 values, needs an array.
        let mut buf64 = [0_u8; 8];
        // For reading u32 values, needs an array.
        let mut buf32 = [0_u8; 4];
        loop {
            if self.bucket_pos < self.elements as usize {
                let mut pos = 8 + (self.bucket_pos * BUCKET_ELEMENT_SIZE);
                buf64.copy_from_slice(&self.buffer[pos..(pos + 8)]);
                let hash = u64::from_ne_bytes(buf64);
                pos += 8;
                buf64.copy_from_slice(&self.buffer[pos..(pos + 8)]);
                let rec_pos = u64::from_ne_bytes(buf64);
                pos += 8;
                buf32.copy_from_slice(&self.buffer[pos..(pos + 4)]);
                let rec_size = u32::from_ne_bytes(buf32);
                if hash == 0 && rec_pos == 0 {
                    self.bucket_pos += 1;
                } else {
                    self.bucket_pos += 1;
                    return Some((hash, rec_pos, rec_size));
                }
            } else if self.overflow_pos > 0 {
                // We have an overflow bucket to search as well.
                self.dat_file
                    .seek(SeekFrom::Start(self.overflow_pos))
                    .ok()?;
                self.dat_file.read_exact(&mut self.buffer[..]).ok()?;
                self.bucket_pos = 0;
                buf64.copy_from_slice(&self.buffer[0..8]);
                self.overflow_pos = u64::from_ne_bytes(buf64);
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time;

    #[derive(Hash, PartialEq, Eq, Copy, Clone, Debug)]
    struct Key([u8; 32]);

    impl DbKey<32> for Key {}
    impl DbBytes<Key> for Key {
        fn serialize(&self, buffer: &mut Vec<u8>) {
            buffer.resize(32, 0);
            buffer.copy_from_slice(&self.0);
        }

        fn deserialize(buffer: &[u8]) -> DBResult<Key> {
            let mut key = [0_u8; 32];
            key.copy_from_slice(buffer);
            Ok(Key(key))
        }
    }

    impl DbKey<0> for String {}
    impl DbBytes<String> for String {
        fn serialize(&self, buffer: &mut Vec<u8>) {
            let bytes = self.as_bytes();
            buffer.resize(bytes.len(), 0);
            buffer.copy_from_slice(bytes);
        }

        fn deserialize(buffer: &[u8]) -> DBResult<String> {
            Ok(String::from_utf8_lossy(buffer).to_string())
        }
    }

    impl DbKey<8> for u64 {}
    impl DbBytes<u64> for u64 {
        fn serialize(&self, buffer: &mut Vec<u8>) {
            buffer.resize(8, 0);
            buffer.copy_from_slice(&self.to_ne_bytes());
        }

        fn deserialize(buffer: &[u8]) -> DBResult<u64> {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(buffer);
            Ok(Self::from_ne_bytes(buf))
        }
    }

    type TestDb = Db<Key, String, 32>;
    type TestDbRawIter = DbRawIter<Key, String, 32>;

    #[test]
    fn test_one() {
        {
            let db = TestDb::open(".", "xxx1").unwrap();
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
            let mut iter = db.raw_iter().unwrap();
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
        let db = TestDb::open(".", "xxx1").unwrap();
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
        let db = TestDb::open(".", "xxx1").unwrap();
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

        let mut iter = TestDbRawIter::open(".", "xxx1").unwrap();
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

        let db = TestDb::open(".", "xxx1").unwrap();
        let mut iter = db.raw_iter().unwrap();
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
    fn test_50k() {
        let db = Db::<u64, String, 8>::open(".", "xxx50k").unwrap();
        assert!(!db.contains_key(&0).unwrap());
        assert!(!db.contains_key(&10).unwrap());
        assert!(!db.contains_key(&35_000).unwrap());
        assert!(!db.contains_key(&49_000).unwrap());
        assert!(!db.contains_key(&50_000).unwrap());
        let start = time::Instant::now();
        for i in 0_u64..50_000 {
            db.insert(i, &format!("Value {}", i)).unwrap();
        }
        println!("XXXX insert time {}", start.elapsed().as_secs_f64());
        assert_eq!(db.len(), 50_000);
        assert!(db.contains_key(&0).unwrap());
        assert!(db.contains_key(&10).unwrap());
        assert!(db.contains_key(&35_000).unwrap());
        assert!(db.contains_key(&49_000).unwrap());
        assert!(!db.contains_key(&50_000).unwrap());
        let start = time::Instant::now();
        db.flush().unwrap();
        println!("XXXX flush time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        let vals: Vec<String> = db.raw_iter().unwrap().map(|(_k, v)| v).collect();
        assert_eq!(vals.len(), 50_000);
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(v, &format!("Value {}", i));
        }
        println!("XXXX iter time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        assert_eq!(&db.fetch(&35_000).unwrap(), "Value 35000");
        for i in 0..50_000 {
            let item = db.fetch(&(i as u64));
            assert!(item.is_ok(), "Failed on item {}", i);
            assert_eq!(&item.unwrap(), &format!("Value {}", i));
        }
        println!("XXXX fetch time {}", start.elapsed().as_secs_f64());
    }

    #[test]
    fn test_x50k_str() {
        let db = Db::<String, String, 0>::open(".", "xxx50k_str").unwrap();
        for i in 0..50_000 {
            db.insert(format!("key {i}"), &format!("Value {}", i))
                .unwrap();
        }
        assert_eq!(db.len(), 50_000);
        db.flush().unwrap();
        let vals: Vec<String> = db.raw_iter().unwrap().map(|(_k, v)| v).collect();
        assert_eq!(vals.len(), 50_000);
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(v, &format!("Value {}", i));
        }
        assert_eq!(&db.fetch(&"key 35000".to_string()).unwrap(), "Value 35000");
        for i in 0..50_000 {
            assert_eq!(
                &db.fetch(&format!("key {i}")).unwrap(),
                &format!("Value {}", i)
            );
        }
    }
}
