use crate::error::DBError;
use crate::error::DBResult;
use crate::fxhasher::FxHasher;
use std::cell::{Cell, RefCell};
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::Path;

trait ByteTrans<T: ByteTrans<T> + Copy> {
    const SIZE: usize = std::mem::size_of::<T>();

    unsafe fn as_bytes(t: &T) -> &[u8] {
        std::slice::from_raw_parts((t as *const T) as *const u8, T::SIZE)
    }

    unsafe fn as_bytes_mut(t: &mut T) -> &mut [u8] {
        std::slice::from_raw_parts_mut((t as *mut T) as *mut u8, T::SIZE)
    }
}

impl<T: Copy> ByteTrans<T> for T {}

/// Required trait for a key.  Note that setting KSIZE to 0 indicates a variable sized key.
pub trait DbKey<const KSIZE: u16>: Eq + Hash + Debug {
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

pub trait VariableKey {}

pub trait DbBytes<T> {
    fn serialize(&self, buffer: &mut Vec<u8>);
    fn deserialize(buffer: &[u8]) -> DBResult<T>;
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct DataHeader {
    type_id: [u8; 8],   // The characters "sldb.dat"
    version: u16,       //Holds the version number
    uid: u64,           // Unique ID generated on creation
    appnum: u64,        // Application defined constant
    reserved: [u8; 64], // Zeroes
}
//impl ByteTrans<DataHeader> for DataHeader {}
impl AsRef<[u8]> for DataHeader {
    fn as_ref(&self) -> &[u8] {
        unsafe { Self::as_bytes(self) }
    }
}

impl Default for DataHeader {
    fn default() -> Self {
        Self {
            type_id: *b"sldb.dat",
            version: 0,
            uid: 0,
            appnum: 0,
            reserved: [0; 64],
        }
    }
}

#[derive(Debug, Copy, Clone)]
#[repr(C)]
struct HdxHeader {
    type_id: [u8; 8], // The characters "sldb.hdx"
    version: u16,     //Holds the version number
    uid: u64,         // Unique ID generated on creation
    appnum: u64,      // Application defined constant
    buckets: u32,
    bucket_elements: u16,
    bucket_size: u16,
    salt: u64,
    pepper: u64,
    load_factor: u16,
    values: u64,
    reserved: [u8; 64], // Zeroes
}

impl AsRef<[u8]> for HdxHeader {
    fn as_ref(&self) -> &[u8] {
        unsafe { Self::as_bytes(self) }
    }
}

impl Default for HdxHeader {
    fn default() -> Self {
        let buckets = 128;
        let elements = 5;
        // Each bucket is:
        // u64 (pos of overflow record)
        // elements[] each one is:
        //     (u64 (hash), u64 (record pos)).
        let bucket_size = 8 + (16 * elements);
        Self {
            type_id: *b"sldb.hdx",
            version: 0,
            uid: 0,
            appnum: 0,
            buckets,
            bucket_elements: elements,
            bucket_size,
            salt: 0,
            pepper: 0,
            load_factor: u16::MAX / 2, // .5
            values: 0,
            reserved: [0; 64],
        }
    }
}

pub struct Db<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>> {
    _header: DataHeader,
    data_file: RefCell<File>,
    hdx_file: RefCell<File>,
    hasher: S,
    // Keep a buffer around to share and avoid allocations.
    buffer: Cell<Vec<u8>>,
    write_buffer: Vec<u8>,
    hdx_header: HdxHeader,
    modulus: u32,
    load_factor: f32,
    capacity: u64,
    allow_bucket_expansion: bool, // don't allow more buckets- for testing lots of overflows...
    _key: PhantomData<K>,
    _value: PhantomData<V>,
}

impl<K, V, const KSIZE: u16, S> Db<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Open a new or reopen an existing database.
    pub fn open<P: AsRef<Path>>(dir: P, base_name: P) -> DBResult<Self> {
        let data_name = dir.as_ref().join(base_name.as_ref()).with_extension("dat");
        let hdx_name = dir.as_ref().join(base_name.as_ref()).with_extension("hdx");
        let (data_file, header) = Self::open_data_file(&data_name)?;
        let (hdx_file, hdx_header) = Self::open_hdx_file(&hdx_name)?;
        // Don't want buckets and modulus to be the same, so +1
        let modulus = (hdx_header.buckets + 1).next_power_of_two();
        Ok(Self {
            _header: header,
            data_file: RefCell::new(data_file),
            hdx_file: RefCell::new(hdx_file),
            hasher: S::default(),
            hdx_header,
            modulus,
            load_factor: hdx_header.load_factor as f32 / u16::MAX as f32,
            capacity: hdx_header.buckets as u64 * hdx_header.bucket_elements as u64,
            buffer: Cell::new(Vec::new()),
            write_buffer: Vec::new(),
            allow_bucket_expansion: true,
            _key: PhantomData,
            _value: PhantomData,
        })
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub fn fetch(&self, key: K) -> DBResult<V> {
        let hash = self.hash(&key);
        let bucket = self.get_bucket(hash) as usize;
        let iter = self.bucket_iter(bucket)?;
        for (rec_hash, rec_pos) in iter {
            // rec_pos > 0 handles degenerate case of a 0 hash.
            if hash == rec_hash && rec_pos > 0 {
                let ((rkey, val), _) = self.read_record(rec_pos)?;
                if rkey == key {
                    return Ok(val);
                }
            }
        }
        Err(DBError::NotFound)
    }

    /// Insert a new key/value pair in Db.
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    pub fn insert(&mut self, key: K, value: V) -> DBResult<()> {
        let mut file = self.data_file.borrow_mut();
        let mut buffer = self.buffer.take();
        self.write_buffer.clear();
        file.seek(SeekFrom::End(0))?;
        let record_pos = file.seek(SeekFrom::Current(0))?;
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
        file.write_all(&self.write_buffer)?;
        // An early return on error could cause a new buffer to be created, should not be a big deal.
        self.buffer.set(buffer);
        let bucket = self.get_bucket(hash);
        drop(file);
        self.save_to_bucket((hash, record_pos), bucket)?;
        self.hdx_header.values += 1;
        let mut hdx_file = self.hdx_file.borrow_mut();
        // XXXSLS
        hdx_file.seek(SeekFrom::Start(0))?;
        hdx_file.write_all(self.hdx_header.as_ref())?;
        drop(hdx_file);
        if self.allow_bucket_expansion
            && self.len() >= (self.capacity as f32 * self.load_factor) as usize
        {
            self.expand_buckets()?;
        }
        Ok(())
    }

    /// Return the number of records in Db.
    pub fn len(&self) -> usize {
        self.hdx_header.values as usize
    }

    /// Is the DB empty?
    pub fn is_empty(&self) -> bool {
        self.len() == 0
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
            file.write_all(header.as_ref())?;
            header
        } else if file_end >= DataHeader::SIZE as u64 {
            let mut header = DataHeader::default();
            file.seek(SeekFrom::Start(0))?;
            unsafe {
                file.read_exact(DataHeader::as_bytes_mut(&mut header))?;
            }
            file.seek(SeekFrom::End(0))?;

            if &header.type_id != b"sldb.dat" {
                return Err(DBError::InvalidDataHeader);
            }
            header
        } else {
            return Err(DBError::InvalidDataFile);
        };
        Ok((file, header))
    }

    fn open_hdx_file(data_name: &Path) -> DBResult<(File, HdxHeader)> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(data_name)?;
        file.seek(SeekFrom::End(0))?;
        let file_end = file.seek(SeekFrom::Current(0))?;

        let header = if file_end == 0 {
            let header = HdxHeader::default();
            file.write_all(header.as_ref())?;
            let mut buffer: Vec<u8> = Vec::new();
            buffer.resize(header.buckets as usize * header.bucket_size as usize, 0);
            file.write_all(&buffer)?;
            header
        } else if file_end >= HdxHeader::SIZE as u64 {
            let mut header = HdxHeader::default();
            file.seek(SeekFrom::Start(0))?;
            unsafe {
                file.read_exact(HdxHeader::as_bytes_mut(&mut header))?;
            }
            file.seek(SeekFrom::End(0))?;

            if &header.type_id != b"sldb.hdx" {
                return Err(DBError::InvalidIndexHeader);
            }
            header
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
        if bucket >= self.hdx_header.buckets as u64 {
            bucket - modulus / 2
        } else {
            bucket
        }
    }

    /// Add one new bucket hash index.
    fn split_one_bucket(&mut self) -> DBResult<()> {
        let old_modulus = self.modulus;
        let split_bucket = (self.hdx_header.buckets - (old_modulus / 2)) as usize;
        self.hdx_header.buckets += 1;
        // Don't want buckets and modulus to be the same, so +1
        self.modulus = (self.hdx_header.buckets + 1).next_power_of_two();
        // Grab the iter before we clear the bucket in the block below.
        let iter = self.bucket_iter(split_bucket)?;

        {
            let mut buffer = self.buffer.take();
            buffer.clear();
            buffer.resize(self.hdx_header.bucket_size as usize, 0);
            // Overwrite the existing bucket with a new blank bucket and add a new empty bucket to the end.
            let mut hdx_file = self.hdx_file.borrow_mut();
            let bucket_pos: u64 = (HdxHeader::SIZE
                + (split_bucket as usize * self.hdx_header.bucket_size as usize))
                as u64;
            hdx_file.seek(SeekFrom::Start(bucket_pos))?;
            hdx_file.write_all(&buffer)?;
            hdx_file.seek(SeekFrom::End(0))?;
            hdx_file.write_all(&buffer)?;
            self.buffer.set(buffer);
        }

        for (rec_hash, rec_pos) in iter {
            // rec_pos > 0 indicates a live entry.
            if rec_pos > 0 {
                let bucket = self.get_bucket(rec_hash);
                if bucket != split_bucket as u64 && bucket != self.hdx_header.buckets as u64 - 1 {
                    panic!(
                        "got bucket {}, expected {} or {}, mod {}",
                        bucket,
                        split_bucket,
                        self.hdx_header.buckets - 1,
                        self.modulus
                    );
                }
                self.save_to_bucket((rec_hash, rec_pos), bucket)?;
            }
        }

        Ok(())
    }

    /// Add buckets to expand capacity.
    fn expand_buckets(&mut self) -> DBResult<()> {
        //return Ok(());
        // arbitrarily add 10 buckets when we need more capacity.
        for _ in 0..10 {
            self.split_one_bucket()?;
        }
        self.capacity = self.hdx_header.buckets as u64 * self.hdx_header.bucket_elements as u64;
        Ok(())
    }

    /// Save the (hash, position) tuple to the bucket.  Handles overflow records.
    fn save_to_bucket(&mut self, hash_pos: (u64, u64), bucket: u64) -> DBResult<()> {
        let (hash, record_pos) = hash_pos;
        if self.get_bucket(hash) != bucket {
            panic!(
                "tried to insert into wrong bucket! {} into {}",
                self.get_bucket(hash),
                bucket
            );
        }
        let mut hdx_file = self.hdx_file.borrow_mut();
        let bucket_pos: u64 =
            (HdxHeader::SIZE + (bucket as usize * self.hdx_header.bucket_size as usize)) as u64;
        hdx_file.seek(SeekFrom::Start(bucket_pos))?;
        let mut buffer = self.buffer.take();
        buffer.resize(self.hdx_header.bucket_size as usize, 0);
        hdx_file.read_exact(&mut buffer)?;
        let mut pos = 8; // Skip the overflow file pos.
        for i in 0..self.hdx_header.bucket_elements as u64 {
            let mut buf = [0_u8; 8];
            buf.copy_from_slice(&buffer[pos..(pos + 8)]);
            let rec_hash = u64::from_ne_bytes(buf);
            pos += 8;
            buf.copy_from_slice(&buffer[pos..(pos + 8)]);
            let rec_pos = u64::from_ne_bytes(buf);
            pos += 8;
            // Test rec_pos == 0 to handle degenerate case of a hash of 0.
            if rec_hash == 0 && rec_pos == 0 {
                // Seek to the element we found that was empty and write the hash and position into it.
                hdx_file.seek(SeekFrom::Start(bucket_pos + 8 + (i * 16)))?;
                hdx_file.write_all(&hash.to_ne_bytes())?;
                hdx_file.write_all(&record_pos.to_ne_bytes())?;
                //hdx_file.flush()?;
                self.buffer.set(buffer);
                return Ok(());
            }
        }
        // Overflow, save bucket as an overflow record and add to the fresh bucket.
        let mut dat_file = self.data_file.borrow_mut();
        // Need this allow, clippy can not tell the difference in 0_u16 and 0_u32 apparently.
        #[allow(clippy::if_same_then_else)]
        if K::is_variable_key_size() {
            // Write a 0 key size to indicate this is an overflow bucket not a data record.
            dat_file.write_all(&0_u16.to_ne_bytes())?;
        } else {
            // Write a 0 value size to indicate this is an overflow bucket not a data record.
            dat_file.write_all(&0_u32.to_ne_bytes())?;
        }
        let overflow_pos = dat_file.seek(SeekFrom::Current(0))?;
        // Write the old buffer into the data file as an overflow record.
        dat_file.write_all(&buffer)?;
        // clear buffer and reset to 0.
        buffer.clear();
        buffer.resize(self.hdx_header.bucket_size as usize, 0);
        // Copy the position of the overflow record into the first u64.
        buffer[0..8].copy_from_slice(&overflow_pos.to_ne_bytes());
        // First element will be the hash and position being saved (rest of new bucket is empty).
        buffer[8..16].copy_from_slice(&hash.to_ne_bytes());
        buffer[16..24].copy_from_slice(&record_pos.to_ne_bytes());
        // Write the new bucket out.
        hdx_file.seek(SeekFrom::Start(bucket_pos))?;
        hdx_file.write_all(&buffer)?;
        self.buffer.set(buffer);
        Ok(())
    }

    /// Read the record at position.
    /// Returns the key, value tuple as well as the position of the next record.
    /// Will produce an error for IO or if the record at position is actually an overflow bucket not
    /// a data record.
    fn read_record_raw<R: Read + Seek>(
        file: &mut R,
        buffer: &mut Vec<u8>,
        position: u64,
    ) -> DBResult<((K, V), u64)> {
        // Move the rea cursor to position.  Note the data file is opened in append mode so write
        // cursor is always EOF.
        file.seek(SeekFrom::Start(position))?;
        let key_size = if K::is_variable_key_size() {
            let mut key_size = [0_u8; 2];
            file.read_exact(&mut key_size)?;
            let key_size = u16::from_ne_bytes(key_size);
            if key_size == 0 {
                // Overflow bucket, can not read as data so error.
                return Err(DBError::UnexpectedOverflowBucket);
            }
            key_size
        } else {
            K::KEY_SIZE
        };

        let mut val_size_buf = [0_u8; 4];
        file.read_exact(&mut val_size_buf)?;
        let val_size = u32::from_ne_bytes(val_size_buf);
        if K::is_fixed_key_size() && val_size == 0 {
            // No key size so overflow indicated by a 0 value size.
            return Err(DBError::UnexpectedOverflowBucket);
        }
        buffer.resize(key_size as usize, 0);
        file.read_exact(buffer)?;
        let key = K::deserialize(buffer)?;
        buffer.resize(val_size as usize, 0);
        file.read_exact(buffer)?;
        let val = V::deserialize(buffer)?;
        let pos = file.seek(SeekFrom::Current(0))?;
        // An early return on error could cause a new buffer to be created, should not be a big deal.
        Ok(((key, val), pos))
    }

    /// Read the record at position.
    /// Returns the key, value tuple as well as the position of the next record.
    /// Will produce an error for IO or if the record at position is actually an overflow bucket not
    /// a data record.
    fn read_record(&self, position: u64) -> DBResult<((K, V), u64)> {
        let mut buffer = self.buffer.take();
        //let dat_file = { self.data_file.borrow().try_clone()? };
        //let mut file = BufReader::with_capacity(4096, dat_file);
        //let result = Self::read_record_raw(&mut file, &mut buffer, position);
        let result =
            Self::read_record_raw(&mut *self.data_file.borrow_mut(), &mut buffer, position);
        self.buffer.set(buffer);
        result
    }

    fn read_record_file<R: Read + Seek>(
        &self,
        file: &mut R,
        buffer: &mut Vec<u8>,
        position: u64,
    ) -> DBResult<((K, V), u64)> {
        Self::read_record_raw(file, buffer, position)
    }

    fn bucket_iter(&self, bucket: usize) -> DBResult<BucketIter> {
        let dat_file = { self.data_file.borrow().try_clone()? };
        let mut hdx_file = self.hdx_file.borrow_mut();
        BucketIter::new(
            dat_file,
            &mut hdx_file,
            self.hdx_header.bucket_size,
            self.hdx_header.bucket_elements,
            bucket,
        )
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    pub fn raw_iter(&self) -> DBResult<DbRawIter<K, V, KSIZE, S>> {
        DbRawIter::new(self)
    }
}

/// Iterate over a Db's key, value pairs in insert order.
/// This iterator is "raw", it does not use any indexes just the data file.
pub struct DbRawIter<'db, K, V, const KSIZE: u16, S> {
    db: &'db Db<K, V, KSIZE, S>,
    file: BufReader<File>,
    buffer: Vec<u8>,
    position: u64,
}

impl<'db, K, V, const KSIZE: u16, S> DbRawIter<'db, K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    fn new(db: &'db Db<K, V, KSIZE, S>) -> DBResult<Self> {
        let dat_file = { db.data_file.borrow().try_clone()? };
        let file = BufReader::new(dat_file);
        Ok(Self {
            db,
            file,
            buffer: Vec::new(),
            position: DataHeader::SIZE as u64,
        })
    }
}

impl<'db, K, V, const KSIZE: u16, S> Iterator for DbRawIter<'db, K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let mut rec = self
            .db
            .read_record_file(&mut self.file, &mut self.buffer, self.position);
        let mut el = 1_u64;
        while let Err(err) = &rec {
            // TODO- proper errors....
            if let DBError::UnexpectedOverflowBucket = err {
                rec = self.db.read_record_file(
                    &mut self.file,
                    &mut self.buffer,
                    // A overflow bucket has a slightly different size for fixed and variable keys.
                    // Fixed keys don't need a key size (u16) but have to set value size (u32).
                    if K::is_variable_key_size() {
                        self.position + (el * (2 + self.db.hdx_header.bucket_size as u64))
                    } else {
                        self.position + (el * (4 + self.db.hdx_header.bucket_size as u64))
                    },
                );
                el += 1;
            } else {
                break;
            }
        }
        if let Ok((ret, position)) = rec {
            self.position = position;
            Some(ret)
        } else {
            None
        }
    }
}

/// Iterates over the (hash, record_position) values contained in a bucket.
struct BucketIter {
    dat_file: File,
    buffer: Vec<u8>,
    bucket_pos: usize,
    overflow_pos: u64,
    elements: u16,
}

impl BucketIter {
    fn new(
        dat_file: File,
        hdx_file: &mut File,
        bucket_size: u16,
        elements: u16,
        bucket: usize,
    ) -> DBResult<Self> {
        let mut buffer = Vec::with_capacity(bucket_size as usize);

        let bucket_pos: u64 = (HdxHeader::SIZE + (bucket * bucket_size as usize)) as u64;
        hdx_file.seek(SeekFrom::Start(bucket_pos))?;
        buffer.resize(bucket_size as usize, 0);
        hdx_file.read_exact(&mut buffer)?;
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

impl Iterator for BucketIter {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        // For reading u64 values, needs an array.
        let mut buf = [0_u8; 8];
        loop {
            if self.bucket_pos < self.elements as usize {
                let mut pos = 8 + (self.bucket_pos * 16);
                buf.copy_from_slice(&self.buffer[pos..(pos + 8)]);
                let hash = u64::from_ne_bytes(buf);
                pos += 8;
                buf.copy_from_slice(&self.buffer[pos..(pos + 8)]);
                let rec_pos = u64::from_ne_bytes(buf);
                if hash == 0 && rec_pos == 0 {
                    self.bucket_pos += 1;
                } else {
                    self.bucket_pos += 1;
                    return Some((hash, rec_pos));
                }
            } else if self.overflow_pos > 0 {
                // We have an overflow bucket to search as well.
                self.dat_file
                    .seek(SeekFrom::Start(self.overflow_pos))
                    .ok()?;
                self.dat_file.read_exact(&mut self.buffer).ok()?;
                self.bucket_pos = 0;
                buf.copy_from_slice(&self.buffer[0..8]);
                self.overflow_pos = u64::from_ne_bytes(buf);
            } else {
                return None;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_one() {
        {
            let mut db = TestDb::open(".", "xxx1").unwrap();
            let key = Key([1_u8; 32]);
            db.insert(key, "Value One".to_string()).unwrap();
            let key = Key([2_u8; 32]);
            db.insert(key, "Value Two".to_string()).unwrap();
            let key = Key([3_u8; 32]);
            db.insert(key, "Value Three".to_string()).unwrap();
            let key = Key([4_u8; 32]);
            db.insert(key, "Value Four".to_string()).unwrap();
            let key = Key([5_u8; 32]);
            db.insert(key, "Value Five".to_string()).unwrap();

            let v = db.fetch(key).unwrap();
            assert_eq!(v, "Value Five");
            let key = Key([1_u8; 32]);
            let v = db.fetch(key).unwrap();
            assert_eq!(v, "Value One");
            let key = Key([3_u8; 32]);
            let v = db.fetch(key).unwrap();
            assert_eq!(v, "Value Three");
            let key = Key([2_u8; 32]);
            let v = db.fetch(key).unwrap();
            assert_eq!(v, "Value Two");
            let key = Key([4_u8; 32]);
            let v = db.fetch(key).unwrap();
            assert_eq!(v, "Value Four");

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
        let mut db = TestDb::open(".", "xxx1").unwrap();
        let key = Key([6_u8; 32]);
        db.insert(key, "Value One2".to_string()).unwrap();
        let key = Key([7_u8; 32]);
        db.insert(key, "Value Two2".to_string()).unwrap();
        let key = Key([8_u8; 32]);
        db.insert(key, "Value Three2".to_string()).unwrap();

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
        let mut db = Db::<u64, String, 8>::open(".", "xxx50k").unwrap();
        for i in 0..50_000 {
            db.insert(i as u64, format!("Value {}", i)).unwrap();
        }
        assert_eq!(db.len(), 50_000);
        let vals: Vec<String> = db.raw_iter().unwrap().map(|(_k, v)| v).collect();
        assert_eq!(vals.len(), 50_000);
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(v, &format!("Value {}", i));
        }
        assert_eq!(&db.fetch(35_000).unwrap(), "Value 35000");
        for i in 0..50_000 {
            assert_eq!(&db.fetch(i as u64).unwrap(), &format!("Value {}", i));
        }
    }

    #[test]
    fn test_x50k_str() {
        let mut db = Db::<String, String, 0>::open(".", "xxx50k_str").unwrap();
        for i in 0..50_000 {
            db.insert(format!("key {i}"), format!("Value {}", i))
                .unwrap();
        }
        assert_eq!(db.len(), 50_000);
        let vals: Vec<String> = db.raw_iter().unwrap().map(|(_k, v)| v).collect();
        assert_eq!(vals.len(), 50_000);
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(v, &format!("Value {}", i));
        }
        assert_eq!(&db.fetch("key 35000".to_string()).unwrap(), "Value 35000");
        for i in 0..50_000 {
            assert_eq!(
                &db.fetch(format!("key {i}")).unwrap(),
                &format!("Value {}", i)
            );
        }
    }
}
