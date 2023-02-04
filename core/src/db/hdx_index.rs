//! Contains the Hash Index (HDX) structure and code.

use crate::crc::{add_crc32, check_crc};
use crate::db::bucket_iter::BucketIter;
use crate::db::data_header::{DataHeader, BUCKET_ELEMENT_SIZE, DATA_HEADER_BYTES};
use crate::db::odx_header::OdxHeader;
use crate::db_bytes::DbBytes;
use crate::db_config::DbConfig;
use crate::db_key::DbKey;
use crate::error::insert::InsertError;
use crate::error::{CommitError, LoadHeaderError, ReadKeyError};
use crate::fxhasher::{FxHashMap, FxHasher};
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;

/// Minimum size to hold a header.  THe header will be padded to bucket_size if it is larger.
/// This is to allow the hdx file to accessed in sector sized chunks.
const MIN_HEADER_SIZE: usize = 72;

/// Header for an hdx (index) file.  This contains the hash buckets for lookups.
/// This file is not a log file and the header and buckets will change in place over time.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug)]
pub(crate) struct HdxHeader {
    type_id: [u8; 8], // The characters "sldb.hdx"
    version: u16,     // Holds the version number
    uid: u64,         // Unique ID generated on creation
    appnum: u64,      // Application defined constant
    buckets: u32,
    bucket_elements: u16,
    bucket_size: u16,
    salt: u64,
    pepper: u64,
    load_factor: u16,
    values: u64,
    data_file_length: u64,
}

impl HdxHeader {
    /// Return a default HdxHeader with any values from data_header overridden.
    /// This includes the version, uid, appnum, bucket_size and bucket_elements.
    fn from_data_header(
        data_header: &DataHeader,
        config: &DbConfig,
        salt: u64,
        pepper: u64,
    ) -> Self {
        Self {
            type_id: *b"sldb.hdx",
            version: data_header.version(),
            uid: data_header.uid(),
            appnum: data_header.appnum(),
            bucket_elements: config.bucket_elements,
            bucket_size: config.bucket_size,
            buckets: config.initial_buckets,
            load_factor: (u16::MAX as f32 * config.load_factor) as u16,
            salt,
            pepper,
            values: 0,
            data_file_length: DATA_HEADER_BYTES as u64,
        }
    }

    /// Load a HdxHeader from a file.  This will seek to the beginning and leave the file
    /// positioned after the header.
    fn load_header(header_size: u16, hdx_file: &mut File) -> Result<Self, LoadHeaderError> {
        let header_size = if (header_size as usize) < MIN_HEADER_SIZE {
            MIN_HEADER_SIZE
        } else {
            header_size as usize
        };
        hdx_file.seek(SeekFrom::Start(0))?;
        let mut buffer = vec![0_u8; header_size];
        let mut buf16 = [0_u8; 2];
        let mut buf32 = [0_u8; 4];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        hdx_file.read_exact(&mut buffer[..])?;
        if !check_crc(&buffer[..]) {
            return Err(LoadHeaderError::CrcFailed);
        }
        let mut type_id = [0_u8; 8];
        type_id.copy_from_slice(&buffer[0..8]);
        pos += 8;
        if &type_id != b"sldb.hdx" {
            return Err(LoadHeaderError::InvalidType);
        }
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let version = u16::from_le_bytes(buf16);
        pos += 2;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let uid = u64::from_le_bytes(buf64);
        pos += 8;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let appnum = u64::from_le_bytes(buf64);
        pos += 8;
        buf32.copy_from_slice(&buffer[pos..(pos + 4)]);
        let buckets = u32::from_le_bytes(buf32);
        pos += 4;
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let bucket_elements = u16::from_le_bytes(buf16);
        pos += 2;
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let bucket_size = u16::from_le_bytes(buf16);
        pos += 2;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let salt = u64::from_le_bytes(buf64);
        pos += 8;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let pepper = u64::from_le_bytes(buf64);
        pos += 8;
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let load_factor = u16::from_le_bytes(buf16);
        pos += 2;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let values = u64::from_le_bytes(buf64);
        pos += 8;
        buf64.copy_from_slice(&buffer[pos..(pos + 8)]);
        let data_file_length = u64::from_le_bytes(buf64);
        let header = Self {
            type_id,
            version,
            uid,
            appnum,
            buckets,
            bucket_elements,
            bucket_size,
            salt,
            pepper,
            load_factor,
            values,
            data_file_length,
        };
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    fn write_header(&mut self, hdx_file: &mut File) -> Result<(), io::Error> {
        hdx_file.seek(SeekFrom::Start(0))?;
        let header_size = self.header_size();
        let mut buffer = vec![0_u8; header_size];
        let mut pos = 0;
        buffer[pos..8].copy_from_slice(&self.type_id);
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.version.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.uid.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.appnum.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 4)].copy_from_slice(&self.buckets.to_le_bytes());
        pos += 4;
        buffer[pos..(pos + 2)].copy_from_slice(&self.bucket_elements.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 2)].copy_from_slice(&self.bucket_size.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.salt.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.pepper.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.load_factor.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.values.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.data_file_length.to_le_bytes());
        add_crc32(&mut buffer[..]);
        hdx_file.write_all(&buffer[..])?;
        Ok(())
    }

    /// Return the size of the HDX header.
    pub fn header_size(&self) -> usize {
        if (self.bucket_size as usize) < MIN_HEADER_SIZE {
            MIN_HEADER_SIZE
        } else {
            self.bucket_size as usize
        }
    }

    /// Number of buckets in this index file.
    pub fn buckets(&self) -> u32 {
        self.buckets
    }

    /// Number of elements in each bucket.
    pub fn bucket_elements(&self) -> u16 {
        self.bucket_elements
    }

    /// Size in bytes of a bucket.
    pub fn bucket_size(&self) -> u16 {
        self.bucket_size
    }

    /// Load factor converted to a f32.
    pub fn load_factor(&self) -> f32 {
        self.load_factor as f32 / u16::MAX as f32
    }

    /// Number of elements stored in this DB.
    pub fn values(&self) -> u64 {
        self.values
    }

    /// File version number.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Unique ID generated on creation
    pub fn uid(&self) -> u64 {
        self.uid
    }

    /// Application defined constant
    pub fn appnum(&self) -> u64 {
        self.appnum
    }

    /// Return the index salt.
    pub fn salt(&self) -> u64 {
        self.salt
    }

    /// Return the index pepper.
    pub fn pepper(&self) -> u64 {
        self.pepper
    }

    /// How long this index thinks the data file is.
    pub fn _data_file_length(&self) -> u64 {
        self.data_file_length
    }
}
/// Header for an hdx (index) file.  This contains the hash buckets for lookups.
/// This file is not a log file and the header and buckets will change in place over time.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug)]
pub(crate) struct HdxIndex<K, const KSIZE: u16>
where
    K: DbKey<KSIZE> + DbBytes<K>,
{
    header: HdxHeader,
    config: DbConfig,
    modulus: u32,
    bucket_cache: FxHashMap<u64, Vec<u8>>,
    dirty_bucket_cache: FxHashMap<u64, Vec<u8>>,
    hdx_file: File,
    // Note, if odx_file is ever replaced in HdxIndex then see bucket_iter for undefined behaviour.
    odx_file: File,
    capacity: u64,
    cached_buckets: usize,
    _key: PhantomData<K>,
}

// Used for bucket_iter().
/// Combine the Read and Seek traits.
pub trait ReadSeek: Read + Seek {}
impl ReadSeek for File {}

impl<K, const KSIZE: u16> HdxIndex<K, KSIZE>
where
    K: DbKey<KSIZE> + DbBytes<K>,
{
    /// Open an HDX index file and return the open file and the header.
    pub fn open_hdx_file<S: BuildHasher + Default>(
        data_header: &DataHeader,
        config: DbConfig,
        hasher: &S,
    ) -> Result<HdxIndex<K, KSIZE>, LoadHeaderError> {
        let mut hdx_file = OpenOptions::new()
            .read(true)
            .write(config.write)
            .create(config.create && config.write)
            .truncate(config.truncate && config.write)
            .open(&config.files.hdx_path())?;
        let file_end = hdx_file.seek(SeekFrom::End(0))?;

        let header = if file_end == 0 {
            let mut fx_hasher = FxHasher::default();
            fx_hasher.write_u64(data_header.uid());
            let salt = fx_hasher.finish();
            let mut hasher = hasher.build_hasher();
            salt.hash(&mut hasher);
            let pepper = hasher.finish();
            let mut header = HdxHeader::from_data_header(data_header, &config, salt, pepper);
            header.write_header(&mut hdx_file)?;
            let bucket_size = header.bucket_size() as usize;
            let mut buffer = vec![0_u8; bucket_size];
            add_crc32(&mut buffer[..]);
            for _ in 0..header.buckets() {
                hdx_file.write_all(&buffer[..])?;
            }
            header
        } else {
            let header = HdxHeader::load_header(config.bucket_size, &mut hdx_file)?;
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
        let (odx_file, _odx_header) =
            OdxHeader::open_odx_file(header.version(), header.uid(), header.appnum(), &config)?;
        // Don't want buckets and modulus to be the same, so +1
        let modulus = (header.buckets + 1).next_power_of_two();
        let capacity = header.buckets() as u64 * header.bucket_elements() as u64;
        let cached_buckets = (config.bucket_cache_size / config.bucket_size as u32) as usize;
        let bucket_cache = FxHashMap::with_capacity_and_hasher(
            cached_buckets,
            BuildHasherDefault::<FxHasher>::default(),
        );
        Ok(Self {
            header,
            config,
            modulus,
            bucket_cache,
            dirty_bucket_cache: FxHashMap::default(),
            hdx_file,
            odx_file,
            capacity,
            cached_buckets,
            _key: PhantomData,
        })
    }

    /// Return an iterator over a buckets elements.
    /// This is for use within DB functions ONLY.  This provides an iterator with it;s own lifetime
    /// to allow it to be used in DbInner without borrowing issues.
    /// Safety: The returned iterator has a reference into self that has a separate lifetime.  This
    /// means that is can not outlive the HdxIndex that created it.  If odx_file is ever recreated that
    /// can also lead to undefined behaviour if an iterator is in use (odx_file is never recreated at
    /// time of writing).  In short use these iterators locally and let them go- never save or return
    /// them to a public API.
    pub(crate) unsafe fn bucket_iter<'a, 'b>(
        &'a mut self,
        bucket: u64,
    ) -> BucketIter<'b, dyn ReadSeek> {
        // Turn odx_file into a reference to a Read + Seek trait.
        // Need this to break away the odx file lifetime to use in the iter.
        // The ODX file should not change under the iterator and is therefore safe.
        let odx_reader: &mut dyn ReadSeek = unsafe {
            (&mut self.odx_file as *mut dyn ReadSeek)
                .as_mut()
                .expect("this can't be null")
        };
        // Note, maybe investigate a try_clone on odx_file to get rid of some of the unsafety.
        let buffer = self.get_bucket(bucket);
        BucketIter::new(odx_reader, buffer, self.header().bucket_elements())
    }

    /// Set the data_file_length field.
    pub fn set_data_file_length(&mut self, data_file_length: u64) {
        self.header.data_file_length = data_file_length;
    }

    /// Write the indexes header tyo disk.
    pub fn write_header(&mut self) -> Result<(), io::Error> {
        self.header.write_header(&mut self.hdx_file)
    }

    /// Increment the buckets count by 1.
    pub fn inc_buckets(&mut self) {
        self.header.buckets += 1;
    }

    /// Number of buckets in the index.
    pub fn buckets(&self) -> u32 {
        self.header.buckets()
    }

    /// Increment the values by 1.
    pub fn inc_values(&mut self) {
        self.header.values += 1;
    }

    /// Number of elements stored in this DB.
    pub fn values(&self) -> u64 {
        self.header.values()
    }

    /// Get a value from a cache, try the dirty buckets then the read cached buckets.
    fn get_bucket_cache(&self, bucket: u64) -> Option<&Vec<u8>> {
        if let Some(buffer) = self.dirty_bucket_cache.get(&bucket) {
            Some(buffer)
        } else {
            self.bucket_cache.get(&bucket)
        }
    }

    /// Remove a value from a cache, try the dirty buckets then the read cached buckets.
    fn remove_bucket_cache(&mut self, bucket: u64) -> Option<Vec<u8>> {
        if let Some(buffer) = self.dirty_bucket_cache.remove(&bucket) {
            Some(buffer)
        } else {
            self.bucket_cache.remove(&bucket)
        }
    }

    /// Read bucket from source.  Always allocate the buffer, if in cache copy it.
    /// If there is an IO error then return an empty buffer.
    pub fn get_bucket(&mut self, bucket: u64) -> Vec<u8> {
        let mut buffer = vec![0; self.header.bucket_size as usize];
        if let Some(bucket) = self.get_bucket_cache(bucket) {
            buffer.copy_from_slice(bucket);
            // These cached buffers may be missing their crc32 so add it now.
            add_crc32(&mut buffer[..]);
        } else {
            let bucket_size = self.header.bucket_size as usize;
            let bucket_pos: u64 =
                (self.header.header_size() + (bucket as usize * bucket_size)) as u64;
            if self.hdx_file.seek(SeekFrom::Start(bucket_pos)).is_ok() {
                let _ = self.hdx_file.read_exact(&mut buffer[..]);
                self.bucket_cache.insert(bucket, buffer.clone());
            }
        }
        buffer
    }

    /// Return the buffer for bucket, if found in cache then remove and return that buffer vs allocate.
    pub fn remove_bucket(&mut self, bucket: u64) -> Result<Vec<u8>, InsertError> {
        if let Some(buf) = self.remove_bucket_cache(bucket) {
            // Get the bucket from the bucket cache.
            Ok(buf)
        } else {
            // Read the bucket from the index and verify (crc32) it.
            let bucket_size = self.header.bucket_size as usize;
            let mut buffer = vec![0_u8; bucket_size];
            let bucket_pos: u64 =
                (self.header.header_size() + (bucket as usize * bucket_size)) as u64;
            {
                self.hdx_file
                    .seek(SeekFrom::Start(bucket_pos))
                    .map_err(|e| InsertError::KeyError(e.into()))?;
                self.hdx_file
                    .read_exact(&mut buffer[..])
                    .map_err(|e| InsertError::KeyError(e.into()))?;
                if !check_crc(&buffer[..]) {
                    return Err(InsertError::KeyError(ReadKeyError::CrcFailed));
                }
            }
            Ok(buffer)
        }
    }

    /// Return the index header.
    pub fn header(&self) -> &HdxHeader {
        &self.header
    }

    /// Flush (save) the hash bucket cache to disk.
    pub(crate) fn save_bucket_cache(&mut self) -> Result<(), io::Error> {
        let bucket_size = self.header.bucket_size as usize;
        let header_size = self.header.header_size();
        if self.bucket_cache.len() > self.cached_buckets {
            // Simple cache clear when it gets to large.
            self.bucket_cache.clear();
        }
        for (bucket, mut buffer) in self.dirty_bucket_cache.drain() {
            let bucket_pos: u64 = (header_size + (bucket as usize * bucket_size)) as u64;
            add_crc32(&mut buffer[..]);
            // Seeking and writing past the file end extends it.
            self.hdx_file.seek(SeekFrom::Start(bucket_pos))?;
            self.hdx_file.write_all(&buffer[..])?;
            self.bucket_cache.insert(bucket, buffer);
        }
        Ok(())
    }

    /// Return the bucket that will contain hash (if hash is available).
    pub fn hash_to_bucket(&self, hash: u64) -> u64 {
        let modulus = self.modulus as u64;
        let bucket = hash % modulus;
        if bucket >= self.buckets() as u64 {
            bucket - modulus / 2
        } else {
            bucket
        }
    }

    /// Reload the header from disk.
    pub fn reload_header(&mut self) {
        if let Ok(header) = HdxHeader::load_header(self.header.bucket_size, &mut self.hdx_file) {
            self.header = header;
            self.modulus = (self.header.buckets() + 1).next_power_of_two();
        }
    }

    pub fn sync(&mut self) -> Result<(), CommitError> {
        self.hdx_file
            .sync_all()
            .map_err(CommitError::IndexFileSync)?;
        self.odx_file
            .sync_all()
            .map_err(CommitError::IndexFileSync)?;
        Ok(())
    }

    /// Add buckets to expand capacity.
    /// Capacity is number of elements per bucket * number of buckets.
    /// If current length >= capacity * load factor then split buckets until this is not true.
    pub fn expand_buckets(
        &mut self,
        read_key: &mut dyn FnMut(u64) -> Result<K, ReadKeyError>,
    ) -> Result<(), InsertError> {
        if self.config.allow_bucket_expansion {
            while self.header.values >= (self.capacity as f32 * self.header.load_factor()) as u64 {
                self.split_one_bucket(read_key)?;
                self.capacity = self.buckets() as u64 * self.header.bucket_elements() as u64;
            }
        }
        Ok(())
    }

    /// Add one new bucket to the hash index.
    /// Buckets are split "in order" determined by the current modulus not based on how full any
    /// bucket is.
    fn split_one_bucket(
        &mut self,
        read_key: &mut dyn FnMut(u64) -> Result<K, ReadKeyError>,
    ) -> Result<(), InsertError> {
        let old_modulus = self.modulus;
        // This is the bucket that is being split.
        let split_bucket = (self.buckets() - (old_modulus / 2)) as u64;
        self.inc_buckets();
        // This is the newly created bucket that the items in split_bucket will possibly be moved into.
        let new_bucket = self.buckets() as u64 - 1;
        // Don't want buckets and modulus to be the same, so +1
        self.modulus = (self.buckets() + 1).next_power_of_two();

        let bucket_size = self.header.bucket_size() as usize;
        let mut buffer = vec![0; bucket_size];
        let mut buffer2 = vec![0; bucket_size];

        let mut iter = unsafe { self.bucket_iter(split_bucket) };
        for (rec_hash, rec_pos) in &mut iter {
            if rec_pos > 0 {
                let bucket = self.hash_to_bucket(rec_hash);
                if bucket != split_bucket && bucket != new_bucket {
                    panic!(
                        "got bucket {}, expected {} or {}, mod {}",
                        bucket,
                        split_bucket,
                        self.buckets() - 1,
                        self.modulus
                    );
                }
                if bucket == split_bucket {
                    self.save_to_bucket_buffer(None, rec_hash, rec_pos, &mut buffer, read_key)
                        .map_err(|_| InsertError::IndexOverflow)?;
                } else {
                    self.save_to_bucket_buffer(None, rec_hash, rec_pos, &mut buffer2, read_key)
                        .map_err(|_| InsertError::IndexOverflow)?;
                }
            }
        }
        if iter.crc_failure() {
            return Err(InsertError::IndexCrcError);
        }
        self.dirty_bucket_cache.insert(split_bucket, buffer);
        self.dirty_bucket_cache.insert(new_bucket, buffer2);
        Ok(())
    }

    /// Save the (hash, position) tuple to the bucket.  Handles overflow records.
    pub fn save_to_bucket(
        &mut self,
        key: &K,
        hash: u64,
        record_pos: u64,
        read_key: &mut dyn FnMut(u64) -> Result<K, ReadKeyError>,
    ) -> Result<(), InsertError> {
        let bucket = self.hash_to_bucket(hash);
        let mut buffer = self.remove_bucket(bucket)?;

        let result =
            self.save_to_bucket_buffer(Some(key), hash, record_pos, &mut buffer[..], read_key);
        // Need to make sure the bucket goes into the cache even on error.
        self.dirty_bucket_cache.insert(bucket, buffer);
        result
    }

    /// Save the (hash, position) tuple to the bucket.  Handles overflow records.
    /// If this produces and Error then buffer will contain the same data.
    fn save_to_bucket_buffer(
        &mut self,
        key: Option<&K>,
        hash: u64,
        record_pos: u64,
        buffer: &mut [u8],
        read_key: &mut dyn FnMut(u64) -> Result<K, ReadKeyError>,
    ) -> Result<(), InsertError> {
        let mut pos = 8; // Skip the overflow file pos.
        for i in 0..self.header.bucket_elements() as u64 {
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
                    if let Ok(rkey) = read_key(rec_pos) {
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
}
