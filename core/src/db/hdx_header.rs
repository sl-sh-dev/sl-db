//! Contains the Hash Index (HDX) structure and code.

use crate::db::byte_trans::ByteTrans;
use crate::db::data_header::DataHeader;
use crate::db_config::DbConfig;
use crate::error::LoadHeaderError;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};

#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub(crate) struct HdxHeader {
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

impl HdxHeader {
    /// Return a default HdxHeader with any values from data_header overridden.
    /// This includes the version, uid, appnum, bucket_size and bucket_elements.
    pub fn from_data_header(data_header: &DataHeader, config: &DbConfig) -> Self {
        Self {
            type_id: *b"sldb.hdx",
            version: data_header.version(),
            uid: data_header.uid(),
            appnum: data_header.appnum(),
            bucket_elements: data_header.bucket_elements(),
            bucket_size: data_header.bucket_size(),
            buckets: config.initial_buckets,
            load_factor: (u16::MAX as f32 * config.load_factor) as u16,
            salt: 0,
            pepper: 0,
            values: 0,
            reserved: [0; 64],
        }
    }

    /// Load a HdxHeader from a file.  This will seek to the beginning and leave the file
    /// positioned after the header.
    pub fn load_header<R: Read + Seek>(source: &mut R) -> Result<Self, LoadHeaderError> {
        let mut header = Self {
            type_id: *b"sldb.hdx",
            version: 0,
            uid: 0,
            appnum: 0,
            buckets: 0,
            bucket_elements: 0,
            bucket_size: 0,
            salt: 0,
            pepper: 0,
            load_factor: u16::MAX / 2, // .5
            values: 0,
            reserved: [0; 64],
        };
        source.seek(SeekFrom::Start(0))?;
        // TODO- load this in pieces to avoid the unsafe as well handle byte ordering.
        unsafe {
            source.read_exact(HdxHeader::as_bytes_mut(&mut header))?;
        }

        if &header.type_id != b"sldb.hdx" {
            return Err(LoadHeaderError::InvalidType);
        }
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    pub fn write_header<R: Write + Seek>(&self, sync: &mut R) -> Result<(), io::Error> {
        sync.write_all(self.as_ref())?;
        Ok(())
    }

    /// Number of buckets in this index file.
    pub fn buckets(&self) -> u32 {
        self.buckets
    }

    /// Increment the buckets count by 1.
    pub fn inc_buckets(&mut self) {
        self.buckets += 1;
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

    /// Increment the values by 1.
    pub fn inc_values(&mut self) {
        self.values += 1;
    }
}
