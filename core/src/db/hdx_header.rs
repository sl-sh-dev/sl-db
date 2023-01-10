//! Contains the Hash Index (HDX) structure and code.

use crate::db::data_header::DataHeader;
use crate::db_config::DbConfig;
use crate::error::LoadHeaderError;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};

/// Size of an index header.
pub const HDX_HEADER_SIZE: usize = 64;

#[derive(Debug, Copy, Clone)]
#[repr(C)]
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
        }
    }

    /// Load a HdxHeader from a file.  This will seek to the beginning and leave the file
    /// positioned after the header.
    pub fn load_header<R: Read + Seek>(source: &mut R) -> Result<Self, LoadHeaderError> {
        source.seek(SeekFrom::Start(0))?;
        let mut buffer = [0_u8; HDX_HEADER_SIZE];
        let mut buf16 = [0_u8; 2];
        let mut buf32 = [0_u8; 4];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        source.read_exact(&mut buffer[..])?;
        let mut crc32_hasher = crc32fast::Hasher::new();
        crc32_hasher.update(&buffer[..(HDX_HEADER_SIZE - 4)]);
        let calc_crc32 = crc32_hasher.finalize();
        buf32.copy_from_slice(&buffer[(HDX_HEADER_SIZE - 4)..]);
        let read_crc32 = u32::from_le_bytes(buf32);
        if calc_crc32 != read_crc32 {
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
        };
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    pub fn write_header<R: Write + Seek>(&self, sync: &mut R) -> Result<(), io::Error> {
        let mut buffer = [0_u8; HDX_HEADER_SIZE];
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
        let mut crc32_hasher = crc32fast::Hasher::new();
        crc32_hasher.update(&buffer[..pos]);
        let crc32 = crc32_hasher.finalize();
        buffer[pos..(pos + 4)].copy_from_slice(&crc32.to_le_bytes());
        pos += 4;
        assert_eq!(pos, HDX_HEADER_SIZE);
        sync.write_all(&buffer)?;
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
