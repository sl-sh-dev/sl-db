//! Define and manage a data file header.

use crate::db_config::DbConfig;
use crate::error::LoadHeaderError;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};

// Each bucket element is a (u64, u64, u32)- (hash, record_pos, record_size).
pub(crate) const BUCKET_ELEMENT_SIZE: usize = 20;
const HEADER_BYTES: usize = 94;

/// Struct that contains the header for a sldb data file.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub(crate) struct DataHeader {
    type_id: [u8; 8],     // The characters "sldb.dat"
    version: u16,         // Holds the version number
    uid: u64,             // Unique ID generated on creation
    appnum: u64,          // Application defined constant
    bucket_size: u16,     // Size of a bucket, record this here in case index is lost.
    bucket_elements: u16, // Elements in each bucket, record this here in case index is lost.
    reserved: [u8; 64],   // Zeroes
}

impl DataHeader {
    pub fn new(config: &DbConfig) -> Self {
        let bucket_size: u16 = config.bucket_size; //8 + (BUCKET_ELEMENT_SIZE as u16 * config.bucket_elements);
        Self {
            type_id: *b"sldb.dat",
            version: 0,
            uid: 0,
            appnum: config.appnum,
            bucket_elements: config.bucket_elements,
            bucket_size,
            reserved: [0; 64],
        }
    }

    /// Load a DataHeader from source.
    pub fn load_header<R: Read + Seek>(source: &mut R) -> Result<Self, LoadHeaderError> {
        source.seek(SeekFrom::Start(0))?;
        let mut buffer = [0_u8; HEADER_BYTES];
        let mut buf16 = [0_u8; 2];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        source.read_exact(&mut buffer[..])?;
        let mut type_id = [0_u8; 8];
        type_id.copy_from_slice(&buffer[0..8]);
        pos += 8;
        if &type_id != b"sldb.dat" {
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
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let bucket_size = u16::from_le_bytes(buf16);
        pos += 2;
        buf16.copy_from_slice(&buffer[pos..(pos + 2)]);
        let bucket_elements = u16::from_le_bytes(buf16);
        pos += 2;
        let mut reserved = [0_u8; 64];
        reserved.copy_from_slice(&buffer[pos..(pos + 64)]);
        let header = Self {
            type_id,
            version,
            uid,
            appnum,
            bucket_elements,
            bucket_size,
            reserved,
        };
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    pub fn write_header<R: Write + Seek>(&self, sync: &mut R) -> Result<(), io::Error> {
        let mut buffer = [0_u8; HEADER_BYTES];
        let mut pos = 0;
        buffer[pos..8].copy_from_slice(&self.type_id);
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.version.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.uid.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.appnum.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.bucket_size.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 2)].copy_from_slice(&self.bucket_elements.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 64)].copy_from_slice(&self.reserved);
        pos += 64;
        assert_eq!(pos, HEADER_BYTES);
        sync.write_all(&buffer)?;
        Ok(())
    }

    /// Version of the DB file.
    pub fn version(&self) -> u16 {
        self.version
    }

    /// Generated uid for this DB.
    pub fn uid(&self) -> u64 {
        self.uid
    }

    /// User defined appnum.
    pub fn appnum(&self) -> u64 {
        self.appnum
    }

    /// Return the bucket size for an index on this DB.
    /// The data file needs this to skip over overflow buckets in some cases and keeping it's own
    /// copy allows this to work without an index file.
    pub fn bucket_size(&self) -> u16 {
        self.bucket_size
    }

    /// Number of elements in each bucket.
    pub fn bucket_elements(&self) -> u16 {
        self.bucket_elements
    }
}
