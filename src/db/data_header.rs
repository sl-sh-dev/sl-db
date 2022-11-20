//! Define and manage a data file header.

use crate::db::byte_trans::ByteTrans;
use crate::db_config::DbConfig;
use crate::error::{DBError, DBResult};
use std::io::{Read, Seek, SeekFrom, Write};

// Each bucket element is a (u64, u64, u32)- (hash, record_pos, record_size).
pub(crate) const BUCKET_ELEMENT_SIZE: usize = 20;

/// Struct that contains the header for a sldb data file.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub(crate) struct DataHeader {
    type_id: [u8; 8],     // The characters "sldb.dat"
    version: u16,         //Holds the version number
    uid: u64,             // Unique ID generated on creation
    appnum: u64,          // Application defined constant
    bucket_size: u16,     // Size of a bucket, record this here in case index is lost.
    bucket_elements: u16, // Elements in each bucket, record this here in case index is lost.
    reserved: [u8; 64],   // Zeroes
}

impl AsRef<[u8]> for DataHeader {
    fn as_ref(&self) -> &[u8] {
        unsafe { Self::as_bytes(self) }
    }
}

impl Default for DataHeader {
    fn default() -> Self {
        let bucket_elements = 255; //5;
                                   // Each bucket is:
                                   // u64 (pos of overflow record)
                                   // elements[] each one is:
                                   //     (u64 (hash), u64 (record pos), u32 (record size)).
        let bucket_size: u16 = 8 + (BUCKET_ELEMENT_SIZE as u16 * bucket_elements);
        Self {
            type_id: *b"sldb.dat",
            version: 0,
            uid: 0,
            appnum: 0,
            bucket_elements,
            bucket_size,
            reserved: [0; 64],
        }
    }
}

impl DataHeader {
    pub fn new(config: &DbConfig) -> Self {
        Self {
            bucket_elements: config.bucket_elements,
            appnum: config.appnum,
            ..Default::default()
        }
    }

    /// Load a DataHeader from source.
    pub fn load_header<R: Read + Seek>(source: &mut R) -> DBResult<Self> {
        let source = source;
        let mut header = Self::default();
        source.seek(SeekFrom::Start(0))?;
        unsafe {
            source.read_exact(DataHeader::as_bytes_mut(&mut header))?;
        }

        if &header.type_id != b"sldb.dat" {
            return Err(DBError::InvalidDataHeader);
        }
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    pub fn write_header<R: Write + Seek>(&self, sync: &mut R) -> DBResult<()> {
        sync.write_all(self.as_ref())?;
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
