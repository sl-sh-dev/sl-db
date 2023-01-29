//! Contains the Hash Index (HDX) structure and code.

use crate::crc::{add_crc32, check_crc};
use crate::db::data_header::DataHeader;
use crate::db_config::DbConfig;
use crate::error::LoadHeaderError;
use crate::fxhasher::FxHasher;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, Hash, Hasher};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};

/// Minimum size to hold a header.  THe header will be padded to bucket_size if it is larger.
/// This is to allow the hdx file to accessed in sector sized chunks.
const MIN_HEADER_SIZE: usize = 64;

/// Header for an hdx (index) file.  This contains the hash buckets for lookups.
/// This file is not a log file and the header and buckets will change in place over time.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
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
    /// Open an HDX index file and return the open file and the header.
    pub fn open_hdx_file<S: BuildHasher + Default>(
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
        let file_end = file.seek(SeekFrom::End(0))?;

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
            let header = HdxHeader::load_header(&mut file, config.bucket_size)?;
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

    /// Return a default HdxHeader with any values from data_header overridden.
    /// This includes the version, uid, appnum, bucket_size and bucket_elements.
    pub fn from_data_header(
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
        }
    }

    /// Load a HdxHeader from a file.  This will seek to the beginning and leave the file
    /// positioned after the header.
    pub fn load_header<R: Read + Seek>(
        source: &mut R,
        header_size: u16,
    ) -> Result<Self, LoadHeaderError> {
        let header_size = if (header_size as usize) < MIN_HEADER_SIZE {
            MIN_HEADER_SIZE
        } else {
            header_size as usize
        };
        source.seek(SeekFrom::Start(0))?;
        let mut buffer = vec![0_u8; header_size];
        let mut buf16 = [0_u8; 2];
        let mut buf32 = [0_u8; 4];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        source.read_exact(&mut buffer[..])?;
        if !check_crc(&buffer) {
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
        add_crc32(&mut buffer);
        sync.write_all(&buffer)?;
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
}
