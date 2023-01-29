//! Contains the Hash Index overflow buckets (ODX) structure and code.

use crate::crc::{add_crc32, check_crc};
use crate::db_config::DbConfig;
use crate::error::LoadHeaderError;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};

/// Minimum size of an index overflow header, includes the crc32 checksum following the header.
const MIN_HEADER_SIZE: usize = 30;

/// Header for an odx (index overflow) file.  This contains the overflow hash buckets for lookups.
/// This file is an append only log file and the header and buckets will NOT change in place over time.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub(crate) struct OdxHeader {
    type_id: [u8; 8],   // The characters "sldb.odx"
    version: u16,       // Holds the version number
    uid: u64,           // Unique ID generated on creation
    appnum: u64,        // Application defined constant
    header_size: usize, // Size of the header (not saved to file, max of bucket_size or MIN_HEADER_SIZE).
}

impl OdxHeader {
    /// Open the index overflow file (odx file) and return the open file and header.
    pub fn open_odx_file(
        version: u16,
        uid: u64,
        appnum: u64,
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
        let file_end = file.seek(SeekFrom::End(0))?;

        let header = if file_end == 0 {
            let header = OdxHeader::new(version, uid, appnum, config.bucket_size);
            header.write_header(&mut file)?;
            header
        } else {
            let header = OdxHeader::load_header(&mut file, config.bucket_size)?;
            // Basic validation of the odx header.
            if header.version() != version {
                return Err(LoadHeaderError::InvalidIndexVersion);
            }
            if header.appnum() != appnum {
                return Err(LoadHeaderError::InvalidIndexAppNum);
            }
            if header.uid() != uid {
                return Err(LoadHeaderError::InvalidIndexUID);
            }
            header
        };
        Ok((file, header))
    }

    /// Return a default OdxHeader with any values from hdx_header overridden.
    /// This includes the version, uid, appnum and bucket_size.
    pub fn new(version: u16, uid: u64, appnum: u64, bucket_size: u16) -> Self {
        let header_size = if (bucket_size as usize) < MIN_HEADER_SIZE {
            MIN_HEADER_SIZE
        } else {
            bucket_size as usize
        };
        Self {
            type_id: *b"sldb.odx",
            version,
            uid,
            appnum,
            header_size,
        }
    }

    /// Load a HdxHeader from a file.  This will seek to the beginning and leave the file
    /// positioned after the header.
    pub fn load_header<R: Read + Seek>(
        source: &mut R,
        bucket_size: u16,
    ) -> Result<Self, LoadHeaderError> {
        let header_size = if (bucket_size as usize) < MIN_HEADER_SIZE {
            MIN_HEADER_SIZE
        } else {
            bucket_size as usize
        };
        source.seek(SeekFrom::Start(0))?;
        let mut buffer = vec![0_u8; header_size];
        let mut buf16 = [0_u8; 2];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        source.read_exact(&mut buffer[..])?;
        if !check_crc(&buffer) {
            return Err(LoadHeaderError::CrcFailed);
        }
        let mut type_id = [0_u8; 8];
        type_id.copy_from_slice(&buffer[0..8]);
        pos += 8;
        if &type_id != b"sldb.odx" {
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
        let header = Self {
            type_id,
            version,
            uid,
            appnum,
            header_size,
        };
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    pub fn write_header<R: Write + Seek>(&self, sync: &mut R) -> Result<(), io::Error> {
        let mut buffer = vec![0_u8; self.header_size];
        let mut pos = 0;
        buffer[pos..8].copy_from_slice(&self.type_id);
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.version.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.uid.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.appnum.to_le_bytes());
        add_crc32(&mut buffer);
        sync.write_all(&buffer)?;
        Ok(())
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
}
