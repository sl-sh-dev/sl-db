//! Contains the Hash Index overflow buckets (ODX) structure and code.

use crate::db::add_crc32;
use crate::db::hdx_header::HdxHeader;
use crate::error::LoadHeaderError;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};

/// Size of an index header, includes the crc32 checksum following the header.
pub const ODX_HEADER_SIZE: usize = 30;

/// Header for an odx (index overflow) file.  This contains the overflow hash buckets for lookups.
/// This file is an append only log file and the header and buckets will NOT change in place over time.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub(crate) struct OdxHeader {
    type_id: [u8; 8], // The characters "sldb.odx"
    version: u16,     // Holds the version number
    uid: u64,         // Unique ID generated on creation
    appnum: u64,      // Application defined constant
}

impl OdxHeader {
    /// Return a default OdxHeader with any values from hdx_header overridden.
    /// This includes the version, uid, appnum and bucket_size.
    pub fn from_hdx_header(hdx_header: &HdxHeader) -> Self {
        Self {
            type_id: *b"sldb.odx",
            version: hdx_header.version(),
            uid: hdx_header.uid(),
            appnum: hdx_header.appnum(),
        }
    }

    /// Load a HdxHeader from a file.  This will seek to the beginning and leave the file
    /// positioned after the header.
    pub fn load_header<R: Read + Seek>(source: &mut R) -> Result<Self, LoadHeaderError> {
        source.seek(SeekFrom::Start(0))?;
        let mut buffer = [0_u8; ODX_HEADER_SIZE];
        let mut buf16 = [0_u8; 2];
        let mut buf32 = [0_u8; 4];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        source.read_exact(&mut buffer[..])?;
        let mut crc32_hasher = crc32fast::Hasher::new();
        crc32_hasher.update(&buffer[..(ODX_HEADER_SIZE - 4)]);
        let calc_crc32 = crc32_hasher.finalize();
        buf32.copy_from_slice(&buffer[(ODX_HEADER_SIZE - 4)..]);
        let read_crc32 = u32::from_le_bytes(buf32);
        if calc_crc32 != read_crc32 {
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
        };
        Ok(header)
    }

    /// Write this header to sync at current seek position.
    pub fn write_header<R: Write + Seek>(&self, sync: &mut R) -> Result<(), io::Error> {
        let mut buffer = [0_u8; ODX_HEADER_SIZE];
        let mut pos = 0;
        buffer[pos..8].copy_from_slice(&self.type_id);
        pos += 8;
        buffer[pos..(pos + 2)].copy_from_slice(&self.version.to_le_bytes());
        pos += 2;
        buffer[pos..(pos + 8)].copy_from_slice(&self.uid.to_le_bytes());
        pos += 8;
        buffer[pos..(pos + 8)].copy_from_slice(&self.appnum.to_le_bytes());
        pos += 8;
        add_crc32(&mut buffer);
        pos += 4;
        assert_eq!(pos, ODX_HEADER_SIZE);
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
