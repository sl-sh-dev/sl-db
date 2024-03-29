//! Define and manage a data file header.

use crate::db::add_crc32;
use crate::db_config::DbConfig;
use crate::error::LoadHeaderError;
use crate::fxhasher::FxHasher;
use std::hash::Hasher;
use std::io::{Read, Seek, Write};
use std::time::UNIX_EPOCH;
use std::{io, time};

// Each bucket element is a (u64, u64)- (hash, record_pos).
pub(crate) const BUCKET_ELEMENT_SIZE: usize = 16;
/// Size of the data file header.
pub const DATA_HEADER_BYTES: usize = 30;

/// Struct that contains the header for a sldb data file.
/// This data is immutable was written, the data file is an append only log file and will only be
/// truncated to maintain consistency.
/// This data in the file will be followed by a CRC32 checksum value to verify it.
#[derive(Debug, Copy, Clone)]
#[repr(C)]
pub(crate) struct DataHeader {
    type_id: [u8; 8], // The characters "sldb.dat"
    version: u16,     // Holds the version number
    uid: u64,         // Unique ID generated on creation
    appnum: u64,      // Application defined constant
}

impl DataHeader {
    pub fn new(config: &DbConfig) -> Self {
        let mut hasher = FxHasher::default();
        let now = time::SystemTime::now();
        let now_millis = now
            .duration_since(UNIX_EPOCH)
            // Time went backwards, WTF?  This could lead to common uid I guess if using
            // machines with broken time (i.e. clocks set before the Unix epoch)...
            .unwrap_or_else(|_| time::Duration::from_millis(66))
            .as_millis();
        hasher.write_u128(now_millis);
        // This is pretty basic, just has the current millis with FX to get a UID.
        // This is just to make sure sets of files belong together so not going crazy here.
        let uid = hasher.finish();
        Self {
            type_id: *b"sldb.dat",
            version: 0,
            uid,
            appnum: config.appnum,
        }
    }

    /// Load a DataHeader from source.
    pub fn load_header<R: Read + Seek>(source: &mut R) -> Result<Self, LoadHeaderError> {
        source.rewind()?;
        let mut buffer = [0_u8; DATA_HEADER_BYTES];
        let mut buf16 = [0_u8; 2];
        let mut buf32 = [0_u8; 4];
        let mut buf64 = [0_u8; 8];
        let mut pos = 0;
        source.read_exact(&mut buffer[..])?;
        let mut crc32_hasher = crc32fast::Hasher::new();
        crc32_hasher.update(&buffer[..(DATA_HEADER_BYTES - 4)]);
        let calc_crc32 = crc32_hasher.finalize();
        buf32.copy_from_slice(&buffer[(DATA_HEADER_BYTES - 4)..]);
        let read_crc32 = u32::from_le_bytes(buf32);
        if calc_crc32 != read_crc32 {
            return Err(LoadHeaderError::CrcFailed);
        }
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
        let mut buffer = [0_u8; DATA_HEADER_BYTES];
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
        assert_eq!(pos, DATA_HEADER_BYTES);
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
}
