use crate::db::byte_trans::ByteTrans;
use crate::db::data_header::BUCKET_ELEMENT_SIZE;
use crate::error::{DBError, DBResult};
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

impl Default for HdxHeader {
    fn default() -> Self {
        let buckets = 1; //128;
        let elements = 255; //5;
                            // Each bucket is:
                            // u64 (pos of overflow record)
                            // elements[] each one is:
                            //     (u64 (hash), u64 (record pos), u32 (record size)).
        let bucket_size: u16 = 8 + (BUCKET_ELEMENT_SIZE as u16 * elements);
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

impl HdxHeader {
    pub fn load_header<R: Read + Seek>(source: &mut R) -> DBResult<Self> {
        let mut header = HdxHeader::default();
        source.seek(SeekFrom::Start(0))?;
        unsafe {
            source.read_exact(HdxHeader::as_bytes_mut(&mut header))?;
        }

        if &header.type_id != b"sldb.hdx" {
            return Err(DBError::InvalidIndexHeader);
        }
        Ok(header)
    }

    /// Write this header to sync.
    pub fn write_header<R: Write + Seek>(&self, sync: &mut R) -> DBResult<()> {
        sync.write_all(self.as_ref())?;
        Ok(())
    }

    pub fn buckets(&self) -> u32 {
        self.buckets
    }

    pub fn inc_buckets(&mut self) {
        self.buckets += 1;
    }

    pub fn bucket_elements(&self) -> u16 {
        self.bucket_elements
    }

    pub fn bucket_size(&self) -> u16 {
        self.bucket_size
    }

    pub fn load_factor(&self) -> f32 {
        self.load_factor as f32 / u16::MAX as f32
    }

    pub fn values(&self) -> u64 {
        self.values
    }

    pub fn inc_values(&mut self) {
        self.values += 1;
    }
}
