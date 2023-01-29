//! Implemts the iterator for a buckets elements.  This also handles overflow buckets and allows
//! buckets to accessed without worring about underlying structure or files.
//! NOTE: This is ONLY appropropriate for the core DB.

use crate::crc::check_crc;
use crate::db::data_header::BUCKET_ELEMENT_SIZE;
use std::io::{Read, Seek, SeekFrom};

/// Iterates over the (hash, record_position) values contained in a bucket.
pub(super) struct BucketIter<'src, R: Read + Seek + ?Sized> {
    odx_file: &'src mut R,
    buffer: Vec<u8>,
    bucket_pos: usize,
    overflow_pos: u64,
    elements: u16,
    crc_failure: bool,
}

impl<'src, R: Read + Seek + ?Sized> BucketIter<'src, R> {
    pub(super) fn new(odx_file: &'src mut R, buffer: Vec<u8>, elements: u16) -> Self {
        let mut buf = [0_u8; 8]; // buffer for converting to u64s (needs an array)
        buf.copy_from_slice(&buffer[0..8]);
        let overflow_pos = u64::from_le_bytes(buf);
        let crc_failure = !check_crc(&buffer);
        Self {
            odx_file,
            buffer,
            bucket_pos: 0,
            overflow_pos,
            elements,
            crc_failure,
        }
    }

    pub(super) fn crc_failure(&self) -> bool {
        self.crc_failure
    }
}

impl<'src, R: Read + Seek + ?Sized> Iterator for &mut BucketIter<'src, R> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.crc_failure {
            return None;
        }
        // For reading u64 values, needs an array.
        let mut buf64 = [0_u8; 8];
        loop {
            if self.bucket_pos < self.elements as usize {
                let mut pos = 8 + (self.bucket_pos * BUCKET_ELEMENT_SIZE);
                buf64.copy_from_slice(&self.buffer[pos..(pos + 8)]);
                let hash = u64::from_le_bytes(buf64);
                pos += 8;
                buf64.copy_from_slice(&self.buffer[pos..(pos + 8)]);
                let rec_pos = u64::from_le_bytes(buf64);
                if hash == 0 && rec_pos == 0 {
                    self.bucket_pos += 1;
                } else {
                    self.bucket_pos += 1;
                    return Some((hash, rec_pos));
                }
            } else if self.overflow_pos > 0 {
                // We have an overflow bucket to search as well.
                self.odx_file
                    .seek(SeekFrom::Start(self.overflow_pos))
                    .ok()?;
                self.odx_file.read_exact(&mut self.buffer[..]).ok()?;
                if !check_crc(&self.buffer) {
                    self.crc_failure = true;
                    return None;
                }
                self.bucket_pos = 0;
                buf64.copy_from_slice(&self.buffer[0..8]);
                self.overflow_pos = u64::from_le_bytes(buf64);
            } else {
                return None;
            }
        }
    }
}