//! Implements the iterator for a buckets elements.  This also handles overflow buckets and allows
//! buckets to accessed without worrying about underlying structure or files.
//! NOTE: This is ONLY appropriate for the core DB.

use crate::crc::check_crc;
use crate::db::data_header::BUCKET_ELEMENT_SIZE;
use std::io::{Read, Seek, SeekFrom};

/// Iterates over the (hash, record_position) values contained in a bucket.
/// If supplied with a hash will only return the has/positions that match hash.
/// Uses a binary search of buckets when given a hash.
pub(crate) struct BucketIter<'src, R: Read + Seek + ?Sized> {
    odx_file: &'src mut R,
    buffer: Vec<u8>,
    bucket_pos: usize,
    start_pos: usize,
    end_pos: usize,
    overflow_pos: u64,
    elements: u32,
    hash: Option<u64>,
    hash_found: bool,
    crc_failure: bool,
}

impl<'src, R: Read + Seek + ?Sized> BucketIter<'src, R> {
    pub(super) fn new(odx_file: &'src mut R, buffer: Vec<u8>, hash: Option<u64>) -> Self {
        let mut buf = [0_u8; 8]; // buffer for converting to u64s (needs an array)
        buf.copy_from_slice(&buffer[0..8]);
        let overflow_pos = u64::from_le_bytes(buf);
        let mut buf = [0_u8; 4]; // buffer for converting to u32s (needs an array)
        buf.copy_from_slice(&buffer[8..12]);
        let elements = u32::from_le_bytes(buf);
        let start_pos = 0;
        let end_pos = elements as usize;
        let bucket_pos = if hash.is_some() { end_pos / 2 } else { 0 };
        let crc_failure = !check_crc(&buffer);
        Self {
            odx_file,
            buffer,
            bucket_pos,
            start_pos,
            end_pos,
            overflow_pos,
            elements,
            hash,
            hash_found: false,
            crc_failure,
        }
    }

    pub(super) fn crc_failure(&self) -> bool {
        self.crc_failure
    }

    fn get_element(&self, bucket_pos: usize) -> (u64, u64) {
        let mut buf64 = [0_u8; 8];
        // 12- 8 bytes for overflow position and 4 for the elements in the bucket.
        let mut pos = 12 + (bucket_pos * BUCKET_ELEMENT_SIZE);
        buf64.copy_from_slice(&self.buffer[pos..(pos + 8)]);
        let hash = u64::from_le_bytes(buf64);
        pos += 8;
        buf64.copy_from_slice(&self.buffer[pos..(pos + 8)]);
        let rec_pos = u64::from_le_bytes(buf64);
        (hash, rec_pos)
    }

    fn reset_next_overflow(&mut self) -> Option<()> {
        // For reading u64 values, needs an array.
        let mut buf64 = [0_u8; 8];
        self.odx_file
            .seek(SeekFrom::Start(self.overflow_pos))
            .ok()?;
        self.odx_file.read_exact(&mut self.buffer[..]).ok()?;
        if !check_crc(&self.buffer) {
            self.crc_failure = true;
            return None;
        }
        buf64.copy_from_slice(&self.buffer[0..8]);
        self.overflow_pos = u64::from_le_bytes(buf64);
        let mut buf = [0_u8; 4];
        buf.copy_from_slice(&self.buffer[8..12]);
        self.hash_found = false;
        self.elements = u32::from_le_bytes(buf);
        self.start_pos = 0;
        self.end_pos = self.elements as usize;
        self.bucket_pos = if self.hash.is_some() {
            self.end_pos / 2
        } else {
            0
        };
        Some(())
    }
}

impl<'src, R: Read + Seek + ?Sized> Iterator for &mut BucketIter<'src, R> {
    type Item = (u64, u64);

    fn next(&mut self) -> Option<Self::Item> {
        if self.crc_failure {
            return None;
        }
        loop {
            if self.elements == 0 {
                if self.overflow_pos > 0 {
                    self.reset_next_overflow()?;
                    continue;
                }
                return None;
            }
            if let Some(hash) = self.hash {
                if self.bucket_pos >= self.elements as usize || self.bucket_pos >= self.end_pos {
                    if self.overflow_pos > 0 {
                        self.reset_next_overflow()?;
                        continue;
                    }
                    return None;
                }
                let (rec_hash, rec_pos) = self.get_element(self.bucket_pos);
                if self.hash_found {
                    return if rec_hash == hash {
                        self.bucket_pos += 1;
                        Some((rec_hash, rec_pos))
                    } else {
                        if self.overflow_pos > 0 {
                            self.reset_next_overflow()?;
                            continue;
                        }
                        None
                    };
                }
                if hash == rec_hash {
                    // We might have a run of the same hash so back up to the first one and setup to
                    // produce them all.
                    let (mut rec_hash, mut rec_pos) = (rec_hash, rec_pos);
                    while self.bucket_pos > 0 && self.bucket_pos >= self.start_pos {
                        self.bucket_pos -= 1;
                        let (prev_rec_hash, prev_rec_pos) = self.get_element(self.bucket_pos);
                        if prev_rec_hash != hash {
                            self.bucket_pos += 1;
                            break;
                        }
                        (rec_hash, rec_pos) = (prev_rec_hash, prev_rec_pos);
                    }
                    self.hash_found = true;
                    self.bucket_pos += 1;
                    return Some((rec_hash, rec_pos));
                }
                if self.start_pos >= self.end_pos {
                    if self.overflow_pos > 0 {
                        self.reset_next_overflow()?;
                        continue;
                    }
                    return None;
                } else if hash < rec_hash {
                    self.end_pos = self.bucket_pos;
                } else if self.start_pos < self.bucket_pos {
                    self.start_pos = self.bucket_pos;
                } else {
                    self.start_pos = self.bucket_pos + 1;
                }
                self.bucket_pos = self.start_pos + ((self.end_pos - self.start_pos) / 2);
                continue;
            } else if self.bucket_pos < self.elements as usize {
                let (rec_hash, rec_pos) = self.get_element(self.bucket_pos);
                self.bucket_pos += 1;
                return Some((rec_hash, rec_pos));
            } else if self.overflow_pos > 0 {
                // We have an overflow bucket to search as well.
                self.reset_next_overflow()?;
            } else {
                return None;
            }
        }
    }
}
