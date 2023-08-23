//! Implements the iterator for a buckets elements.  This also handles overflow buckets and allows
//! buckets to accessed without worrying about underlying structure or files.
//! NOTE: This is ONLY appropriate for the core DB.

use crate::crc::check_crc;
use crate::db::data_header::BUCKET_ELEMENT_SIZE;
use crate::db::hdx_index::HdxIndex;
use crate::db_bytes::DbBytes;
use crate::db_key::DbKey;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

/// Iterates over the (hash, record_position) values contained in a bucket.
/// If supplied with a hash will only return the hash/positions that match hash (i.e. hash will always be the same).
/// Uses a binary search of buckets when given a hash.
pub(crate) struct BucketIter {
    buffer: &'static Vec<u8>,
    overflow_buffer: Vec<u8>,
    bucket_pos: usize,
    start_pos: usize,
    end_pos: usize,
    overflow_pos: u64,
    elements: u32,
    hash: Option<u64>,
    hash_found: bool,
    crc_failure: bool,
}

impl BucketIter {
    pub(super) fn new(buffer: &'static Vec<u8>, hash: Option<u64>) -> Self {
        let mut buf = [0_u8; 8]; // buffer for converting to u64s (needs an array)
        buf.copy_from_slice(&buffer[0..8]);
        let overflow_pos = u64::from_le_bytes(buf);
        let mut buf = [0_u8; 4]; // buffer for converting to u32s (needs an array)
        buf.copy_from_slice(&buffer[8..12]);
        let elements = u32::from_le_bytes(buf);
        let start_pos = 0;
        let end_pos = elements as usize;
        let bucket_pos = if hash.is_some() { end_pos / 2 } else { 0 };
        Self {
            buffer,
            overflow_buffer: vec![],
            bucket_pos,
            start_pos,
            end_pos,
            overflow_pos,
            elements,
            hash,
            hash_found: false,
            crc_failure: false, // Assume the initial bucket we are provided is valid.
        }
    }

    pub(super) fn new_empty() -> Self {
        let overflow_pos = 0;
        let elements = 0;
        let start_pos = 0;
        let end_pos = elements as usize;
        let bucket_pos = 0;
        let overflow_buffer = vec![];
        let buffer = unsafe {
            (&overflow_buffer as *const Vec<u8>)
                .as_ref()
                .expect("this can't be null")
        };
        Self {
            buffer,
            overflow_buffer,
            bucket_pos,
            start_pos,
            end_pos,
            overflow_pos,
            elements,
            hash: None,
            hash_found: false,
            crc_failure: true, // Force next to always return None.
        }
    }

    pub(super) fn new_from_overflow(overflow_pos: u64, hash: Option<u64>) -> Self {
        let elements = 0;
        let start_pos = 0;
        let end_pos = elements as usize;
        let bucket_pos = 0;
        let overflow_buffer = vec![];
        let buffer = unsafe {
            (&overflow_buffer as *const Vec<u8>)
                .as_ref()
                .expect("this can't be null")
        };
        Self {
            buffer,
            overflow_buffer,
            bucket_pos,
            start_pos,
            end_pos,
            overflow_pos,
            elements,
            hash,
            hash_found: false,
            crc_failure: true, // Force next to always return None.
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

    fn reset_next_overflow(&mut self, odx_file: &mut File) -> Option<()> {
        self.overflow_buffer.resize(self.buffer.len(), 0);
        // For reading u64 values, needs an array.
        let mut buf64 = [0_u8; 8];
        odx_file.seek(SeekFrom::Start(self.overflow_pos)).ok()?;
        odx_file.read_exact(&mut self.overflow_buffer[..]).ok()?;
        if !check_crc(&self.overflow_buffer) {
            self.crc_failure = true;
            return None;
        }
        buf64.copy_from_slice(&self.overflow_buffer[0..8]);
        self.overflow_pos = u64::from_le_bytes(buf64);
        let mut buf = [0_u8; 4];
        buf.copy_from_slice(&self.overflow_buffer[8..12]);
        self.hash_found = false;
        self.elements = u32::from_le_bytes(buf);
        self.start_pos = 0;
        self.end_pos = self.elements as usize;
        self.bucket_pos = if self.hash.is_some() {
            self.end_pos / 2
        } else {
            0
        };
        let buffer = unsafe {
            (&self.overflow_buffer as *const Vec<u8>)
                .as_ref()
                .expect("this can't be null")
        };
        self.buffer = buffer;
        Some(())
    }
}

impl<K, const KSIZE: u16> HdxIndex<K, KSIZE>
where
    K: DbKey<KSIZE> + DbBytes<K>,
{
    /// Advance and return the next hash/position for the bucket defined by BucketIter.
    /// Setup and get a BucketIter with a call to bucket_iter().  If the BucketIter is given a hash
    /// then all returned hash values will match it (will return the elements that have that hash).
    pub fn next_bucket_element(&mut self, iter: &mut BucketIter) -> Option<(u64, u64)> {
        if iter.crc_failure {
            return None;
        }
        loop {
            if iter.elements == 0 {
                if iter.overflow_pos > 0 {
                    iter.reset_next_overflow(&mut self.odx_file)?;
                    continue;
                }
                return None;
            }
            if let Some(hash) = iter.hash {
                if iter.bucket_pos >= iter.elements as usize || iter.bucket_pos >= iter.end_pos {
                    if iter.overflow_pos > 0 {
                        iter.reset_next_overflow(&mut self.odx_file)?;
                        continue;
                    }
                    return None;
                }
                let (rec_hash, rec_pos) = iter.get_element(iter.bucket_pos);
                if iter.hash_found {
                    return if rec_hash == hash {
                        iter.bucket_pos += 1;
                        Some((rec_hash, rec_pos))
                    } else {
                        if iter.overflow_pos > 0 {
                            iter.reset_next_overflow(&mut self.odx_file)?;
                            continue;
                        }
                        None
                    };
                }
                if hash == rec_hash {
                    // We might have a run of the same hash so back up to the first one and setup to
                    // produce them all.
                    let (mut rec_hash, mut rec_pos) = (rec_hash, rec_pos);
                    while iter.bucket_pos > 0 && iter.bucket_pos >= iter.start_pos {
                        iter.bucket_pos -= 1;
                        let (prev_rec_hash, prev_rec_pos) = iter.get_element(iter.bucket_pos);
                        if prev_rec_hash != hash {
                            iter.bucket_pos += 1;
                            break;
                        }
                        (rec_hash, rec_pos) = (prev_rec_hash, prev_rec_pos);
                    }
                    iter.hash_found = true;
                    iter.bucket_pos += 1;
                    return Some((rec_hash, rec_pos));
                }
                if iter.start_pos >= iter.end_pos {
                    if iter.overflow_pos > 0 {
                        iter.reset_next_overflow(&mut self.odx_file)?;
                        continue;
                    }
                    return None;
                } else if hash < rec_hash {
                    iter.end_pos = iter.bucket_pos;
                } else if iter.start_pos < iter.bucket_pos {
                    iter.start_pos = iter.bucket_pos;
                } else {
                    iter.start_pos = iter.bucket_pos + 1;
                }
                iter.bucket_pos = iter.start_pos + ((iter.end_pos - iter.start_pos) / 2);
                continue;
            } else if iter.bucket_pos < iter.elements as usize {
                let (rec_hash, rec_pos) = iter.get_element(iter.bucket_pos);
                iter.bucket_pos += 1;
                return Some((rec_hash, rec_pos));
            } else if iter.overflow_pos > 0 {
                // We have an overflow bucket to search as well.
                iter.reset_next_overflow(&mut self.odx_file)?;
            } else {
                return None;
            }
        }
    }
}
