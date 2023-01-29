//! Implement the core io traints (Read, Seek and Write) on DbCore.  This allows other code to
//! easily access the underlying data without worring about the read or write buffers.

use crate::db::DbCore;
use crate::db_bytes::DbBytes;
use crate::db_key::DbKey;
use std::fmt::Debug;
use std::hash::BuildHasher;
use std::io;
use std::io::{Read, Seek, SeekFrom, Write};

impl<K, V, const KSIZE: u16, S> Read for DbCore<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Read for the DbInner.  This allows other code to not worry about whether data is read from
    /// the file, write buffer or the read buffer.  The file and write buffer will not have overlapping records
    /// so this will not read across them in one call.  This will not happen on a proper DB although
    /// the Read contract should handle this fine.
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.seek_pos >= self.data_file_end {
            let write_pos = (self.seek_pos - self.data_file_end) as usize;
            if write_pos < self.write_buffer.len() {
                let mut size = buf.len();
                if write_pos + size > self.write_buffer.len() {
                    size = self.write_buffer.len() - write_pos;
                }
                buf[..size].copy_from_slice(&self.write_buffer[write_pos..write_pos + size]);
                self.seek_pos += size as u64;
                Ok(size)
            } else {
                Ok(0)
            }
        } else if self.seek_pos >= self.read_buffer_start as u64
            && self.seek_pos < (self.read_buffer_start + self.read_buffer_len) as u64
        {
            self.copy_read_buffer(buf)
        } else {
            let mut seek_pos = self.seek_pos;
            let mut end = self.data_file_end - seek_pos;
            if end < self.config.read_buffer_size as u64 {
                // If remaining bytes are less then the buffer pull back seek_pos to fill the buffer.
                seek_pos = if self.data_file_end > self.config.read_buffer_size as u64 {
                    self.data_file_end - self.config.read_buffer_size as u64
                } else {
                    0
                };
            } else {
                // Put the seek position in the mid point of the read buffer.  This might help increase
                // buffer hits or might do nothing or hurt depending on fetch patterns.
                seek_pos = if seek_pos > (self.config.read_buffer_size / 2) as u64 {
                    seek_pos - (self.config.read_buffer_size / 2) as u64
                } else {
                    0
                };
            }
            end = self.data_file_end - seek_pos;
            if end > 0 {
                self.data_file.seek(SeekFrom::Start(seek_pos))?;
                if end < self.config.read_buffer_size as u64 {
                    self.data_file
                        .read_exact(&mut self.read_buffer[..end as usize])?;
                    self.read_buffer_len = end as usize;
                } else {
                    self.data_file.read_exact(&mut self.read_buffer[..])?;
                    self.read_buffer_len = self.config.read_buffer_size as usize;
                }
                self.read_buffer_start = seek_pos as usize;
                self.copy_read_buffer(buf)
            } else {
                Ok(0)
            }
        }
    }
}

impl<K, V, const KSIZE: u16, S> Seek for DbCore<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Seek on the DbInner treating the file and write cache as one byte array.
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        match pos {
            SeekFrom::Start(pos) => self.seek_pos = pos,
            SeekFrom::End(pos) => {
                let end = (self.data_file_end + self.write_buffer.len() as u64) as i64 + pos;
                if end >= 0 {
                    self.seek_pos = end as u64;
                } else {
                    self.seek_pos = 0;
                }
            }
            SeekFrom::Current(pos) => {
                let end = self.seek_pos as i64 + pos;
                if end >= 0 {
                    self.seek_pos = end as u64;
                } else {
                    self.seek_pos = 0;
                }
            }
        }
        Ok(self.seek_pos)
    }
}

impl<K, V, const KSIZE: u16, S> Write for DbCore<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        if self.write_buffer.len() >= self.config.write_buffer_size as usize {
            self.data_file.write_all(&self.write_buffer)?;
            self.data_file_end += self.write_buffer.len() as u64;
            self.write_buffer.clear();
        }
        let write_buffer_len = self.write_buffer.len();
        let write_capacity = self.config.write_buffer_size as usize - write_buffer_len;
        let buf_len = buf.len();
        if write_capacity > buf_len {
            self.write_buffer.write_all(buf)?;
            Ok(buf_len)
        } else {
            self.write_buffer.write_all(&buf[..write_capacity])?;
            self.data_file.write_all(&self.write_buffer)?;
            self.data_file_end += self.write_buffer.len() as u64;
            self.write_buffer.clear();
            Ok(write_capacity)
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        self.data_file.write_all(&self.write_buffer)?;
        self.data_file_end += self.write_buffer.len() as u64;
        self.write_buffer.clear();
        Ok(())
    }
}
