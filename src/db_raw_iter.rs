//! Raw iterator over a sldb data file.  This will work without an index file and can be used to
//! open the file directly.

use crate::db::byte_trans::ByteTrans;
use crate::db::data_header::DataHeader;
use crate::db::{DbBytes, DbKey};
use crate::error::DBError;
use crate::error::DBResult;
use crate::fxhasher::FxHasher;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::hash::{BuildHasher, BuildHasherDefault};
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::marker::PhantomData;
use std::path::Path;

/// Iterate over a Db's key, value pairs in insert order.
/// This iterator is "raw", it does not use any indexes just the data file.
pub struct DbRawIter<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    _db: PhantomData<(K, V, S)>,
    bucket_size: u16,
    file: BufReader<File>,
    buffer: Vec<u8>,
    position: u64,
}

impl<K, V, const KSIZE: u16, S> DbRawIter<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    /// Open the data file in dir with base_name (note do not include the .dat- that is appended).
    /// Produces an iterator over all the (key, values).  Does not use the index at all and records
    /// are returned in insert order.
    pub fn open<P: AsRef<Path>>(dir: P, base_name: P) -> DBResult<Self> {
        let data_name = dir.as_ref().join(base_name.as_ref()).with_extension("dat");
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(data_name)?;

        let header = DataHeader::load_header(&mut data_file)?;
        let file = BufReader::new(data_file);
        Ok(Self {
            _db: PhantomData,
            bucket_size: header.bucket_size(),
            file,
            buffer: Vec::new(),
            position: DataHeader::SIZE as u64,
        })
    }

    /// Same as open but created from an existing File.
    pub fn new(dat_file: File) -> DBResult<Self> {
        let mut dat_file = dat_file;
        let header = DataHeader::load_header(&mut dat_file)?;
        let file = BufReader::new(dat_file);
        Ok(Self {
            _db: PhantomData,
            bucket_size: header.bucket_size(),
            file,
            buffer: Vec::new(),
            position: DataHeader::SIZE as u64,
        })
    }

    fn read_record_file<R: Read + Seek>(
        file: &mut R,
        buffer: &mut Vec<u8>,
        position: u64,
    ) -> DBResult<((K, V), u64)> {
        let mut next_record_pos = position;
        file.seek(SeekFrom::Start(position))?;
        let key_size = if K::is_variable_key_size() {
            let mut key_size = [0_u8; 2];
            file.read_exact(&mut key_size)?;
            next_record_pos += 2;
            let key_size = u16::from_ne_bytes(key_size);
            if key_size == 0 {
                // Overflow bucket, can not read as data so error.
                return Err(DBError::UnexpectedOverflowBucket);
            }
            key_size
        } else {
            K::KEY_SIZE
        } as usize;

        let mut val_size_buf = [0_u8; 4];
        file.read_exact(&mut val_size_buf)?;
        next_record_pos += 4;
        let val_size = u32::from_ne_bytes(val_size_buf);
        if K::is_fixed_key_size() && val_size == 0 {
            // No key size so overflow indicated by a 0 value size.
            return Err(DBError::UnexpectedOverflowBucket);
        }
        buffer.resize(key_size as usize, 0);
        file.read_exact(buffer)?;
        next_record_pos += buffer.len() as u64;
        let key = K::deserialize(&buffer[..])?;
        buffer.resize(val_size as usize, 0);
        file.read_exact(buffer)?;
        next_record_pos += buffer.len() as u64;
        let val = V::deserialize(&buffer[..])?;
        Ok(((key, val), next_record_pos))
    }
}

impl<K, V, const KSIZE: u16, S> Iterator for DbRawIter<K, V, KSIZE, S>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
    S: BuildHasher + Default,
{
    type Item = (K, V);

    fn next(&mut self) -> Option<Self::Item> {
        let mut rec = Self::read_record_file(&mut self.file, &mut self.buffer, self.position);
        let mut el = 1_u64;
        while let Err(err) = &rec {
            if let DBError::UnexpectedOverflowBucket = err {
                rec = Self::read_record_file(
                    &mut self.file,
                    &mut self.buffer,
                    // A overflow bucket has a slightly different size for fixed and variable keys.
                    // Fixed keys don't need a key size (u16) but have to set value size (u32).
                    if K::is_variable_key_size() {
                        self.position + (el * (2 + self.bucket_size as u64))
                    } else {
                        self.position + (el * (4 + self.bucket_size as u64))
                    },
                );
                el += 1;
            } else {
                break;
            }
        }
        if let Ok((ret, position)) = rec {
            self.position = position;
            Some(ret)
        } else {
            None
        }
    }
}
