//! Raw iterator over a sldb data file.  This will work without an index file and can be used to
//! open the file directly.

use crate::db::data_header::DataHeader;
use crate::db_bytes::DbBytes;
use crate::db_key::DbKey;
use crate::error::FetchError;
use crate::error::LoadHeaderError;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::{BufReader, Read, Seek};
use std::marker::PhantomData;
use std::path::Path;

/// Iterate over a Db's key, value pairs in insert order.
/// This iterator is "raw", it does not use any indexes just the data file.
pub struct DbRawIter<K, V, const KSIZE: u16>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
{
    _key: PhantomData<K>,
    _val: PhantomData<V>,
    file: BufReader<File>,
    buffer: Vec<u8>,
}

impl<K, V, const KSIZE: u16> DbRawIter<K, V, KSIZE>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
{
    /// Open the data file in dir with base_name (note do not include the .dat- that is appended).
    /// Produces an iterator over all the (key, values).  Does not use the index at all and records
    /// are returned in insert order.
    pub fn open<P: AsRef<Path>>(dir: P, base_name: P) -> Result<Self, LoadHeaderError> {
        let data_name = dir
            .as_ref()
            .join(base_name.as_ref())
            .join("db")
            .with_extension("dat");
        let mut data_file = OpenOptions::new()
            .read(true)
            .write(false)
            .create(false)
            .open(data_name)?;

        let _header = DataHeader::load_header(&mut data_file)?;
        let file = BufReader::new(data_file);
        Ok(Self {
            _key: PhantomData,
            _val: PhantomData,
            file,
            buffer: Vec::new(),
        })
    }

    /// Same as open but created from an existing File.
    pub fn with_file(dat_file: File) -> Result<Self, LoadHeaderError> {
        let mut dat_file = dat_file;
        let _header = DataHeader::load_header(&mut dat_file)?;
        let file = BufReader::new(dat_file);
        Ok(Self {
            _key: PhantomData,
            _val: PhantomData,
            file,
            buffer: Vec::new(),
        })
    }

    /// Return the current position of the data file.
    pub fn position(&mut self) -> io::Result<u64> {
        self.file.stream_position()
    }

    /// Read the next record or return an error if an overflow bucket.
    /// This expects the file cursor to be positioned at the records first byte.
    fn read_record_file<R: Read + Seek>(
        file: &mut R,
        buffer: &mut Vec<u8>,
    ) -> Result<(K, V), FetchError> {
        let mut crc32_hasher = crc32fast::Hasher::new();
        let key_size = if K::is_variable_key_size() {
            let mut key_size = [0_u8; 2];
            if let Err(err) = file.read_exact(&mut key_size) {
                // An EOF here should be caused by no more records although it is possible there
                // was a bit of garbage at the end of the file, not worrying about that now (maybe ever).
                if let io::ErrorKind::UnexpectedEof = err.kind() {
                    return Err(FetchError::NotFound);
                }
                return Err(FetchError::IO(err));
            }
            crc32_hasher.update(&key_size);
            u16::from_le_bytes(key_size)
        } else {
            K::KEY_SIZE
        } as usize;

        let mut val_size_buf = [0_u8; 4];
        if K::is_variable_key_size() {
            file.read_exact(&mut val_size_buf)?;
        } else if let Err(err) = file.read_exact(&mut val_size_buf) {
            // An EOF here should be caused by no more records although it is possible there
            // was a bit of garbage at the end of the file, not worrying about that now (maybe ever).
            if let io::ErrorKind::UnexpectedEof = err.kind() {
                return Err(FetchError::NotFound);
            }
            return Err(FetchError::IO(err));
        }
        crc32_hasher.update(&val_size_buf);
        let val_size = u32::from_le_bytes(val_size_buf);
        buffer.resize(key_size, 0);
        file.read_exact(buffer)?;
        crc32_hasher.update(buffer);
        let key = K::deserialize(&buffer[..]).map_err(FetchError::DeserializeKey)?;
        buffer.resize(val_size as usize, 0);
        file.read_exact(buffer)?;
        crc32_hasher.update(buffer);
        let calc_crc32 = crc32_hasher.finalize();
        let val = V::deserialize(&buffer[..]).map_err(FetchError::DeserializeValue)?;
        let mut buf_u32 = [0_u8; 4];
        file.read_exact(&mut buf_u32)?;
        let read_crc32 = u32::from_le_bytes(buf_u32);
        if calc_crc32 != read_crc32 {
            return Err(FetchError::CrcFailed);
        }
        Ok((key, val))
    }
}

impl<K, V, const KSIZE: u16> Iterator for DbRawIter<K, V, KSIZE>
where
    K: DbKey<KSIZE> + DbBytes<K>,
    V: Debug + DbBytes<V>,
{
    type Item = Result<(K, V), FetchError>;

    fn next(&mut self) -> Option<Self::Item> {
        match Self::read_record_file(&mut self.file, &mut self.buffer) {
            Ok(kv) => Some(Ok(kv)),
            Err(err) => match err {
                FetchError::NotFound => None,
                _ => Some(Err(err)),
            },
        }
    }
}
