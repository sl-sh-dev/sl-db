#![deny(missing_docs)]

//! Provide a tokio async wrapper around DbCore.  This is tokio specific but should be easily
//! adaptable to other runtimes.  This version shards the DB in an attempt to reduce contention, etc.

use crate::write_thread::{write_thread, InsertCommand};
use crate::CommitError;
use dashmap::DashMap;
use sldb_core::db::DbCore;
use sldb_core::db_bytes::DbBytes;
use sldb_core::db_config::DbConfig;
use sldb_core::db_files::{DbFiles, RenameError};
use sldb_core::db_key::DbKey;
use sldb_core::error::{FetchError, LoadHeaderError, OpenError, ReadKeyError};
use sldb_core::fxhasher::FxHasher;
use std::fmt::Debug;
use std::hash::{BuildHasher, BuildHasherDefault, Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;
use std::{fs, io};
use tokio::sync::*;

/// This provides an async wrapper around DbCore.
/// It will shard the DB into multiple DbCore instances to increase performance.  It is also
/// multi-thread safe and takes immutable self references for ease of use.
/// The fetch operation uses synchronous reads once it grabs the shard lock.  This is because the
/// current methods to make this async (thread pool, channels, etc) are extremely slow compared to
/// just making the blocking read.  Note that the read will not wait for data and so it should not
/// be a big deal however be aware that fetch will make a standard sync read call at some point to
/// maintain performance.  Inserts and commits do not have this issue, each shard has a write thread
/// these are offloaded onto- this is fine for writes since the write can return fast after
/// offloading the work.
pub struct ShardedDb<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    db_shards: Vec<Arc<Mutex<DbCore<K, V, KSIZE, S>>>>,
    //write_cache: Arc<DashMap<K, V, S>>,
    write_caches: Vec<Arc<DashMap<K, V, S>>>,
    insert_txs: Vec<mpsc::Sender<InsertCommand<K, KSIZE>>>,
    insert_threads: Option<Vec<std::thread::JoinHandle<()>>>,
    // There should be little to no contention for this and using async can be inconvenient as
    // using blocking_lock() if a tokio runtime is in use will panic.
    config: parking_lot::Mutex<DbConfig>,
    hasher: S,
    shard_bits: usize,
    shards: usize,
}

impl<K, V, const KSIZE: u16, S> Drop for ShardedDb<K, V, KSIZE, S>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    fn drop(&mut self) {
        for insert_tx in &self.insert_txs {
            let _ = insert_tx.try_send(InsertCommand::Done);
        }
        if let Some(mut db_threads) = self.insert_threads.take() {
            for db_thread in db_threads.drain(..) {
                let _ = db_thread.join();
            }
        }
    }
}

impl<K, V, const KSIZE: u16, S> ShardedDb<K, V, KSIZE, S>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    /// Open a new or reopen an existing database.
    /// The config must not contain explicit file paths, this will return an InvalidFiles error.
    /// This restriction is because it shards the DB into multiple sub-DBs.
    pub fn open(config: DbConfig, shard_bits: usize) -> Result<Self, OpenError> {
        let shards: usize = 1 << shard_bits;
        let config = config.no_auto_flush(); // Wrapper needs to control commit.

        // Make sure we were not given explicit file paths, that won't work with sharding.
        // Also create a template DbFiles for each shard DB (must set a valid name).
        let shard_files = if let Some(dir) = config.files().dir() {
            if let Some(index_dir) = config.files().index_dir() {
                DbFiles::with_data_index(
                    dir.join(config.files().name()),
                    index_dir.join(config.files().name()),
                    "".to_string(),
                )
            } else {
                DbFiles::with_data(dir.join(config.files().name()), "".to_string())
            }
        } else {
            return Err(OpenError::InvalidFiles);
        };
        let mut db_shards = Vec::with_capacity(shards);
        let mut write_caches = Vec::with_capacity(shards);
        for i in 0..shards {
            let mut files = shard_files.clone();
            files.set_name(format!("shard_{i}"));
            let db_config = config.clone().set_files(files);
            let db = DbCore::open(db_config)?;
            db_shards.push(Arc::new(Mutex::new(db)));
            write_caches.push(Arc::new(DashMap::with_hasher(S::default())));
        }
        let mut insert_txs = Vec::with_capacity(shards);
        let mut db_threads = Vec::with_capacity(shards);
        for (db, write_cache) in db_shards.iter().zip(write_caches.iter()) {
            let (insert_tx, insert_rx) = mpsc::channel(10_000);
            let write_cache_clone = write_cache.clone();
            let db_clone = db.clone();
            let db_thread =
                std::thread::spawn(move || write_thread(db_clone, write_cache_clone, insert_rx));
            db_threads.push(db_thread);
            insert_txs.push(insert_tx);
        }
        Ok(Self {
            db_shards,
            write_caches,
            insert_txs,
            insert_threads: Some(db_threads),
            config: parking_lot::Mutex::new(config),
            hasher: S::default(),
            shard_bits,
            shards,
        })
    }

    /// Root directory for this DB.
    /// The actual DB shards will be stored in dir/name.
    pub fn dir(&self) -> PathBuf {
        self.config
            .lock()
            .files()
            .dir()
            .expect("ShardedDb requires a directory")
            .to_path_buf()
    }

    /// Name of this DB.
    pub fn name(&self) -> String {
        self.config.lock().files().name().to_string()
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub async fn fetch(&self, key: K) -> Result<V, FetchError> {
        let shard = self.shard(&key);
        if let Some(val) = self.write_caches[shard].get(&key) {
            Ok(val.clone())
        } else {
            let db = &self.db_shards[shard];
            db.lock().await.fetch(&key)
            //let db = self.db.clone();
            //tokio::task::spawn_blocking(move || db.blocking_lock().fetch(&key)).await.unwrap()//.map_err(|_|FetchError::NotFound)
        }
    }

    /// True if the database contains key.
    pub async fn contains_key(&self, key: K) -> Result<bool, ReadKeyError> {
        let shard = self.shard(&key);
        if self.write_caches[shard].contains_key(&key) {
            Ok(true)
        } else {
            let db = &self.db_shards[shard];
            db.lock().await.contains_key(&key)
        }
    }

    /// Insert a new key/value pair in Db.
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    pub async fn insert(&self, key: K, value: V) {
        let shard = self.shard(&key);
        if !self.write_caches[shard].contains_key(&key) {
            self.write_caches[shard].insert(key.clone(), value);
            // An error here indicates the receiver in the insert thread was closed/dropped.
            let _ = self.insert_txs[shard]
                .send(InsertCommand::Insert(key))
                .await;
        }
    }

    /// Return the number of records in Db.
    /// It is possible for len() to lag a bit if records are in flight to the DB
    /// (cached but not written).
    pub async fn len(&self) -> usize {
        let mut length = 0;
        for db in &self.db_shards {
            length += db.lock().await.len();
        }
        length
    }

    /// Is the DB empty?
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    pub async fn commit(&self) -> Result<(), CommitError> {
        let mut result = Ok(());
        let mut rxs = Vec::with_capacity(self.shards);
        for insert_tx in &self.insert_txs {
            let (tx, rx) = tokio::sync::oneshot::channel();
            // If we can't send then do something drastic (?).
            if insert_tx.send(InsertCommand::Commit(tx)).await.is_ok() {
                rxs.push(rx);
            } else {
                // An error here means the receiver in the insert thread was dropped/closed.
                result = Err(CommitError::SendChannelClosed);
            }
        }
        for rx in rxs.drain(..) {
            match rx.await {
                Ok(r) => match r {
                    Ok(()) => {}
                    Err(err) => result = Err(err),
                },
                Err(_err) => result = Err(CommitError::ReceiveFailed),
            }
        }
        result
    }

    /// Schedule a commit but don't wait for it to complete.
    pub async fn commit_bg(&self) {
        for insert_tx in &self.insert_txs {
            // An error here indicates the receiver in the insert thread was closed/dropped.
            let _ = insert_tx.send(InsertCommand::CommitBG).await;
        }
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    pub async fn raw_iter(
        &self,
    ) -> Result<impl Iterator<Item = Result<(K, V), FetchError>>, LoadHeaderError> {
        let mut iters = Vec::with_capacity(self.shards);
        for i in 0..self.shards {
            iters.push(self.db_shards[i].lock().await.raw_iter()?);
        }
        Ok(iters.into_iter().flatten())
    }

    /// Close the DB and delete the files.
    pub fn destroy(self) -> io::Result<()> {
        let config = self.config.lock();
        let dir = config
            .files()
            .dir()
            .expect("ShardedDb currently does not support explicit file paths!")
            .join(config.files().name());
        drop(config);
        drop(self);
        std::fs::remove_dir_all(&dir)
    }

    /// Rename the database to new_name.
    pub fn rename<Q: Into<String>>(&self, new_name: Q) -> Result<(), RenameError> {
        let mut config = self.config.lock();
        self.rename_inner(&mut config, new_name)
    }

    /// Private implementation of rename.
    fn rename_inner<Q: Into<String>>(
        &self,
        config: &mut DbConfig,
        new_name: Q,
    ) -> Result<(), RenameError> {
        let new_name: String = new_name.into();
        if config.files().name() == new_name {
            return Ok(());
        }
        if let Some(dir) = config.files().dir() {
            let old_dir = dir.join(config.files().name());
            let new_dir = dir.join(&new_name);
            // If the new dir exists and is empty then remove it.
            // If dir does not exist or is not empty this should do nothing.
            let _ = fs::remove_dir(&new_dir);
            if new_dir.exists() {
                Err(RenameError::FilesExist)
            } else {
                let res = fs::rename(&old_dir, &new_dir);
                if res.is_ok() {
                    config.set_name(new_name);
                }
                res.map_err(RenameError::RenameIO)
            }
        } else {
            Err(RenameError::CanNotRename)
        }
    }

    /// Return the shard for a key.
    fn shard(&self, key: &K) -> usize {
        if self.shard_bits == 0 {
            0
        } else {
            let mut hasher = self.hasher.build_hasher();
            key.hash(&mut hasher);
            // Use the top bits for shard that should not overlap with buckets.
            hasher.finish() as usize >> (64 - self.shard_bits)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time;
    //use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_50k_tok_shard() {
        let shard_bits = 2;
        let config = DbConfig::with_data_path("shard_test", "xxx50k", 1)
            .no_auto_flush()
            .create()
            //.set_bucket_elements(100)
            //.set_load_factor(0.6)
            .truncate(); //.no_write_cache();
        let db: Arc<ShardedDb<u64, String, 8>> =
            Arc::new(ShardedDb::open(config, shard_bits).unwrap());
        assert!(db.is_empty().await);
        assert!(!db.contains_key(0).await.unwrap());
        assert!(!db.contains_key(10).await.unwrap());
        assert!(!db.contains_key(35_000).await.unwrap());
        assert!(!db.contains_key(49_000).await.unwrap());
        assert!(!db.contains_key(50_000).await.unwrap());
        let max = 1_000_000;

        let start = time::Instant::now();
        for i in 0_u64..max {
            db.insert(i, format!("Value {}", i)).await;
            if i % 100_000 == 0 {
                db.commit_bg().await; //.unwrap();
                                      //db.commit().await.unwrap();
            }
        }
        println!(
            "XXXX TOK SHARD insert({}) time {}",
            max,
            start.elapsed().as_secs_f64()
        );
        //assert_eq!(db.len(), 50_000);
        assert!(db.contains_key(0).await.unwrap());
        assert!(db.contains_key(10).await.unwrap());
        assert!(db.contains_key(35_000).await.unwrap());
        assert!(db.contains_key(49_000).await.unwrap());
        assert!(!db.contains_key(max).await.unwrap());

        let start = time::Instant::now();
        for i in 0..max {
            let item = db.fetch(i as u64).await;
            assert!(item.is_ok(), "Failed on item {}, {:?}", i, item);
            assert_eq!(&item.unwrap(), &format!("Value {}", i));
        }
        println!(
            "XXXX TOK SHARD fetch ({}) (pre commit) time {}",
            max,
            start.elapsed().as_secs_f64()
        );

        let start = time::Instant::now();
        db.commit().await.unwrap();
        println!(
            "XXXX TOK SHARD commit time {}",
            start.elapsed().as_secs_f64()
        );
        assert_eq!(db.len().await, max as usize);
        assert!(!db.is_empty().await);
        let start = time::Instant::now();
        let vals = db.raw_iter().await.unwrap().map(|r| r.unwrap().1);
        assert_eq!(vals.count(), max as usize);
        //for (i, v) in vals.iter().enumerate() {
        //    assert_eq!(v, &format!("Value {}", i));
        //}
        println!("XXXX TOK SHARD iter time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        //let mut fetch_set = JoinSet::new();
        for i in 0..max {
            let item = db.fetch(i as u64).await;
            assert!(item.is_ok(), "Failed on item {}, {:?}", i, item);
            assert_eq!(&item.unwrap(), &format!("Value {}", i));
            //let db_clone = db.clone();
            //fetch_set.spawn(async move {
            //    let item = db_clone.fetch(i as u64).await;
            //    assert!(item.is_ok(), "Failed on item {}, {:?}", i, item);
            //    assert_eq!(&item.unwrap(), &format!("Value {}", i));
            //});
        }
        //while fetch_set.join_next().await.is_some() {}
        println!(
            "XXXX TOK SHARD fetch ({}) time {}",
            max,
            start.elapsed().as_secs_f64()
        );
    }

    #[tokio::test]
    async fn test_shard_rename() {
        let max = 1_000;
        let val = vec![0_u8; 512];
        let shard_bits = 2;
        {
            let config = DbConfig::with_data_path("shard_tests", "xxx_rename1", 1)
                .no_auto_flush()
                .create()
                .truncate();
            let db: ShardedDb<u64, Vec<u8>, 8> = ShardedDb::open(config, shard_bits).unwrap();
            assert_eq!(&db.name(), "xxx_rename1");
            for i in 0_u64..max {
                db.insert(i, val.clone()).await;
            }

            for i in 0..max {
                let item = db.fetch(i as u64).await;
                assert!(item.is_ok(), "Failed on item {}, {:?}", i, item);
                assert_eq!(&item.unwrap(), &val);
            }

            db.commit().await.unwrap();
            assert_eq!(db.len().await, max as usize);
            for i in 0..max {
                let item = db.fetch(i as u64).await;
                assert!(item.is_ok(), "Failed on item {}", i);
                assert_eq!(&item.unwrap(), &val);
            }
            db.rename("xxx_rename2").unwrap();
            assert_eq!(&db.name(), "xxx_rename2");
        }
        {
            let config = DbConfig::with_data_path("shard_tests", "xxx_rename3", 1);
            let _db: ShardedDb<u64, Vec<u8>, 8> =
                ShardedDb::open(config.create(), shard_bits).unwrap();
        }
        let config = DbConfig::with_data_path("shard_tests", "xxx_rename2", 1);
        let db = ShardedDb::<u64, Vec<u8>, 8>::open(config.create(), shard_bits).unwrap();
        assert_eq!(db.len().await, max as usize);
        for i in 0..max {
            let item = db.fetch(i as u64).await;
            assert!(item.is_ok(), "Failed on item {}/{:?}", i, item);
            assert_eq!(&item.unwrap(), &val);
        }
        assert!(db.rename("xxx_rename3").is_err());
        assert_eq!(&db.name(), "xxx_rename2");
        db.destroy().unwrap();
        {
            // Clean up db files.
            let config = DbConfig::with_data_path("shard_tests", "xxx_rename3", 1);
            let db = ShardedDb::<u64, Vec<u8>, 8>::open(config.create(), shard_bits).unwrap();
            assert_eq!(&db.name(), "xxx_rename3");
            db.destroy().unwrap();
        }
    }
}
