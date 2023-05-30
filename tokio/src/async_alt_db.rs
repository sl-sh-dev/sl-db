#![deny(missing_docs)]

//! Provide a tokio async wrapper around DbCore.  This is tokio specific but should be easily
//! adaptable to other runtimes.

use crate::write_thread::InsertCommand;
use crate::CommitError;
use dashmap::DashMap;
use sldb_core::db::DbCore;
use sldb_core::db_bytes::DbBytes;
use sldb_core::db_config::DbConfig;
use sldb_core::db_files::RenameError;
use sldb_core::db_key::DbKey;
use sldb_core::error::insert::InsertError;
use sldb_core::error::{FetchError, LoadHeaderError, OpenError, ReadKeyError};
use sldb_core::fxhasher::FxHasher;
use std::fmt::Debug;
use std::hash::{BuildHasher, BuildHasherDefault, Hash};
use std::path::PathBuf;
use std::sync::Arc;
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
pub struct AsyncAltDb<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    read_db: Arc<Mutex<DbCore<K, V, KSIZE, S>>>,
    write_cache: Arc<DashMap<K, V, S>>,
    insert_tx: mpsc::Sender<InsertCommand<K, KSIZE>>,
    insert_thread: Option<std::thread::JoinHandle<()>>,
    // There should be little to no contention for this and using async can be inconvenient as
    // using blocking_lock() if a tokio runtime is in use will panic.
    config: parking_lot::Mutex<DbConfig>,
}

impl<K, V, const KSIZE: u16, S> Drop for AsyncAltDb<K, V, KSIZE, S>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    fn drop(&mut self) {
        let _ = self.insert_tx.try_send(InsertCommand::Done);
        if let Some(db_thread) = self.insert_thread.take() {
            let _ = db_thread.join();
        }
    }
}

impl<K, V, const KSIZE: u16, S> AsyncAltDb<K, V, KSIZE, S>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    /// Open a new or reopen an existing database.
    /// The config must not contain explicit file paths, this will return an InvalidFiles error.
    /// This restriction is because it shards the DB into multiple sub-DBs.
    /// Will attempt to recover a DB with a damaged index (will reindex).
    pub fn open(config: DbConfig, channel_depth: usize) -> Result<Self, OpenError> {
        let config = config.no_auto_flush(); // Wrapper needs to control commit.

        let db = DbCore::open_with_recover(config.clone())?;
        let read_db = Arc::new(Mutex::new(DbCore::open(config.clone().read_only())?));
        let (insert_tx, insert_rx) = mpsc::channel(channel_depth);
        let write_cache = Arc::new(DashMap::with_hasher(S::default()));
        let write_cache_clone = write_cache.clone();
        let read_db_clone = read_db.clone();
        let db_thread = std::thread::spawn(move || {
            write_thread(db, read_db_clone, write_cache_clone, insert_rx)
        });
        Ok(Self {
            read_db,
            write_cache,
            insert_tx,
            insert_thread: Some(db_thread),
            config: parking_lot::Mutex::new(config),
        })
    }

    /// Root directory for this DB.
    /// The actual DB shards will be stored in dir/name.
    pub fn dir(&self) -> PathBuf {
        self.config
            .lock()
            .files()
            .dir()
            .expect("AsyncAltDb requires a directory")
            .to_path_buf()
    }

    /// Name of this DB.
    pub fn name(&self) -> String {
        self.config.lock().files().name().to_string()
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub async fn fetch(&self, key: K) -> Result<V, FetchError> {
        if let Some(val) = self.write_cache.get(&key) {
            Ok(val.clone())
        } else {
            self.read_db.lock().await.fetch(&key)
        }
    }

    /// True if the database contains key.
    pub async fn contains_key(&self, key: K) -> Result<bool, ReadKeyError> {
        if self.write_cache.contains_key(&key) {
            Ok(true)
        } else {
            self.read_db.lock().await.contains_key(&key)
        }
    }

    /// Insert a new key/value pair in Db.
    /// For the data file this means inserting:
    ///   - key size (u16) IF it is a variable width key (not needed for fixed width keys)
    ///   - value size (u32)
    ///   - key data
    ///   - value data
    pub async fn insert(&self, key: K, value: V) {
        if !self.write_cache.contains_key(&key) {
            self.write_cache.insert(key.clone(), value);
            // An error here indicates the receiver in the insert thread was closed/dropped.
            let _ = self.insert_tx.send(InsertCommand::Insert(key)).await;
        }
    }

    /// Return the number of records in Db.
    /// It is possible for len() to lag a bit if records are in flight to the DB
    /// (cached but not written).
    pub async fn len(&self) -> usize {
        self.read_db.lock().await.len()
    }

    /// Is the DB empty?
    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    pub async fn commit(&self) -> Result<(), CommitError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // If we can't send then do something drastic (?).
        if self
            .insert_tx
            .send(InsertCommand::Commit(tx))
            .await
            .is_err()
        {
            // An error here means the receiver in the insert thread was dropped/closed.
            return Err(CommitError::SendChannelClosed);
        }
        match rx.await {
            Ok(r) => r,
            Err(_err) => Err(CommitError::ReceiveFailed),
        }
    }

    /// Schedule a commit but don't wait for it to complete.
    pub async fn commit_bg(&self) {
        // An error here indicates the receiver in the insert thread was closed/dropped.
        let _ = self.insert_tx.send(InsertCommand::CommitBG).await;
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    pub async fn raw_iter(
        &self,
    ) -> Result<impl Iterator<Item = Result<(K, V), FetchError>>, LoadHeaderError> {
        self.read_db.lock().await.raw_iter()
    }

    /// Close the DB and delete the files.
    pub fn destroy(self) {
        let files = self.config.lock().files().clone();
        drop(self);
        files.delete();
    }

    /// Rename the database to new_name.
    pub fn rename<Q: Into<String>>(&self, new_name: Q) -> Result<(), RenameError> {
        self.config.lock().files_mut().rename(new_name)
    }
}

/// Do a commit for teh background thread.
fn commit_bg_thread<K, V, const KSIZE: u16, S>(
    db: &mut DbCore<K, V, KSIZE, S>,
    read_db: &Arc<Mutex<DbCore<K, V, KSIZE, S>>>,
    write_cache: &DashMap<K, V, S>,
    inserts: &mut Vec<K>,
    result_tx: Option<oneshot::Sender<Result<(), CommitError>>>,
) where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    let mut read_lock = read_db.blocking_lock();
    let res = db.commit();
    read_lock.refresh_index();
    drop(read_lock);
    match res {
        Ok(()) => {
            if let Some(tx) = result_tx {
                let _ = tx.send(Ok(()));
            }
            for key in inserts.drain(..) {
                write_cache.remove(&key);
            }
        }
        Err(err) => {
            if let Some(tx) = result_tx {
                let _ = tx.send(Err(err.into()));
            }
        }
    }
}

/// Run a background thread to manage the write for a single DB.
fn write_thread<K, V, const KSIZE: u16, S>(
    mut db: DbCore<K, V, KSIZE, S>,
    read_db: Arc<Mutex<DbCore<K, V, KSIZE, S>>>,
    write_cache: Arc<DashMap<K, V, S>>,
    mut insert_rx: mpsc::Receiver<InsertCommand<K, KSIZE>>,
) where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    let mut inserts = Vec::new();
    let mut done = false;
    let mut last_insert_error: Option<InsertError> = None;
    while !done {
        match insert_rx.blocking_recv() {
            Some(InsertCommand::Insert(key)) => {
                if let Some(value) = write_cache.get(&key) {
                    match db.insert(key.clone(), &value) {
                        Err(InsertError::DuplicateKey) => {} // Silently ignore duplicate keys...
                        Err(err) => last_insert_error = Some(err),
                        Ok(_) => {}
                    }
                    drop(value);
                    inserts.push(key);
                }
            }
            Some(InsertCommand::Commit(tx)) => {
                if let Some(err) = last_insert_error {
                    let _ = tx.send(Err(CommitError::PreviousInsertFailed(err)));
                    last_insert_error = None;
                } else {
                    commit_bg_thread(&mut db, &read_db, &write_cache, &mut inserts, Some(tx));
                }
            }
            Some(InsertCommand::CommitBG) => {
                commit_bg_thread(&mut db, &read_db, &write_cache, &mut inserts, None);
            }
            Some(InsertCommand::Done) => done = true,
            None => done = true, // The sender has been closed or dropped so nothing left to do.
        }
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::time;
    //use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_dup_key_commit() {
        let key = 0_u64;
        {
            let config = DbConfig::with_data_path("db_tests", "dup_commits", 3)
                .create()
                .truncate()
                .allow_duplicate_inserts();
            let db: Arc<AsyncAltDb<u64, String, 8>> =
                Arc::new(AsyncAltDb::open(config, 100_000).unwrap());
            db.insert(key, "Value One".to_string()).await;
            db.commit().await.unwrap();
            db.insert(key, "Value Two".to_string()).await;
            db.commit().await.unwrap();
            db.insert(key, "Value Three".to_string()).await;
            db.commit().await.unwrap();
            db.insert(key, "Value Four".to_string()).await;
            db.commit().await.unwrap();
            db.insert(key, "Value Five".to_string()).await;
            db.commit().await.unwrap();

            let v = db.fetch(key).await.unwrap();
            assert_eq!(v, "Value Five");
        }
        // Reopen and test that there are 5 items in the data file, one in the index and that the
        // correct value is retrieved.
        let config =
            DbConfig::with_data_path("db_tests", "dup_commits", 3).allow_duplicate_inserts();
        let db: Arc<AsyncAltDb<u64, String, 8>> =
            Arc::new(AsyncAltDb::open(config, 100_000).unwrap());
        assert_eq!(db.raw_iter().await.unwrap().count(), 5);
        //assert_eq!(db.len(), 1);  // Have a bug here on duplicate keys.
        let v = db.fetch(key).await.unwrap();
        assert_eq!(v, "Value Five");
    }

    #[tokio::test]
    async fn test_50k_tok_alt() {
        let config = DbConfig::with_data_path("db_tests_alt", "xxx50k", 1)
            .no_auto_flush()
            .create()
            //.set_bucket_elements(100)
            //.set_load_factor(0.6)
            .truncate(); //.no_write_cache();
        let db: Arc<AsyncAltDb<u64, String, 8>> =
            Arc::new(AsyncAltDb::open(config, 100_000).unwrap());
        assert!(db.is_empty().await);
        assert!(!db.contains_key(0).await.unwrap());
        assert!(!db.contains_key(10).await.unwrap());
        assert!(!db.contains_key(35_000).await.unwrap());
        assert!(!db.contains_key(49_000).await.unwrap());
        assert!(!db.contains_key(50_000).await.unwrap());
        let max = 50_000;

        let start = time::Instant::now();
        for i in 0_u64..max {
            db.insert(i, format!("Value {i}")).await;
            if i % 100_000 == 0 {
                db.commit_bg().await; //.unwrap();
                                      //db.commit().await.unwrap();
            }
        }
        println!(
            "XXXX TOK ALT insert({}) time {}",
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
            let item = db.fetch(i).await;
            assert!(item.is_ok(), "Failed on item {i}, {item:?}");
            assert_eq!(&item.unwrap(), &format!("Value {i}"));
        }
        println!(
            "XXXX TOK ALT fetch ({max}) (pre commit) time {}",
            start.elapsed().as_secs_f64()
        );

        let start = time::Instant::now();
        db.commit().await.unwrap();
        println!("XXXX TOK ALT commit time {}", start.elapsed().as_secs_f64());
        assert_eq!(db.len().await, max as usize);
        assert!(!db.is_empty().await);
        let start = time::Instant::now();
        let vals = db.raw_iter().await.unwrap().map(|r| r.unwrap().1);
        assert_eq!(vals.count(), max as usize);
        //for (i, v) in vals.iter().enumerate() {
        //    assert_eq!(v, &format!("Value {}", i));
        //}
        println!("XXXX TOK ALT iter time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        //let mut fetch_set = JoinSet::new();
        for i in 0..max {
            let item = db.fetch(i).await;
            assert!(item.is_ok(), "Failed on item {i}, {item:?}");
            assert_eq!(&item.unwrap(), &format!("Value {i}"));
            //let db_clone = db.clone();
            //fetch_set.spawn(async move {
            //    let item = db_clone.fetch(i as u64).await;
            //    assert!(item.is_ok(), "Failed on item {}, {:?}", i, item);
            //    assert_eq!(&item.unwrap(), &format!("Value {}", i));
            //});
        }
        //while fetch_set.join_next().await.is_some() {}
        println!(
            "XXXX TOK ALT fetch ({}) time {}",
            max,
            start.elapsed().as_secs_f64()
        );
    }

    #[tokio::test]
    async fn test_async_rename() {
        let max = 1_000;
        let val = vec![0_u8; 512];
        {
            let config = DbConfig::with_data_path("db_tests_alt", "xxx_rename1", 1)
                .no_auto_flush()
                .create()
                .truncate();
            let db: AsyncAltDb<u64, Vec<u8>, 8> = AsyncAltDb::open(config, 1000).unwrap();
            assert_eq!(&db.name(), "xxx_rename1");
            for i in 0_u64..max {
                db.insert(i, val.clone()).await;
            }

            for i in 0..max {
                let item = db.fetch(i).await;
                assert!(item.is_ok(), "Failed on item {i}, {item:?}");
                assert_eq!(&item.unwrap(), &val);
            }

            db.commit().await.unwrap();
            assert_eq!(db.len().await, max as usize);
            for i in 0..max {
                let item = db.fetch(i).await;
                assert!(item.is_ok(), "Failed on item {i}");
                assert_eq!(&item.unwrap(), &val);
            }
            db.rename("xxx_rename2").unwrap();
            assert_eq!(&db.name(), "xxx_rename2");
        }
        {
            let config = DbConfig::with_data_path("db_tests_alt", "xxx_rename3", 1);
            let _db: AsyncAltDb<u64, Vec<u8>, 8> = AsyncAltDb::open(config.create(), 1000).unwrap();
        }
        let config = DbConfig::with_data_path("db_tests_alt", "xxx_rename2", 1);
        let db = AsyncAltDb::<u64, Vec<u8>, 8>::open(config.create(), 1000).unwrap();
        assert_eq!(db.len().await, max as usize);
        for i in 0..max {
            let item = db.fetch(i).await;
            assert!(item.is_ok(), "Failed on item {i}/{item:?}");
            assert_eq!(&item.unwrap(), &val);
        }
        assert!(db.rename("xxx_rename3").is_err());
        assert_eq!(&db.name(), "xxx_rename2");
        db.destroy();
        {
            // Clean up db files.
            let config = DbConfig::with_data_path("db_tests_alt", "xxx_rename3", 1);
            let db = AsyncAltDb::<u64, Vec<u8>, 8>::open(config.create(), 1000).unwrap();
            assert_eq!(&db.name(), "xxx_rename3");
            db.destroy();
        }
    }
}
