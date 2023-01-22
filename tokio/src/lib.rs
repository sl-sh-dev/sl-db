use dashmap::DashMap;
use sldb_core::db::{DbBytes, DbCore, DbKey};
use sldb_core::db_config::{DbConfig, DbFiles};
use sldb_core::db_raw_iter::DbRawIter;
use sldb_core::error::flush::FlushError;
use sldb_core::error::insert::InsertError;
use sldb_core::error::{FetchError, LoadHeaderError, OpenError, ReadKeyError};
use sldb_core::fxhasher::FxHasher;
use std::error::Error;
use std::fmt::Debug;
use std::hash::{BuildHasher, BuildHasherDefault, Hash};
use std::sync::Arc;
use std::{fmt, io};
use tokio::sync::*;

pub struct AsyncDb<K, V, const KSIZE: u16, S = BuildHasherDefault<FxHasher>>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    db_read: Arc<tokio::sync::Mutex<DbCore<K, V, KSIZE, S>>>,
    write_cache: Arc<DashMap<K, V>>,
    insert_tx: mpsc::Sender<InsertCommand<K, KSIZE>>,
    insert_thread: Option<std::thread::JoinHandle<()>>,
    config: DbConfig,
}

impl<K, V, const KSIZE: u16, S> Drop for AsyncDb<K, V, KSIZE, S>
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

impl<K, V, const KSIZE: u16, S> AsyncDb<K, V, KSIZE, S>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    /// Open a new or reopen an existing database.
    pub fn open(config: DbConfig) -> Result<Self, OpenError> {
        let auto_commit = config.auto_flush();
        let config = config.no_auto_flush(); // Wrapper needs to control commit.
        let (insert_tx, insert_rx) = mpsc::channel(100_000);
        // Open the write db (passed to the insert thread).
        let db = DbCore::open(config.clone())?;
        // Open a read only DB for fetching, reduces locks.
        let db_read = Arc::new(tokio::sync::Mutex::new(DbCore::open(
            config.clone().read_only(),
        )?));
        let write_cache: Arc<DashMap<K, V>> = Arc::new(DashMap::new());
        let write_cache_clone = write_cache.clone();
        let db_read_clone = db_read.clone();
        let db_thread = std::thread::spawn(move || {
            Self::insert_thread(db, db_read_clone, write_cache_clone, insert_rx, auto_commit)
        });
        Ok(Self {
            db_read,
            write_cache,
            insert_tx,
            insert_thread: Some(db_thread),
            config,
        })
    }

    /// Returns a reference to the file names for this DB.
    pub fn files(&self) -> &DbFiles {
        self.config.files()
    }

    /// Fetch the value stored at key.  Will return an error if not found.
    pub async fn fetch(&self, key: K) -> Result<V, FetchError> {
        if let Some(val) = self.write_cache.get(&key) {
            Ok(val.clone())
        } else {
            self.db_read.lock().await.fetch(&key)
            //let db_read = self.db_read.clone();
            //tokio::task::spawn_blocking(move || db_read.blocking_lock().fetch(&key)).await.unwrap()//.map_err(|_|FetchError::NotFound)
        }
    }

    /// True if the database contains key.
    pub async fn contains_key(&self, key: K) -> Result<bool, ReadKeyError> {
        if self.write_cache.contains_key(&key) {
            Ok(true)
        } else {
            self.db_read.lock().await.contains_key(&key)
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
    /*
    /// Return the number of records in Db.
    pub fn len(&self) -> usize {
        self.inner.borrow().len()
    }

    /// Is the DB empty?
    pub fn is_empty(&self) -> bool {
        self.inner.borrow().is_empty()
    }
    */

    /// Flush any caches to disk and sync the data and index file.
    /// All data should be safely on disk if this call succeeds.
    pub async fn commit(&self) -> Result<(), CommitError> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        // If we can't send then do something drastic (?).
        if self.insert_tx.send(InsertCommand::Commit(tx)).await.is_ok() {
            match rx.await {
                Ok(r) => r,
                Err(_err) => Err(CommitError::ReceiveFailed),
            }
        } else {
            // An error here means the receiver in the insert thread was dropped/closed.
            Err(CommitError::SendChannelClosed)
        }
    }

    /// Schedule a commit but don't wait for it to complete.
    pub async fn commit_bg(&self) {
        // An error here indicates the receiver in the insert thread was closed/dropped.
        let _ = self.insert_tx.send(InsertCommand::CommitBG).await;
    }

    /// Return an iterator over the key values in insertion order.
    /// Note this iterator only uses the data file not the indexes.
    pub async fn raw_iter(&self) -> Result<DbRawIter<K, V, KSIZE>, LoadHeaderError> {
        self.db_read.lock().await.raw_iter()
    }

    fn commit_bg_thread(
        db: &mut DbCore<K, V, KSIZE, S>,
        db_read: &Arc<tokio::sync::Mutex<DbCore<K, V, KSIZE, S>>>,
        write_cache: &DashMap<K, V>,
        inserts: &mut Vec<K>,
    ) -> Result<(), CommitError> {
        let mut db_read = db_read.blocking_lock();
        let _ = db.flush();
        db_read.refresh_index();
        drop(db_read);
        let res = db.commit();
        match res {
            Ok(()) => {
                for key in inserts.drain(..) {
                    write_cache.remove(&key);
                }
                Ok(())
            }
            Err(err) => Err(err.into()),
        }
    }

    fn insert_thread(
        mut db: DbCore<K, V, KSIZE, S>,
        db_read: Arc<tokio::sync::Mutex<DbCore<K, V, KSIZE, S>>>,
        write_cache: Arc<DashMap<K, V>>,
        mut insert_rx: mpsc::Receiver<InsertCommand<K, KSIZE>>,
        auto_commit: bool,
    ) {
        let mut inserts = Vec::new();
        let mut done = false;
        let mut last_insert_error: Option<InsertError> = None;
        let mut insert_count: u32 = 0;
        while !done {
            match insert_rx.blocking_recv() {
                Some(InsertCommand::Insert(key)) => {
                    if let Some(value) = write_cache.get(&key) {
                        match db.insert(key.clone(), &value) {
                            Err(InsertError::DuplicateKey) => {} // Silently ignore duplicate keys...
                            Err(err) => last_insert_error = Some(err),
                            Ok(_) => {}
                        }
                        inserts.push(key);
                        if auto_commit && insert_count >= 10_000 {
                            insert_count += 1;
                            let _ = Self::commit_bg_thread(
                                &mut db,
                                &db_read,
                                &write_cache,
                                &mut inserts,
                            );
                        }
                    }
                }
                Some(InsertCommand::Commit(tx)) => {
                    if let Some(err) = last_insert_error {
                        let _ = tx.send(Err(CommitError::PreviousInsertFailed(err)));
                        last_insert_error = None;
                    } else {
                        let mut db_read = db_read.blocking_lock();
                        let _ = db.flush();
                        db_read.refresh_index();
                        drop(db_read);
                        let res = db.commit();
                        match res {
                            Ok(()) => {
                                let _ = tx.send(Ok(()));
                                for key in inserts.drain(..) {
                                    write_cache.remove(&key);
                                }
                            }
                            Err(err) => {
                                let _ = tx.send(Err(err.into()));
                            }
                        }
                    }
                }
                Some(InsertCommand::CommitBG) => {
                    let _ = Self::commit_bg_thread(&mut db, &db_read, &write_cache, &mut inserts);
                }
                Some(InsertCommand::Done) => done = true,
                None => done = true, // The sender has been closed or dropped so nothing left to do.
            }
        }
        println!("XXXX Ending db thread");
    }
}

/// Error from async commit().
#[derive(Debug)]
pub enum CommitError {
    /// An error flushing any cached data.
    Flush(FlushError),
    /// An io error occured syncing the data file.
    DataFileSync(io::Error),
    /// An io error occured syncing the index file.
    IndexFileSync(io::Error),
    /// If a previous insert() failed return the error.
    PreviousInsertFailed(InsertError),
    /// The channel to send commands to the insert db thread has closed (this is fatal for the DB).
    SendChannelClosed,
    /// Failed to receive a response, this indicates a failed DB in an unknown state.
    ReceiveFailed,
    /// DB is opened read-only.
    ReadOnly,
}

impl Error for CommitError {}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::Flush(e) => write!(f, "flush: {}", e),
            Self::DataFileSync(io_err) => write!(f, "data sync: {}", io_err),
            Self::IndexFileSync(io_err) => write!(f, "index sync: {}", io_err),
            Self::PreviousInsertFailed(err) => write!(f, "insert failed: {}", err),
            Self::SendChannelClosed => write!(f, "send channel closed"),
            Self::ReceiveFailed => write!(f, "receive failed"),
            Self::ReadOnly => write!(f, "receive failed"),
        }
    }
}

impl From<sldb_core::error::CommitError> for CommitError {
    fn from(err: sldb_core::error::CommitError) -> Self {
        match err {
            sldb_core::error::CommitError::Flush(e) => Self::Flush(e),
            sldb_core::error::CommitError::DataFileSync(e) => Self::DataFileSync(e),
            sldb_core::error::CommitError::IndexFileSync(e) => Self::IndexFileSync(e),
            sldb_core::error::CommitError::ReadOnly => Self::ReadOnly,
        }
    }
}

enum InsertCommand<K, const KSIZE: u16>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
{
    Insert(K),
    Commit(tokio::sync::oneshot::Sender<Result<(), CommitError>>),
    CommitBG,
    Done,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time;
    //use tokio::task::JoinSet;

    #[tokio::test]
    async fn test_50k() {
        let config = DbConfig::new(".", "xxx50k", 1)
            //.no_auto_flush()
            .create()
            //.set_bucket_elements(100)
            //.set_load_factor(0.6)
            .truncate(); //.no_write_cache();
        let db: Arc<AsyncDb<u64, String, 8>> = Arc::new(AsyncDb::open(config).unwrap());
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
            "XXXX TOK insert({}) time {}",
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
            "XXXX TOK fetch ({}) (pre commit) time {}",
            max,
            start.elapsed().as_secs_f64()
        );

        let start = time::Instant::now();
        db.commit().await.unwrap();
        println!("XXXX TOK commit time {}", start.elapsed().as_secs_f64());
        let start = time::Instant::now();
        let vals: Vec<String> = db.raw_iter().await.unwrap().map(|r| r.unwrap().1).collect();
        assert_eq!(vals.len(), max as usize);
        for (i, v) in vals.iter().enumerate() {
            assert_eq!(v, &format!("Value {}", i));
        }
        println!("XXXX TOK iter time {}", start.elapsed().as_secs_f64());
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
            "XXXX TOK fetch ({}) time {}",
            max,
            start.elapsed().as_secs_f64()
        );
    }
}
