use crate::CommitError;
use dashmap::DashMap;
use sldb_core::db::DbCore;
use sldb_core::db_bytes::DbBytes;
use sldb_core::db_key::DbKey;
use sldb_core::error::insert::InsertError;
use std::fmt::Debug;
use std::hash::{BuildHasher, Hash};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::sync::*;

/// Commands that can be sent to a write thread.
pub(crate) enum InsertCommand<K, const KSIZE: u16>
where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
{
    Insert(K),
    Commit(tokio::sync::oneshot::Sender<Result<(), CommitError>>),
    CommitBG,
    Done,
}

/// Do a commit for teh background thread.
fn commit_bg_thread<K, V, const KSIZE: u16, S>(
    db: &Arc<Mutex<DbCore<K, V, KSIZE, S>>>,
    write_cache: &DashMap<K, V, S>,
    inserts: &mut Vec<K>,
    result_tx: Option<oneshot::Sender<Result<(), CommitError>>>,
) where
    K: Send + Sync + Eq + Hash + DbKey<KSIZE> + DbBytes<K> + Clone + 'static,
    V: Send + Sync + Debug + DbBytes<V> + Clone + 'static,
    S: Send + Sync + Clone + BuildHasher + Default + 'static,
{
    let res = db.blocking_lock().commit();
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
pub(crate) fn write_thread<K, V, const KSIZE: u16, S>(
    db: Arc<Mutex<DbCore<K, V, KSIZE, S>>>,
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
                let mut db = db.blocking_lock();
                if let Some(value) = write_cache.get(&key) {
                    match db.insert(key.clone(), &value) {
                        Err(InsertError::DuplicateKey) => {} // Silently ignore duplicate keys...
                        Err(err) => last_insert_error = Some(err),
                        Ok(_) => {}
                    }
                    drop(value);
                    drop(db);
                    inserts.push(key);
                }
            }
            Some(InsertCommand::Commit(tx)) => {
                if let Some(err) = last_insert_error {
                    let _ = tx.send(Err(CommitError::PreviousInsertFailed(err)));
                    last_insert_error = None;
                } else {
                    commit_bg_thread(&db, &write_cache, &mut inserts, Some(tx));
                }
            }
            Some(InsertCommand::CommitBG) => {
                commit_bg_thread(&db, &write_cache, &mut inserts, None);
            }
            Some(InsertCommand::Done) => done = true,
            None => done = true, // The sender has been closed or dropped so nothing left to do.
        }
    }
}
