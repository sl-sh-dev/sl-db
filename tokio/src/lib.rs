#![deny(missing_docs)]

//! Provide tokio async wrappers around DbCore.  This is tokio specific but should be easily
//! adaptable to other runtimes.  Provides a simple wrapper AsyncDb as well as a sharded version
//! ShardedDb.

pub mod async_db;
pub use async_db::AsyncDb;

pub mod sharded_db;
pub use sharded_db::ShardedDb;

pub mod commit_error;

pub use commit_error::CommitError;

mod write_thread;
