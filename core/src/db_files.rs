//! Define the files used to create a SLDB.

use std::error::Error;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::{fmt, fs, io};

/// Contains the file names, paths etc for all the files in a DB.
///
/// There are three ways to create a set of files:
/// - Supply a directory and name.  This will use the the directory as a root, add a new path
/// element for name and store the files in that directory with default names (db.dat, db.hdx, db.obx).
/// The name is incorporated into the directory path to for the DB.
/// For example with directory set to "/some/dir" and name set to "test_db" all the DB files will be
/// stored at the path /some/dir/test_db/{db.dat, db.hdx, db.odx}.
/// - Supply a data directory, index directory and name.  This works the same as above except the data
/// file (db.dat) will be stored in the data directory and the index files (db.hdx and db.odx) will
/// be stored in the index directory.  Use this to put the data and index files on different devices
/// for instance.
/// - Supply the full paths to each file (db.dat, db.hdx and db.odx).  Use this for full control of
/// where the files are stored.  If you use this method it is an error to try to rename the DB.
/// Also name will just be informational when using this method.
///
/// The first way should be your default, second if you need to split files across devices and third
/// if you have some requirement the first two methods don't satisfy.
#[derive(Clone, Debug)]
pub struct DbFiles {
    /// The directory containing the DB.
    dir: Option<PathBuf>,
    /// The directory containing the index files (if not stored in dir with data).
    index_dir: Option<PathBuf>,
    /// Base name (without directory) of the DB.
    name: String,
    /// The full path and name of the data file.
    data_file: Option<PathBuf>,
    /// The full path and name of the index file.
    hdx_file: Option<PathBuf>,
    /// The full path and name of the index overflow file.
    odx_file: Option<PathBuf>,
}

impl DbFiles {
    /// Create a new DbFiles struct from a directory and name.
    /// This should be your default constructor unless you need to split the devices containing
    /// the data and index files or you need full control over the paths.
    pub fn with_data<S, P>(dir: P, name: S) -> Self
    where
        S: Into<String>,
        P: Into<PathBuf>,
    {
        let dir: Option<PathBuf> = Some(dir.into());
        DbFiles {
            dir,
            index_dir: None,
            name: name.into(),
            data_file: None,
            hdx_file: None,
            odx_file: None,
        }
    }

    /// Create a new DbFiles struct from a dir, index_dir and name.
    /// Data will be stored in dir and the index files in index_dir.
    /// Use this to split the data and index devices for instance.
    /// This will still allow renames.
    pub fn with_data_index<S, P, Q>(dir: P, index_dir: Q, name: S) -> Self
    where
        S: Into<String>,
        P: Into<PathBuf>,
        Q: Into<PathBuf>,
    {
        let dir: Option<PathBuf> = Some(dir.into());
        let index_dir: Option<PathBuf> = Some(index_dir.into());
        DbFiles {
            dir,
            index_dir,
            name: name.into(),
            data_file: None,
            hdx_file: None,
            odx_file: None,
        }
    }

    /// Create a new DbFiles struct for name with file paths.
    /// When using this name is strictly informational and renames will NOT be supported but it
    /// allows maximum control over the DB files and there locations.
    /// In general use one of the directory based constructors instead of this unless you really
    /// need it.
    /// This allows explicit control over the files paths and the devices they are stored on.
    /// Note: Include the full paths with each file.
    pub fn with_paths<S, P, Q, R>(name: S, data: P, index: Q, overflow: R) -> Self
    where
        S: Into<String>,
        P: Into<PathBuf>,
        Q: Into<PathBuf>,
        R: Into<PathBuf>,
    {
        DbFiles {
            dir: None,
            index_dir: None,
            name: name.into(),
            data_file: Some(data.into()),
            hdx_file: Some(index.into()),
            odx_file: Some(overflow.into()),
        }
    }

    /// Change DB name.
    /// This will change reported file paths (if not using explicit files) so ONLY use during
    /// creation of if moving files yourself.
    pub fn set_name<Q: Into<String>>(&mut self, name: Q) {
        self.name = name.into();
    }

    /// Return the root directory if not using explicit files.
    pub fn dir(&self) -> Option<&Path> {
        if let Some(dir) = &self.dir {
            Some(dir.as_path())
        } else {
            None
        }
    }

    /// Return the root directory of index files if not using explicit files.
    /// Note: will jsut return dir if that is where the index files are stores.
    pub fn index_dir(&self) -> Option<&Path> {
        if let Some(dir) = &self.index_dir {
            Some(dir.as_path())
        } else if let Some(dir) = &self.dir {
            Some(dir.as_path())
        } else {
            None
        }
    }

    /// THe name of the database.  If files are not explicitly set this will be appended to dir and
    /// contain all the DB files.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// True if the explicit filenames are set instead of a root dir to contain generated file names.
    pub fn has_explicit_files(&self) -> bool {
        self.dir.is_none()
    }

    /// Path to the data file.
    pub fn data_path(&self) -> PathBuf {
        if let Some(path) = &self.data_file {
            path.clone()
        } else {
            self.dir
                .as_ref()
                .expect("dir must be set if no path")
                .join(&self.name)
                .join("db")
                .with_extension("dat")
        }
    }

    /// Directory containing the data file.
    pub fn data_dir(&self) -> PathBuf {
        self.get_dir(&self.data_file, false)
    }

    /// Path to the index file.
    pub fn hdx_path(&self) -> PathBuf {
        if let Some(path) = &self.hdx_file {
            path.clone()
        } else {
            if let Some(index_dir) = &self.index_dir {
                return index_dir.join(&self.name).join("db").with_extension("hdx");
            }
            self.dir
                .as_ref()
                .expect("dir must be set if no path")
                .join(&self.name)
                .join("db")
                .with_extension("hdx")
        }
    }

    /// Directory containing the index file.
    pub fn hdx_dir(&self) -> PathBuf {
        self.get_dir(&self.hdx_file, true)
    }

    /// Path to the index overflow file.
    pub fn odx_path(&self) -> PathBuf {
        if let Some(path) = &self.odx_file {
            path.clone()
        } else {
            if let Some(index_dir) = &self.index_dir {
                return index_dir.join(&self.name).join("db").with_extension("odx");
            }
            self.dir
                .as_ref()
                .expect("dir must be set if no path")
                .join(&self.name)
                .join("db")
                .with_extension("odx")
        }
    }

    /// Directory containing the index file.
    pub fn odx_dir(&self) -> PathBuf {
        self.get_dir(&self.odx_file, true)
    }

    /// Delete the referenced DB files and directories if empty.
    /// If it can not remove a file it will silently ignore this.
    pub fn delete(self) {
        let _ = fs::remove_file(self.data_path());
        let _ = fs::remove_file(self.hdx_path());
        let _ = fs::remove_file(self.odx_path());
        if let Some(dir) = &self.index_dir {
            let _ = fs::remove_dir(dir.join(&self.name));
            let _ = fs::remove_dir(dir);
        }
        if let Some(dir) = &self.dir {
            let _ = fs::remove_dir(dir.join(&self.name));
            let _ = fs::remove_dir(dir);
        }
    }

    /// Rename the database to name.
    /// This will also rename the directories (final element based on name) as well.
    /// It is an error to try to rename if file paths were manually set.
    pub fn rename<Q: Into<String>>(&mut self, name: Q) -> Result<(), RenameError> {
        let name: String = name.into();
        if self.name == name {
            // Changing name to itself, nothing to do.
            return Ok(());
        }
        match (&self.dir, &self.index_dir) {
            (Some(dir), Some(index_dir)) => {
                let old_dir = dir.join(&self.name);
                let new_dir = dir.join(&name);
                let old_index_dir = index_dir.join(&self.name);
                let new_index_dir = index_dir.join(&name);
                // If the dirs are empty  and exist then remove them.
                let _ = fs::remove_dir(&new_dir);
                let _ = fs::remove_dir(&new_index_dir);
                if new_dir.exists() || new_index_dir.exists() {
                    Err(RenameError::FilesExist)
                } else {
                    let mut res = fs::rename(&old_index_dir, &new_index_dir);
                    if res.is_ok() {
                        res = fs::rename(old_dir, &new_dir);
                        if res.is_ok() {
                            self.name = name;
                        } else {
                            // Try to undo the index dir rename- should work since we just did the oposite rename...
                            let _ = fs::rename(&new_index_dir, &old_index_dir);
                        }
                    }
                    res.map_err(RenameError::RenameIO)
                }
            }
            (Some(dir), None) => {
                let old_dir = dir.join(&self.name);
                let new_dir = dir.join(&name);
                // If the new dir exists and is empty then remove it.
                // If dir does not exist or is not empty this should do nothing.
                let _ = fs::remove_dir(&new_dir);
                if new_dir.exists() {
                    Err(RenameError::FilesExist)
                } else {
                    let res = fs::rename(old_dir, &new_dir);
                    if res.is_ok() {
                        self.name = name;
                    }
                    res.map_err(RenameError::RenameIO)
                }
            }
            (None, _) => Err(RenameError::CanNotRename),
        }
    }

    /// Directory containing path.
    fn get_dir(&self, path: &Option<PathBuf>, is_index: bool) -> PathBuf {
        if let Some(path) = path {
            if let Some(dir) = path.parent() {
                dir.into()
            } else {
                PathBuf::new()
            }
        } else {
            if is_index {
                if let Some(index_dir) = &self.index_dir {
                    return index_dir.join(&self.name);
                }
            }
            self.dir
                .as_ref()
                .expect("dir must be set if no path")
                .join(&self.name)
        }
    }
}

/// Error renaming a DB.
#[derive(Debug)]
pub enum RenameError {
    /// One or more of the target named files already exist.
    FilesExist,
    /// Error renaming the data file.
    RenameIO(io::Error),
    /// Rename is not supported because file paths were individual set by user.
    CanNotRename,
}

impl Error for RenameError {}

impl fmt::Display for RenameError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match &self {
            Self::FilesExist => write!(f, "target directory for rename already exist"),
            Self::RenameIO(e) => write!(f, "rename failed: {e}"),
            Self::CanNotRename => write!(f, "rename not supported for manually set file paths"),
        }
    }
}
