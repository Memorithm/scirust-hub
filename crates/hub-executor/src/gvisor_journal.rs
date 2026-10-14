//! Private controller metadata and nonblocking task/workspace fencing.
//! The guest never receives this sibling of its task workspace.

use std::fs::{DirBuilder, File, OpenOptions};
use std::os::unix::fs::{DirBuilderExt, MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Path, PathBuf};

use hub_store_sqlite::SqliteStore;
use sha2::{Digest, Sha256};

pub(crate) struct AttemptLocks {
    _task: File,
    _workspace: File,
}

pub(crate) fn open(root: &Path) -> Result<(SqliteStore, PathBuf), String> {
    let parent =
        std::fs::metadata(root).map_err(|e| format!("controller root unavailable: {e}"))?;
    if parent.permissions().mode() & 0o022 != 0 {
        return Err("controller root must not be group/world writable".into());
    }
    let directory = root.join(".gvisor-control");
    match DirBuilder::new().mode(0o700).create(&directory) {
        Ok(()) => File::open(root)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())?,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
        Err(e) => return Err(format!("private controller directory unavailable: {e}")),
    }
    let metadata = std::fs::symlink_metadata(&directory).map_err(|e| e.to_string())?;
    if !metadata.is_dir()
        || metadata.file_type().is_symlink()
        || metadata.uid() != parent.uid()
        || metadata.permissions().mode() & 0o777 != 0o700
    {
        return Err("controller state directory must be private, owned and nonsymlink".into());
    }
    let _initialization = lock_file(&directory.join("initialization.lock"), metadata.uid())?;
    let database = directory.join("hub.sqlite");
    let _db_file = private_file(&database, metadata.uid())?;
    let store = SqliteStore::open(&database).map_err(|e| e.to_string())?;
    Ok((store, directory))
}

pub(crate) fn fence(
    directory: &Path,
    task_id: &str,
    workspace: &str,
) -> Result<AttemptLocks, String> {
    let owner = std::fs::metadata(directory)
        .map_err(|e| e.to_string())?
        .uid();
    let task = format!("task-{:x}.lock", Sha256::digest(task_id.as_bytes()));
    let workspace = format!("workspace-{:x}.lock", Sha256::digest(workspace.as_bytes()));
    Ok(AttemptLocks {
        _task: lock_file(&directory.join(task), owner)?,
        _workspace: lock_file(&directory.join(workspace), owner)?,
    })
}

fn private_file(path: &Path, owner: u32) -> Result<File, String> {
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .mode(0o600)
        .custom_flags(nix::libc::O_NOFOLLOW | nix::libc::O_NONBLOCK)
        .open(path)
        .map_err(|e| format!("private controller file unavailable: {e}"))?;
    let metadata = file.metadata().map_err(|e| e.to_string())?;
    if !metadata.is_file()
        || metadata.uid() != owner
        || metadata.nlink() != 1
        || metadata.permissions().mode() & 0o777 != 0o600
    {
        return Err("controller file must be private, owned, regular and not hard-linked".into());
    }
    Ok(file)
}

fn lock_file(path: &Path, owner: u32) -> Result<File, String> {
    let file = private_file(path, owner)?;
    file.try_lock()
        .map_err(|e| format!("controller attempt/workspace is busy: {e}"))?;
    Ok(file)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn live_fences_are_nonblocking_and_release_with_the_controller() {
        let root = std::env::temp_dir().join(format!("hub-fence-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        let (_store, dir) = open(&root).unwrap();
        let first = fence(&dir, "task-1", "/workspace-1").unwrap();
        assert!(fence(&dir, "task-1", "/workspace-2").is_err());
        assert!(fence(&dir, "task-2", "/workspace-1").is_err());
        assert!(fence(&dir, "task-2", "/workspace-2").is_ok());
        drop(first);
        assert!(fence(&dir, "task-1", "/workspace-1").is_ok());
        drop(_store);
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn controller_directory_symlink_fails_closed() {
        let root = std::env::temp_dir().join(format!("hub-fence-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir(&root).unwrap();
        std::os::unix::fs::symlink(&root, root.join(".gvisor-control")).unwrap();
        assert!(open(&root).is_err());
        std::fs::remove_dir_all(root).unwrap();
    }
}
