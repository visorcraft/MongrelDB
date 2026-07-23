//! Cross-platform durability primitives.

use std::io;
use std::path::Path;

/// Flushes directory metadata so published entries survive a crash.
pub fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(path)?.sync_all()
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Foundation::{
            CloseHandle, GENERIC_READ, GENERIC_WRITE, INVALID_HANDLE_VALUE,
        };
        use windows_sys::Win32::Storage::FileSystem::{
            CreateFileW, FlushFileBuffers, FILE_FLAG_BACKUP_SEMANTICS, FILE_SHARE_DELETE,
            FILE_SHARE_READ, FILE_SHARE_WRITE, OPEN_EXISTING,
        };

        let path = path
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let handle = unsafe {
            CreateFileW(
                path.as_ptr(),
                GENERIC_READ | GENERIC_WRITE,
                FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE,
                std::ptr::null(),
                OPEN_EXISTING,
                FILE_FLAG_BACKUP_SEMANTICS,
                std::ptr::null_mut(),
            )
        };
        if handle == INVALID_HANDLE_VALUE {
            return Err(io::Error::last_os_error());
        }
        let flushed = unsafe { FlushFileBuffers(handle) };
        let flush_error = (flushed == 0).then(io::Error::last_os_error);
        let closed = unsafe { CloseHandle(handle) };
        if let Some(error) = flush_error {
            return Err(error);
        }
        if closed == 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "directory synchronization is unsupported on this platform",
        ))
    }
}

/// Recursively flush file contents and directory entries under `root` so a
/// batch of newly-created files survives a crash. Used by batch-create paths
/// (e.g. `Table::create`) that defer per-operation fsyncs and make everything
/// durable in one pass at the end. Reads each entry through a single
/// `read_dir`, fsyncs regular files for data durability, recurses into
/// subdirectories, then fsyncs each directory for entry-name durability.
pub fn sync_tree_recursive(root: &Path) -> io::Result<()> {
    sync_tree_recursive_impl(root)
}

fn sync_tree_recursive_impl(dir: &Path) -> io::Result<()> {
    for entry in (std::fs::read_dir(dir)?).flatten() {
        let path = entry.path();
        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        if meta.is_dir() {
            sync_tree_recursive_impl(&path)?;
            sync_directory(&path)?;
        } else if meta.is_file() {
            std::fs::File::open(&path)?.sync_all()?;
        }
    }
    sync_directory(dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn syncs_directory() {
        sync_directory(&std::env::current_dir().unwrap()).unwrap();
    }
}
