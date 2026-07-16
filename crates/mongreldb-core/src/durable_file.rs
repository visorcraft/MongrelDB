use std::ffi::{OsStr, OsString};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

/// Stable identity for a descriptor-pinned durable directory.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum DurableFileIdentity {
    #[cfg(unix)]
    Unix { device: u64, inode: u64 },
    #[cfg(windows)]
    Windows { volume_serial: u32, file_index: u64 },
}

/// Descriptor-pinned root for durable state owned by a database.
///
/// Every relative operation rejects `..`, symlinks, reparse points, and
/// non-regular final files. Unix operations stay descriptor-relative. Windows
/// keeps the root and each traversed ancestor open without delete sharing.
pub struct DurableRoot {
    canonical_path: PathBuf,
    #[cfg(any(
        windows,
        all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        )
    ))]
    directory: std::fs::File,
}

impl std::fmt::Debug for DurableRoot {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DurableRoot")
            .field("canonical_path", &self.canonical_path)
            .finish_non_exhaustive()
    }
}

impl DurableRoot {
    pub fn try_clone(&self) -> io::Result<Self> {
        Ok(Self {
            canonical_path: self.canonical_path.clone(),
            #[cfg(any(
                windows,
                all(
                    unix,
                    any(target_os = "linux", target_os = "android", target_vendor = "apple")
                )
            ))]
            directory: self.directory.try_clone()?,
        })
    }

    pub fn open(root: impl AsRef<Path>) -> io::Result<Self> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            if let Ok(relative) = root.as_ref().strip_prefix("/proc/self/fd") {
                let mut components =
                    relative
                        .components()
                        .filter_map(|component| match component {
                            std::path::Component::Normal(value) => Some(value),
                            _ => None,
                        });
                if let Some(fd) = components.next().filter(|value| {
                    value
                        .to_str()
                        .is_some_and(|value| value.parse::<i32>().is_ok())
                }) {
                    use rustix::fs::{openat, Mode, OFlags};
                    let mut directory = std::fs::File::open(Path::new("/proc/self/fd").join(fd))?;
                    for component in components {
                        let opened = openat(
                            &directory,
                            Path::new(component),
                            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
                            Mode::empty(),
                        )
                        .map_err(io::Error::from)?;
                        directory = std::fs::File::from(opened);
                    }
                    use std::os::fd::AsRawFd;
                    let canonical_path = std::fs::read_link(
                        Path::new("/proc/self/fd").join(directory.as_raw_fd().to_string()),
                    )?;
                    return Ok(Self {
                        canonical_path,
                        directory,
                    });
                }
            }
            let directory = open_unix_directory_path(root.as_ref())?;
            let directory = std::fs::File::from(directory);
            #[cfg(any(target_os = "linux", target_os = "android"))]
            let canonical_path = {
                use std::os::fd::AsRawFd;
                std::fs::read_link(
                    Path::new("/proc/self/fd").join(directory.as_raw_fd().to_string()),
                )?
            };
            #[cfg(target_vendor = "apple")]
            let canonical_path = {
                use std::os::unix::ffi::OsStrExt;
                let path = rustix::fs::getpath(&directory).map_err(io::Error::from)?;
                PathBuf::from(OsStr::from_bytes(path.to_bytes()))
            };
            Ok(Self {
                canonical_path,
                directory,
            })
        }

        #[cfg(windows)]
        {
            let directory = open_windows_nofollow(root.as_ref())?;
            ensure_windows_directory(&directory, root.as_ref()).map_err(mongrel_error_to_io)?;
            let canonical_path = root.as_ref().canonicalize()?;
            return Ok(Self {
                canonical_path,
                directory,
            });
        }

        #[cfg(not(any(
            windows,
            all(
                unix,
                any(target_os = "linux", target_os = "android", target_vendor = "apple")
            )
        )))]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub fn canonical_path(&self) -> &Path {
        &self.canonical_path
    }

    /// Return the stable identity of the pinned directory handle.
    pub fn file_identity(&self) -> io::Result<DurableFileIdentity> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use std::os::unix::fs::MetadataExt;
            let metadata = self.directory.metadata()?;
            return Ok(DurableFileIdentity::Unix {
                device: metadata.dev(),
                inode: metadata.ino(),
            });
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::MetadataExt;
            let metadata = self.directory.metadata()?;
            return Ok(DurableFileIdentity::Windows {
                volume_serial: metadata.volume_serial_number().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::Unsupported,
                        "directory has no volume identity",
                    )
                })?,
                file_index: metadata.file_index().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::Unsupported, "directory has no file identity")
                })?,
            });
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "durable root identity is unsupported on this platform",
        ))
    }

    /// Stable operational path backed by the pinned directory descriptor.
    /// Use this for legacy path-based code while the `DurableRoot` stays alive.
    pub fn io_path(&self) -> io::Result<PathBuf> {
        #[cfg(any(target_os = "linux", target_os = "android"))]
        {
            use std::os::fd::AsRawFd;
            let prefix = "/proc/self/fd";
            return Ok(Path::new(prefix)
                .join(self.directory.as_raw_fd().to_string())
                .join("."));
        }

        #[cfg(target_vendor = "apple")]
        {
            use std::os::unix::ffi::OsStrExt;
            let path = rustix::fs::getpath(&self.directory).map_err(io::Error::from)?;
            return Ok(PathBuf::from(OsStr::from_bytes(path.to_bytes())));
        }

        #[cfg(windows)]
        return Ok(self.canonical_path.clone());

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "durable root has no supported operational path",
        ))
    }

    pub fn open_directory(&self, relative: impl AsRef<Path>) -> io::Result<DurableRoot> {
        let relative = relative.as_ref();
        let components = checked_components(relative)?;
        let canonical_path = components
            .iter()
            .fold(self.canonical_path.clone(), |path, component| {
                path.join(component)
            });

        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            let directory = self.unix_directory(relative)?;
            return Ok(DurableRoot {
                canonical_path,
                directory: std::fs::File::from(directory),
            });
        }

        #[cfg(windows)]
        {
            let (path, _ancestors) = self.windows_path(relative)?;
            let directory = open_windows_nofollow(&path)?;
            ensure_windows_directory(&directory, &path).map_err(mongrel_error_to_io)?;
            return Ok(DurableRoot {
                canonical_path,
                directory,
            });
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub fn create_directory_all(&self, relative: impl AsRef<Path>) -> io::Result<()> {
        self.create_directory_all_pinned(relative).map(|_| ())
    }

    pub fn create_directory_all_pinned(
        &self,
        relative: impl AsRef<Path>,
    ) -> io::Result<DurableRoot> {
        let components = checked_components(relative.as_ref())?;
        let canonical_path = components
            .iter()
            .fold(self.canonical_path.clone(), |path, component| {
                path.join(component)
            });

        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fsync, mkdirat, openat, Mode, OFlags};
            let mut directory = self.duplicate_unix_root()?;
            for component in components {
                let opened = openat(
                    &directory,
                    Path::new(&component),
                    OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
                    Mode::empty(),
                );
                directory = match opened {
                    Ok(opened) => opened,
                    Err(error) if error == rustix::io::Errno::NOENT => {
                        mkdirat(
                            &directory,
                            Path::new(&component),
                            Mode::from_raw_mode(0o700),
                        )
                        .map_err(io::Error::from)?;
                        fsync(&directory).map_err(io::Error::from)?;
                        openat(
                            &directory,
                            Path::new(&component),
                            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
                            Mode::empty(),
                        )
                        .map_err(io::Error::from)?
                    }
                    Err(error) => return Err(io::Error::from(error)),
                };
            }
            return Ok(DurableRoot {
                canonical_path,
                directory: std::fs::File::from(directory),
            });
        }

        #[cfg(windows)]
        {
            let mut path = self.canonical_path.clone();
            let mut ancestors = Vec::new();
            for component in components {
                path.push(component);
                match open_windows_nofollow(&path) {
                    Ok(directory) => {
                        ensure_windows_directory(&directory, &path).map_err(mongrel_error_to_io)?;
                        ancestors.push(directory);
                    }
                    Err(error) if error.kind() == io::ErrorKind::NotFound => {
                        std::fs::create_dir(&path)?;
                        sync_directory(path.parent().unwrap_or(&self.canonical_path))?;
                        let directory = open_windows_nofollow(&path)?;
                        ensure_windows_directory(&directory, &path).map_err(mongrel_error_to_io)?;
                        ancestors.push(directory);
                    }
                    Err(error) => return Err(error),
                }
            }
            let directory = ancestors.pop().ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "durable relative path is empty",
                )
            })?;
            return Ok(DurableRoot {
                canonical_path,
                directory,
            });
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub fn create_directory_new(&self, relative: impl AsRef<Path>) -> io::Result<()> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fsync, mkdirat, Mode};
            let (directory, name) = self.unix_parent(relative.as_ref())?;
            mkdirat(&directory, Path::new(&name), Mode::from_raw_mode(0o700))
                .map_err(io::Error::from)?;
            return fsync(directory).map_err(io::Error::from);
        }

        #[cfg(windows)]
        {
            let (path, _ancestors) = self.windows_path(relative.as_ref())?;
            std::fs::create_dir(&path)?;
            return sync_directory(path.parent().unwrap_or(&self.canonical_path));
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub fn open_regular(&self, relative: impl AsRef<Path>) -> io::Result<std::fs::File> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fstat, openat, FileType, Mode, OFlags};
            let (directory, name) = self.unix_parent(relative.as_ref())?;
            let file = openat(
                &directory,
                Path::new(&name),
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(io::Error::from)?;
            if FileType::from_raw_mode(fstat(&file).map_err(io::Error::from)?.st_mode)
                != FileType::RegularFile
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "durable entry is not a regular file",
                ));
            }
            return Ok(std::fs::File::from(file));
        }

        #[cfg(windows)]
        {
            let (path, _ancestors) = self.windows_path(relative.as_ref())?;
            let file = open_windows_nofollow(&path)?;
            ensure_windows_regular(&file, &path).map_err(mongrel_error_to_io)?;
            return Ok(file);
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub(crate) fn open_regular_read_write(
        &self,
        relative: impl AsRef<Path>,
    ) -> io::Result<std::fs::File> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fstat, openat, FileType, Mode, OFlags};
            let (directory, name) = self.unix_parent(relative.as_ref())?;
            let file = openat(
                &directory,
                Path::new(&name),
                OFlags::RDWR | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                Mode::empty(),
            )
            .map_err(io::Error::from)?;
            if FileType::from_raw_mode(fstat(&file).map_err(io::Error::from)?.st_mode)
                != FileType::RegularFile
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "durable entry is not a regular file",
                ));
            }
            return Ok(std::fs::File::from(file));
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
            };
            let (path, _ancestors) = self.windows_path(relative.as_ref())?;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
                .open(&path)?;
            ensure_windows_regular(&file, &path).map_err(mongrel_error_to_io)?;
            return Ok(file);
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub fn entry_exists(&self, relative: impl AsRef<Path>) -> io::Result<bool> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{openat, Mode, OFlags};
            let (directory, name) = self.unix_parent(relative.as_ref())?;
            return match openat(
                &directory,
                Path::new(&name),
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                Mode::empty(),
            ) {
                Ok(_) => Ok(true),
                Err(error) if error == rustix::io::Errno::NOENT => Ok(false),
                Err(error) => Err(io::Error::from(error)),
            };
        }

        #[cfg(windows)]
        {
            let (path, _ancestors) = self.windows_path(relative.as_ref())?;
            return match open_windows_nofollow(&path) {
                Ok(file) => {
                    ensure_windows_not_reparse(&file, &path).map_err(mongrel_error_to_io)?;
                    Ok(true)
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
                Err(error) => Err(error),
            };
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub fn write_new(&self, relative: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
        let relative = relative.as_ref();
        let mut file = self.open_create_new(relative)?;
        let result = (|| {
            file.write_all(bytes)?;
            file.sync_all()
        })();
        if result.is_err() {
            let _ = self.remove_file(relative);
            return result;
        }
        self.sync_relative_parent(relative)
    }

    pub(crate) fn create_regular_new(
        &self,
        relative: impl AsRef<Path>,
    ) -> io::Result<std::fs::File> {
        self.open_create_new(relative.as_ref())
    }

    pub(crate) fn sync_entry_parent(&self, relative: impl AsRef<Path>) -> io::Result<()> {
        self.sync_relative_parent(relative.as_ref())
    }

    pub fn copy_new_from(
        &self,
        relative: impl AsRef<Path>,
        source: &mut std::fs::File,
    ) -> io::Result<u64> {
        let relative = relative.as_ref();
        let mut destination = self.open_create_new(relative)?;
        let result = (|| {
            let bytes = io::copy(source, &mut destination)?;
            destination.sync_all()?;
            Ok(bytes)
        })();
        if result.is_err() {
            let _ = self.remove_file(relative);
            return result;
        }
        self.sync_relative_parent(relative)?;
        result
    }

    pub fn write_atomic(&self, relative: impl AsRef<Path>, bytes: &[u8]) -> io::Result<()> {
        self.write_atomic_with_after(relative, bytes, || {})
    }

    /// Write an authoritative file atomically, invoking `after_publish` once
    /// the replacement is visible and before any later directory-sync error.
    pub(crate) fn write_atomic_with_after<F>(
        &self,
        relative: impl AsRef<Path>,
        bytes: &[u8],
        after_publish: F,
    ) -> io::Result<()>
    where
        F: FnOnce(),
    {
        self.write_atomic_controlled_with_after(
            relative,
            bytes,
            || Ok::<(), io::Error>(()),
            after_publish,
        )
    }

    /// Prepare and fsync a unique replacement, run `before_publish`, then
    /// atomically replace the destination. `after_publish` runs once the new
    /// name is visible and before the parent-directory fsync.
    pub(crate) fn write_atomic_controlled_with_after<B, A, E>(
        &self,
        relative: impl AsRef<Path>,
        bytes: &[u8],
        before_publish: B,
        after_publish: A,
    ) -> std::result::Result<(), E>
    where
        B: FnOnce() -> std::result::Result<(), E>,
        A: FnOnce(),
        E: From<io::Error>,
    {
        let relative = relative.as_ref();
        let (_, name) = checked_parent(relative).map_err(E::from)?;
        let mut before_publish = Some(before_publish);
        let mut after_publish = Some(after_publish);
        for _ in 0..128 {
            let mut nonce = [0_u8; 8];
            getrandom::getrandom(&mut nonce)
                .map_err(|error| E::from(io::Error::other(error.to_string())))?;
            let suffix = nonce
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let temporary = relative.with_file_name(format!(
                ".{}.tmp-{}-{suffix}",
                name.to_string_lossy(),
                std::process::id()
            ));
            match self.write_new(&temporary, bytes) {
                Ok(()) => {
                    let prepare = before_publish.take().ok_or_else(|| {
                        E::from(io::Error::other(
                            "durable atomic prepare callback already consumed",
                        ))
                    })?;
                    if let Err(error) = prepare() {
                        let _ = self.remove_file(&temporary);
                        return Err(error);
                    }
                    let publish = after_publish.take().ok_or_else(|| {
                        E::from(io::Error::other(
                            "durable atomic publish callback already consumed",
                        ))
                    })?;
                    let result = self.replace_file_with_after(&temporary, relative, publish);
                    if result.is_err() {
                        let _ = self.remove_file(&temporary);
                    }
                    return result.map_err(E::from);
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(E::from(error)),
            }
        }
        Err(E::from(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate durable temporary file",
        )))
    }

    pub fn open_lock_file(&self, relative: impl AsRef<Path>) -> io::Result<std::fs::File> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fstat, openat, FileType, Mode, OFlags};
            let (directory, name) = self.unix_parent(relative.as_ref())?;
            let file = openat(
                &directory,
                Path::new(&name),
                OFlags::RDWR | OFlags::CREATE | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::from_raw_mode(0o600),
            )
            .map_err(io::Error::from)?;
            if FileType::from_raw_mode(fstat(&file).map_err(io::Error::from)?.st_mode)
                != FileType::RegularFile
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "durable lock is not a regular file",
                ));
            }
            return Ok(std::fs::File::from(file));
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
            };
            let (path, _ancestors) = self.windows_path(relative.as_ref())?;
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
                .open(&path)?;
            ensure_windows_regular(&file, &path).map_err(mongrel_error_to_io)?;
            return Ok(file);
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub fn list_regular_files(&self, relative: impl AsRef<Path>) -> io::Result<Vec<OsString>> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fstat, openat, Dir, FileType, Mode, OFlags};
            let relative = relative.as_ref();
            let directory = if relative.as_os_str().is_empty() || relative == Path::new(".") {
                self.duplicate_unix_root()?
            } else {
                self.unix_directory(relative)?
            };
            let mut entries = Dir::read_from(&directory).map_err(io::Error::from)?;
            let mut names = Vec::new();
            for entry in &mut entries {
                let entry = entry.map_err(io::Error::from)?;
                use std::os::unix::ffi::OsStrExt;
                let bytes = entry.file_name().to_bytes();
                if bytes == b"." || bytes == b".." {
                    continue;
                }
                let name = OsStr::from_bytes(bytes).to_os_string();
                let child = openat(
                    &directory,
                    Path::new(&name),
                    OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
                    Mode::empty(),
                )
                .map_err(io::Error::from)?;
                if FileType::from_raw_mode(fstat(&child).map_err(io::Error::from)?.st_mode)
                    != FileType::RegularFile
                {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "durable directory contains a non-regular entry",
                    ));
                }
                names.push(name);
            }
            names.sort();
            return Ok(names);
        }

        #[cfg(windows)]
        {
            let relative = relative.as_ref();
            let (path, mut ancestors) =
                if relative.as_os_str().is_empty() || relative == Path::new(".") {
                    (self.canonical_path.clone(), Vec::new())
                } else {
                    self.windows_path(relative)?
                };
            let directory = open_windows_nofollow(&path)?;
            ensure_windows_directory(&directory, &path).map_err(mongrel_error_to_io)?;
            ancestors.push(directory);
            let mut names = Vec::new();
            for entry in std::fs::read_dir(&path)? {
                let entry = entry?;
                let child = open_windows_nofollow(&entry.path())?;
                ensure_windows_regular(&child, &entry.path()).map_err(mongrel_error_to_io)?;
                names.push(entry.file_name());
            }
            names.sort();
            return Ok(names);
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    /// Walk every regular file beneath this pinned root without reopening the
    /// root or any discovered child by an ambient filesystem path.
    pub(crate) fn walk_regular_files<P, D, F>(
        &self,
        mut include: P,
        mut on_directory: D,
        mut on_file: F,
    ) -> crate::Result<()>
    where
        P: FnMut(&Path, bool) -> crate::Result<bool>,
        D: FnMut(&Path) -> crate::Result<()>,
        F: FnMut(&Path, &mut std::fs::File) -> crate::Result<()>,
    {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            return walk_unix_directory(
                self.duplicate_unix_root()?,
                Path::new(""),
                &mut include,
                &mut on_directory,
                &mut on_file,
            );
        }

        #[cfg(windows)]
        {
            return walk_windows_directory(
                &self.canonical_path,
                Path::new(""),
                self.directory.try_clone()?,
                &mut include,
                &mut on_directory,
                &mut on_file,
            );
        }

        #[allow(unreachable_code)]
        {
            let _ = (&mut include, &mut on_directory, &mut on_file);
            Err(crate::MongrelError::Other(
                "no-follow directory traversal is unsupported on this platform".into(),
            ))
        }
    }

    pub fn remove_file(&self, relative: impl AsRef<Path>) -> io::Result<()> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fsync, unlinkat, AtFlags};
            let (directory, name) = self.unix_parent(relative.as_ref())?;
            match unlinkat(&directory, Path::new(&name), AtFlags::empty()) {
                Ok(()) => fsync(&directory).map_err(io::Error::from),
                Err(error) if error == rustix::io::Errno::NOENT => Ok(()),
                Err(error) => Err(io::Error::from(error)),
            }
        }

        #[cfg(windows)]
        {
            let (path, _ancestors) = self.windows_path(relative.as_ref())?;
            match std::fs::remove_file(&path) {
                Ok(()) => sync_directory(path.parent().unwrap_or(&self.canonical_path)),
                Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            }
        }

        #[cfg(not(any(
            windows,
            all(
                unix,
                any(target_os = "linux", target_os = "android", target_vendor = "apple")
            )
        )))]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub(crate) fn rename_file_new(
        &self,
        source: impl AsRef<Path>,
        destination: impl AsRef<Path>,
    ) -> io::Result<()> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fsync, renameat_with, RenameFlags};
            use std::os::fd::AsRawFd;
            let (source_parent, source_name) = self.unix_parent(source.as_ref())?;
            let (destination_parent, destination_name) = self.unix_parent(destination.as_ref())?;
            renameat_with(
                &source_parent,
                Path::new(&source_name),
                &destination_parent,
                Path::new(&destination_name),
                RenameFlags::NOREPLACE,
            )
            .map_err(io::Error::from)?;
            fsync(&destination_parent).map_err(io::Error::from)?;
            if source_parent.as_raw_fd() != destination_parent.as_raw_fd() {
                fsync(&source_parent).map_err(io::Error::from)?;
            }
            return Ok(());
        }

        #[cfg(windows)]
        {
            let (source, _source_ancestors) = self.windows_path(source.as_ref())?;
            let (destination, _destination_ancestors) = self.windows_path(destination.as_ref())?;
            return rename(&source, &destination);
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative file rename is unsupported on this platform",
        ))
    }

    pub fn rename_directory_new(
        &self,
        source: impl AsRef<Path>,
        destination_root: &DurableRoot,
        destination: impl AsRef<Path>,
    ) -> io::Result<()> {
        self.rename_directory_new_with_after(source, destination_root, destination, || {})
    }

    pub fn rename_directory_new_with_after<F>(
        &self,
        source: impl AsRef<Path>,
        destination_root: &DurableRoot,
        destination: impl AsRef<Path>,
        after_publish: F,
    ) -> io::Result<()>
    where
        F: FnOnce(),
    {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fsync, renameat_with, RenameFlags};
            let (source_parent, source_name) = self.unix_parent(source.as_ref())?;
            let (destination_parent, destination_name) =
                destination_root.unix_parent(destination.as_ref())?;
            renameat_with(
                &source_parent,
                Path::new(&source_name),
                &destination_parent,
                Path::new(&destination_name),
                RenameFlags::NOREPLACE,
            )
            .map_err(io::Error::from)?;
            after_publish();
            fsync(&destination_parent).map_err(io::Error::from)?;
            return fsync(&source_parent).map_err(io::Error::from);
        }

        #[cfg(windows)]
        {
            let (source, mut source_ancestors) = self.windows_path(source.as_ref())?;
            let (destination, destination_ancestors) =
                destination_root.windows_path(destination.as_ref())?;
            source_ancestors.extend(destination_ancestors);
            let result = rename(&source, &destination);
            if result.is_ok() {
                after_publish();
            }
            return result;
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    pub fn remove_directory_all(&self, relative: impl AsRef<Path>) -> io::Result<()> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fsync, unlinkat, AtFlags};
            let (parent, name) = self.unix_parent(relative.as_ref())?;
            let directory = match self.unix_directory(relative.as_ref()) {
                Ok(directory) => directory,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };
            remove_unix_directory_contents(&directory)?;
            unlinkat(&parent, Path::new(&name), AtFlags::REMOVEDIR).map_err(io::Error::from)?;
            return fsync(parent).map_err(io::Error::from);
        }

        #[cfg(windows)]
        {
            let (path, mut ancestors) = self.windows_path(relative.as_ref())?;
            let directory = match open_windows_nofollow(&path) {
                Ok(directory) => directory,
                Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                Err(error) => return Err(error),
            };
            ensure_windows_directory(&directory, &path).map_err(mongrel_error_to_io)?;
            remove_windows_directory_contents(&path, &directory)?;
            ancestors.push(directory);
            std::fs::remove_dir(&path)?;
            return sync_directory(path.parent().unwrap_or(&self.canonical_path));
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    #[cfg(all(
        unix,
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    fn duplicate_unix_root(&self) -> io::Result<rustix::fd::OwnedFd> {
        use rustix::fs::{openat, Mode, OFlags};
        openat(
            &self.directory,
            Path::new("."),
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .map_err(io::Error::from)
    }

    #[cfg(all(
        unix,
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    fn unix_directory(&self, relative: &Path) -> io::Result<rustix::fd::OwnedFd> {
        use rustix::fs::{openat, Mode, OFlags};
        let mut directory = self.duplicate_unix_root()?;
        for component in checked_components_allow_empty(relative)? {
            directory = openat(
                &directory,
                Path::new(&component),
                OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
                Mode::empty(),
            )
            .map_err(io::Error::from)?;
        }
        Ok(directory)
    }

    #[cfg(all(
        unix,
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    fn unix_parent(&self, relative: &Path) -> io::Result<(rustix::fd::OwnedFd, OsString)> {
        let (parent, name) = checked_parent(relative)?;
        Ok((self.unix_directory(&parent)?, name))
    }

    #[cfg(windows)]
    fn windows_path(&self, relative: &Path) -> io::Result<(PathBuf, Vec<std::fs::File>)> {
        let components = checked_components(relative)?;
        let mut path = self.canonical_path.clone();
        let mut ancestors = Vec::new();
        for component in components.iter().take(components.len().saturating_sub(1)) {
            path.push(component);
            let directory = open_windows_nofollow(&path)?;
            ensure_windows_directory(&directory, &path).map_err(mongrel_error_to_io)?;
            ancestors.push(directory);
        }
        if let Some(name) = components.last() {
            path.push(name);
        }
        Ok((path, ancestors))
    }

    fn open_create_new(&self, relative: &Path) -> io::Result<std::fs::File> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{openat, Mode, OFlags};
            let (directory, name) = self.unix_parent(relative)?;
            return openat(
                &directory,
                Path::new(&name),
                OFlags::WRONLY | OFlags::CREATE | OFlags::EXCL | OFlags::CLOEXEC | OFlags::NOFOLLOW,
                Mode::from_raw_mode(0o600),
            )
            .map(std::fs::File::from)
            .map_err(io::Error::from);
        }

        #[cfg(windows)]
        {
            use std::os::windows::fs::OpenOptionsExt;
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
            };
            let (path, _ancestors) = self.windows_path(relative)?;
            return std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
                .custom_flags(FILE_FLAG_OPEN_REPARSE_POINT)
                .open(path);
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    fn replace_file_with_after<F>(
        &self,
        source: &Path,
        destination: &Path,
        after_publish: F,
    ) -> io::Result<()>
    where
        F: FnOnce(),
    {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::{fsync, renameat};
            let (source_parent, source_name) = self.unix_parent(source)?;
            let (destination_parent, destination_name) = self.unix_parent(destination)?;
            renameat(
                &source_parent,
                Path::new(&source_name),
                &destination_parent,
                Path::new(&destination_name),
            )
            .map_err(io::Error::from)?;
            after_publish();
            fsync(&destination_parent).map_err(io::Error::from)?;
            if source.parent() != destination.parent() {
                fsync(&source_parent).map_err(io::Error::from)?;
            }
            return Ok(());
        }

        #[cfg(windows)]
        {
            let (source, mut source_ancestors) = self.windows_path(source)?;
            let (destination, destination_ancestors) = self.windows_path(destination)?;
            source_ancestors.extend(destination_ancestors);
            return replace_with_after(&source, &destination, after_publish);
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }

    fn sync_relative_parent(&self, relative: &Path) -> io::Result<()> {
        #[cfg(all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        ))]
        {
            use rustix::fs::fsync;
            let (directory, _) = self.unix_parent(relative)?;
            return fsync(directory).map_err(io::Error::from);
        }

        #[cfg(windows)]
        {
            let (path, _ancestors) = self.windows_path(relative)?;
            return sync_directory(path.parent().unwrap_or(&self.canonical_path));
        }

        #[allow(unreachable_code)]
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "descriptor-relative durable files are unsupported on this platform",
        ))
    }
}

#[cfg(all(
    unix,
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
))]
fn open_unix_directory_path(path: &Path) -> io::Result<std::os::fd::OwnedFd> {
    use rustix::fs::{openat, Mode, OFlags, CWD};
    use std::path::Component;

    let flags = OFlags::RDONLY | OFlags::CLOEXEC | OFlags::DIRECTORY;
    for component in path.components() {
        match component {
            Component::RootDir | Component::CurDir | Component::Normal(_) => {}
            Component::ParentDir | Component::Prefix(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "durable root path contains an unsafe component",
                ))
            }
        }
    }
    // Resolve platform aliases and an explicitly supplied root symlink in one
    // kernel lookup, then pin the resulting directory descriptor. Replacing
    // the alias afterward cannot redirect any operation. Every operation below
    // this pinned root remains component-by-component NOFOLLOW.
    openat(CWD, path, flags, Mode::empty()).map_err(io::Error::from)
}

#[cfg(all(
    unix,
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
))]
fn remove_unix_directory_contents(directory: &rustix::fd::OwnedFd) -> io::Result<()> {
    use rustix::fs::{fstat, fsync, openat, unlinkat, AtFlags, Dir, FileType, Mode, OFlags};
    use std::os::unix::ffi::OsStrExt;

    let mut entries = Dir::read_from(directory).map_err(io::Error::from)?;
    let mut names = Vec::new();
    for entry in &mut entries {
        let entry = entry.map_err(io::Error::from)?;
        let bytes = entry.file_name().to_bytes();
        if bytes != b"." && bytes != b".." {
            names.push(OsStr::from_bytes(bytes).to_os_string());
        }
    }
    for name in names {
        let child = openat(
            directory,
            Path::new(&name),
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(io::Error::from)?;
        match FileType::from_raw_mode(fstat(&child).map_err(io::Error::from)?.st_mode) {
            FileType::Directory => {
                remove_unix_directory_contents(&child)?;
                unlinkat(directory, Path::new(&name), AtFlags::REMOVEDIR)
                    .map_err(io::Error::from)?;
            }
            FileType::RegularFile => {
                unlinkat(directory, Path::new(&name), AtFlags::empty()).map_err(io::Error::from)?;
            }
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "refuses non-regular durable entry",
                ))
            }
        }
    }
    fsync(directory).map_err(io::Error::from)
}

#[cfg(windows)]
fn remove_windows_directory_contents(path: &Path, _directory: &std::fs::File) -> io::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let child_path = entry.path();
        let child = open_windows_nofollow(&child_path)?;
        let metadata =
            ensure_windows_not_reparse(&child, &child_path).map_err(mongrel_error_to_io)?;
        if metadata.is_dir() {
            remove_windows_directory_contents(&child_path, &child)?;
            std::fs::remove_dir(&child_path)?;
        } else if metadata.is_file() {
            std::fs::remove_file(&child_path)?;
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "refuses non-regular durable entry",
            ));
        }
    }
    sync_directory(path)
}

fn checked_components(path: &Path) -> io::Result<Vec<OsString>> {
    let components = checked_components_allow_empty(path)?;
    if components.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "durable relative path must not be empty",
        ));
    }
    Ok(components)
}

fn checked_components_allow_empty(path: &Path) -> io::Result<Vec<OsString>> {
    let mut components = Vec::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(name) => components.push(name.to_os_string()),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("invalid durable relative path {}", path.display()),
                ))
            }
        }
    }
    Ok(components)
}

fn checked_parent(path: &Path) -> io::Result<(PathBuf, OsString)> {
    let components = checked_components(path)?;
    let name = components.last().cloned().unwrap();
    let parent = components[..components.len() - 1]
        .iter()
        .collect::<PathBuf>();
    Ok((parent, name))
}

#[cfg(windows)]
fn mongrel_error_to_io(error: crate::MongrelError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}

/// Open a regular file without following its final symlink/reparse component.
pub(crate) fn open_regular_nofollow(path: &Path) -> crate::Result<std::fs::File> {
    #[cfg(all(
        unix,
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    {
        use rustix::fs::{fstat, openat, FileType, Mode, OFlags, CWD};

        let fd = openat(
            CWD,
            path,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(io::Error::from)?;
        if FileType::from_raw_mode(fstat(&fd).map_err(io::Error::from)?.st_mode)
            != FileType::RegularFile
        {
            return Err(crate::MongrelError::InvalidArgument(format!(
                "refuses non-regular file {}",
                path.display()
            )));
        }
        Ok(std::fs::File::from(fd))
    }

    #[cfg(windows)]
    {
        let file = open_windows_nofollow(path)?;
        ensure_windows_regular(&file, path)?;
        return Ok(file);
    }

    #[cfg(not(any(
        windows,
        all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        )
    )))]
    {
        let _ = path;
        Err(crate::MongrelError::Other(
            "no-follow file opens are unsupported on this platform".into(),
        ))
    }
}

/// Walk regular files beneath `root` without reopening discovered entries by
/// path. Unix traversal is descriptor-relative. Windows keeps each ancestor
/// open without delete sharing, so its path cannot be replaced while walking.
pub(crate) fn walk_regular_files_nofollow<P, D, F>(
    root: &Path,
    mut include: P,
    mut on_directory: D,
    mut on_file: F,
) -> crate::Result<()>
where
    P: FnMut(&Path, bool) -> crate::Result<bool>,
    D: FnMut(&Path) -> crate::Result<()>,
    F: FnMut(&Path, &mut std::fs::File) -> crate::Result<()>,
{
    #[cfg(all(
        unix,
        any(target_os = "linux", target_os = "android", target_vendor = "apple")
    ))]
    {
        use rustix::fs::{openat, Mode, OFlags, CWD};

        let directory = openat(
            CWD,
            root,
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::DIRECTORY,
            Mode::empty(),
        )
        .map_err(io::Error::from)?;
        walk_unix_directory(
            directory,
            Path::new(""),
            &mut include,
            &mut on_directory,
            &mut on_file,
        )
    }

    #[cfg(windows)]
    {
        let directory = open_windows_nofollow(root)?;
        ensure_windows_directory(&directory, root)?;
        return walk_windows_directory(
            root,
            Path::new(""),
            directory,
            &mut include,
            &mut on_directory,
            &mut on_file,
        );
    }

    #[cfg(not(any(
        windows,
        all(
            unix,
            any(target_os = "linux", target_os = "android", target_vendor = "apple")
        )
    )))]
    {
        let _ = (root, &mut include, &mut on_directory, &mut on_file);
        Err(crate::MongrelError::Other(
            "no-follow directory traversal is unsupported on this platform".into(),
        ))
    }
}

#[cfg(all(
    unix,
    any(target_os = "linux", target_os = "android", target_vendor = "apple")
))]
fn walk_unix_directory<P, D, F>(
    directory: rustix::fd::OwnedFd,
    relative: &Path,
    include: &mut P,
    on_directory: &mut D,
    on_file: &mut F,
) -> crate::Result<()>
where
    P: FnMut(&Path, bool) -> crate::Result<bool>,
    D: FnMut(&Path) -> crate::Result<()>,
    F: FnMut(&Path, &mut std::fs::File) -> crate::Result<()>,
{
    use rustix::fs::{fstat, openat, Dir, FileType, Mode, OFlags};
    use std::ffi::OsStr;
    use std::os::unix::ffi::OsStrExt;

    let mut entries = Dir::read_from(&directory).map_err(io::Error::from)?;
    let mut names = Vec::new();
    for entry in &mut entries {
        let entry = entry.map_err(io::Error::from)?;
        let bytes = entry.file_name().to_bytes();
        if bytes != b"." && bytes != b".." {
            names.push(OsStr::from_bytes(bytes).to_os_string());
        }
    }
    names.sort();

    for name in names {
        let child_relative = relative.join(&name);
        let child = openat(
            &directory,
            Path::new(&name),
            OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NOFOLLOW | OFlags::NONBLOCK,
            Mode::empty(),
        )
        .map_err(io::Error::from)?;
        match FileType::from_raw_mode(fstat(&child).map_err(io::Error::from)?.st_mode) {
            FileType::Directory => {
                if include(&child_relative, true)? {
                    on_directory(&child_relative)?;
                    walk_unix_directory(child, &child_relative, include, on_directory, on_file)?;
                }
            }
            FileType::RegularFile => {
                if include(&child_relative, false)? {
                    on_file(&child_relative, &mut std::fs::File::from(child))?;
                }
            }
            _ => {
                return Err(crate::MongrelError::InvalidArgument(format!(
                    "refuses non-regular entry {}",
                    child_relative.display()
                )));
            }
        }
    }
    Ok(())
}

#[cfg(windows)]
fn open_windows_nofollow(path: &Path) -> io::Result<std::fs::File> {
    use std::os::windows::fs::OpenOptionsExt;
    use windows_sys::Win32::Storage::FileSystem::{
        FILE_FLAG_BACKUP_SEMANTICS, FILE_FLAG_OPEN_REPARSE_POINT, FILE_SHARE_READ, FILE_SHARE_WRITE,
    };

    let mut options = std::fs::OpenOptions::new();
    options
        .read(true)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS | FILE_FLAG_OPEN_REPARSE_POINT);
    options.open(path)
}

#[cfg(windows)]
fn ensure_windows_not_reparse(
    file: &std::fs::File,
    path: &Path,
) -> crate::Result<std::fs::Metadata> {
    use std::os::windows::fs::MetadataExt;
    use windows_sys::Win32::Storage::FileSystem::FILE_ATTRIBUTE_REPARSE_POINT;

    let metadata = file.metadata()?;
    if metadata.file_attributes() & FILE_ATTRIBUTE_REPARSE_POINT != 0 {
        return Err(crate::MongrelError::InvalidArgument(format!(
            "refuses reparse point {}",
            path.display()
        )));
    }
    Ok(metadata)
}

#[cfg(windows)]
fn ensure_windows_regular(file: &std::fs::File, path: &Path) -> crate::Result<()> {
    if !ensure_windows_not_reparse(file, path)?.is_file() {
        return Err(crate::MongrelError::InvalidArgument(format!(
            "refuses non-regular file {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(windows)]
fn ensure_windows_directory(file: &std::fs::File, path: &Path) -> crate::Result<()> {
    if !ensure_windows_not_reparse(file, path)?.is_dir() {
        return Err(crate::MongrelError::InvalidArgument(format!(
            "refuses non-directory {}",
            path.display()
        )));
    }
    Ok(())
}

#[cfg(windows)]
fn walk_windows_directory<P, D, F>(
    path: &Path,
    relative: &Path,
    _directory: std::fs::File,
    include: &mut P,
    on_directory: &mut D,
    on_file: &mut F,
) -> crate::Result<()>
where
    P: FnMut(&Path, bool) -> crate::Result<bool>,
    D: FnMut(&Path) -> crate::Result<()>,
    F: FnMut(&Path, &mut std::fs::File) -> crate::Result<()>,
{
    let mut entries = std::fs::read_dir(path)?.collect::<io::Result<Vec<_>>>()?;
    entries.sort_by_key(std::fs::DirEntry::file_name);
    for entry in entries {
        let child_path = entry.path();
        let child_relative = relative.join(entry.file_name());
        let mut child = open_windows_nofollow(&child_path)?;
        let metadata = ensure_windows_not_reparse(&child, &child_path)?;
        if metadata.is_dir() {
            if include(&child_relative, true)? {
                on_directory(&child_relative)?;
                walk_windows_directory(
                    &child_path,
                    &child_relative,
                    child,
                    include,
                    on_directory,
                    on_file,
                )?;
            }
        } else if metadata.is_file() {
            if include(&child_relative, false)? {
                on_file(&child_relative, &mut child)?;
            }
        } else {
            return Err(crate::MongrelError::InvalidArgument(format!(
                "refuses non-regular entry {}",
                child_path.display()
            )));
        }
    }
    Ok(())
}

fn parent_or_current(path: &Path) -> &Path {
    path.parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
}

/// Recursively create a directory tree, durably linking every new component.
pub(crate) fn create_directory_all(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        return sync_parent(path);
    }
    if path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} exists and is not a directory", path.display()),
        ));
    }
    let parent = parent_or_current(path);
    if !parent.is_dir() {
        create_directory_all(parent)?;
    }
    create_directory(path)
}

/// Create one directory and make its link in the parent durable. Existing
/// directories are accepted, but non-directory entries are rejected.
pub(crate) fn create_directory(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        return sync_parent(path);
    }
    if path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{} exists and is not a directory", path.display()),
        ));
    }

    #[cfg(unix)]
    {
        match std::fs::create_dir(path) {
            Ok(()) => sync_parent(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && path.is_dir() => {
                sync_parent(path)
            }
            Err(error) => Err(error),
        }
    }

    #[cfg(windows)]
    {
        let parent = parent_or_current(path);
        let stage = parent.join(format!(
            ".mongreldb-dir-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));
        std::fs::create_dir(&stage)?;
        match rename(&stage, path) {
            Ok(()) => Ok(()),
            Err(error) if path.is_dir() => {
                let _ = std::fs::remove_dir(&stage);
                Ok(())
            }
            Err(error) => {
                let _ = std::fs::remove_dir(&stage);
                Err(error)
            }
        }
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = path;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "durable directory creation is unsupported on this platform",
        ))
    }
}

/// Remove a directory tree and make removal of its top-level link durable.
pub(crate) fn remove_directory_all(path: &Path) -> io::Result<()> {
    remove_directory_all_with_after(path, || {})
}

fn remove_directory_all_with_after<F>(path: &Path, after_unlink: F) -> io::Result<()>
where
    F: FnOnce(),
{
    match std::fs::remove_dir_all(path) {
        Ok(()) => {
            after_unlink();
            sync_parent(path)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            // A retry after a prior unlink with an inconclusive parent fsync
            // must finish that fsync before reporting durable cleanup.
            after_unlink();
            sync_parent(path)
        }
        Err(error) => Err(error),
    }
}

/// Write an authoritative file through a unique synced temporary and durable
/// atomic replacement.
#[cfg_attr(not(feature = "encryption"), allow(dead_code))]
pub(crate) fn write_atomic(path: &Path, bytes: &[u8]) -> io::Result<()> {
    write_atomic_with_after(path, bytes, || {})
}

/// Write an authoritative file atomically, invoking `after_publish` once the
/// replacement is visible and before a later directory-sync error is returned.
pub(crate) fn write_atomic_with_after<F>(
    path: &Path,
    bytes: &[u8],
    after_publish: F,
) -> io::Result<()>
where
    F: FnOnce(),
{
    let parent = parent_or_current(path);
    if !parent.is_dir() {
        create_directory_all(parent)?;
    }
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{} has no file name", path.display()),
        )
    })?;
    let root = DurableRoot::open(parent)?;
    root.write_atomic_with_after(Path::new(file_name), bytes, after_publish)
}

/// Rename an entry without replacement and durably publish the source removal
/// and destination link.
pub(crate) fn rename(source: &Path, destination: &Path) -> io::Result<()> {
    rename_with_after(source, destination, || {})
}

/// Rename without replacement, invoking `after_publish` once the destination
/// is visible and before any later Unix directory-sync failure is returned.
pub(crate) fn rename_with_after<F>(
    source: &Path,
    destination: &Path,
    after_publish: F,
) -> io::Result<()>
where
    F: FnOnce(),
{
    #[cfg(all(
        unix,
        any(
            target_os = "linux",
            target_os = "android",
            target_vendor = "apple",
            target_os = "redox"
        )
    ))]
    {
        use rustix::fs::{renameat_with, RenameFlags, CWD};

        renameat_with(CWD, source, CWD, destination, RenameFlags::NOREPLACE)
            .map_err(io::Error::from)?;
        after_publish();
        if source.parent() == destination.parent() {
            sync_parent(destination)?;
        } else {
            // Publish the only remaining name before making removal of the old
            // name durable. A crash may leave both names, never neither.
            sync_parent(destination)?;
            sync_parent(source)?;
        }
        Ok(())
    }

    #[cfg(all(
        unix,
        not(any(
            target_os = "linux",
            target_os = "android",
            target_vendor = "apple",
            target_os = "redox"
        ))
    ))]
    {
        let _ = (source, destination, after_publish);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "atomic no-replace rename is unsupported on this platform",
        ))
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{MoveFileExW, MOVEFILE_WRITE_THROUGH};

        let source = source
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let destination = destination
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let result = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }
        after_publish();
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (source, destination, after_publish);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "durable rename is unsupported on this platform",
        ))
    }
}

/// Flush directory metadata on every supported platform.
pub(crate) fn sync_directory(path: &Path) -> io::Result<()> {
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

fn sync_parent(path: &Path) -> io::Result<()> {
    sync_directory(parent_or_current(path))
}

/// Atomically replace `destination` and make the directory entry durable.
pub(crate) fn replace(source: &Path, destination: &Path) -> io::Result<()> {
    replace_with_after(source, destination, || {})
}

/// Replace a file, invoking `after_publish` once the replacement is visible.
/// On Unix, a later parent-directory fsync error is returned after the callback.
pub(crate) fn replace_with_after<F>(
    source: &Path,
    destination: &Path,
    after_publish: F,
) -> io::Result<()>
where
    F: FnOnce(),
{
    #[cfg(unix)]
    {
        std::fs::rename(source, destination)?;
        after_publish();
        std::fs::File::open(destination.parent().unwrap_or_else(|| Path::new(".")))?.sync_all()
    }

    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        use windows_sys::Win32::Storage::FileSystem::{
            MoveFileExW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
        };

        let source = source
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let destination = destination
            .as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect::<Vec<_>>();
        let result = unsafe {
            MoveFileExW(
                source.as_ptr(),
                destination.as_ptr(),
                MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
            )
        };
        if result == 0 {
            return Err(io::Error::last_os_error());
        }
        after_publish();
        Ok(())
    }

    #[cfg(not(any(unix, windows)))]
    {
        let _ = (source, destination, after_publish);
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "durable atomic replacement is unsupported on this platform",
        ))
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use std::io::Read;
    use std::os::unix::fs::symlink;
    use std::sync::{Arc, Barrier};

    #[test]
    fn no_follow_file_and_tree_reject_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::NamedTempFile::new().unwrap();
        let link = root.path().join("escape");
        symlink(outside.path(), &link).unwrap();

        assert!(open_regular_nofollow(&link).is_err());
        let result = walk_regular_files_nofollow(
            root.path(),
            |_, _| Ok(true),
            |_| Ok(()),
            |_, file| {
                let mut bytes = Vec::new();
                file.read_to_end(&mut bytes)?;
                Ok(())
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn descriptor_root_rejects_intermediate_symlinks() {
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::create_dir(root.path().join("state")).unwrap();
        std::fs::write(outside.path().join("receipt"), b"outside").unwrap();
        symlink(outside.path(), root.path().join("state").join("escape")).unwrap();
        let durable = DurableRoot::open(root.path()).unwrap();

        assert!(durable.open_regular("state/escape/receipt").is_err());
        assert!(durable.write_new("state/escape/new", b"bad").is_err());
        assert!(!outside.path().join("new").exists());
    }

    #[test]
    fn atomic_write_ignores_orphaned_fixed_temporary_name() {
        let root = tempfile::tempdir().unwrap();
        let destination = root.path().join("state");
        let orphan = root.path().join(".state.tmp");
        std::fs::write(&orphan, b"orphan").unwrap();

        write_atomic(&destination, b"published").unwrap();

        assert_eq!(std::fs::read(destination).unwrap(), b"published");
        assert_eq!(std::fs::read(orphan).unwrap(), b"orphan");
    }

    #[test]
    fn descriptor_atomic_write_reports_visible_publication_before_return() {
        let root = tempfile::tempdir().unwrap();
        let durable = DurableRoot::open(root.path()).unwrap();
        let published = std::cell::Cell::new(false);

        durable
            .write_atomic_with_after("state", b"published", || {
                assert_eq!(
                    std::fs::read(root.path().join("state")).unwrap(),
                    b"published"
                );
                published.set(true);
            })
            .unwrap();

        assert!(published.get());
    }

    #[test]
    fn parent_sync_failure_is_not_swallowed_after_publication() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("parent");
        let moved = root.path().join("moved");
        std::fs::create_dir(&parent).unwrap();
        let source = parent.join("source");
        let destination = parent.join("destination");
        std::fs::write(&source, b"durable").unwrap();

        let error = replace_with_after(&source, &destination, || {
            std::fs::rename(&parent, &moved).unwrap();
        })
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert_eq!(
            std::fs::read(moved.join("destination")).unwrap(),
            b"durable"
        );
    }

    #[test]
    fn cross_directory_rename_syncs_destination_before_source() {
        let root = tempfile::tempdir().unwrap();
        let source_parent = root.path().join("source-parent");
        let destination_parent = root.path().join("destination-parent");
        let moved_source_parent = root.path().join("moved-source-parent");
        std::fs::create_dir(&source_parent).unwrap();
        std::fs::create_dir(&destination_parent).unwrap();
        let source = source_parent.join("source");
        let destination = destination_parent.join("destination");
        std::fs::write(&source, b"durable").unwrap();

        let error = rename_with_after(&source, &destination, || {
            std::fs::rename(&source_parent, &moved_source_parent).unwrap();
        })
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
        assert_eq!(std::fs::read(&destination).unwrap(), b"durable");
    }

    #[test]
    fn no_replace_rename_never_clobbers_a_racing_destination() {
        for iteration in 0..128 {
            let root = tempfile::tempdir().unwrap();
            let source = root.path().join("source");
            let destination = root.path().join("destination");
            std::fs::write(&source, b"source").unwrap();
            let barrier = Arc::new(Barrier::new(3));

            let create_barrier = Arc::clone(&barrier);
            let create_destination = destination.clone();
            let creator = std::thread::spawn(move || {
                create_barrier.wait();
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(create_destination)
                    .map(|mut file| file.write_all(b"racer"))
            });

            let rename_barrier = Arc::clone(&barrier);
            let rename_source = source.clone();
            let rename_destination = destination.clone();
            let renamer = std::thread::spawn(move || {
                rename_barrier.wait();
                rename(&rename_source, &rename_destination)
            });

            barrier.wait();
            let create_result = creator.join().unwrap();
            let rename_result = renamer.join().unwrap();
            let destination_bytes = std::fs::read(&destination).unwrap();

            match (create_result, rename_result) {
                (Ok(Ok(())), Err(error)) => {
                    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
                    assert_eq!(destination_bytes, b"racer", "iteration {iteration}");
                    assert_eq!(std::fs::read(&source).unwrap(), b"source");
                }
                (Err(error), Ok(())) => {
                    assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
                    assert_eq!(destination_bytes, b"source", "iteration {iteration}");
                    assert!(!source.exists());
                }
                (create_result, rename_result) => panic!(
                    "unexpected race results at iteration {iteration}: create={create_result:?}, rename={rename_result:?}"
                ),
            }
        }
    }

    #[test]
    fn missing_directory_retry_still_reports_parent_sync_failure() {
        let root = tempfile::tempdir().unwrap();
        let parent = root.path().join("parent");
        let moved_parent = root.path().join("moved-parent");
        std::fs::create_dir(&parent).unwrap();
        let missing = parent.join("already-removed");

        let error = remove_directory_all_with_after(&missing, || {
            std::fs::rename(&parent, &moved_parent).unwrap();
        })
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }
}
