//! Replication bootstrap image and follower metadata.

use crate::{MongrelError, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::collections::HashSet;
use std::io::{self, Read, Write};
use std::path::{Component, Path, PathBuf};

const FORMAT_VERSION: u16 = 2;
const REPLICA_MARKER: &str = "replica";
const REPLICA_EPOCH: &str = "repl_epoch";
const REPLICATION_ID: &str = "replication_id";
const REPLICATION_SOURCE_ID: &str = "replication_source_id";
const REPLICATION_WAL_FLOOR: &str = "replication_wal_floor";
const REPLICATION_PROOF_DOMAIN: &[u8] = b"mongreldb-replication-batch-v2\0";
const REPLICATION_ID_LEN: usize = 32;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub(crate) struct ReplicationFile {
    path: PathBuf,
    data: Vec<u8>,
}

impl ReplicationFile {
    pub(crate) fn new(path: PathBuf, data: Vec<u8>) -> Self {
        Self { path, data }
    }
}

pub(crate) fn capture_files(root: &Path) -> Result<Vec<ReplicationFile>> {
    let mut files = Vec::new();
    crate::durable_file::walk_regular_files_nofollow(
        root,
        |relative, _| {
            Ok(!(relative == Path::new("_meta/.lock")
                || relative == Path::new("_meta/replica")
                || relative == Path::new("_meta/repl_epoch")
                || relative == Path::new("_meta/replication_id")
                || relative == Path::new("_meta/replication_source_id")
                || relative == Path::new("_meta/replication_wal_floor")
                || relative.components().any(
                    |component| matches!(component, Component::Normal(name) if name == "_cache"),
                )))
        },
        |_| Ok(()),
        |relative, file| {
            let mut data = Vec::new();
            file.read_to_end(&mut data)?;
            files.push(ReplicationFile::new(relative.to_path_buf(), data));
            Ok(())
        },
    )?;
    files.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(files)
}

/// A consistent database-directory image plus the leader commit epoch it
/// covers. The image is opaque to HTTP; encode/decode use the core's versioned
/// bincode envelope so server and client share one format.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplicationSnapshot {
    version: u16,
    source_id: [u8; REPLICATION_ID_LEN],
    epoch: u64,
    files: Vec<ReplicationFile>,
}

/// Complete committed WAL transactions available after a follower epoch.
#[derive(Debug, Clone)]
pub struct ReplicationBatch {
    pub source_id: [u8; REPLICATION_ID_LEN],
    source_bound: bool,
    pub from_epoch: u64,
    pub current_epoch: u64,
    pub earliest_epoch: Option<u64>,
    pub requires_snapshot: bool,
    pub retention_gap: bool,
    pub contains_spilled_commits: bool,
    pub commit_count: u64,
    pub records_sha256: [u8; 32],
    pub records: Vec<crate::wal::Record>,
}

impl ReplicationBatch {
    pub(crate) fn is_source_bound(&self) -> bool {
        self.source_bound
    }

    pub(crate) fn complete(
        from_epoch: u64,
        current_epoch: u64,
        earliest_epoch: Option<u64>,
        retention_gap: bool,
        contains_spilled_commits: bool,
        records: Vec<crate::wal::Record>,
    ) -> Result<Self> {
        Self::complete_inner(
            None,
            from_epoch,
            current_epoch,
            earliest_epoch,
            retention_gap,
            contains_spilled_commits,
            records,
        )
    }

    pub(crate) fn complete_for_source(
        source_id: [u8; REPLICATION_ID_LEN],
        from_epoch: u64,
        current_epoch: u64,
        earliest_epoch: Option<u64>,
        retention_gap: bool,
        contains_spilled_commits: bool,
        records: Vec<crate::wal::Record>,
    ) -> Result<Self> {
        Self::complete_inner(
            Some(source_id),
            from_epoch,
            current_epoch,
            earliest_epoch,
            retention_gap,
            contains_spilled_commits,
            records,
        )
    }

    fn complete_inner(
        source_id: Option<[u8; REPLICATION_ID_LEN]>,
        from_epoch: u64,
        current_epoch: u64,
        earliest_epoch: Option<u64>,
        retention_gap: bool,
        contains_spilled_commits: bool,
        records: Vec<crate::wal::Record>,
    ) -> Result<Self> {
        let commit_count = records
            .iter()
            .filter(|record| matches!(record.op, crate::wal::Op::TxnCommit { .. }))
            .count() as u64;
        let source_bound = source_id.is_some();
        let source_id = source_id.unwrap_or([0; REPLICATION_ID_LEN]);
        if source_bound {
            validate_source_id(source_id)?;
        }
        let records_sha256 =
            replication_records_sha256(source_id, from_epoch, current_epoch, &records)?;
        Ok(Self {
            source_id,
            source_bound,
            from_epoch,
            current_epoch,
            earliest_epoch,
            requires_snapshot: retention_gap || contains_spilled_commits,
            retention_gap,
            contains_spilled_commits,
            commit_count,
            records_sha256,
            records,
        })
    }

    pub fn from_wire(
        source_id: [u8; REPLICATION_ID_LEN],
        from_epoch: u64,
        current_epoch: u64,
        earliest_epoch: Option<u64>,
        commit_count: u64,
        records_sha256: [u8; 32],
        records: Vec<crate::wal::Record>,
    ) -> Self {
        let contains_spilled_commits = records.iter().any(|record| {
            matches!(
                &record.op,
                crate::wal::Op::TxnCommit { added_runs, .. } if !added_runs.is_empty()
            )
        });
        Self {
            source_id,
            source_bound: true,
            from_epoch,
            current_epoch,
            earliest_epoch,
            requires_snapshot: contains_spilled_commits,
            retention_gap: false,
            contains_spilled_commits,
            commit_count,
            records_sha256,
            records,
        }
    }

    pub(crate) fn validate_proof(&self) -> Result<()> {
        if self.source_bound {
            validate_source_id(self.source_id)?;
        }
        let actual_count = self
            .records
            .iter()
            .filter(|record| matches!(record.op, crate::wal::Op::TxnCommit { .. }))
            .count() as u64;
        if actual_count != self.commit_count {
            return Err(MongrelError::InvalidArgument(format!(
                "replication commit count mismatch: expected {}, received {actual_count}",
                self.commit_count
            )));
        }
        let digest = replication_records_sha256(
            self.source_id,
            self.from_epoch,
            self.current_epoch,
            &self.records,
        )?;
        if digest != self.records_sha256 {
            return Err(MongrelError::InvalidArgument(
                "replication batch digest mismatch".into(),
            ));
        }
        Ok(())
    }
}

fn replication_records_sha256(
    source_id: [u8; REPLICATION_ID_LEN],
    from_epoch: u64,
    current_epoch: u64,
    records: &[crate::wal::Record],
) -> Result<[u8; 32]> {
    let mut digest = Sha256::new();
    digest.update(REPLICATION_PROOF_DOMAIN);
    digest.update(source_id);
    digest.update(from_epoch.to_le_bytes());
    digest.update(current_epoch.to_le_bytes());
    digest.update((records.len() as u64).to_le_bytes());
    for record in records {
        let encoded = bincode::serialize(record)?;
        digest.update((encoded.len() as u64).to_le_bytes());
        digest.update(encoded);
    }
    Ok(digest.finalize().into())
}

impl ReplicationSnapshot {
    pub(crate) fn new(
        source_id: [u8; REPLICATION_ID_LEN],
        epoch: u64,
        files: Vec<ReplicationFile>,
    ) -> Self {
        Self {
            version: FORMAT_VERSION,
            source_id,
            epoch,
            files,
        }
    }

    pub fn source_id(&self) -> [u8; REPLICATION_ID_LEN] {
        self.source_id
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    pub fn encode(&self) -> Result<Vec<u8>> {
        Ok(bincode::serialize(self)?)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {
        let snapshot: Self = bincode::deserialize(bytes)?;
        if snapshot.version != FORMAT_VERSION {
            return Err(MongrelError::InvalidArgument(format!(
                "unsupported replication snapshot version {}",
                snapshot.version
            )));
        }
        validate_source_id(snapshot.source_id)?;
        Ok(snapshot)
    }

    /// Atomically replace `destination` with this snapshot and mark it as a
    /// read-only replica. Files are first written and fsynced in a sibling
    /// staging directory; an existing destination is retained until install.
    pub fn install(&self, destination: impl AsRef<Path>) -> Result<()> {
        self.install_validated(destination, |stage| {
            let database = crate::Database::open(stage)?;
            drop(database);
            Ok(())
        })
    }

    /// Install after the caller has semantically opened and validated the
    /// staged database. Authenticated or encrypted followers must use this
    /// form so their credentials/key are checked before the working replica is
    /// renamed away.
    pub fn install_validated<F>(
        &self,
        destination: impl AsRef<Path>,
        validate_stage: F,
    ) -> Result<()>
    where
        F: FnOnce(&Path) -> Result<()>,
    {
        validate_source_id(self.source_id)?;
        let destination = destination.as_ref();
        let parent = destination
            .parent()
            .filter(|path| !path.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        crate::durable_file::create_directory_all(parent)?;
        if destination.exists() {
            let metadata = std::fs::symlink_metadata(destination)?;
            if metadata.file_type().is_symlink() || !metadata.is_dir() {
                return Err(MongrelError::InvalidArgument(format!(
                    "replica destination is not a directory: {}",
                    destination.display()
                )));
            }
            if !is_replica(destination) {
                return Err(MongrelError::Conflict(format!(
                    "refusing to replace non-replica destination {}",
                    destination.display()
                )));
            }
            match replica_source_id(destination) {
                Ok(current_source) if current_source != self.source_id => {
                    return Err(MongrelError::Conflict(
                        "replication snapshot source does not match destination binding".into(),
                    ))
                }
                Ok(_) | Err(MongrelError::NotFound(_)) => {
                    // A pre-v2 replica has no source marker. Only a complete
                    // staged snapshot install may establish that binding; WAL
                    // apply never does so.
                }
                Err(error) => return Err(error),
            }
            let current_epoch = replica_epoch(destination)?;
            if self.epoch < current_epoch {
                return Err(MongrelError::Conflict(format!(
                    "replication snapshot epoch {} precedes destination epoch {current_epoch}",
                    self.epoch
                )));
            }
        }
        let name = destination
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| MongrelError::InvalidArgument("invalid replica destination".into()))?;
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let stage = parent.join(format!(
            ".{name}.replica-stage-{}-{nonce}",
            std::process::id()
        ));
        let validation = parent.join(format!(
            ".{name}.replica-validate-{}-{nonce}",
            std::process::id()
        ));
        let backup = parent.join(format!(
            ".{name}.replica-old-{}-{nonce}",
            std::process::id()
        ));

        if stage.exists() || validation.exists() || backup.exists() {
            return Err(MongrelError::Conflict(
                "replication staging path already exists".into(),
            ));
        }
        crate::durable_file::create_directory(&stage)?;
        if let Err(error) = self.write_into(&stage) {
            if let Err(cleanup) = remove_directory(&stage) {
                return Err(MongrelError::Other(format!(
                    "{error}; failed to remove replication staging directory: {cleanup}"
                )));
            }
            return Err(error);
        }
        // Database open performs durable recovery bookkeeping. Validate an
        // identical disposable tree so those writes cannot alter the exact
        // snapshot that will be published.
        let validation_write = (|| -> Result<()> {
            crate::durable_file::create_directory(&validation)?;
            self.write_into(&validation)
        })();
        if let Err(error) = validation_write {
            let validation_cleanup = validation
                .exists()
                .then(|| remove_directory(&validation))
                .transpose();
            let stage_cleanup = remove_directory(&stage);
            if let Err(cleanup) = validation_cleanup.and(stage_cleanup) {
                return Err(MongrelError::Other(format!(
                    "{error}; failed to remove replication validation trees: {cleanup}"
                )));
            }
            return Err(error);
        }
        if let Err(error) = validate_stage(&validation) {
            let validation_cleanup = remove_directory(&validation);
            let stage_cleanup = remove_directory(&stage);
            if let Err(cleanup) = validation_cleanup.and(stage_cleanup) {
                return Err(MongrelError::Other(format!(
                    "{error}; failed to remove invalid replication staging directories: {cleanup}"
                )));
            }
            return Err(error);
        }
        if let Err(error) = remove_directory(&validation) {
            if let Err(cleanup) = remove_directory(&stage) {
                return Err(MongrelError::Other(format!(
                    "failed to remove replication validation directory: {error}; failed to remove staging directory: {cleanup}"
                )));
            }
            return Err(error.into());
        }

        let had_destination = destination.exists();
        if had_destination {
            if let Err(failure) = rename_entry(destination, &backup) {
                if failure.published {
                    if let Err(rollback) = rename_entry(&backup, destination) {
                        return Err(uncertain_install_error(
                            self.epoch,
                            &failure.error,
                            &rollback.error,
                        ));
                    }
                }
                if let Err(cleanup) = remove_directory(&stage) {
                    return Err(MongrelError::Other(format!(
                        "{}; previous replica was restored, but staging cleanup failed: {cleanup}",
                        failure.error
                    )));
                }
                return Err(failure.error.into());
            }
        }

        if let Err(failure) = rename_entry(&stage, destination) {
            if failure.published {
                match rename_entry(destination, &stage) {
                    Ok(()) => {}
                    Err(rollback) if rollback.published => {
                        // Restoring the old destination below syncs the same
                        // parent and therefore also publishes this move.
                    }
                    Err(rollback) => {
                        return Err(uncertain_install_error(
                            self.epoch,
                            &failure.error,
                            &rollback.error,
                        ));
                    }
                }
            }
            if had_destination {
                if let Err(rollback) = rename_entry(&backup, destination) {
                    return Err(uncertain_install_error(
                        self.epoch,
                        &failure.error,
                        &rollback.error,
                    ));
                }
            }
            if stage.exists() {
                if let Err(cleanup) = remove_directory(&stage) {
                    return Err(MongrelError::Other(format!(
                        "{}; previous destination was restored, but staging cleanup failed: {cleanup}",
                        failure.error
                    )));
                }
            }
            return Err(failure.error.into());
        }

        if had_destination {
            if let Err(error) = remove_directory(&backup) {
                return Err(MongrelError::Other(format!(
                    "replication snapshot at epoch {} is installed, but old snapshot cleanup failed: {error}",
                    self.epoch
                )));
            }
        }
        Ok(())
    }

    fn write_into(&self, root: &Path) -> Result<()> {
        let mut seen = HashSet::new();
        let mut table_directories = HashSet::new();
        for file in &self.files {
            validate_relative_path(&file.path)?;
            if !seen.insert(file.path.clone()) {
                return Err(MongrelError::InvalidArgument(format!(
                    "duplicate replication snapshot path {:?}",
                    file.path
                )));
            }
            if file.path == Path::new("_meta/replica")
                || file.path == Path::new("_meta/repl_epoch")
                || file.path == Path::new("_meta/replication_id")
                || file.path == Path::new("_meta/replication_source_id")
                || file.path == Path::new("_meta/replication_wal_floor")
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "reserved replication snapshot path {:?}",
                    file.path
                )));
            }
            if let Ok(relative) = file.path.strip_prefix("tables") {
                if let Some(Component::Normal(table_id)) = relative.components().next() {
                    table_directories.insert(Path::new("tables").join(table_id));
                }
            }
        }
        for file in &self.files {
            let path = root.join(&file.path);
            let parent = path.parent().expect("validated file has parent");
            crate::durable_file::create_directory_all(parent)?;
            let mut output = std::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&path)?;
            output.write_all(&file.data)?;
            output.sync_all()?;
            crate::durable_file::sync_directory(parent)?;
        }
        // Empty run directories carry no file entry in the snapshot, but every
        // mounted table requires `_runs` to exist during semantic open.
        for table in table_directories {
            crate::durable_file::create_directory_all(&root.join(table).join("_runs"))?;
        }
        if !root.join(crate::catalog::CATALOG_FILENAME).is_file() {
            return Err(MongrelError::InvalidArgument(
                "replication snapshot has no CATALOG".into(),
            ));
        }
        let meta = root.join("_meta");
        crate::durable_file::create_directory_all(&meta)?;
        write_new_synced(&meta.join(REPLICA_MARKER), b"read-only replica\n")?;
        write_new_synced(&meta.join(REPLICATION_ID), &self.source_id)?;
        write_new_synced(&meta.join(REPLICATION_SOURCE_ID), &self.source_id)?;
        write_replica_epoch(root, self.epoch)?;
        crate::durable_file::sync_directory(&meta)?;
        crate::durable_file::sync_directory(root)?;
        Ok(())
    }
}

pub fn is_replica(root: impl AsRef<Path>) -> bool {
    root.as_ref().join("_meta").join(REPLICA_MARKER).is_file()
}

fn validate_source_id(source_id: [u8; REPLICATION_ID_LEN]) -> Result<()> {
    if source_id.iter().all(|byte| *byte == 0) {
        return Err(MongrelError::InvalidArgument(
            "replication source identity must not be zero".into(),
        ));
    }
    Ok(())
}

fn read_source_id_file(mut file: std::fs::File, label: &str) -> Result<[u8; REPLICATION_ID_LEN]> {
    let length = file.metadata()?.len();
    if length != REPLICATION_ID_LEN as u64 {
        return Err(MongrelError::InvalidArgument(format!(
            "invalid {label} length {length}; expected {REPLICATION_ID_LEN}"
        )));
    }
    let mut source_id = [0_u8; REPLICATION_ID_LEN];
    file.read_exact(&mut source_id)?;
    validate_source_id(source_id)?;
    Ok(source_id)
}

pub fn replica_source_id(root: impl AsRef<Path>) -> Result<[u8; REPLICATION_ID_LEN]> {
    let path = root.as_ref().join("_meta").join(REPLICATION_SOURCE_ID);
    let file = match crate::durable_file::open_regular_nofollow(&path) {
        Ok(file) => file,
        Err(MongrelError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
            return Err(MongrelError::NotFound(format!(
                "{}: {error}",
                path.display()
            )))
        }
        Err(error) => return Err(error),
    };
    read_source_id_file(file, "replication source identity")
}

pub(crate) fn replication_identity_durable(
    root: &crate::durable_file::DurableRoot,
) -> Result<[u8; REPLICATION_ID_LEN]> {
    let relative = Path::new("_meta").join(REPLICATION_ID);
    match root.open_regular(&relative) {
        Ok(file) => return read_source_id_file(file, "database replication identity"),
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    root.create_directory_all("_meta")?;
    let mut source_id = [0_u8; REPLICATION_ID_LEN];
    loop {
        getrandom::getrandom(&mut source_id)
            .map_err(|error| MongrelError::EntropyUnavailable(error.to_string()))?;
        if source_id.iter().any(|byte| *byte != 0) {
            break;
        }
    }
    match root.write_new(&relative, &source_id) {
        Ok(()) => Ok(source_id),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => read_source_id_file(
            root.open_regular(&relative)?,
            "database replication identity",
        ),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn replica_source_id_durable(
    root: &crate::durable_file::DurableRoot,
) -> Result<[u8; REPLICATION_ID_LEN]> {
    read_source_id_file(
        root.open_regular(Path::new("_meta").join(REPLICATION_SOURCE_ID))?,
        "replication source identity",
    )
}

pub fn replica_epoch(root: impl AsRef<Path>) -> Result<u64> {
    let path = root.as_ref().join("_meta").join(REPLICA_EPOCH);
    let value = std::fs::read_to_string(&path)
        .map_err(|error| MongrelError::NotFound(format!("{}: {error}", path.display())))?;
    value.trim().parse().map_err(|error| {
        MongrelError::InvalidArgument(format!(
            "invalid replica epoch in {}: {error}",
            path.display()
        ))
    })
}

pub fn write_replica_epoch(root: impl AsRef<Path>, epoch: u64) -> Result<()> {
    match replica_epoch(root.as_ref()) {
        Ok(current) if epoch < current => {
            return Err(MongrelError::Conflict(format!(
                "replica epoch cannot move backward from {current} to {epoch}"
            )))
        }
        Ok(current) if epoch == current => return Ok(()),
        Ok(_) | Err(MongrelError::NotFound(_)) => {}
        Err(error) => return Err(error),
    }
    write_meta_u64(root.as_ref(), REPLICA_EPOCH, epoch, "replica epoch")
}

pub(crate) fn reconcile_replica_epoch_durable(
    root: &crate::durable_file::DurableRoot,
    recovered_epoch: u64,
) -> Result<()> {
    let relative = Path::new("_meta").join(REPLICA_EPOCH);
    let current = if root.entry_exists(&relative)? {
        let mut value = String::new();
        root.open_regular(&relative)?.read_to_string(&mut value)?;
        Some(value.trim().parse::<u64>().map_err(|error| {
            MongrelError::InvalidArgument(format!("invalid replica epoch: {error}"))
        })?)
    } else {
        None
    };
    match current {
        Some(current) if recovered_epoch < current => Err(MongrelError::Conflict(format!(
            "recovered replica epoch {recovered_epoch} precedes durable watermark {current}"
        ))),
        Some(current) if recovered_epoch == current => Ok(()),
        _ => {
            root.create_directory_all("_meta")?;
            root.write_atomic(&relative, recovered_epoch.to_string().as_bytes())?;
            Ok(())
        }
    }
}

pub(crate) fn replication_wal_floor(root: impl AsRef<Path>) -> Result<u64> {
    let path = root.as_ref().join("_meta").join(REPLICATION_WAL_FLOOR);
    match std::fs::read_to_string(&path) {
        Ok(value) => value.trim().parse().map_err(|error| {
            MongrelError::InvalidArgument(format!(
                "invalid replication WAL floor in {}: {error}",
                path.display()
            ))
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(0),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn replication_wal_floor_durable(
    root: &crate::durable_file::DurableRoot,
) -> Result<u64> {
    let relative = Path::new("_meta").join(REPLICATION_WAL_FLOOR);
    if !root.entry_exists(&relative)? {
        return Ok(0);
    }
    let mut value = String::new();
    root.open_regular(&relative)?.read_to_string(&mut value)?;
    value.trim().parse().map_err(|error| {
        MongrelError::InvalidArgument(format!("invalid replication WAL floor: {error}"))
    })
}

pub(crate) fn advance_replication_wal_floor_durable(
    root: &crate::durable_file::DurableRoot,
    epoch: u64,
) -> Result<()> {
    if epoch <= replication_wal_floor_durable(root)? {
        return Ok(());
    }
    root.create_directory_all("_meta")?;
    root.write_atomic(
        Path::new("_meta").join(REPLICATION_WAL_FLOOR),
        epoch.to_string().as_bytes(),
    )?;
    Ok(())
}

fn write_meta_u64(root: &Path, name: &str, value: u64, label: &str) -> Result<()> {
    let meta = root.join("_meta");
    crate::durable_file::create_directory_all(&meta)?;
    let path = meta.join(name);
    let temp = meta.join(format!(
        ".{name}.tmp-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos()
    ));
    if let Err(error) = write_new_synced(&temp, value.to_string().as_bytes()) {
        if temp.exists() {
            if let Err(cleanup) = remove_file(&temp) {
                return Err(MongrelError::Other(format!(
                    "{error}; failed to remove replica epoch temporary file: {cleanup}"
                )));
            }
        }
        return Err(error);
    }
    let published = Cell::new(false);
    if let Err(error) =
        crate::durable_file::replace_with_after(&temp, &path, || published.set(true))
    {
        if published.get() {
            return match crate::durable_file::sync_directory(&meta) {
                Ok(()) => Ok(()),
                Err(retry) => Err(MongrelError::Other(format!(
                    "{label} {value} was atomically replaced, but its durability is unknown: {error}; directory sync retry failed: {retry}"
                ))),
            };
        }
        if let Err(cleanup) = remove_file(&temp) {
            return Err(MongrelError::Other(format!(
                "{error}; failed to remove replica epoch temporary file: {cleanup}"
            )));
        }
        return Err(error.into());
    }
    Ok(())
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty()
        || path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(MongrelError::InvalidArgument(format!(
            "unsafe replication snapshot path {:?}",
            path
        )));
    }
    Ok(())
}

fn write_new_synced(path: &Path, bytes: &[u8]) -> Result<()> {
    let mut file = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn remove_directory(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::remove_dir_all(path)?;
    crate::durable_file::sync_directory(parent)
}

fn remove_file(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::remove_file(path)?;
    crate::durable_file::sync_directory(parent)
}

#[derive(Debug)]
struct RenameFailure {
    error: io::Error,
    published: bool,
}

fn rename_entry(source: &Path, destination: &Path) -> std::result::Result<(), RenameFailure> {
    let published = Cell::new(false);
    match crate::durable_file::rename_with_after(source, destination, || published.set(true)) {
        Ok(()) => Ok(()),
        Err(error) if published.get() => match sync_rename_directories(source, destination) {
            Ok(()) => Ok(()),
            Err(retry) => Err(RenameFailure {
                error: io::Error::new(
                    retry.kind(),
                    format!("{error}; directory sync retry failed: {retry}"),
                ),
                published: true,
            }),
        },
        Err(error) => Err(RenameFailure {
            error,
            published: false,
        }),
    }
}

fn sync_rename_directories(source: &Path, destination: &Path) -> io::Result<()> {
    let destination_parent = destination.parent().unwrap_or_else(|| Path::new("."));
    crate::durable_file::sync_directory(destination_parent)?;
    let source_parent = source.parent().unwrap_or_else(|| Path::new("."));
    if source_parent != destination_parent {
        crate::durable_file::sync_directory(source_parent)?;
    }
    Ok(())
}

fn uncertain_install_error(epoch: u64, error: &io::Error, rollback: &io::Error) -> MongrelError {
    MongrelError::Other(format!(
        "replication snapshot install outcome at epoch {epoch} is unknown: {error}; rollback failed: {rollback}"
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_SOURCE_ID: [u8; REPLICATION_ID_LEN] = [7; REPLICATION_ID_LEN];

    fn mark_replica(path: &Path, epoch: u64, include_source: bool) {
        let meta = path.join("_meta");
        std::fs::create_dir_all(&meta).unwrap();
        std::fs::write(meta.join(REPLICA_MARKER), b"read-only replica\n").unwrap();
        std::fs::write(meta.join(REPLICA_EPOCH), epoch.to_string()).unwrap();
        if include_source {
            std::fs::write(meta.join(REPLICATION_SOURCE_ID), TEST_SOURCE_ID).unwrap();
        }
    }

    fn snapshot(epoch: u64, files: Vec<(&str, &[u8])>) -> ReplicationSnapshot {
        ReplicationSnapshot {
            version: FORMAT_VERSION,
            source_id: TEST_SOURCE_ID,
            epoch,
            files: files
                .into_iter()
                .map(|(path, data)| ReplicationFile::new(path.into(), data.to_vec()))
                .collect(),
        }
    }

    #[test]
    fn snapshot_install_rejects_path_traversal() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot = ReplicationSnapshot {
            version: FORMAT_VERSION,
            source_id: TEST_SOURCE_ID,
            epoch: 1,
            files: vec![ReplicationFile::new("../escape".into(), vec![1])],
        };
        assert!(snapshot
            .install_validated(dir.path().join("replica"), |_| Ok(()))
            .is_err());
        assert!(!dir.path().join("escape").exists());
    }

    #[test]
    fn snapshot_install_durably_replaces_existing_destination() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("replica");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("old"), b"old").unwrap();
        mark_replica(&destination, 41, true);

        snapshot(
            42,
            vec![
                (crate::catalog::CATALOG_FILENAME, b"new"),
                ("tables/1/data", b"nested"),
            ],
        )
        .install_validated(&destination, |_| Ok(()))
        .unwrap();

        assert_eq!(
            std::fs::read(destination.join(crate::catalog::CATALOG_FILENAME)).unwrap(),
            b"new"
        );
        assert_eq!(
            std::fs::read(destination.join("tables/1/data")).unwrap(),
            b"nested"
        );
        assert!(!destination.join("old").exists());
        assert_eq!(replica_epoch(&destination).unwrap(), 42);
        assert!(is_replica(&destination));
        let names = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(names, vec![destination.file_name().unwrap()]);
    }

    #[test]
    fn invalid_snapshot_leaves_existing_destination_untouched() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("replica");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("old"), b"old").unwrap();
        mark_replica(&destination, 41, true);
        let invalid = snapshot(
            42,
            vec![
                (crate::catalog::CATALOG_FILENAME, b"new"),
                ("_meta/replica", b"forged"),
            ],
        );

        assert!(invalid.install_validated(&destination, |_| Ok(())).is_err());
        assert_eq!(std::fs::read(destination.join("old")).unwrap(), b"old");
        assert!(!destination.join(crate::catalog::CATALOG_FILENAME).exists());
        let names = std::fs::read_dir(dir.path())
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect::<Vec<_>>();
        assert_eq!(names, vec![destination.file_name().unwrap()]);
    }

    #[test]
    fn replica_epoch_replace_leaves_no_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        write_replica_epoch(dir.path(), 1).unwrap();
        write_replica_epoch(dir.path(), 2).unwrap();

        assert_eq!(replica_epoch(dir.path()).unwrap(), 2);
        let meta = dir.path().join("_meta");
        assert_eq!(
            std::fs::read_dir(meta)
                .unwrap()
                .map(|entry| entry.unwrap().file_name())
                .collect::<Vec<_>>(),
            vec![std::ffi::OsString::from(REPLICA_EPOCH)]
        );
    }

    #[test]
    fn snapshot_rejects_stale_or_foreign_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("replica");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("old"), b"old").unwrap();
        mark_replica(&destination, 50, true);

        let stale = snapshot(49, vec![(crate::catalog::CATALOG_FILENAME, b"new")]);
        assert!(stale
            .install_validated(&destination, |_| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("precedes destination epoch"));

        let mut foreign = snapshot(51, vec![(crate::catalog::CATALOG_FILENAME, b"new")]);
        foreign.source_id = [8; REPLICATION_ID_LEN];
        assert!(foreign
            .install_validated(&destination, |_| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("source does not match"));
        assert_eq!(std::fs::read(destination.join("old")).unwrap(), b"old");
    }

    #[test]
    fn validator_failure_and_legacy_binding_preserve_or_upgrade_destination() {
        let dir = tempfile::tempdir().unwrap();
        let destination = dir.path().join("replica");
        std::fs::create_dir(&destination).unwrap();
        std::fs::write(destination.join("old"), b"old").unwrap();
        mark_replica(&destination, 1, false);
        let next = snapshot(2, vec![(crate::catalog::CATALOG_FILENAME, b"new")]);

        let error = next
            .install_validated(&destination, |_| {
                Err(MongrelError::InvalidArgument("bad staged catalog".into()))
            })
            .unwrap_err();
        assert!(error.to_string().contains("bad staged catalog"));
        assert_eq!(std::fs::read(destination.join("old")).unwrap(), b"old");

        next.install_validated(&destination, |_| Ok(())).unwrap();
        assert_eq!(replica_source_id(&destination).unwrap(), TEST_SOURCE_ID);
        assert_eq!(replica_epoch(&destination).unwrap(), 2);
    }

    #[test]
    fn replica_epoch_never_moves_backward() {
        let dir = tempfile::tempdir().unwrap();
        write_replica_epoch(dir.path(), 2).unwrap();
        assert!(write_replica_epoch(dir.path(), 1)
            .unwrap_err()
            .to_string()
            .contains("cannot move backward"));
        assert_eq!(replica_epoch(dir.path()).unwrap(), 2);
    }

    #[test]
    fn corrupt_semantic_snapshot_never_replaces_working_replica() {
        let dir = tempfile::tempdir().unwrap();
        let leader_path = dir.path().join("leader");
        let destination = dir.path().join("replica");
        let leader = crate::Database::create(&leader_path).unwrap();
        let good = leader.replication_snapshot().unwrap();
        good.install(&destination).unwrap();
        drop(crate::Database::open(&destination).unwrap());

        let mut corrupt = leader.replication_snapshot().unwrap();
        let catalog = corrupt
            .files
            .iter_mut()
            .find(|file| file.path == Path::new(crate::catalog::CATALOG_FILENAME))
            .unwrap();
        catalog.data[0] ^= 0x80;
        assert!(corrupt.install(&destination).is_err());

        let existing = crate::Database::open(&destination).unwrap();
        assert!(existing.is_read_only_replica());
        assert_eq!(replica_epoch(&destination).unwrap(), good.epoch());
    }
}
