//! Cluster backup and PITR (spec section 12.12, Stage 3L).
//!
//! A cluster backup is a validated multi-tablet artifact set plus one
//! published [`ClusterBackupManifest`]. The protocol is strict:
//!
//! ```text
//! 1. Choose cluster backup timestamp.
//! 2. Pin metadata version (meta plane snapshot of tablet descriptors).
//! 3. Ask each tablet for a snapshot covering the timestamp.
//! 4. Archive logs needed to advance to the timestamp.
//! 5. Validate every tablet entry (hashes, coverage).
//! 6. Publish one backup manifest last (atomic rename).
//! ```
//!
//! The manifest is published **last** so a crashed backup never leaves a
//! readable incomplete manifest: restore only trusts a fully published file.
//! Restore creates a new cluster/database identity unless the caller
//! explicitly opts into disaster-recovery identity reuse.
//!
//! # Core-free design
//!
//! This crate never depends on `mongreldb-core`. Snapshot capture and file
//! copy go through the [`BackupSource`] trait so the server/runtime (which
//! can open tablet storage cores) supplies the I/O while this module owns
//! the protocol, validation, and manifest format.
//!
//! # Fault hooks
//!
//! ```text
//! cluster.backup.before          — before any durable side effect
//! cluster.backup.pin             — after meta pin is recorded
//! cluster.backup.tablet          — after each tablet snapshot is written
//! cluster.backup.validate        — after full validation, before publish
//! cluster.backup.publish.before  — immediately before manifest publish
//! cluster.backup.publish.after   — immediately after manifest is durable
//! cluster.backup.after           — end of a successful run
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{ClusterId, DatabaseId, MetadataVersion, RaftGroupId, TabletId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::node::{
    decode_json, encode_json, read_meta_file, write_meta_atomic, ClusterError, MAX_META_BYTES,
};
use crate::tablet::TabletDescriptor;

/// Manifest format version written by this build.
pub const CLUSTER_BACKUP_FORMAT_VERSION: u32 = 1;
/// Oldest format version this build accepts.
pub const MIN_CLUSTER_BACKUP_FORMAT_VERSION: u32 = 1;
/// Manifest filename inside a backup destination directory.
pub const CLUSTER_BACKUP_MANIFEST_FILENAME: &str = "cluster-backup.json";
/// Subdirectory holding per-tablet snapshot artifacts.
pub const TABLET_SNAPSHOTS_DIR: &str = "tablets";
/// Subdirectory holding archived raft log tails.
pub const LOG_ARCHIVE_DIR: &str = "logs";
/// Upper bound on a single artifact file listed in the manifest.
const MAX_ARTIFACT_BYTES: u64 = 512 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Errors of the cluster backup / restore surface.
#[derive(Debug, thiserror::Error)]
pub enum ClusterBackupError {
    /// Caller-supplied parameters failed validation.
    #[error("invalid cluster backup request: {0}")]
    InvalidRequest(&'static str),
    /// A tablet or source failed during capture.
    #[error("cluster backup source error for tablet {tablet_id}: {detail}")]
    Source {
        /// Tablet that failed.
        tablet_id: TabletId,
        /// Underlying detail.
        detail: String,
    },
    /// Validation of a captured entry failed.
    #[error("cluster backup validation failed: {0}")]
    Validation(String),
    /// The backup destination is not usable.
    #[error("cluster backup destination error: {0}")]
    Destination(String),
    /// A durable metadata/manifest file failed verification.
    #[error("cluster backup manifest error: {0}")]
    Manifest(String),
    /// An injected fault aborted the protocol.
    #[error("injected fault at `{0}`")]
    Fault(&'static str),
    /// Cluster metadata I/O failed.
    #[error("cluster backup I/O error: {0}")]
    Io(#[from] io::Error),
    /// Underlying cluster error (encoding helpers).
    #[error(transparent)]
    Cluster(#[from] ClusterError),
}

impl From<mongreldb_fault::Fault> for ClusterBackupError {
    fn from(fault: mongreldb_fault::Fault) -> Self {
        match fault {
            mongreldb_fault::Fault::Injected(name) => ClusterBackupError::Fault(name),
        }
    }
}

// ---------------------------------------------------------------------------
// Manifest types
// ---------------------------------------------------------------------------

/// Encryption / KMS metadata recorded on a cluster backup (spec §12.12).
///
/// The KEK material itself never travels in the manifest; only scheme
/// identifiers and optional key-id references are stored so operators can
/// locate the wrapping key in their KMS.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBackupEncryption {
    /// Key-derivation / wrap scheme (e.g. `"aes-256-gcm-kms"`).
    pub scheme: String,
    /// Optional KMS key id used to wrap the DEK.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kms_key_id: Option<String>,
    /// Optional key version / generation for online rotation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_version: Option<String>,
}

/// One file artifact listed in the manifest (relative path + integrity).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterBackupFile {
    /// Path relative to the backup root.
    pub path: String,
    /// File size in bytes.
    pub bytes: u64,
    /// Lowercase hex SHA-256 of the file contents.
    pub sha256: String,
}

/// Per-tablet entry inside a cluster backup (spec §12.12).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletBackupEntry {
    /// Tablet identity.
    pub tablet_id: TabletId,
    /// Table the tablet belongs to (opaque u64 for core-free manifests).
    pub table_id: u64,
    /// Raft group replicating the tablet.
    pub raft_group_id: RaftGroupId,
    /// Descriptor generation pinned at backup time.
    pub generation: u64,
    /// Snapshot covering the chosen backup timestamp.
    pub snapshot_files: Vec<ClusterBackupFile>,
    /// Archived raft log tail needed to advance to the backup timestamp.
    pub log_archive: Option<ClusterBackupFile>,
    /// Log continuation position after the archived tail
    /// (`term`/`index` of the last included entry; `0/0` when empty).
    pub log_continuation_term: u64,
    /// See [`Self::log_continuation_term`].
    pub log_continuation_index: u64,
    /// HLC commit timestamp covered by the tablet snapshot.
    pub covered_commit_ts: HlcTimestamp,
}

/// The published cluster backup manifest (spec §12.12).
///
/// Published **last** via atomic rename. A destination without this file is
/// an incomplete backup and must not be restored.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClusterBackupManifest {
    /// Manifest format version.
    pub format_version: u32,
    /// Source cluster identity.
    pub cluster_id: ClusterId,
    /// Source database identity.
    pub database_id: DatabaseId,
    /// Meta-plane metadata version pinned for this backup.
    pub meta_version: MetadataVersion,
    /// Chosen cluster backup timestamp (HLC).
    pub backup_ts: HlcTimestamp,
    /// Wall-clock capture time (unix micros) for operator tooling; not used
    /// for correctness.
    pub created_unix_micros: u64,
    /// Per-tablet entries, ordered by tablet id for determinism.
    pub tablets: Vec<TabletBackupEntry>,
    /// Optional encryption/KMS metadata.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<ClusterBackupEncryption>,
    /// SHA-256 over the canonical tablet-entry payload (excluding this field).
    /// Computed at publish time; verified by [`verify_backup`].
    pub content_sha256: String,
}

impl ClusterBackupManifest {
    /// Total bytes of all listed tablet artifacts.
    pub fn total_bytes(&self) -> u64 {
        self.tablets
            .iter()
            .map(|t| {
                let snap: u64 = t.snapshot_files.iter().map(|f| f.bytes).sum();
                let log = t.log_archive.as_ref().map(|f| f.bytes).unwrap_or(0);
                snap + log
            })
            .sum()
    }

    /// Number of tablets in the backup.
    pub fn tablet_count(&self) -> usize {
        self.tablets.len()
    }
}

/// Outcome of a successful cluster backup run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBackupReport {
    /// Destination directory that holds the published manifest.
    pub destination: PathBuf,
    /// The published manifest.
    pub manifest: ClusterBackupManifest,
    /// Number of tablets captured.
    pub tablets: usize,
    /// Total artifact bytes.
    pub bytes: u64,
}

/// Outcome of [`verify_backup`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterBackupVerifyReport {
    /// Manifest structure and content hash held.
    pub manifest_ok: bool,
    /// Every listed file re-hashed and matched.
    pub files_ok: bool,
    /// Files re-checked.
    pub files_checked: usize,
    /// Total bytes re-hashed.
    pub bytes_checked: u64,
    /// Soft findings (non-fatal).
    pub issues: Vec<String>,
}

/// How restore should treat cluster/database identity (spec §12.12).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RestoreIdentityMode {
    /// Mint a fresh cluster id and database id (default, safe).
    NewIdentity,
    /// Keep the source identities — disaster recovery only.
    DisasterRecovery,
}

/// A pure restore plan derived from a verified manifest.
///
/// The plan describes *what* restore must do; actual file materialization is
/// performed by the runtime that owns tablet storage cores.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterRestorePlan {
    /// Identity mode for the restored cluster.
    pub identity_mode: RestoreIdentityMode,
    /// Cluster id the restored deployment will use.
    pub target_cluster_id: ClusterId,
    /// Database id the restored deployment will use.
    pub target_database_id: DatabaseId,
    /// Source cluster id recorded in the backup.
    pub source_cluster_id: ClusterId,
    /// Source database id recorded in the backup.
    pub source_database_id: DatabaseId,
    /// Backup timestamp the restore targets.
    pub backup_ts: HlcTimestamp,
    /// Meta version to re-seed.
    pub meta_version: MetadataVersion,
    /// Per-tablet restore steps, ordered by tablet id.
    pub tablets: Vec<TabletRestoreStep>,
}

/// One tablet's restore step inside a [`ClusterRestorePlan`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TabletRestoreStep {
    /// Tablet identity (preserved; tablet ids are never reused).
    pub tablet_id: TabletId,
    /// Table the tablet belongs to.
    pub table_id: u64,
    /// Raft group id to re-create.
    pub raft_group_id: RaftGroupId,
    /// Descriptor generation at backup time.
    pub generation: u64,
    /// Relative snapshot artifact paths under the backup root.
    pub snapshot_paths: Vec<String>,
    /// Relative log-archive path, if any.
    pub log_archive_path: Option<String>,
    /// Log continuation after archive install.
    pub log_continuation_term: u64,
    /// See [`Self::log_continuation_term`].
    pub log_continuation_index: u64,
    /// Commit timestamp covered by the snapshot.
    pub covered_commit_ts: HlcTimestamp,
}

// ---------------------------------------------------------------------------
// BackupSource trait (keeps cluster free of core)
// ---------------------------------------------------------------------------

/// Snapshot artifact produced by a [`BackupSource`] for one tablet.
#[derive(Debug, Clone)]
pub struct TabletSnapshotArtifact {
    /// Opaque snapshot payload bytes (EngineSnapshot-derived or equivalent).
    pub snapshot_payload: Vec<u8>,
    /// Optional additional named files (relative name → bytes).
    pub extra_files: BTreeMap<String, Vec<u8>>,
    /// Commit timestamp covered by the snapshot (≥ requested backup_ts when
    /// the source can provide an exact cover; otherwise the best available).
    pub covered_commit_ts: HlcTimestamp,
    /// Term of the last log entry included in the snapshot/log archive.
    pub log_continuation_term: u64,
    /// Index of the last log entry included.
    pub log_continuation_index: u64,
    /// Optional raft log-tail archive covering entries after the snapshot
    /// base up through the backup timestamp.
    pub log_archive: Option<Vec<u8>>,
}

/// Abstraction over tablet storage used by the backup protocol.
///
/// Implemented by the node runtime / server (which may open tablet cores).
/// Tests supply an in-memory source.
pub trait BackupSource {
    /// Capture a snapshot of `tablet` covering (at least) `backup_ts`.
    fn capture_tablet(
        &self,
        tablet: &TabletDescriptor,
        backup_ts: HlcTimestamp,
    ) -> Result<TabletSnapshotArtifact, ClusterBackupError>;
}

// ---------------------------------------------------------------------------
// Request / driver
// ---------------------------------------------------------------------------

/// Inputs for one cluster backup run.
#[derive(Debug, Clone)]
pub struct ClusterBackupRequest {
    /// Source cluster id.
    pub cluster_id: ClusterId,
    /// Source database id.
    pub database_id: DatabaseId,
    /// Meta-plane metadata version to pin.
    pub meta_version: MetadataVersion,
    /// Chosen backup timestamp. When `None`, the driver uses the maximum
    /// covered commit ts observed across tablets after capture (step 1 is
    /// then "choose after first pass"). Preferred path: supply an explicit ts.
    pub backup_ts: Option<HlcTimestamp>,
    /// Tablets to include (must be the full Active/Splitting/Merging set for
    /// a consistent database backup).
    pub tablets: Vec<TabletDescriptor>,
    /// Destination directory (created if absent). The manifest is published
    /// here last.
    pub destination: PathBuf,
    /// Optional encryption metadata to record.
    pub encryption: Option<ClusterBackupEncryption>,
}

/// Drive the six-step cluster backup protocol against a [`BackupSource`].
pub fn run_cluster_backup<S: BackupSource>(
    request: &ClusterBackupRequest,
    source: &S,
) -> Result<ClusterBackupReport, ClusterBackupError> {
    if request.tablets.is_empty() {
        return Err(ClusterBackupError::InvalidRequest(
            "backup requires at least one tablet",
        ));
    }
    if request.cluster_id == ClusterId::ZERO {
        return Err(ClusterBackupError::InvalidRequest("cluster_id is ZERO"));
    }
    if request.database_id == DatabaseId::ZERO {
        return Err(ClusterBackupError::InvalidRequest("database_id is ZERO"));
    }

    // Dedup tablet ids; reject duplicates.
    let mut seen = BTreeSet::new();
    for t in &request.tablets {
        if !seen.insert(t.tablet_id) {
            return Err(ClusterBackupError::InvalidRequest(
                "duplicate tablet_id in backup request",
            ));
        }
        if t.tablet_id == TabletId::ZERO {
            return Err(ClusterBackupError::InvalidRequest("tablet_id is ZERO"));
        }
    }

    mongreldb_fault::inject("cluster.backup.before")?;

    fs::create_dir_all(&request.destination)?;
    let tablets_dir = request.destination.join(TABLET_SNAPSHOTS_DIR);
    let logs_dir = request.destination.join(LOG_ARCHIVE_DIR);
    fs::create_dir_all(&tablets_dir)?;
    fs::create_dir_all(&logs_dir)?;

    // Step 1–2: choose/pin. Meta version is caller-supplied (already pinned
    // by the control plane before this call). Backup ts is either supplied
    // or derived after capture from covered commit timestamps.
    let pinned_meta = request.meta_version;
    mongreldb_fault::inject("cluster.backup.pin")?;

    // Steps 3–4: per-tablet snapshots + log archives.
    let mut entries: Vec<TabletBackupEntry> = Vec::with_capacity(request.tablets.len());
    // Stable order by tablet id for deterministic manifests.
    let mut ordered: Vec<&TabletDescriptor> = request.tablets.iter().collect();
    ordered.sort_by_key(|t| t.tablet_id);

    for tablet in ordered {
        let artifact =
            source.capture_tablet(tablet, request.backup_ts.unwrap_or(HlcTimestamp::ZERO))?;
        let entry = materialize_tablet_entry(tablet, &artifact, &tablets_dir, &logs_dir)?;
        mongreldb_fault::inject("cluster.backup.tablet")?;
        entries.push(entry);
    }

    // Resolve backup_ts: explicit request wins; else max covered commit ts.
    let backup_ts = match request.backup_ts {
        Some(ts) => {
            // Every tablet must cover the requested ts.
            for e in &entries {
                if e.covered_commit_ts < ts {
                    return Err(ClusterBackupError::Validation(format!(
                        "tablet {} covered_commit_ts {:?} is behind requested backup_ts {:?}",
                        e.tablet_id, e.covered_commit_ts, ts
                    )));
                }
            }
            ts
        }
        None => entries
            .iter()
            .map(|e| e.covered_commit_ts)
            .max()
            .ok_or(ClusterBackupError::InvalidRequest("no tablet entries"))?,
    };

    // Step 5: validate every entry (re-hash listed files under destination).
    validate_entries(&request.destination, &entries)?;
    mongreldb_fault::inject("cluster.backup.validate")?;

    // Step 6: publish manifest last.
    let created_unix_micros = unix_micros_now();
    let content_sha256 = content_hash(
        &request.cluster_id,
        &request.database_id,
        pinned_meta,
        backup_ts,
        &entries,
    );
    let manifest = ClusterBackupManifest {
        format_version: CLUSTER_BACKUP_FORMAT_VERSION,
        cluster_id: request.cluster_id,
        database_id: request.database_id,
        meta_version: pinned_meta,
        backup_ts,
        created_unix_micros,
        tablets: entries,
        encryption: request.encryption.clone(),
        content_sha256,
    };

    mongreldb_fault::inject("cluster.backup.publish.before")?;
    publish_manifest(&request.destination, &manifest)?;
    mongreldb_fault::inject("cluster.backup.publish.after")?;
    mongreldb_fault::inject("cluster.backup.after")?;

    let bytes = manifest.total_bytes();
    let tablets = manifest.tablet_count();
    Ok(ClusterBackupReport {
        destination: request.destination.clone(),
        manifest,
        tablets,
        bytes,
    })
}

/// Re-verify a published backup directory: load manifest, check format,
/// re-hash every listed file, recompute content hash.
pub fn verify_backup(
    backup_root: impl AsRef<Path>,
) -> Result<(ClusterBackupManifest, ClusterBackupVerifyReport), ClusterBackupError> {
    let root = backup_root.as_ref();
    let manifest = load_manifest(root)?;
    let mut report = ClusterBackupVerifyReport {
        manifest_ok: true,
        files_ok: true,
        files_checked: 0,
        bytes_checked: 0,
        issues: Vec::new(),
    };

    if manifest.format_version < MIN_CLUSTER_BACKUP_FORMAT_VERSION
        || manifest.format_version > CLUSTER_BACKUP_FORMAT_VERSION
    {
        return Err(ClusterBackupError::Manifest(format!(
            "unsupported format_version {}",
            manifest.format_version
        )));
    }

    let expected = content_hash(
        &manifest.cluster_id,
        &manifest.database_id,
        manifest.meta_version,
        manifest.backup_ts,
        &manifest.tablets,
    );
    if expected != manifest.content_sha256 {
        report.manifest_ok = false;
        return Err(ClusterBackupError::Validation(format!(
            "content_sha256 mismatch: manifest has {}, recomputed {}",
            manifest.content_sha256, expected
        )));
    }

    for entry in &manifest.tablets {
        for file in entry.snapshot_files.iter().chain(entry.log_archive.iter()) {
            report.files_checked += 1;
            report.bytes_checked += file.bytes;
            let path = root.join(&file.path);
            match hash_file(&path) {
                Ok((bytes, sha)) => {
                    if bytes != file.bytes || sha != file.sha256 {
                        report.files_ok = false;
                        return Err(ClusterBackupError::Validation(format!(
                            "file {} hash/size mismatch",
                            file.path
                        )));
                    }
                }
                Err(error) => {
                    report.files_ok = false;
                    return Err(ClusterBackupError::Validation(format!(
                        "file {} unreadable: {error}",
                        file.path
                    )));
                }
            }
        }
    }

    Ok((manifest, report))
}

/// Build a restore plan from a verified backup. Default identity mode mints
/// fresh cluster/database ids; disaster recovery reuses source identities.
pub fn plan_restore(
    manifest: &ClusterBackupManifest,
    identity_mode: RestoreIdentityMode,
    fresh_ids: Option<(ClusterId, DatabaseId)>,
) -> Result<ClusterRestorePlan, ClusterBackupError> {
    let (target_cluster_id, target_database_id) = match identity_mode {
        RestoreIdentityMode::NewIdentity => {
            let (c, d) = fresh_ids.ok_or(ClusterBackupError::InvalidRequest(
                "NewIdentity restore requires fresh cluster/database ids",
            ))?;
            if c == ClusterId::ZERO || d == DatabaseId::ZERO {
                return Err(ClusterBackupError::InvalidRequest(
                    "fresh ids must be non-zero",
                ));
            }
            if c == manifest.cluster_id && d == manifest.database_id {
                return Err(ClusterBackupError::InvalidRequest(
                    "NewIdentity restore must not reuse both source identities",
                ));
            }
            (c, d)
        }
        RestoreIdentityMode::DisasterRecovery => (manifest.cluster_id, manifest.database_id),
    };

    let tablets = manifest
        .tablets
        .iter()
        .map(|t| TabletRestoreStep {
            tablet_id: t.tablet_id,
            table_id: t.table_id,
            raft_group_id: t.raft_group_id,
            generation: t.generation,
            snapshot_paths: t.snapshot_files.iter().map(|f| f.path.clone()).collect(),
            log_archive_path: t.log_archive.as_ref().map(|f| f.path.clone()),
            log_continuation_term: t.log_continuation_term,
            log_continuation_index: t.log_continuation_index,
            covered_commit_ts: t.covered_commit_ts,
        })
        .collect();

    Ok(ClusterRestorePlan {
        identity_mode,
        target_cluster_id,
        target_database_id,
        source_cluster_id: manifest.cluster_id,
        source_database_id: manifest.database_id,
        backup_ts: manifest.backup_ts,
        meta_version: manifest.meta_version,
        tablets,
    })
}

/// Load a published manifest from a backup root.
pub fn load_manifest(
    backup_root: impl AsRef<Path>,
) -> Result<ClusterBackupManifest, ClusterBackupError> {
    let path = backup_root.as_ref().join(CLUSTER_BACKUP_MANIFEST_FILENAME);
    let Some(bytes) = read_meta_file(&path)? else {
        return Err(ClusterBackupError::Manifest(format!(
            "missing {CLUSTER_BACKUP_MANIFEST_FILENAME} (backup incomplete or not published)"
        )));
    };
    if bytes.len() as u64 > MAX_META_BYTES {
        return Err(ClusterBackupError::Manifest(
            "manifest exceeds size limit".into(),
        ));
    }
    let manifest: ClusterBackupManifest = decode_json(CLUSTER_BACKUP_MANIFEST_FILENAME, &bytes)?;
    if manifest.format_version < MIN_CLUSTER_BACKUP_FORMAT_VERSION
        || manifest.format_version > CLUSTER_BACKUP_FORMAT_VERSION
    {
        return Err(ClusterBackupError::Manifest(format!(
            "unsupported format_version {}",
            manifest.format_version
        )));
    }
    Ok(manifest)
}

// ---------------------------------------------------------------------------
// Internals
// ---------------------------------------------------------------------------

fn materialize_tablet_entry(
    tablet: &TabletDescriptor,
    artifact: &TabletSnapshotArtifact,
    tablets_dir: &Path,
    logs_dir: &Path,
) -> Result<TabletBackupEntry, ClusterBackupError> {
    let tablet_hex = tablet.tablet_id.to_string();
    let tablet_dir = tablets_dir.join(&tablet_hex);
    fs::create_dir_all(&tablet_dir)?;

    let mut snapshot_files = Vec::new();

    // Primary snapshot payload.
    let snap_rel = format!("{TABLET_SNAPSHOTS_DIR}/{tablet_hex}/snapshot.bin");
    let snap_abs = tablet_dir.join("snapshot.bin");
    write_bytes_exclusive(&snap_abs, &artifact.snapshot_payload)?;
    snapshot_files.push(ClusterBackupFile {
        path: snap_rel,
        bytes: artifact.snapshot_payload.len() as u64,
        sha256: sha256_hex(&artifact.snapshot_payload),
    });

    for (name, bytes) in &artifact.extra_files {
        if name.contains("..") || name.contains('/') || name.contains('\\') {
            return Err(ClusterBackupError::Source {
                tablet_id: tablet.tablet_id,
                detail: format!("illegal extra file name {name:?}"),
            });
        }
        let rel = format!("{TABLET_SNAPSHOTS_DIR}/{tablet_hex}/{name}");
        let abs = tablet_dir.join(name);
        write_bytes_exclusive(&abs, bytes)?;
        snapshot_files.push(ClusterBackupFile {
            path: rel,
            bytes: bytes.len() as u64,
            sha256: sha256_hex(bytes),
        });
    }

    let log_archive = match &artifact.log_archive {
        Some(bytes) => {
            let rel = format!("{LOG_ARCHIVE_DIR}/{tablet_hex}.log");
            let abs = logs_dir.join(format!("{tablet_hex}.log"));
            write_bytes_exclusive(&abs, bytes)?;
            Some(ClusterBackupFile {
                path: rel,
                bytes: bytes.len() as u64,
                sha256: sha256_hex(bytes),
            })
        }
        None => None,
    };

    Ok(TabletBackupEntry {
        tablet_id: tablet.tablet_id,
        table_id: tablet.table_id.get(),
        raft_group_id: tablet.raft_group_id,
        generation: tablet.generation,
        snapshot_files,
        log_archive,
        log_continuation_term: artifact.log_continuation_term,
        log_continuation_index: artifact.log_continuation_index,
        covered_commit_ts: artifact.covered_commit_ts,
    })
}

fn validate_entries(root: &Path, entries: &[TabletBackupEntry]) -> Result<(), ClusterBackupError> {
    if entries.is_empty() {
        return Err(ClusterBackupError::Validation(
            "no tablet entries to validate".into(),
        ));
    }
    for entry in entries {
        if entry.snapshot_files.is_empty() {
            return Err(ClusterBackupError::Validation(format!(
                "tablet {} has no snapshot files",
                entry.tablet_id
            )));
        }
        for file in entry.snapshot_files.iter().chain(entry.log_archive.iter()) {
            if file.bytes > MAX_ARTIFACT_BYTES {
                return Err(ClusterBackupError::Validation(format!(
                    "file {} exceeds size limit",
                    file.path
                )));
            }
            let path = root.join(&file.path);
            let (bytes, sha) = hash_file(&path).map_err(|e| {
                ClusterBackupError::Validation(format!("validate {}: {e}", file.path))
            })?;
            if bytes != file.bytes || sha != file.sha256 {
                return Err(ClusterBackupError::Validation(format!(
                    "file {} failed hash verification",
                    file.path
                )));
            }
        }
    }
    Ok(())
}

fn publish_manifest(
    destination: &Path,
    manifest: &ClusterBackupManifest,
) -> Result<(), ClusterBackupError> {
    let bytes = encode_json(CLUSTER_BACKUP_MANIFEST_FILENAME, manifest)?;
    write_meta_atomic(destination, CLUSTER_BACKUP_MANIFEST_FILENAME, &bytes)?;
    // Confirm the published file is readable.
    let _ = load_manifest(destination)?;
    Ok(())
}

fn content_hash(
    cluster_id: &ClusterId,
    database_id: &DatabaseId,
    meta_version: MetadataVersion,
    backup_ts: HlcTimestamp,
    tablets: &[TabletBackupEntry],
) -> String {
    // Canonical payload: ids + meta + ts + each tablet entry serialized
    // without relying on the outer manifest's content_sha256 field.
    let mut hasher = Sha256::new();
    hasher.update(cluster_id.to_string().as_bytes());
    hasher.update([0]);
    hasher.update(database_id.to_string().as_bytes());
    hasher.update([0]);
    hasher.update(meta_version.get().to_le_bytes());
    hasher.update(backup_ts.physical_micros.to_le_bytes());
    hasher.update(backup_ts.logical.to_le_bytes());
    hasher.update(backup_ts.node_tiebreaker.to_le_bytes());
    for t in tablets {
        // Deterministic: serde_json on the entry.
        let encoded = serde_json::to_vec(t).expect("tablet entry serializes");
        hasher.update((encoded.len() as u64).to_le_bytes());
        hasher.update(&encoded);
    }
    hex_encode(hasher.finalize())
}

fn write_bytes_exclusive(path: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = File::create(path)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    Ok(())
}

fn hash_file(path: &Path) -> io::Result<(u64, String)> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, hex_encode(hasher.finalize())))
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hex_encode(hasher.finalize())
}

fn hex_encode(bytes: impl AsRef<[u8]>) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let bytes = bytes.as_ref();
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn unix_micros_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

impl fmt::Display for RestoreIdentityMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NewIdentity => f.write_str("new_identity"),
            Self::DisasterRecovery => f.write_str("disaster_recovery"),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tablet::{ReplicaDescriptor, ReplicaRole, TabletDescriptor, TabletState};
    use mongreldb_types::ids::{NodeId, TableId};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, OnceLock};

    /// Fault hooks are process-global; serialize backup tests that arm them.
    fn fault_lock() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }

    fn tid(n: u8) -> TabletId {
        let mut b = [0u8; 16];
        b[15] = n;
        TabletId::from_bytes(b)
    }
    fn rid(n: u8) -> RaftGroupId {
        let mut b = [0u8; 16];
        b[15] = n;
        RaftGroupId::from_bytes(b)
    }
    fn nid(n: u8) -> NodeId {
        let mut b = [0u8; 16];
        b[15] = n;
        NodeId::from_bytes(b)
    }
    fn cid(n: u8) -> ClusterId {
        let mut b = [0u8; 16];
        b[15] = n;
        ClusterId::from_bytes(b)
    }
    fn did(n: u8) -> DatabaseId {
        let mut b = [0u8; 16];
        b[15] = n;
        DatabaseId::from_bytes(b)
    }
    fn hlc(micros: u64) -> HlcTimestamp {
        HlcTimestamp {
            physical_micros: micros,
            logical: 0,
            node_tiebreaker: 1,
        }
    }

    fn descriptor(tablet: u8, table: u64, gen: u64) -> TabletDescriptor {
        TabletDescriptor {
            tablet_id: tid(tablet),
            table_id: TableId::new(table),
            raft_group_id: rid(tablet),
            partition: crate::tablet::PartitionBounds::unbounded(),
            replicas: vec![ReplicaDescriptor {
                node_id: nid(1),
                role: ReplicaRole::Voter,
                raft_node_id: 1,
            }],
            leader_hint: Some(nid(1)),
            generation: gen,
            state: TabletState::Active,
        }
    }

    struct MemSource {
        /// tablet_id byte → payload
        payloads: BTreeMap<u8, Vec<u8>>,
        covered: HlcTimestamp,
        captures: Arc<AtomicUsize>,
    }

    impl BackupSource for MemSource {
        fn capture_tablet(
            &self,
            tablet: &TabletDescriptor,
            _backup_ts: HlcTimestamp,
        ) -> Result<TabletSnapshotArtifact, ClusterBackupError> {
            self.captures.fetch_add(1, Ordering::SeqCst);
            let key = tablet.tablet_id.as_bytes()[15];
            let payload = self
                .payloads
                .get(&key)
                .cloned()
                .unwrap_or_else(|| format!("snap-{key}").into_bytes());
            Ok(TabletSnapshotArtifact {
                snapshot_payload: payload,
                extra_files: BTreeMap::new(),
                covered_commit_ts: self.covered,
                log_continuation_term: 3,
                log_continuation_index: 42 + key as u64,
                log_archive: Some(format!("log-tail-{key}").into_bytes()),
            })
        }
    }

    fn mem_source(covered: HlcTimestamp) -> MemSource {
        let mut payloads = BTreeMap::new();
        payloads.insert(1, b"tablet-one-snapshot".to_vec());
        payloads.insert(2, b"tablet-two-snapshot".to_vec());
        payloads.insert(3, b"tablet-three-snapshot".to_vec());
        MemSource {
            payloads,
            covered,
            captures: Arc::new(AtomicUsize::new(0)),
        }
    }

    #[test]
    fn backup_publishes_manifest_last_and_verifies() {
        let _serial = fault_lock();
        mongreldb_fault::clear();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("backup-1");
        let covered = hlc(1_700_000_000_000_000);
        let source = mem_source(covered);
        let request = ClusterBackupRequest {
            cluster_id: cid(0xAA),
            database_id: did(0xBB),
            meta_version: MetadataVersion::new(7),
            backup_ts: Some(covered),
            tablets: vec![
                descriptor(1, 10, 5),
                descriptor(2, 10, 5),
                descriptor(3, 11, 2),
            ],
            destination: dest.clone(),
            encryption: Some(ClusterBackupEncryption {
                scheme: "aes-256-gcm-kms".into(),
                kms_key_id: Some("kms/test-key".into()),
                key_version: Some("v1".into()),
            }),
        };

        // Before run: no manifest.
        assert!(load_manifest(&dest).is_err());

        let report = run_cluster_backup(&request, &source).expect("backup");
        assert_eq!(report.tablets, 3);
        assert!(report.bytes > 0);
        assert_eq!(source.captures.load(Ordering::SeqCst), 3);

        // Manifest published and ordered by tablet id.
        let (manifest, verify) = verify_backup(&dest).expect("verify");
        assert!(verify.manifest_ok);
        assert!(verify.files_ok);
        assert_eq!(verify.files_checked, 6); // 3 snapshots + 3 log archives
        assert_eq!(manifest.cluster_id, cid(0xAA));
        assert_eq!(manifest.database_id, did(0xBB));
        assert_eq!(manifest.meta_version, MetadataVersion::new(7));
        assert_eq!(manifest.backup_ts, covered);
        assert_eq!(manifest.tablets.len(), 3);
        assert!(manifest
            .tablets
            .windows(2)
            .all(|w| w[0].tablet_id <= w[1].tablet_id));
        assert_eq!(
            manifest.encryption.as_ref().map(|e| e.scheme.as_str()),
            Some("aes-256-gcm-kms")
        );

        // Content hash is stable across reload.
        let reloaded = load_manifest(&dest).unwrap();
        assert_eq!(reloaded.content_sha256, manifest.content_sha256);
        assert_eq!(reloaded, manifest);
    }

    #[test]
    fn incomplete_backup_without_manifest_is_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("partial");
        fs::create_dir_all(dest.join(TABLET_SNAPSHOTS_DIR)).unwrap();
        // Write a stray file but no manifest — restore/verify must fail closed.
        fs::write(dest.join("tablets/x.bin"), b"orphan").unwrap();
        let err = load_manifest(&dest).unwrap_err();
        assert!(
            matches!(err, ClusterBackupError::Manifest(ref m) if m.contains("missing")),
            "got {err:?}"
        );
        let err = verify_backup(&dest).unwrap_err();
        assert!(matches!(err, ClusterBackupError::Manifest(_)));
    }

    #[test]
    fn restore_plan_mints_new_identity_by_default() {
        let _serial = fault_lock();
        mongreldb_fault::clear();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("backup-2");
        let covered = hlc(100);
        let source = mem_source(covered);
        let report = run_cluster_backup(
            &ClusterBackupRequest {
                cluster_id: cid(1),
                database_id: did(2),
                meta_version: MetadataVersion::new(1),
                backup_ts: Some(covered),
                tablets: vec![descriptor(1, 1, 1)],
                destination: dest,
                encryption: None,
            },
            &source,
        )
        .unwrap();

        let plan = plan_restore(
            &report.manifest,
            RestoreIdentityMode::NewIdentity,
            Some((cid(9), did(8))),
        )
        .unwrap();
        assert_eq!(plan.identity_mode, RestoreIdentityMode::NewIdentity);
        assert_eq!(plan.target_cluster_id, cid(9));
        assert_eq!(plan.target_database_id, did(8));
        assert_eq!(plan.source_cluster_id, cid(1));
        assert_eq!(plan.source_database_id, did(2));
        assert_eq!(plan.tablets.len(), 1);
        assert_eq!(plan.tablets[0].tablet_id, tid(1));

        // DR mode reuses source ids.
        let dr = plan_restore(
            &report.manifest,
            RestoreIdentityMode::DisasterRecovery,
            None,
        )
        .unwrap();
        assert_eq!(dr.target_cluster_id, cid(1));
        assert_eq!(dr.target_database_id, did(2));

        // NewIdentity refusing source reuse.
        let err = plan_restore(
            &report.manifest,
            RestoreIdentityMode::NewIdentity,
            Some((cid(1), did(2))),
        )
        .unwrap_err();
        assert!(matches!(err, ClusterBackupError::InvalidRequest(_)));
    }

    #[test]
    fn publish_last_survives_fault_before_publish() {
        let _serial = fault_lock();
        mongreldb_fault::clear();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("backup-fault");
        let covered = hlc(50);
        let source = mem_source(covered);
        let request = ClusterBackupRequest {
            cluster_id: cid(3),
            database_id: did(4),
            meta_version: MetadataVersion::new(2),
            backup_ts: Some(covered),
            tablets: vec![descriptor(1, 1, 1), descriptor(2, 1, 1)],
            destination: dest.clone(),
            encryption: None,
        };

        let _guard = mongreldb_fault::ScopedGuard::new(
            "cluster.backup.publish.before",
            mongreldb_fault::Action::Fail,
        );
        let err = run_cluster_backup(&request, &source).unwrap_err();
        assert!(matches!(
            err,
            ClusterBackupError::Fault("cluster.backup.publish.before")
        ));
        // Artifacts may exist, but manifest must NOT be published.
        assert!(
            load_manifest(&dest).is_err(),
            "manifest must not exist after pre-publish fault"
        );
        // Tablet artifacts were written (protocol reached validate).
        assert!(dest.join(TABLET_SNAPSHOTS_DIR).exists());
    }

    #[test]
    fn tablet_coverage_behind_requested_ts_fails_validation() {
        let _serial = fault_lock();
        mongreldb_fault::clear();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("backup-stale");
        // Source covers ts=10, request asks for ts=20.
        let source = mem_source(hlc(10));
        let err = run_cluster_backup(
            &ClusterBackupRequest {
                cluster_id: cid(1),
                database_id: did(1),
                meta_version: MetadataVersion::new(1),
                backup_ts: Some(hlc(20)),
                tablets: vec![descriptor(1, 1, 1)],
                destination: dest,
                encryption: None,
            },
            &source,
        )
        .unwrap_err();
        assert!(
            matches!(err, ClusterBackupError::Validation(ref m) if m.contains("behind")),
            "got {err:?}"
        );
    }

    #[test]
    fn tampered_snapshot_fails_verify() {
        let _serial = fault_lock();
        mongreldb_fault::clear();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("backup-tamper");
        let covered = hlc(1);
        let source = mem_source(covered);
        run_cluster_backup(
            &ClusterBackupRequest {
                cluster_id: cid(1),
                database_id: did(1),
                meta_version: MetadataVersion::new(1),
                backup_ts: Some(covered),
                tablets: vec![descriptor(1, 1, 1)],
                destination: dest.clone(),
                encryption: None,
            },
            &source,
        )
        .unwrap();

        // Tamper with the snapshot bytes after publish.
        let snap = dest
            .join(TABLET_SNAPSHOTS_DIR)
            .join(tid(1).to_string())
            .join("snapshot.bin");
        fs::write(&snap, b"TAMPERED").unwrap();
        let err = verify_backup(&dest).unwrap_err();
        assert!(matches!(err, ClusterBackupError::Validation(_)));
    }

    #[test]
    fn empty_tablet_set_rejected() {
        let _serial = fault_lock();
        mongreldb_fault::clear();
        let dir = tempfile::tempdir().unwrap();
        let err = run_cluster_backup(
            &ClusterBackupRequest {
                cluster_id: cid(1),
                database_id: did(1),
                meta_version: MetadataVersion::new(1),
                backup_ts: None,
                tablets: vec![],
                destination: dir.path().join("x"),
                encryption: None,
            },
            &mem_source(hlc(1)),
        )
        .unwrap_err();
        assert!(matches!(err, ClusterBackupError::InvalidRequest(_)));
    }

    #[test]
    fn backup_ts_derived_when_unspecified() {
        let _serial = fault_lock();
        mongreldb_fault::clear();
        let dir = tempfile::tempdir().unwrap();
        let dest = dir.path().join("backup-derived-ts");
        let covered = hlc(999);
        let source = mem_source(covered);
        let report = run_cluster_backup(
            &ClusterBackupRequest {
                cluster_id: cid(1),
                database_id: did(1),
                meta_version: MetadataVersion::new(1),
                backup_ts: None,
                tablets: vec![descriptor(1, 1, 1), descriptor(2, 1, 1)],
                destination: dest,
                encryption: None,
            },
            &source,
        )
        .unwrap();
        assert_eq!(report.manifest.backup_ts, covered);
    }
}
