//! Durable storage-mode marker (spec section 5.3, Stage 2E).
//!
//! Every database root carries `_meta/storage-mode`: a versioned, checksummed
//! marker declaring which runtime owns the directory. The marker is written
//! atomically through the pinned [`crate::durable_file::DurableRoot`] (temp +
//! rename + fsync, mirroring the `_meta/generation` idiom in
//! [`crate::catalog`]).
//!
//! # Rules (spec section 5.3)
//!
//! - [`StorageMode::Standalone`] may be opened embedded.
//! - [`StorageMode::ServerOwnedStandalone`] is functionally standalone; the
//!   server owns the lock (the existing `_meta/.lock` arbitration enforces
//!   that; embedded opens succeed when the lock is free).
//! - [`StorageMode::ClusterReplica`] may be opened only by the cluster node
//!   runtime (`Database::open_cluster_replica`); every other open path rejects
//!   it with [`StorageModeError::ClusterReplicaRequiresClusterRuntime`].
//! - A backup validator may open any mode read-only through the special
//!   offline-validation API (`OpenOptions::offline_validation`).
//!
//! Databases created before the marker existed have no marker file: they open
//! as [`StorageMode::Standalone`] and the marker is written on first open (the
//! on-disk format is otherwise unchanged — the marker is purely additive).
//!
//! Mode transitions are never rewritten in place (spec section 5.2: no
//! in-place "magic conversion"); [`write`] fails closed when the existing
//! marker disagrees with the requested mode.

use std::path::Path;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use mongreldb_types::ids::{ClusterId, DatabaseId, NodeId};

use crate::MongrelError;

/// Marker file name under `_meta/`.
pub const STORAGE_MODE_FILENAME: &str = "storage-mode";

const MAGIC: &[u8; 8] = b"MMODE001";
/// The only marker format version this build reads and writes.
pub const STORAGE_MODE_FORMAT_VERSION: u16 = 1;

const TAG_STANDALONE: u8 = 0;
const TAG_SERVER_OWNED_STANDALONE: u8 = 1;
const TAG_CLUSTER_REPLICA: u8 = 2;

/// Which runtime owns a database directory (spec section 5.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StorageMode {
    /// Openable embedded and in single-node-server mode.
    Standalone,
    /// Functionally standalone, but the owning server holds the lock.
    ServerOwnedStandalone,
    /// Owned by the cluster node runtime; rejected by every normal open.
    ClusterReplica {
        /// The owning cluster.
        cluster_id: ClusterId,
        /// The node this replica belongs to.
        node_id: NodeId,
        /// The replicated logical database.
        database_id: DatabaseId,
    },
}

impl StorageMode {
    /// The cluster identity of a [`StorageMode::ClusterReplica`], `None` for
    /// standalone modes.
    pub fn cluster_identity(&self) -> Option<(ClusterId, NodeId, DatabaseId)> {
        match *self {
            StorageMode::ClusterReplica {
                cluster_id,
                node_id,
                database_id,
            } => Some((cluster_id, node_id, database_id)),
            _ => None,
        }
    }

    fn body(&self) -> Vec<u8> {
        let mut body = Vec::with_capacity(3 + 48);
        body.extend_from_slice(&STORAGE_MODE_FORMAT_VERSION.to_le_bytes());
        match self {
            StorageMode::Standalone => body.push(TAG_STANDALONE),
            StorageMode::ServerOwnedStandalone => body.push(TAG_SERVER_OWNED_STANDALONE),
            StorageMode::ClusterReplica {
                cluster_id,
                node_id,
                database_id,
            } => {
                body.push(TAG_CLUSTER_REPLICA);
                body.extend_from_slice(cluster_id.as_bytes());
                body.extend_from_slice(node_id.as_bytes());
                body.extend_from_slice(database_id.as_bytes());
            }
        }
        body
    }

    fn decode_body(body: &[u8]) -> Result<Self, StorageModeError> {
        if body.len() < 3 {
            return Err(StorageModeError::Corrupt {
                reason: format!("marker body truncated: {} bytes", body.len()),
            });
        }
        let version = u16::from_le_bytes([body[0], body[1]]);
        if version != STORAGE_MODE_FORMAT_VERSION {
            return Err(StorageModeError::UnsupportedVersion {
                found: version,
                supported: STORAGE_MODE_FORMAT_VERSION,
            });
        }
        let expect_len = |len: usize, what: &str| -> Result<(), StorageModeError> {
            if body.len() != len {
                return Err(StorageModeError::Corrupt {
                    reason: format!(
                        "marker body for {what} must be {len} bytes, got {}",
                        body.len()
                    ),
                });
            }
            Ok(())
        };
        match body[2] {
            TAG_STANDALONE => {
                expect_len(3, "standalone")?;
                Ok(StorageMode::Standalone)
            }
            TAG_SERVER_OWNED_STANDALONE => {
                expect_len(3, "server-owned standalone")?;
                Ok(StorageMode::ServerOwnedStandalone)
            }
            TAG_CLUSTER_REPLICA => {
                expect_len(3 + 48, "cluster replica")?;
                let id = |offset: usize| -> [u8; 16] {
                    body[3 + offset..3 + offset + 16]
                        .try_into()
                        .expect("length checked")
                };
                Ok(StorageMode::ClusterReplica {
                    cluster_id: ClusterId::from_bytes(id(0)),
                    node_id: NodeId::from_bytes(id(16)),
                    database_id: DatabaseId::from_bytes(id(32)),
                })
            }
            tag => Err(StorageModeError::Corrupt {
                reason: format!("unknown storage-mode tag {tag}"),
            }),
        }
    }

    /// The framed on-disk form: `MAGIC | sha256(body) | body`.
    fn frame(&self) -> Vec<u8> {
        let body = self.body();
        let hash = Sha256::digest(&body);
        let mut out = Vec::with_capacity(8 + 32 + body.len());
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&hash);
        out.extend_from_slice(&body);
        out
    }

    fn deframe(bytes: &[u8]) -> Result<Self, StorageModeError> {
        if bytes.len() < 8 + 32 + 3 || &bytes[..8] != MAGIC {
            return Err(StorageModeError::Corrupt {
                reason: "bad magic or truncated header".into(),
            });
        }
        let (tag, body) = bytes[8..].split_at(32);
        let calc = Sha256::digest(body);
        if tag != calc.as_slice() {
            return Err(StorageModeError::Corrupt {
                reason: "checksum mismatch".into(),
            });
        }
        Self::decode_body(body)
    }
}

/// Typed errors of the storage-mode marker (spec section 5.3). Mirrors the
/// subsystem-error idiom of [`crate::locks::LockError`];
/// [`From<StorageModeError> for MongrelError`] maps onto the closest existing
/// engine variants.
#[derive(Debug, thiserror::Error)]
pub enum StorageModeError {
    /// A normal open path touched a cluster-replica database.
    #[error(
        "database is a replica of cluster {cluster_id} node {node_id} database {database_id}: \
         only the cluster node runtime may open it (Database::open_cluster_replica), or open it \
         read-only with OpenOptions::offline_validation"
    )]
    ClusterReplicaRequiresClusterRuntime {
        /// Owning cluster.
        cluster_id: ClusterId,
        /// Owning node.
        node_id: NodeId,
        /// Replicated database.
        database_id: DatabaseId,
    },
    /// The cluster runtime offered an identity that disagrees with the marker
    /// (fail closed: opening the wrong replica would corrupt two clusters).
    #[error("storage-mode identity mismatch: {0}")]
    IdentityMismatch(String),
    /// The marker failed validation (fails closed).
    #[error("corrupt storage-mode marker: {reason}")]
    Corrupt {
        /// Why the marker failed validation.
        reason: String,
    },
    /// The marker was written by a newer format than this build supports.
    #[error(
        "unsupported storage-mode marker version {found}: this build supports version {supported} only"
    )]
    UnsupportedVersion {
        /// Version found on disk.
        found: u16,
        /// Version this build writes.
        supported: u16,
    },
    /// A mode change was requested (spec section 5.2 forbids in-place
    /// conversion).
    #[error("storage-mode conflict: {0}")]
    Conflict(String),
    /// Underlying I/O failure.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

impl From<StorageModeError> for MongrelError {
    fn from(error: StorageModeError) -> Self {
        match error {
            // The caller used the wrong open contract for this directory
            // (mirrors the "database already exists; use Database::open()"
            // guidance style of `InvalidArgument`).
            StorageModeError::ClusterReplicaRequiresClusterRuntime { .. }
            | StorageModeError::IdentityMismatch(_)
            | StorageModeError::Conflict(_) => MongrelError::InvalidArgument(error.to_string()),
            StorageModeError::Corrupt { reason } => MongrelError::CorruptWal {
                offset: 0,
                reason: format!("storage-mode marker: {reason}"),
            },
            StorageModeError::UnsupportedVersion { found, supported } => {
                MongrelError::UnsupportedStorageVersion {
                    component: "storage-mode marker",
                    found,
                    supported,
                }
            }
            StorageModeError::Io(error) => MongrelError::Io(error),
        }
    }
}

/// Reads the marker from `_meta/storage-mode`. Returns `None` for databases
/// created before the marker existed (they are [`StorageMode::Standalone`]).
/// Corrupt or unsupported markers fail closed.
pub fn read(
    durable_root: &crate::durable_file::DurableRoot,
) -> Result<Option<StorageMode>, StorageModeError> {
    let relative = Path::new(crate::database::META_DIR).join(STORAGE_MODE_FILENAME);
    match durable_root.entry_exists(&relative) {
        Ok(true) => {}
        Ok(false) => return Ok(None),
        // The `_meta` directory itself may not exist yet (fresh roots).
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let mut file = durable_root.open_regular(&relative)?;
    let mut bytes = Vec::new();
    std::io::Read::read_to_end(&mut file, &mut bytes)?;
    Ok(Some(StorageMode::deframe(&bytes)?))
}

/// Reads the marker from a filesystem path (no pinned root). Same contract as
/// [`read`]; used before a durable root exists.
pub fn read_at(root: impl AsRef<Path>) -> Result<Option<StorageMode>, StorageModeError> {
    let path = root
        .as_ref()
        .join(crate::database::META_DIR)
        .join(STORAGE_MODE_FILENAME);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    Ok(Some(StorageMode::deframe(&bytes)?))
}

/// Writes the marker atomically (temp + rename + fsync through the pinned
/// root). Fails closed when a different mode is already recorded: spec section
/// 5.2 forbids in-place conversion for the first cluster release.
pub fn write(
    durable_root: &crate::durable_file::DurableRoot,
    mode: &StorageMode,
) -> Result<(), StorageModeError> {
    if let Some(existing) = read(durable_root)? {
        if existing == *mode {
            return Ok(());
        }
        return Err(StorageModeError::Conflict(format!(
            "existing marker {existing:?} disagrees with requested mode {mode:?}; \
             in-place storage-mode conversion is not supported (spec 5.2)"
        )));
    }
    durable_root.create_directory_all(crate::database::META_DIR)?;
    durable_root.write_atomic(
        Path::new(crate::database::META_DIR).join(STORAGE_MODE_FILENAME),
        &mode.frame(),
    )?;
    Ok(())
}

/// Overwrites the marker atomically with `mode`. Restricted to the cluster
/// snapshot-install path, which rewrites the marker's node identity when a
/// snapshot built by a peer is staged locally (spec section 11.5: the staged
/// image is validated before it replaces anything).
pub(crate) fn rewrite(
    durable_root: &crate::durable_file::DurableRoot,
    mode: &StorageMode,
) -> Result<(), StorageModeError> {
    durable_root.create_directory_all(crate::database::META_DIR)?;
    durable_root.write_atomic(
        Path::new(crate::database::META_DIR).join(STORAGE_MODE_FILENAME),
        &mode.frame(),
    )?;
    Ok(())
}

/// The open-path gate (spec section 5.3). `offline_validation` is the special
/// offline validator: it may open any mode, and the caller forces the opened
/// core read-only.
pub(crate) fn check_open(
    mode: Option<&StorageMode>,
    offline_validation: bool,
) -> Result<(), StorageModeError> {
    match mode {
        Some(StorageMode::ClusterReplica {
            cluster_id,
            node_id,
            database_id,
        }) if !offline_validation => Err(StorageModeError::ClusterReplicaRequiresClusterRuntime {
            cluster_id: *cluster_id,
            node_id: *node_id,
            database_id: *database_id,
        }),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (ClusterId, NodeId, DatabaseId) {
        (
            ClusterId::from_bytes([1; 16]),
            NodeId::from_bytes([2; 16]),
            DatabaseId::from_bytes([3; 16]),
        )
    }

    #[test]
    fn frame_round_trip_every_mode() {
        let (cluster_id, node_id, database_id) = ids();
        let modes = [
            StorageMode::Standalone,
            StorageMode::ServerOwnedStandalone,
            StorageMode::ClusterReplica {
                cluster_id,
                node_id,
                database_id,
            },
        ];
        for mode in modes {
            let framed = mode.frame();
            assert_eq!(StorageMode::deframe(&framed).unwrap(), mode);
        }
    }

    #[test]
    fn corrupt_frames_fail_closed() {
        let mode = StorageMode::Standalone;
        let framed = mode.frame();
        // Truncated.
        assert!(StorageMode::deframe(&framed[..10]).is_err());
        // Bad magic.
        let mut bad_magic = framed.clone();
        bad_magic[0] ^= 0x01;
        assert!(matches!(
            StorageMode::deframe(&bad_magic),
            Err(StorageModeError::Corrupt { .. })
        ));
        // Checksum mismatch.
        let mut bit_flip = framed;
        let last = bit_flip.len() - 1;
        bit_flip[last] ^= 0x01;
        assert!(matches!(
            StorageMode::deframe(&bit_flip),
            Err(StorageModeError::Corrupt { .. })
        ));
    }

    #[test]
    fn unknown_version_and_tag_fail_closed() {
        let (cluster_id, node_id, database_id) = ids();
        let mut body = StorageMode::ClusterReplica {
            cluster_id,
            node_id,
            database_id,
        }
        .body();
        body[0] = (STORAGE_MODE_FORMAT_VERSION + 1) as u8;
        let hash = Sha256::digest(&body);
        let mut framed = Vec::new();
        framed.extend_from_slice(MAGIC);
        framed.extend_from_slice(&hash);
        framed.extend_from_slice(&body);
        assert!(matches!(
            StorageMode::deframe(&framed),
            Err(StorageModeError::UnsupportedVersion { found, .. }) if found == STORAGE_MODE_FORMAT_VERSION + 1
        ));

        let mut tagged = StorageMode::Standalone.frame();
        let n = tagged.len();
        tagged[n - 1] = 99;
        let body = &tagged[40..];
        let hash = Sha256::digest(body);
        tagged[8..40].copy_from_slice(&hash);
        assert!(matches!(
            StorageMode::deframe(&tagged),
            Err(StorageModeError::Corrupt { .. })
        ));
    }

    #[test]
    fn durable_write_read_and_no_conversion() {
        let tmp = tempfile::tempdir().unwrap();
        let root = crate::durable_file::DurableRoot::open(tmp.path()).unwrap();
        assert_eq!(read(&root).unwrap(), None);

        write(&root, &StorageMode::Standalone).unwrap();
        assert_eq!(read(&root).unwrap(), Some(StorageMode::Standalone));
        // Idempotent rewrite of the same mode.
        write(&root, &StorageMode::Standalone).unwrap();

        let (cluster_id, node_id, database_id) = ids();
        let cluster = StorageMode::ClusterReplica {
            cluster_id,
            node_id,
            database_id,
        };
        assert!(matches!(
            write(&root, &cluster),
            Err(StorageModeError::Conflict(_))
        ));
        assert_eq!(read(&root).unwrap(), Some(StorageMode::Standalone));

        rewrite(&root, &cluster).unwrap();
        assert_eq!(read(&root).unwrap(), Some(cluster));
        assert_eq!(read_at(tmp.path()).unwrap(), Some(cluster));
    }

    #[test]
    fn open_gate_matrix() {
        let (cluster_id, node_id, database_id) = ids();
        let cluster = StorageMode::ClusterReplica {
            cluster_id,
            node_id,
            database_id,
        };
        // Normal opens.
        assert!(check_open(None, false).is_ok());
        assert!(check_open(Some(&StorageMode::Standalone), false).is_ok());
        assert!(check_open(Some(&StorageMode::ServerOwnedStandalone), false).is_ok());
        assert!(matches!(
            check_open(Some(&cluster), false),
            Err(StorageModeError::ClusterReplicaRequiresClusterRuntime { .. })
        ));
        // Offline validation opens any mode.
        assert!(check_open(Some(&cluster), true).is_ok());
        assert!(check_open(Some(&StorageMode::Standalone), true).is_ok());
    }

    #[test]
    fn error_display_names_cluster_runtime() {
        let (cluster_id, node_id, database_id) = ids();
        let error = StorageModeError::ClusterReplicaRequiresClusterRuntime {
            cluster_id,
            node_id,
            database_id,
        };
        let display = error.to_string();
        assert!(display.contains(&cluster_id.to_hex()), "{display}");
        assert!(display.contains(&node_id.to_hex()), "{display}");
        assert!(display.contains(&database_id.to_hex()), "{display}");
        assert!(display.contains("cluster node runtime"), "{display}");
        let mapped: MongrelError = error.into();
        assert!(matches!(mapped, MongrelError::InvalidArgument(_)));
    }
}
