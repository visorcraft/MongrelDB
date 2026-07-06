//! DB-wide catalog checkpoint (spec §5.1).
//!
//! The catalog records every table's id, name, schema, and live/dropped state
//! plus the DB-wide monotonic counters (`db_epoch`, `next_table_id`,
//! `open_generation`, `next_segment_no`). It is rewritten atomically on every
//! DDL and persisted to `<root>/CATALOG` with:
//!
//! - a fixed magic + SHA-256 integrity tag for plaintext, or
//! - AES-256-GCM (`meta_dek`-derived via [`Kek::derive_meta_key`]) which both
//!   encrypts and authenticates, plus
//! - a directory `sync_all` after the atomic rename (review fix #19), so a crash
//!   never leaves a half-linked catalog entry.

use crate::error::{MongrelError, Result};
use crate::external_table::ExternalTableEntry;
use crate::procedure::ProcedureEntry;
use crate::schema::Schema;
use crate::trigger::TriggerEntry;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;

pub const CATALOG_FILENAME: &str = "CATALOG";
const MAGIC: &[u8; 8] = b"MONGRCAT";
/// 32-byte meta DEK length (matches [`crate::encryption::DEK_LEN`]).
pub const META_DEK_LEN: usize = 32;

/// Lifecycle state of a catalog table entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TableState {
    /// Live and queryable.
    Live,
    /// Logically dropped at `at_epoch`; the physical subdir is reaped once no
    /// reader pins a snapshot older than `at_epoch`.
    Dropped { at_epoch: u64 },
}

/// One row of the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogEntry {
    pub table_id: u64,
    pub name: String,
    pub schema: Schema,
    pub state: TableState,
    pub created_epoch: u64,
}

/// The full in-memory catalog, mirrored on disk by [`write_atomic`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Catalog {
    /// Highest epoch ever assigned by this DB's commit sequencer.
    pub db_epoch: u64,
    /// Next table id to allocate.
    pub next_table_id: u64,
    /// Bumped (and fsynced) on every open to scope `txn_id` across reopens.
    pub open_generation: u64,
    /// Next shared-WAL segment number to allocate.
    pub next_segment_no: u64,
    pub tables: Vec<CatalogEntry>,
    #[serde(default)]
    pub procedures: Vec<ProcedureEntry>,
    #[serde(default)]
    pub triggers: Vec<TriggerEntry>,
    #[serde(default)]
    pub external_tables: Vec<ExternalTableEntry>,
    /// Catalog-level user accounts (Argon2id-hashed credentials).
    #[serde(default)]
    pub users: Vec<crate::auth::UserEntry>,
    /// Catalog-level role definitions.
    #[serde(default)]
    pub roles: Vec<crate::auth::RoleEntry>,
    /// Next monotonic user id to allocate.
    #[serde(default)]
    pub next_user_id: u64,
    /// When true, every Database/Table/Transaction/MongrelSession operation
    /// requires an authenticated `Principal` with sufficient permission.
    /// Defaults to false → existing credentialless databases open unchanged.
    /// See `docs/auth-enforcement-spec.md`.
    #[serde(default)]
    pub require_auth: bool,
}

impl Catalog {
    /// An empty catalog for a freshly created DB.
    pub fn empty() -> Self {
        Catalog::default()
    }

    /// Look up an entry by name (live only).
    pub fn live(&self, name: &str) -> Option<&CatalogEntry> {
        self.tables
            .iter()
            .find(|t| t.name == name && matches!(t.state, TableState::Live))
    }
}

#[cfg(feature = "encryption")]
fn seal(body: &[u8], meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Vec<u8>> {
    match meta_dek {
        Some(dek) => crate::encryption::encrypt_blob(dek, body),
        None => Ok(plaintext_frame(body)),
    }
}

#[cfg(not(feature = "encryption"))]
fn seal(body: &[u8], _meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Vec<u8>> {
    Ok(plaintext_frame(body))
}

fn plaintext_frame(body: &[u8]) -> Vec<u8> {
    let hash = Sha256::digest(body);
    let mut out = Vec::with_capacity(body.len() + 8 + 32);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&hash);
    out.extend_from_slice(body);
    out
}

/// Atomically write the catalog to `<dir>/CATALOG` (review fix #19: dir-fsync).
///
/// If `meta_dek` is `Some`, the body is AES-256-GCM sealed (confidential +
/// authenticated); otherwise the body carries a SHA-256 tag (integrity only).
pub fn write_atomic(
    dir: &Path,
    cat: &Catalog,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<()> {
    let body = serde_json::to_vec(cat)
        .map_err(|e| MongrelError::Other(format!("catalog serialize: {e}")))?;
    let payload = seal(&body, meta_dek)?;

    let tmp = dir.join(format!(".{CATALOG_FILENAME}.tmp"));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&payload)?;
        f.sync_all()?;
    }
    let dest = dir.join(CATALOG_FILENAME);
    std::fs::rename(&tmp, &dest)?;
    // Directory fsync so the rename is durable across a crash.
    if let Ok(d) = std::fs::File::open(dir) {
        let _ = d.sync_all();
    }
    Ok(())
}

#[cfg(feature = "encryption")]
fn open_payload(bytes: &[u8], meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Option<Catalog>> {
    match meta_dek {
        Some(dek) => match crate::encryption::decrypt_blob(dek, bytes) {
            Ok(body) => deserialize(&body),
            Err(_) => Ok(None),
        },
        None => parse_plaintext(bytes),
    }
}

#[cfg(not(feature = "encryption"))]
fn open_payload(bytes: &[u8], _meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Option<Catalog>> {
    parse_plaintext(bytes)
}

fn deserialize(body: &[u8]) -> Result<Option<Catalog>> {
    serde_json::from_slice(body)
        .map(Some)
        .map_err(|e| MongrelError::Other(format!("catalog deserialize: {e}")))
}

/// Read the catalog from `<dir>/CATALOG`. Returns `Ok(None)` if no catalog is
/// present, or if authentication fails (tampered / wrong key).
pub fn read(dir: &Path, meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Option<Catalog>> {
    let p = dir.join(CATALOG_FILENAME);
    let bytes = match std::fs::read(&p) {
        Ok(b) => b,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    open_payload(&bytes, meta_dek)
}

fn parse_plaintext(bytes: &[u8]) -> Result<Option<Catalog>> {
    if bytes.len() < 8 + 32 || &bytes[..8] != MAGIC {
        return Ok(None);
    }
    let (tag, body) = bytes[8..].split_at(32);
    let calc = Sha256::digest(body);
    if tag != calc.as_slice() {
        // tampered
        return Ok(None);
    }
    deserialize(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_catalog_default() {
        let c = Catalog::empty();
        assert_eq!(c.db_epoch, 0);
        assert_eq!(c.next_table_id, 0);
        assert!(c.tables.is_empty());
    }
}
