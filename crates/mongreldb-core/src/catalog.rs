//! DB-wide catalog checkpoint (spec §5.1).
//!
//! The catalog records every table's id, name, schema, and live/dropped state
//! plus the DB-wide monotonic counters (`db_epoch`, `next_table_id`,
//! `open_generation` (sidecar), `next_segment_no`). CATALOG is rewritten on
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
use std::io::Read;
use std::path::Path;

pub const CATALOG_FILENAME: &str = "CATALOG";
const MAGIC: &[u8; 8] = b"MONGRCAT";
const CATALOG_FORMAT_VERSION: u16 = 1;
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
    /// Hidden CTAS build state. It is mounted only by the creating handle and
    /// becomes queryable through one durable publish operation.
    Building {
        intended_name: String,
        query_id: String,
        created_at_unix_nanos: u64,
        #[serde(default)]
        replaces_table_id: Option<u64>,
    },
}

/// One row of the catalog.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogEntry {
    pub table_id: u64,
    pub name: String,
    pub schema: Schema,
    pub state: TableState,
    pub created_epoch: u64,
}

/// Persistent definition for a physical materialized-view table.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MaterializedViewEntry {
    pub name: String,
    pub query: String,
    pub last_refresh_epoch: u64,
    #[serde(default)]
    pub incremental: Option<IncrementalAggregateView>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IncrementalAggregateView {
    pub source_table: String,
    pub source_table_id: u64,
    pub group_column: u16,
    pub group_output_column: u16,
    pub outputs: Vec<IncrementalAggregateOutput>,
    pub count_output_column: u16,
    pub checkpoint_event_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IncrementalAggregateOutput {
    pub output_column: u16,
    pub kind: IncrementalAggregateKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum IncrementalAggregateKind {
    Count,
    Sum { source_column: u16 },
}

/// The full in-memory catalog, mirrored on disk by [`write_atomic`].
///
/// Note: `open_generation` is intentionally **not** stored here — it bumps on
/// every open, and keeping it in CATALOG would dirty the working tree even for
/// a bare read. It lives in a separate sidecar file (`_meta/generation`) that
/// callers can `.gitignore` for content-addressed storage workflows.
#[derive(Debug, Clone, Serialize, Default)]
pub struct Catalog {
    /// Highest epoch ever assigned by this DB's commit sequencer.
    pub db_epoch: u64,
    /// Next table id to allocate.
    pub next_table_id: u64,
    /// Next shared-WAL segment number to allocate.
    pub next_segment_no: u64,
    pub tables: Vec<CatalogEntry>,
    #[serde(default)]
    pub procedures: Vec<ProcedureEntry>,
    #[serde(default)]
    pub triggers: Vec<TriggerEntry>,
    #[serde(default)]
    pub external_tables: Vec<ExternalTableEntry>,
    #[serde(default)]
    pub materialized_views: Vec<MaterializedViewEntry>,
    #[serde(default)]
    pub security: crate::security::SecurityCatalog,
    /// Monotonic version for optimistic authorization snapshots.
    #[serde(default)]
    pub security_version: u64,
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
    /// See `docs/15-credential-enforcement.md`.
    #[serde(default)]
    pub require_auth: bool,
    /// SQLite-compatible application metadata used by the SQL layer.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_version: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub application_id: Option<i64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogWire {
    db_epoch: u64,
    next_table_id: u64,
    next_segment_no: u64,
    tables: Vec<CatalogEntry>,
    #[serde(default)]
    procedures: Vec<ProcedureEntry>,
    #[serde(default)]
    triggers: Vec<TriggerEntry>,
    #[serde(default)]
    external_tables: Vec<ExternalTableEntry>,
    #[serde(default)]
    materialized_views: Vec<MaterializedViewEntry>,
    #[serde(default)]
    security: crate::security::SecurityCatalog,
    #[serde(default)]
    security_version: u64,
    #[serde(default)]
    users: Vec<crate::auth::UserEntry>,
    #[serde(default)]
    roles: Vec<crate::auth::RoleEntry>,
    #[serde(default)]
    next_user_id: u64,
    #[serde(default)]
    require_auth: bool,
    #[serde(default)]
    user_version: Option<i64>,
    #[serde(default)]
    application_id: Option<i64>,
    // Known pre-sidecar field. It is intentionally ignored during migration;
    // every other unknown top-level field remains rejected.
    #[serde(default)]
    open_generation: Option<u64>,
}

impl<'de> Deserialize<'de> for Catalog {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let wire = CatalogWire::deserialize(deserializer)?;
        let _ = wire.open_generation;
        Ok(Self {
            db_epoch: wire.db_epoch,
            next_table_id: wire.next_table_id,
            next_segment_no: wire.next_segment_no,
            tables: wire.tables,
            procedures: wire.procedures,
            triggers: wire.triggers,
            external_tables: wire.external_tables,
            materialized_views: wire.materialized_views,
            security: wire.security,
            security_version: wire.security_version,
            users: wire.users,
            roles: wire.roles,
            next_user_id: wire.next_user_id,
            require_auth: wire.require_auth,
            user_version: wire.user_version,
            application_id: wire.application_id,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CatalogEnvelope {
    format_version: u16,
    catalog: Catalog,
}

/// The `open_generation` counter, stored in `_meta/generation` (NOT in CATALOG).
/// Bumped on every open to scope `txn_id` across reopens so ids never alias.
///
/// Kept as a sidecar so that CATALOG is stable across bare opens — the only
/// volatile bytes in the database directory during a read-only session are
/// this 8-byte file + `.lock` + caches, all of which can be `.gitignore`-d.
pub const GENERATION_FILENAME: &str = "generation";

/// Read `open_generation` from `_meta/generation`. Missing is reported
/// separately for one-time migration; malformed or unreadable state fails
/// closed because resetting this counter can alias retained WAL transactions.
pub fn read_generation(root: &crate::durable_file::DurableRoot) -> Result<Option<u64>> {
    let relative = Path::new("_meta").join(GENERATION_FILENAME);
    match root.entry_exists(&relative) {
        Ok(true) => {}
        Ok(false) => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let mut file = root.open_regular(&relative)?;
    let length = file.metadata()?.len();
    if length != 8 {
        return Err(MongrelError::Other(format!(
            "invalid open-generation length: got {length}, expected 8"
        )));
    }
    let mut bytes = [0_u8; 8];
    std::io::Read::read_exact(&mut file, &mut bytes)?;
    Ok(Some(u64::from_le_bytes(bytes)))
}

/// Write `open_generation` to `_meta/generation` atomically (temp + rename +
/// fsync). This is intentionally a separate file from CATALOG so that CATALOG
/// stays byte-stable across bare opens (the generation counter is the only
/// field that changes on every open).
pub fn write_generation(root: &crate::durable_file::DurableRoot, generation: u64) -> Result<()> {
    root.create_directory_all("_meta")?;
    root.write_atomic(
        Path::new("_meta").join(GENERATION_FILENAME),
        &generation.to_le_bytes(),
    )?;
    Ok(())
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

    pub(crate) fn building(&self, name: &str) -> Option<&CatalogEntry> {
        self.tables
            .iter()
            .find(|table| table.name == name && matches!(table.state, TableState::Building { .. }))
    }

    pub(crate) fn building_for(&self, intended_name: &str) -> Option<&CatalogEntry> {
        self.tables.iter().find(|table| {
            matches!(
                &table.state,
                TableState::Building {
                    intended_name: candidate,
                    ..
                } if candidate == intended_name
            )
        })
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
    write_atomic_controlled(dir, cat, meta_dek, || Ok(()))
}

/// Prepare and fsync a complete catalog replacement, then invoke
/// `before_publish` immediately before the atomic rename makes it durable.
pub fn write_atomic_controlled<F>(
    dir: &Path,
    cat: &Catalog,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
    before_publish: F,
) -> Result<()>
where
    F: FnOnce() -> Result<()>,
{
    write_atomic_controlled_with_after(dir, cat, meta_dek, before_publish, || {})
}

/// Controlled catalog replacement with a live-publication callback. The
/// callback runs after rename, before the parent directory is fsynced. Thus a
/// later error means the replacement is visible in this process but its
/// crash-durable outcome is unknown.
pub(crate) fn write_atomic_controlled_with_after<F, A>(
    dir: &Path,
    cat: &Catalog,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
    before_publish: F,
    after_publish: A,
) -> Result<()>
where
    F: FnOnce() -> Result<()>,
    A: FnOnce(),
{
    let body = encode(cat)?;
    let payload = seal(&body, meta_dek)?;

    let root = crate::durable_file::DurableRoot::open(dir)?;
    root.write_atomic_controlled_with_after(
        CATALOG_FILENAME,
        &payload,
        before_publish,
        after_publish,
    )?;
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

pub(crate) fn encode(catalog: &Catalog) -> Result<Vec<u8>> {
    serde_json::to_vec(&CatalogEnvelope {
        format_version: CATALOG_FORMAT_VERSION,
        catalog: catalog.clone(),
    })
    .map_err(|error| MongrelError::Other(format!("catalog serialize: {error}")))
}

pub(crate) fn write_durable(
    root: &crate::durable_file::DurableRoot,
    catalog: &Catalog,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<()> {
    let body = encode(catalog)?;
    let payload = seal(&body, meta_dek)?;
    root.write_atomic(CATALOG_FILENAME, &payload)?;
    Ok(())
}

pub(crate) fn decode(body: &[u8]) -> Result<Catalog> {
    let value: serde_json::Value = serde_json::from_slice(body)
        .map_err(|error| MongrelError::Other(format!("catalog deserialize: {error}")))?;
    let is_envelope = value.as_object().is_some_and(|object| {
        object.contains_key("format_version") || object.contains_key("catalog")
    });
    if !is_envelope {
        // Legacy pre-envelope catalogs remain readable, but unknown fields are
        // rejected by `Catalog::deny_unknown_fields`.
        return serde_json::from_value(value)
            .map_err(|error| MongrelError::Other(format!("catalog deserialize: {error}")));
    }
    let envelope: CatalogEnvelope = serde_json::from_value(value)
        .map_err(|error| MongrelError::Other(format!("catalog deserialize: {error}")))?;
    if envelope.format_version != CATALOG_FORMAT_VERSION {
        return Err(MongrelError::Other(format!(
            "unsupported catalog format version {}",
            envelope.format_version
        )));
    }
    Ok(envelope.catalog)
}

fn deserialize(body: &[u8]) -> Result<Option<Catalog>> {
    decode(body).map(Some)
}

/// Read the catalog from `<dir>/CATALOG`. Returns `Ok(None)` if no catalog is
/// present, or if authentication fails (tampered / wrong key).
pub fn read(dir: &Path, meta_dek: Option<&[u8; META_DEK_LEN]>) -> Result<Option<Catalog>> {
    let p = dir.join(CATALOG_FILENAME);
    let file = match crate::durable_file::open_regular_nofollow(&p) {
        Ok(file) => file,
        Err(MongrelError::Io(e)) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    read_file(file, meta_dek)
}

pub(crate) fn read_durable(
    root: &crate::durable_file::DurableRoot,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<Option<Catalog>> {
    let file = match root.open_regular(CATALOG_FILENAME) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    read_file(file, meta_dek)
}

fn read_file(
    file: std::fs::File,
    meta_dek: Option<&[u8; META_DEK_LEN]>,
) -> Result<Option<Catalog>> {
    const MAX_CATALOG_BYTES: u64 = 64 * 1024 * 1024;
    let length = file.metadata()?.len();
    if length > MAX_CATALOG_BYTES {
        return Err(MongrelError::ResourceLimitExceeded {
            resource: "catalog bytes",
            requested: usize::try_from(length).unwrap_or(usize::MAX),
            limit: MAX_CATALOG_BYTES as usize,
        });
    }
    let mut bytes = Vec::with_capacity(length as usize);
    file.take(MAX_CATALOG_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 != length {
        return Err(MongrelError::Other(
            "catalog length changed while reading".into(),
        ));
    }
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
