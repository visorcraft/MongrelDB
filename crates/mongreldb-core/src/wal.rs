//! Append-only, group-commit, torn-write-safe WAL.
//!
//! Sub-ms writes come from the fact that [`Wal::append`] only copies bytes into
//! the OS file buffer (and an in-process [`BufWriter`]); it does **not** fsync.
//! A timer- or threshold-driven [`Wal::sync`] does the `flush() + sync_all()`
//! and bumps the epoch.

use crate::epoch::Epoch;
use crate::manifest::TtlPolicy;
use crate::rowid::RowId;
use crate::schema::{ColumnDef, Schema};
use crate::{MongrelError, Result};
use crc::{Crc, CRC_32_ISCSI};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use zeroize::Zeroizing;

fn unix_nanos_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

pub const WAL_MAGIC: [u8; 8] = *b"MONGRWAL";
/// The only WAL format this engine reads or writes. Older segments (v3 and
/// below) are rejected with [`MongrelError::UnsupportedStorageVersion`].
pub(crate) const WAL_VERSION: u16 = 4;
const HEADER_LEN: u64 = 8 + 2 + 4 + 8 + 8 + 32;
const WAL_FRAME_AAD_DOMAIN: &[u8] = b"mongreldb/wal-frame/v4";
const WAL_HEAD_MAGIC: [u8; 8] = *b"MONGWHED";
const WAL_HEAD_VERSION: u16 = 1;
const WAL_HEAD_FILENAME: &str = "wal-head-v1";
const WAL_HEAD_AUTH_DOMAIN: &[u8] = b"mongreldb/wal-head/v1";
const WAL_HEAD_BODY_LEN: usize = 72;
const WAL_HEAD_LEN: usize = WAL_HEAD_BODY_LEN + 32;
const MAX_RECOVERY_WAL_BYTES: u64 = 512 * 1024 * 1024;
const MAX_RECOVERY_WAL_RECORDS: usize = 1_000_000;
/// Encryption flag stored in reserved[0] of the WAL header.
const ENC_PLAINTEXT: u8 = 0;
const ENC_AES_GCM: u8 = 1;

#[derive(Clone, Copy, Debug)]
struct WalHead {
    segment_no: u64,
    durable_len: u64,
    open_generation: u64,
    prefix_hash: [u8; 32],
}

/// `txn_id` reserved for system records (`Flush`) that are not part of any
/// client transaction.
pub const SYSTEM_TXN_ID: u64 = 0;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// One mutation. `Put.rows` is a self-describing Arrow IPC stream (or, for tiny
/// single-row writes, a compact row batch — both are opaque bytes to the WAL).
/// `txn_id` groups records into a transaction; the group is sealed by a
/// [`Op::TxnCommit`] carrying the same `txn_id`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub seq: Epoch,
    pub txn_id: u64,
    pub op: Op,
}

/// A sorted run made durable as part of a transaction's commit (spec §7.1).
/// Recovery links these into the table's run list at the commit epoch.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddedRun {
    pub table_id: u64,
    pub run_id: u128,
    pub row_count: u64,
    pub level: u8,
    pub min_row_id: u64,
    pub max_row_id: u64,
    pub content_hash: [u8; 32],
}

/// A schema change logged through the WAL (spec §7.1; full DDL wiring in P2.7).
/// The schema/column payload is carried as JSON bytes because `Schema`'s
/// internally-tagged `TypeId` is not representable under the WAL's bincode frame
/// encoding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DdlOp {
    CreateTable {
        table_id: u64,
        name: String,
        schema_json: Vec<u8>,
    },
    DropTable {
        table_id: u64,
    },
    /// Replace one existing column definition with the JSON-encoded
    /// [`ColumnDef`] produced by native ALTER COLUMN validation.
    AlterTable {
        table_id: u64,
        column_json: Vec<u8>,
    },
    /// Rename a live table. The catalog entry's name changes; the `table_id`,
    /// schema, on-disk layout, and in-memory table object are all untouched
    /// (the table is keyed by `table_id`, not name). Recovery applies this by
    /// rewriting the catalog entry's name; it is idempotent when the
    /// checkpoint already carries `new_name`.
    RenameTable {
        table_id: u64,
        new_name: String,
    },
    /// Replace or clear a table's manifest-backed TTL policy.
    SetTtl {
        table_id: u64,
        policy_json: Vec<u8>,
    },
    /// Create or update a persistent materialized-view definition. Appended at
    /// the end of the enum to preserve earlier bincode discriminants.
    SetMaterializedView {
        name: String,
        definition_json: Vec<u8>,
    },
    /// Full persistent RLS/masking catalog replacement.
    SetSecurityCatalog {
        security_json: Vec<u8>,
    },
    /// Create a hidden CTAS build table. Appended to preserve prior bincode
    /// discriminants.
    CreateBuildingTable {
        table_id: u64,
        build_name: String,
        intended_name: String,
        query_id: String,
        created_at_unix_nanos: u64,
        schema_json: Vec<u8>,
    },
    /// Atomically publish a hidden CTAS build under its intended live name.
    PublishBuildingTable {
        table_id: u64,
        new_name: String,
    },
    /// Create a hidden replacement build while its original table stays live.
    /// Appended to preserve prior bincode discriminants.
    CreateRebuildingTable {
        table_id: u64,
        build_name: String,
        intended_name: String,
        query_id: String,
        created_at_unix_nanos: u64,
        replaces_table_id: u64,
        schema_json: Vec<u8>,
    },
    /// Atomically replace a live table with its completed hidden build.
    ReplaceBuildingTable {
        table_id: u64,
        replaced_table_id: u64,
        new_name: String,
    },
    /// Persist SQLite-compatible application metadata. Appended to preserve
    /// all earlier bincode discriminants.
    SetSqlPragma {
        key: String,
        value: i64,
    },
    /// Exact post-commit catalog image. Appended after operation-specific DDL
    /// so recovery, PITR, and logical replication preserve all derived catalog
    /// mutations. Appended last to preserve prior bincode discriminants.
    CatalogSnapshot {
        catalog_json: Vec<u8>,
    },
    /// Remove connector state when an external-table generation is created or
    /// dropped. Appended last to preserve prior bincode discriminants.
    ResetExternalTableState {
        name: String,
        generation_epoch: u64,
    },
    /// One encoded [`mongreldb_log::CommandEnvelope`] proposed through the
    /// standalone commit log (spec §9.4, FND-004). Opaque to the engine:
    /// recovery, PITR, and CDC ignore it (every replay path wildcards unknown
    /// DDL operations). Appended last to preserve prior bincode discriminants.
    Command {
        payload: Vec<u8>,
    },
}

impl DdlOp {
    /// Encode a schema for [`DdlOp::CreateTable`].
    pub fn encode_schema(schema: &Schema) -> Result<Vec<u8>> {
        serde_json::to_vec(schema).map_err(|e| MongrelError::Other(format!("schema json: {e}")))
    }

    /// Decode a schema carried by [`DdlOp::CreateTable`].
    pub fn decode_schema(bytes: &[u8]) -> Result<Schema> {
        serde_json::from_slice(bytes).map_err(|e| MongrelError::Other(format!("schema json: {e}")))
    }

    pub fn encode_column(column: &ColumnDef) -> Result<Vec<u8>> {
        serde_json::to_vec(column).map_err(|e| MongrelError::Other(format!("column json: {e}")))
    }

    pub fn decode_column(bytes: &[u8]) -> Result<ColumnDef> {
        serde_json::from_slice(bytes).map_err(|e| MongrelError::Other(format!("column json: {e}")))
    }

    pub fn encode_ttl(policy: Option<TtlPolicy>) -> Result<Vec<u8>> {
        serde_json::to_vec(&policy).map_err(|e| MongrelError::Other(format!("TTL json: {e}")))
    }

    pub fn decode_ttl(bytes: &[u8]) -> Result<Option<TtlPolicy>> {
        serde_json::from_slice(bytes).map_err(|e| MongrelError::Other(format!("TTL json: {e}")))
    }

    pub fn encode_materialized_view(
        definition: &crate::catalog::MaterializedViewEntry,
    ) -> Result<Vec<u8>> {
        serde_json::to_vec(definition)
            .map_err(|e| MongrelError::Other(format!("materialized view json: {e}")))
    }

    pub fn decode_materialized_view(bytes: &[u8]) -> Result<crate::catalog::MaterializedViewEntry> {
        serde_json::from_slice(bytes)
            .map_err(|e| MongrelError::Other(format!("materialized view json: {e}")))
    }

    pub fn encode_catalog(catalog: &crate::catalog::Catalog) -> Result<Vec<u8>> {
        crate::catalog::encode(catalog)
    }

    pub fn decode_catalog(bytes: &[u8]) -> Result<crate::catalog::Catalog> {
        crate::catalog::decode(bytes)
    }

    pub fn encode_security(security: &crate::security::SecurityCatalog) -> Result<Vec<u8>> {
        serde_json::to_vec(security)
            .map_err(|e| MongrelError::Other(format!("security catalog json: {e}")))
    }

    pub fn decode_security(bytes: &[u8]) -> Result<crate::security::SecurityCatalog> {
        serde_json::from_slice(bytes)
            .map_err(|e| MongrelError::Other(format!("security catalog json: {e}")))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Put {
        table_id: u64,
        rows: Vec<u8>,
    },
    Delete {
        table_id: u64,
        row_ids: Vec<RowId>,
    },
    TruncateTable {
        table_id: u64,
    },
    /// Durable module-owned state for an external table. The payload is opaque
    /// to the core; recovery writes the last committed payloads back under
    /// `_vtab/<name>/state.json`.
    ExternalTableState {
        name: String,
        state: Vec<u8>,
    },
    /// System marker (txn_id == [`SYSTEM_TXN_ID`]): everything up to
    /// `flushed_epoch` for `table_id` is durable in a sorted run, so recovery
    /// may skip replaying older records for that table.
    Flush {
        table_id: u64,
        flushed_epoch: u64,
    },
    /// Seals a transaction: every earlier record with the same `txn_id` is
    /// committed and becomes visible at `epoch`.
    TxnCommit {
        epoch: u64,
        added_runs: Vec<AddedRun>,
    },
    /// Aborts a transaction; its staged records are discarded on recovery.
    TxnAbort,
    Ddl(DdlOp),
    /// Row image captured immediately before a Delete in the same committed
    /// transaction. Recovery ignores it; durable CDC uses it for delete/update
    /// deltas. Appended last to preserve all earlier bincode discriminants.
    BeforeImage {
        table_id: u64,
        row_id: RowId,
        row: Vec<u8>,
    },
    /// Commit-timestamp ledger record written immediately before TxnCommit.
    /// Carries the physical component of the commit's HLC timestamp (spec
    /// §8.1, micros × 1_000) for transactions sequenced through the commit
    /// log, and wall-clock UTC nanoseconds on the legacy single-table/DDL
    /// append paths. PITR uses this durable ledger to map timestamp cutoffs
    /// to commit epochs.
    CommitTimestamp {
        unix_nanos: u64,
    },
    /// Logical rows for a transaction whose normal recovery path links an
    /// immutable spilled run. Ordinary recovery ignores this duplicate
    /// payload; PITR and CDC use it after the source run has been compacted or
    /// garbage-collected. Chunks stay below the WAL reader frame limit.
    SpilledRows {
        table_id: u64,
        rows: Vec<u8>,
    },
}

impl Record {
    pub fn new(seq: Epoch, txn_id: u64, op: Op) -> Self {
        Self { seq, txn_id, op }
    }
}

/// Group-commit WAL writer. Append is O(buffer copy) and never fsyncs; callers
/// (or a timer) drive [`Wal::sync`].
pub struct Wal {
    file: BufWriter<File>,
    path: PathBuf,
    /// Next sequence number to assign; equals `last_assigned.0 + 1`.
    next_seq: u64,
    unflushed_bytes: u64,
    /// `sync()` automatically once this many bytes are buffered (0 = manual).
    sync_byte_threshold: u64,
    /// Optional AEAD cipher for frame-level encryption. When present, each
    /// frame's payload is encrypted before writing.
    cipher: Option<Box<dyn crate::encryption::Cipher>>,
    /// Persisted segment number for this WAL segment. Forms the high 8 bytes
    /// (big-endian) of the 12-byte AES-GCM nonce; the low 4 are the per-segment
    /// frame counter. The WAL DEK is constant across all segments, so cross-
    /// segment nonce uniqueness rests entirely on this number, which is drawn
    /// from the canonical monotonically increasing segment sequence. Existing
    /// segment paths are never truncated or reused.
    segment_no: u64,
    /// Per-segment frame counter. Occupies the low 4 bytes of the nonce, so
    /// `append_record` refuses to write past `u32::MAX` frames in one segment
    /// (that would truncate the counter and reuse a nonce under the DEK).
    /// Segments rotate at flush long before this, so it is unreachable in
    /// practice — but enforced rather than assumed.
    frame_seq: u64,
    previous_segment_hash: [u8; 32],
    header_binding: [u8; 32],
}

impl Wal {
    /// Create a new WAL segment. Existing paths are never replaced.
    pub fn create(path: impl AsRef<Path>, epoch_created: Epoch) -> Result<Self> {
        let path = path.as_ref();
        let segment_no = segment_number_from_path(path).unwrap_or(0);
        Self::create_with_cipher(path, epoch_created, None, segment_no)
    }

    /// Create a new WAL segment with optional frame-level encryption. The
    /// persisted `segment_no` namespaces AES-GCM nonces across segments under
    /// the constant WAL DEK (spec §7.1, review fix #23).
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        epoch_created: Epoch,
        cipher: Option<Box<dyn crate::encryption::Cipher>>,
        segment_no: u64,
    ) -> Result<Self> {
        Self::create_chained(path, epoch_created, cipher, segment_no, [0; 32])
    }

    fn create_chained(
        path: impl AsRef<Path>,
        epoch_created: Epoch,
        cipher: Option<Box<dyn crate::encryption::Cipher>>,
        segment_no: u64,
        previous_segment_hash: [u8; 32],
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        let wal = Self::create_chained_from_file(
            file,
            path,
            epoch_created,
            cipher,
            segment_no,
            previous_segment_hash,
        )?;
        if let Some(parent) = wal.path.parent() {
            crate::durable_file::sync_directory(parent)?;
        }
        Ok(wal)
    }

    fn create_chained_in(
        wal_root: &crate::durable_file::DurableRoot,
        segment_no: u64,
        epoch_created: Epoch,
        cipher: Option<Box<dyn crate::encryption::Cipher>>,
        previous_segment_hash: [u8; 32],
    ) -> Result<Self> {
        let name = segment_filename(segment_no);
        let file = wal_root.create_regular_new(&name)?;
        let wal = Self::create_chained_from_file(
            file,
            wal_root.canonical_path().join(&name),
            epoch_created,
            cipher,
            segment_no,
            previous_segment_hash,
        )?;
        wal_root.sync_entry_parent(&name)?;
        Ok(wal)
    }

    fn create_chained_from_file(
        file: File,
        path: PathBuf,
        epoch_created: Epoch,
        cipher: Option<Box<dyn crate::encryption::Cipher>>,
        segment_no: u64,
        previous_segment_hash: [u8; 32],
    ) -> Result<Self> {
        let next_seq = epoch_created
            .0
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("WAL sequence namespace exhausted".into()))?;
        let mut wal = Self {
            file: BufWriter::with_capacity(1 << 20, file),
            path,
            next_seq,
            unflushed_bytes: 0,
            sync_byte_threshold: 64 * 1024,
            cipher,
            segment_no,
            frame_seq: 0,
            previous_segment_hash,
            header_binding: [0; 32],
        };
        wal.write_header(epoch_created)?;
        // A WAL segment is an authoritative commit prerequisite. Persist both
        // its header and its directory entry before any caller can append a
        // transaction that later reports a durable commit.
        wal.sync()?;
        Ok(wal)
    }

    /// Append a record belonging to transaction `txn_id`. Assigns the next
    /// monotonic sequence (the first record after a WAL created at `E` gets
    /// `E + 1`), writes it, and returns the assigned sequence. Does NOT fsync —
    /// call [`Wal::sync`] (or rely on the byte threshold). The WAL sequence is
    /// independent of the row commit epoch; the engine tracks commit epochs
    /// separately.
    pub fn append_txn(&mut self, txn_id: u64, op: Op) -> Result<Epoch> {
        let seq = Epoch(self.next_seq);
        let next_seq = self
            .next_seq
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("WAL sequence namespace exhausted".into()))?;
        self.append_record(&Record::new(seq, txn_id, op))?;
        self.next_seq = next_seq;
        Ok(seq)
    }

    /// Append a system record (txn_id == [`SYSTEM_TXN_ID`]), e.g. `Flush`.
    pub fn append_system(&mut self, op: Op) -> Result<Epoch> {
        self.append_txn(SYSTEM_TXN_ID, op)
    }

    fn append_record(&mut self, record: &Record) -> Result<()> {
        let payload = bincode::serialize(record)?;

        // Encrypt the payload if a cipher is present. The nonce is prepended
        // to the ciphertext so the reader can extract it from a single read.
        let frame_payload = if let Some(cipher) = &self.cipher {
            // The frame counter occupies the low 4 bytes of the nonce. Refuse to
            // wrap it — a wrapped counter would reuse a nonce under the constant
            // WAL DEK (catastrophic for AES-GCM). Unreachable in practice
            // (segments rotate at flush), but enforced.
            if self.frame_seq > u32::MAX as u64 {
                return Err(MongrelError::Full(
                    "wal segment frame counter exhausted (2^32); rotate the segment".into(),
                ));
            }
            let nonce = self.frame_nonce();
            let ciphertext_len = payload.len().checked_add(16).ok_or_else(|| {
                MongrelError::InvalidArgument("wal payload length overflow".into())
            })?;
            let aad = wal_frame_aad(
                &self.header_binding,
                self.segment_no,
                self.frame_seq as u32,
                record.seq.0,
                record.txn_id,
                ciphertext_len as u64,
            );
            let ciphertext = cipher.encrypt_page_with_aad(&nonce, &payload, &aad)?;
            self.frame_seq += 1;
            ciphertext
        } else {
            payload
        };

        let len = frame_payload.len();
        if len > u32::MAX as usize {
            return Err(MongrelError::InvalidArgument(format!(
                "wal payload too large: {len} bytes"
            )));
        }
        // CRC covers seq + txn_id + (encrypted) payload.
        let mut digest = CRC32C.digest();
        digest.update(&record.seq.0.to_le_bytes());
        digest.update(&record.txn_id.to_le_bytes());
        digest.update(&frame_payload);
        let crc_val = digest.finalize();

        self.file.write_all(&(len as u32).to_le_bytes())?;
        self.file.write_all(&crc_val.to_le_bytes())?;
        self.file.write_all(&record.seq.0.to_le_bytes())?;
        self.file.write_all(&record.txn_id.to_le_bytes())?;
        self.file.write_all(&frame_payload)?;
        self.unflushed_bytes += 4 + 4 + 8 + 8 + len as u64;
        if self.sync_byte_threshold > 0 && self.unflushed_bytes >= self.sync_byte_threshold {
            self.sync()?;
        }
        Ok(())
    }

    /// Build the 12-byte AES-GCM nonce for the current frame:
    /// `[segment_no: 8B BE][frame_seq: 4B LE]`. `segment_no` is persisted and
    /// monotonic across segments; the counter is unique within a segment, so
    /// nonces never repeat under the constant WAL DEK — provided
    /// `frame_seq <= u32::MAX`, which `append_record` enforces before calling.
    fn frame_nonce(&self) -> [u8; 12] {
        frame_nonce_for(self.segment_no, self.frame_seq as u32)
    }

    /// Flush the buffer and fsync the file. This is the durability point.
    pub fn sync(&mut self) -> Result<()> {
        self.file.flush()?;
        self.file.get_ref().sync_all()?;
        self.unflushed_bytes = 0;
        Ok(())
    }

    /// Pending bytes not yet fsynced.
    #[inline]
    pub fn unflushed_bytes(&self) -> u64 {
        self.unflushed_bytes
    }

    /// The next sequence number this writer will assign (i.e. last assigned + 1).
    /// Exposed so a shared-WAL group-sync can report the durable high-water mark.
    #[inline]
    pub fn next_seq_val(&self) -> u64 {
        self.next_seq
    }

    /// Tune the auto-sync threshold (bytes of buffered WAL before an automatic
    /// `fsync`). `0` disables auto-sync entirely (manual [`Wal::sync`] only) —
    /// useful for latency benchmarks and for grouping many writes under one
    /// explicit commit.
    pub fn set_sync_byte_threshold(&mut self, threshold: u64) {
        self.sync_byte_threshold = threshold;
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Publish a fully synced staging segment under its final no-replace name.
    /// The old segment remains authoritative until this succeeds.
    pub(crate) fn publish_as(mut self, destination: PathBuf) -> Result<Self> {
        self.sync()?;
        crate::durable_file::rename(&self.path, &destination)?;
        self.path = destination;
        Ok(self)
    }

    fn write_header(&mut self, epoch_created: Epoch) -> Result<()> {
        let enc_flag = if self.cipher.is_some() {
            ENC_AES_GCM
        } else {
            ENC_PLAINTEXT
        };
        let header = encode_wal_header(
            enc_flag,
            epoch_created.0,
            self.segment_no,
            &self.previous_segment_hash,
        );
        self.header_binding = Sha256::digest(&header).into();
        self.file.write_all(&header)?;
        self.unflushed_bytes = 0;
        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let _ = self.file.flush();
    }
}

fn encode_wal_header(
    encryption: u8,
    epoch_created: u64,
    segment_no: u64,
    previous_segment_hash: &[u8; 32],
) -> Vec<u8> {
    let mut header = Vec::with_capacity(HEADER_LEN as usize);
    header.extend_from_slice(&WAL_MAGIC);
    header.extend_from_slice(&WAL_VERSION.to_le_bytes());
    header.extend_from_slice(&[encryption, 0, 0, 0]);
    header.extend_from_slice(&epoch_created.to_le_bytes());
    header.extend_from_slice(&segment_no.to_le_bytes());
    header.extend_from_slice(previous_segment_hash);
    header
}

fn wal_frame_aad(
    header_binding: &[u8; 32],
    segment_no: u64,
    frame_seq: u32,
    record_seq: u64,
    txn_id: u64,
    ciphertext_len: u64,
) -> Vec<u8> {
    let mut aad = Vec::with_capacity(WAL_FRAME_AAD_DOMAIN.len() + 68);
    aad.extend_from_slice(WAL_FRAME_AAD_DOMAIN);
    aad.extend_from_slice(header_binding);
    aad.extend_from_slice(&segment_no.to_le_bytes());
    aad.extend_from_slice(&frame_seq.to_le_bytes());
    aad.extend_from_slice(&record_seq.to_le_bytes());
    aad.extend_from_slice(&txn_id.to_le_bytes());
    aad.extend_from_slice(&ciphertext_len.to_le_bytes());
    aad
}

fn wal_head_body(head: &WalHead, encrypted: bool) -> [u8; WAL_HEAD_BODY_LEN] {
    let mut body = [0_u8; WAL_HEAD_BODY_LEN];
    body[..8].copy_from_slice(&WAL_HEAD_MAGIC);
    body[8..10].copy_from_slice(&WAL_HEAD_VERSION.to_le_bytes());
    body[10] = if encrypted {
        ENC_AES_GCM
    } else {
        ENC_PLAINTEXT
    };
    body[16..24].copy_from_slice(&head.segment_no.to_le_bytes());
    body[24..32].copy_from_slice(&head.durable_len.to_le_bytes());
    body[32..40].copy_from_slice(&head.open_generation.to_le_bytes());
    body[40..72].copy_from_slice(&head.prefix_hash);
    body
}

fn wal_head_auth(
    body: &[u8; WAL_HEAD_BODY_LEN],
    wal_dek: Option<&Zeroizing<[u8; 32]>>,
) -> Result<[u8; 32]> {
    if let Some(key) = wal_dek {
        {
            let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(&key[..])
                .map_err(|error| MongrelError::Encryption(error.to_string()))?;
            mac.update(WAL_HEAD_AUTH_DOMAIN);
            mac.update(body);
            return Ok(mac.finalize().into_bytes().into());
        }
    }
    let mut hash = Sha256::new();
    hash.update(WAL_HEAD_AUTH_DOMAIN);
    hash.update(body);
    Ok(hash.finalize().into())
}

fn encode_wal_head(
    head: &WalHead,
    wal_dek: Option<&Zeroizing<[u8; 32]>>,
) -> Result<[u8; WAL_HEAD_LEN]> {
    let body = wal_head_body(head, wal_dek.is_some());
    let auth = wal_head_auth(&body, wal_dek)?;
    let mut encoded = [0_u8; WAL_HEAD_LEN];
    encoded[..WAL_HEAD_BODY_LEN].copy_from_slice(&body);
    encoded[WAL_HEAD_BODY_LEN..].copy_from_slice(&auth);
    Ok(encoded)
}

fn decode_wal_head(encoded: &[u8], wal_dek: Option<&Zeroizing<[u8; 32]>>) -> Result<WalHead> {
    if encoded.len() != WAL_HEAD_LEN {
        return Err(MongrelError::CorruptWal {
            offset: 0,
            reason: format!(
                "WAL head is {} bytes, expected {WAL_HEAD_LEN}",
                encoded.len()
            ),
        });
    }
    let body: &[u8; WAL_HEAD_BODY_LEN] =
        encoded[..WAL_HEAD_BODY_LEN]
            .try_into()
            .map_err(|_| MongrelError::CorruptWal {
                offset: 0,
                reason: "invalid WAL head body".into(),
            })?;
    if body[..8] != WAL_HEAD_MAGIC {
        return Err(MongrelError::CorruptWal {
            offset: 0,
            reason: "invalid WAL head magic".into(),
        });
    }
    let version = u16::from_le_bytes([body[8], body[9]]);
    if version != WAL_HEAD_VERSION {
        return Err(MongrelError::CorruptWal {
            offset: 8,
            reason: format!("unsupported WAL head version {version}"),
        });
    }
    let expected_mode = if wal_dek.is_some() {
        ENC_AES_GCM
    } else {
        ENC_PLAINTEXT
    };
    if body[10] != expected_mode || body[11..16] != [0; 5] {
        return Err(MongrelError::CorruptWal {
            offset: 10,
            reason: "WAL head authentication mode or reserved bytes differ".into(),
        });
    }
    let expected_auth = wal_head_auth(body, wal_dek)?;
    if encoded[WAL_HEAD_BODY_LEN..] != expected_auth {
        return Err(MongrelError::CorruptWal {
            offset: WAL_HEAD_BODY_LEN as u64,
            reason: "WAL head authentication failed".into(),
        });
    }
    let mut prefix_hash = [0_u8; 32];
    prefix_hash.copy_from_slice(&body[40..72]);
    let mut segment_no = [0_u8; 8];
    segment_no.copy_from_slice(&body[16..24]);
    let mut durable_len = [0_u8; 8];
    durable_len.copy_from_slice(&body[24..32]);
    let mut open_generation = [0_u8; 8];
    open_generation.copy_from_slice(&body[32..40]);
    Ok(WalHead {
        segment_no: u64::from_le_bytes(segment_no),
        durable_len: u64::from_le_bytes(durable_len),
        open_generation: u64::from_le_bytes(open_generation),
        prefix_hash,
    })
}

fn hash_file_prefix(file: File, length: u64) -> Result<[u8; 32]> {
    if file.metadata()?.len() < length {
        return Err(MongrelError::CorruptWal {
            offset: length,
            reason: "WAL file is shorter than its durable head".into(),
        });
    }
    let mut reader = BufReader::new(file).take(length);
    let mut hash = Sha256::new();
    std::io::copy(&mut reader, &mut HashWriter(&mut hash))?;
    Ok(hash.finalize().into())
}

struct HashWriter<'a>(&'a mut Sha256);

impl Write for HashWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.update(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn hash_segment(wal_root: &crate::durable_file::DurableRoot, segment_no: u64) -> Result<[u8; 32]> {
    let name = segment_filename(segment_no);
    let file = wal_root.open_regular(&name)?;
    let length = file.metadata()?.len();
    hash_file_prefix(file, length)
}

fn read_wal_head(
    root: &crate::durable_file::DurableRoot,
    wal_dek: Option<&Zeroizing<[u8; 32]>>,
) -> Result<Option<WalHead>> {
    let relative = Path::new("_meta").join(WAL_HEAD_FILENAME);
    match root.entry_exists(&relative) {
        Ok(true) => {}
        Ok(false) => return Ok(None),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    }
    let mut file = root.open_regular(&relative)?;
    let length = file.metadata()?.len();
    if length != WAL_HEAD_LEN as u64 {
        return Err(MongrelError::CorruptWal {
            offset: 0,
            reason: format!("WAL head is {length} bytes, expected {WAL_HEAD_LEN}"),
        });
    }
    let mut encoded = [0_u8; WAL_HEAD_LEN];
    file.read_exact(&mut encoded)?;
    decode_wal_head(&encoded, wal_dek).map(Some)
}

fn write_wal_head(
    root: &crate::durable_file::DurableRoot,
    wal_root: &crate::durable_file::DurableRoot,
    segment_no: u64,
    open_generation: u64,
    wal_dek: Option<&Zeroizing<[u8; 32]>>,
) -> Result<WalHead> {
    let name = segment_filename(segment_no);
    let file = wal_root.open_regular(&name)?;
    let durable_len = file.metadata()?.len();
    let head = WalHead {
        segment_no,
        durable_len,
        open_generation,
        prefix_hash: hash_file_prefix(file, durable_len)?,
    };
    root.create_directory_all("_meta")?;
    root.write_atomic(
        Path::new("_meta").join(WAL_HEAD_FILENAME),
        &encode_wal_head(&head, wal_dek)?,
    )?;
    Ok(head)
}

/// Streaming reader used by recovery. Only a physically short final frame is
/// an admissible crash-torn tail; CRC, authentication, and decode failures are
/// corruption.
pub struct WalReader {
    inner: BufReader<File>,
    pos: u64,
    file_len: u64,
    /// True if frames are encrypted (enc_flag in header).
    encrypted: bool,
    /// Optional cipher for decryption.
    cipher: Option<Box<dyn crate::encryption::Cipher>>,
    version: u16,
    segment_no: u64,
    previous_segment_hash: [u8; 32],
    header_binding: [u8; 32],
    frame_seq: u64,
}

impl WalReader {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_cipher(path, None)
    }

    /// Open a WAL segment for reading, optionally with a decryption cipher.
    pub fn open_with_cipher(
        path: impl AsRef<Path>,
        cipher: Option<Box<dyn crate::encryption::Cipher>>,
    ) -> Result<Self> {
        Self::open_with_cipher_expected(path, cipher, None)
    }

    fn open_with_cipher_expected(
        path: impl AsRef<Path>,
        cipher: Option<Box<dyn crate::encryption::Cipher>>,
        expected_segment_no: Option<u64>,
    ) -> Result<Self> {
        let path = path.as_ref();
        let expected_segment_no = expected_segment_no.or_else(|| segment_number_from_path(path));
        let file = crate::durable_file::open_regular_nofollow(path)?;
        Self::open_file_with_cipher_expected(file, cipher, expected_segment_no)
    }

    fn open_file_with_cipher_expected(
        mut file: File,
        cipher: Option<Box<dyn crate::encryption::Cipher>>,
        expected_segment_no: Option<u64>,
    ) -> Result<Self> {
        let file_len = file.metadata()?.len();
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if magic != WAL_MAGIC {
            return Err(MongrelError::MagicMismatch {
                what: "wal",
                expected: WAL_MAGIC,
                got: magic,
            });
        }
        let mut version_buf = [0u8; 2];
        file.read_exact(&mut version_buf)?;
        let version = u16::from_le_bytes(version_buf);
        if version != WAL_VERSION {
            return Err(MongrelError::UnsupportedStorageVersion {
                component: "wal",
                found: version,
                supported: WAL_VERSION,
            });
        }
        let mut reserved = [0u8; 4];
        file.read_exact(&mut reserved)?;
        if !matches!(reserved[0], ENC_PLAINTEXT | ENC_AES_GCM) || reserved[1..] != [0, 0, 0] {
            return Err(MongrelError::CorruptWal {
                offset: 10,
                reason: "invalid WAL header flags".into(),
            });
        }
        let encrypted = reserved[0] == ENC_AES_GCM;
        let mut epoch_buf = [0u8; 8];
        file.read_exact(&mut epoch_buf)?;
        let _epoch_created = Epoch(u64::from_le_bytes(epoch_buf));
        let mut previous_segment_hash = [0_u8; 32];
        let segment_no = {
            let mut segment = [0_u8; 8];
            file.read_exact(&mut segment)?;
            file.read_exact(&mut previous_segment_hash)?;
            u64::from_le_bytes(segment)
        };
        if expected_segment_no.is_some_and(|expected| expected != segment_no) {
            return Err(MongrelError::CorruptWal {
                offset: 0,
                reason: format!(
                    "WAL header segment {segment_no} does not match filename segment {}",
                    expected_segment_no.unwrap()
                ),
            });
        }
        let pos = HEADER_LEN;
        let header_binding = Sha256::digest(encode_wal_header(
            reserved[0],
            _epoch_created.0,
            segment_no,
            &previous_segment_hash,
        ))
        .into();
        if encrypted && cipher.is_none() {
            return Err(MongrelError::Decryption(
                "WAL is encrypted but no passphrase or key was provided. \
                 Use Table::open_encrypted or Table::open_with_key."
                    .into(),
            ));
        }
        Ok(Self {
            inner: BufReader::new(file),
            pos,
            file_len,
            encrypted,
            cipher,
            version,
            segment_no,
            previous_segment_hash,
            header_binding,
            frame_seq: 0,
        })
    }

    /// Read the next record. Returns `Ok(None)` at a clean end-of-records
    /// (zero-length marker or EOF), and `Err(TornWrite)` for a partial record.
    pub fn next_record(&mut self) -> Result<Option<Record>> {
        if self.pos == self.file_len {
            return Ok(None);
        }
        if self.file_len.saturating_sub(self.pos) < 4 {
            return Err(MongrelError::TornWrite { offset: self.pos });
        }
        let mut len_buf = [0u8; 4];
        match self.inner.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(MongrelError::TornWrite { offset: self.pos });
            }
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len == 0 {
            return Err(MongrelError::CorruptWal {
                offset: self.pos,
                reason: "zero-length WAL frames are not valid".into(),
            });
        }
        // A runaway length (torn header or garbage) would trigger a huge
        // allocation; treat anything past the cap as a torn write.
        const MAX_RECORD_LEN: usize = 64 * 1024 * 1024;
        if len > MAX_RECORD_LEN {
            return Err(MongrelError::CorruptWal {
                offset: self.pos,
                reason: format!("WAL frame length {len} exceeds limit"),
            });
        }

        let record_start = self.pos;
        let mut rest = vec![0u8; 4 + 8 + 8 + len];
        match self.inner.read_exact(&mut rest) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Err(MongrelError::TornWrite {
                    offset: record_start,
                });
            }
            Err(e) => return Err(e.into()),
        }
        let crc_val = u32::from_le_bytes([rest[0], rest[1], rest[2], rest[3]]);
        let seq = u64::from_le_bytes([
            rest[4], rest[5], rest[6], rest[7], rest[8], rest[9], rest[10], rest[11],
        ]);
        let txn_id = u64::from_le_bytes([
            rest[12], rest[13], rest[14], rest[15], rest[16], rest[17], rest[18], rest[19],
        ]);
        let payload = &rest[20..];

        let mut digest = CRC32C.digest();
        digest.update(&seq.to_le_bytes());
        digest.update(&txn_id.to_le_bytes());
        digest.update(payload);
        if digest.finalize() != crc_val {
            return Err(MongrelError::CorruptWal {
                offset: record_start,
                reason: "crc mismatch".into(),
            });
        }

        // Decrypt if encrypted.
        let plaintext = if self.encrypted {
            let Some(cipher) = &self.cipher else {
                return Err(MongrelError::Decryption(
                    "WAL is encrypted but no cipher was provided".into(),
                ));
            };
            let minimum = 16;
            if payload.len() < minimum {
                return Err(MongrelError::CorruptWal {
                    offset: record_start,
                    reason: "encrypted frame too short".into(),
                });
            }
            if self.frame_seq > u32::MAX as u64 {
                return Err(MongrelError::CorruptWal {
                    offset: record_start,
                    reason: "WAL frame counter exceeds u32".into(),
                });
            }
            let expected_nonce = frame_nonce_for(self.segment_no, self.frame_seq as u32);
            let aad = wal_frame_aad(
                &self.header_binding,
                self.segment_no,
                self.frame_seq as u32,
                seq,
                txn_id,
                payload.len() as u64,
            );
            cipher
                .decrypt_page_with_aad(&expected_nonce, payload, &aad)
                .map_err(|e| {
                    MongrelError::Decryption(format!(
                        "WAL frame decryption failed — wrong passphrase or key? ({e})"
                    ))
                })?
        } else {
            payload.to_vec()
        };

        let record: Record =
            bincode::deserialize(&plaintext).map_err(|error| MongrelError::CorruptWal {
                offset: record_start,
                reason: format!("WAL record decode failed: {error}"),
            })?;
        if record.seq.0 != seq || record.txn_id != txn_id {
            return Err(MongrelError::CorruptWal {
                offset: record_start,
                reason: "WAL outer and authenticated record identity differ".into(),
            });
        }
        self.frame_seq += 1;
        self.pos += 4 + 4 + 8 + 8 + len as u64;
        Ok(Some(record))
    }

    fn constrain_to_durable_len(&mut self, durable_len: u64) -> Result<()> {
        if durable_len < self.pos || durable_len > self.file_len {
            return Err(MongrelError::CorruptWal {
                offset: durable_len,
                reason: format!(
                    "WAL durable length {durable_len} is outside [{}, {}]",
                    self.pos, self.file_len
                ),
            });
        }
        self.file_len = durable_len;
        Ok(())
    }

    /// Replay all cleanly-committed records. A torn tail (crash mid-append or a
    /// partially-flushed last frame) is treated as end-of-log and truncated —
    /// the valid prefix is returned. A CRC failure or short read that is
    /// followed by a well-formed frame is treated as **interior corruption**
    /// and surfaces as [`MongrelError::CorruptWal`] (spec §8.4, review fix #22).
    pub fn replay(&mut self) -> Result<Vec<Record>> {
        self.replay_with_tail_policy(true)
    }

    fn replay_strict(&mut self) -> Result<Vec<Record>> {
        self.replay_with_tail_policy(false)
    }

    fn replay_bounded(&mut self, max_records: usize, allow_torn_tail: bool) -> Result<Vec<Record>> {
        let mut out = Vec::new();
        loop {
            match self.next_record() {
                Ok(Some(record)) => {
                    if out.len() >= max_records {
                        return Err(MongrelError::ResourceLimitExceeded {
                            resource: "WAL recovery records",
                            requested: max_records.saturating_add(1),
                            limit: max_records,
                        });
                    }
                    out.push(record);
                }
                Ok(None) => break,
                Err(MongrelError::TornWrite { offset }) => {
                    if !allow_torn_tail {
                        return Err(MongrelError::CorruptWal {
                            offset,
                            reason: "torn tail in a non-final WAL segment".into(),
                        });
                    }
                    if self.valid_frame_follows()? {
                        return Err(MongrelError::CorruptWal {
                            offset,
                            reason: "interior torn frame followed by a valid frame".into(),
                        });
                    }
                    break;
                }
                Err(error @ MongrelError::CorruptWal { .. }) => return Err(error),
                Err(error) => return Err(error),
            }
        }
        Ok(out)
    }

    fn replay_with_tail_policy(&mut self, allow_torn_tail: bool) -> Result<Vec<Record>> {
        let mut out = Vec::new();
        loop {
            match self.next_record() {
                Ok(Some(rec)) => out.push(rec),
                Ok(None) => break,
                Err(MongrelError::TornWrite { offset }) => {
                    if !allow_torn_tail {
                        return Err(MongrelError::CorruptWal {
                            offset,
                            reason: "torn tail in a non-final WAL segment".into(),
                        });
                    }
                    // Partial trailing frame: clean EOF unless a valid frame
                    // follows it (which would mean the torn frame is interior).
                    if self.valid_frame_follows()? {
                        return Err(MongrelError::CorruptWal {
                            offset,
                            reason: "interior torn frame followed by a valid frame".into(),
                        });
                    }
                    break;
                }
                Err(error @ MongrelError::CorruptWal { .. }) => return Err(error),
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    /// Replay cooperatively with a hard record bound.
    pub fn replay_controlled(
        &mut self,
        control: &crate::ExecutionControl,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        self.replay_controlled_with_tail_policy(control, max_records, true)
    }

    fn replay_controlled_strict(
        &mut self,
        control: &crate::ExecutionControl,
        max_records: usize,
    ) -> Result<Vec<Record>> {
        self.replay_controlled_with_tail_policy(control, max_records, false)
    }

    fn replay_controlled_with_tail_policy(
        &mut self,
        control: &crate::ExecutionControl,
        max_records: usize,
        allow_torn_tail: bool,
    ) -> Result<Vec<Record>> {
        let mut out = Vec::new();
        loop {
            if out.len() % 256 == 0 {
                control.checkpoint()?;
            }
            match self.next_record() {
                Ok(Some(record)) => {
                    if out.len() >= max_records {
                        return Err(MongrelError::ResourceLimitExceeded {
                            resource: "controlled WAL replay records",
                            requested: max_records.saturating_add(1),
                            limit: max_records,
                        });
                    }
                    out.push(record);
                }
                Ok(None) => break,
                Err(MongrelError::TornWrite { offset }) => {
                    if !allow_torn_tail {
                        return Err(MongrelError::CorruptWal {
                            offset,
                            reason: "torn tail in a non-final WAL segment".into(),
                        });
                    }
                    if self.valid_frame_follows()? {
                        return Err(MongrelError::CorruptWal {
                            offset,
                            reason: "interior torn frame followed by a valid frame".into(),
                        });
                    }
                    break;
                }
                Err(error @ MongrelError::CorruptWal { .. }) => return Err(error),
                Err(error) => return Err(error),
            }
        }
        control.checkpoint()?;
        Ok(out)
    }

    /// Probe whether a well-formed frame remains at the current read position.
    /// Used by [`Self::replay`] to disambiguate a trailing torn frame from
    /// interior corruption. The reader state is left positioned after the
    /// probed frame; `replay` stops after calling this regardless.
    fn valid_frame_follows(&mut self) -> Result<bool> {
        match self.next_record() {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(_) => Ok(false),
        }
    }

    /// Position the write cursor at end of file (for a reopen-and-append path,
    /// to be implemented alongside segment rotation).
    pub fn current_offset(&self) -> u64 {
        self.pos
    }
}

/// Replay every record from a WAL file, stopping at the first torn/corrupt one.
/// Convenience wrapper around [`WalReader`].
pub fn replay(path: impl AsRef<Path>) -> Result<Vec<Record>> {
    WalReader::open(path)?.replay()
}

/// Replay with an optional decryption cipher (for encrypted WAL segments).
pub fn replay_with_cipher(
    path: impl AsRef<Path>,
    cipher: Option<Box<dyn crate::encryption::Cipher>>,
) -> Result<Vec<Record>> {
    WalReader::open_with_cipher(path, cipher)?.replay()
}

/// Build the deterministic 12-byte AES-GCM nonce for `(segment_no, frame)`:
/// `[segment_no: 8B BE][frame: 4B LE]`. The high 8 bytes are unique per
/// segment (persisted monotonic counter) and the low 4 are unique per frame
/// within a segment, so the pair never collides under the constant WAL DEK.
pub fn frame_nonce_for(segment_no: u64, frame: u32) -> [u8; 12] {
    let mut n = [0u8; 12];
    n[..8].copy_from_slice(&segment_no.to_be_bytes());
    n[8..].copy_from_slice(&frame.to_le_bytes());
    n
}

/// A WAL shared across all tables of a `Database`, multiplexing many tables'
/// records onto one fd (spec §7.2). Owns the active `Wal` segment plus the list
/// of rotated segments under `<root>/_wal/`. Appends are buffered; a single
/// [`SharedWal::group_sync`] is the durability point for every concurrent
/// writer that appended since the last sync.
pub struct SharedWal {
    root: Arc<crate::durable_file::DurableRoot>,
    wal_root: Arc<crate::durable_file::DurableRoot>,
    wal_dir: PathBuf,
    active: Wal,
    /// Monotonic segment number of the active segment (namespaces nonces).
    active_segment_no: u64,
    /// Highest sequence number reported durable by the last successful
    /// `group_sync`. P3's group-commit publishes only commits at or below this.
    durable_seq: u64,
    /// Highest database-open generation sealed into the durable WAL head.
    open_generation: u64,
    /// WAL DEK (constant across segments). None for plaintext. Kept so a
    /// `rotate` can rebuild the per-segment cipher under the same key.
    wal_dek: Option<Zeroizing<[u8; 32]>>,
    /// Count of actual fsyncs issued via [`Self::group_sync`]. With real group
    /// commit this is far below the commit count (one leader fsync serves many
    /// followers). Diagnostic / test-facing.
    group_sync_count: u64,
}

#[derive(Default)]
struct ReplayTxnState {
    timestamp_seen: bool,
    terminal: bool,
}

struct WalLayout {
    segments: Vec<u64>,
    head: Option<WalHead>,
}

fn reader_for_segment(
    wal_root: &crate::durable_file::DurableRoot,
    segment_no: u64,
    wal_dek: Option<&Zeroizing<[u8; 32]>>,
) -> Result<WalReader> {
    let cipher = match wal_dek {
        Some(key) => Some(SharedWal::cipher_from_dek(key)?),
        None => None,
    };
    let file = wal_root.open_regular(segment_filename(segment_no))?;
    WalReader::open_file_with_cipher_expected(file, cipher, Some(segment_no))
}

fn inspect_wal_layout(
    root: &crate::durable_file::DurableRoot,
    wal_root: &crate::durable_file::DurableRoot,
    wal_dek: Option<&Zeroizing<[u8; 32]>>,
) -> Result<WalLayout> {
    let segments = list_segment_numbers(wal_root)?;
    let head = read_wal_head(root, wal_dek)?;
    for (index, segment_no) in segments.iter().copied().enumerate() {
        let reader = reader_for_segment(wal_root, segment_no, wal_dek)?;
        if reader.encrypted != wal_dek.is_some() {
            return Err(MongrelError::CorruptWal {
                offset: 10,
                reason: format!(
                    "WAL segment {segment_no} encryption mode differs from the database"
                ),
            });
        }
        if index != 0 {
            let previous_no = segments[index - 1];
            let expected = hash_segment(wal_root, previous_no)?;
            if reader.previous_segment_hash != expected {
                return Err(MongrelError::CorruptWal {
                    offset: 30,
                    reason: format!(
                        "WAL segment {segment_no} does not authenticate segment {previous_no}"
                    ),
                });
            }
        }
    }

    if !segments.is_empty() && head.is_none() {
        return Err(MongrelError::CorruptWal {
            offset: 0,
            reason: "WAL segments exist without a durable WAL head".into(),
        });
    }
    if segments.is_empty() && head.is_some() {
        return Err(MongrelError::CorruptWal {
            offset: 0,
            reason: "a durable WAL head exists without any WAL segment".into(),
        });
    }
    if let Some(head) = head {
        let final_segment = segments
            .last()
            .copied()
            .ok_or_else(|| MongrelError::CorruptWal {
                offset: 0,
                reason: "durable WAL head references an empty WAL directory".into(),
            })?;
        if head.segment_no != final_segment {
            return Err(MongrelError::CorruptWal {
                offset: head.segment_no,
                reason: format!(
                    "durable WAL head references segment {}, newest retained segment is {final_segment}",
                    head.segment_no
                ),
            });
        }
        let file = wal_root.open_regular(segment_filename(head.segment_no))?;
        let actual_len = file.metadata()?.len();
        if head.durable_len > actual_len {
            return Err(MongrelError::CorruptWal {
                offset: head.durable_len,
                reason: format!(
                    "durable WAL head length {} exceeds segment length {actual_len}",
                    head.durable_len
                ),
            });
        }
        if hash_file_prefix(file, head.durable_len)? != head.prefix_hash {
            return Err(MongrelError::CorruptWal {
                offset: head.durable_len,
                reason: "durable WAL prefix hash differs from WAL head".into(),
            });
        }
    }
    Ok(WalLayout { segments, head })
}

fn remove_unpublished_header_only_segment(
    root: &crate::durable_file::DurableRoot,
    wal_root: &crate::durable_file::DurableRoot,
    wal_dek: Option<&Zeroizing<[u8; 32]>>,
) -> Result<bool> {
    let Some(head) = read_wal_head(root, wal_dek)? else {
        return Ok(false);
    };
    let segments = list_segment_numbers(wal_root)?;
    let Some(newest) = segments.last().copied() else {
        return Ok(false);
    };
    let Some(orphan_no) = head.segment_no.checked_add(1) else {
        return Ok(false);
    };
    if newest != orphan_no || !segments.contains(&head.segment_no) {
        return Ok(false);
    }
    let reader = reader_for_segment(wal_root, orphan_no, wal_dek)?;
    if reader.version != WAL_VERSION
        || reader.file_len != HEADER_LEN
        || reader.previous_segment_hash != hash_segment(wal_root, head.segment_no)?
    {
        return Ok(false);
    }
    wal_root.remove_file(segment_filename(orphan_no))?;
    Ok(true)
}

/// One WAL segment's records as replayed from disk. Provenance is kept
/// through sequence validation so errors can name the offending segment;
/// records are flattened for recovery only after validation.
struct ReplayedSegment {
    segment_no: u64,
    records: Vec<Record>,
}

/// Validate the v4 writer invariant: record sequence numbers are globally
/// contiguous across every retained segment and every session. A new session
/// continues at `highest durable sequence + 1`, so any gap, reset, or
/// duplicate is corruption.
fn validate_v4_sequence_continuity(segments: &[ReplayedSegment]) -> Result<()> {
    let mut previous: Option<(u64, u64)> = None;
    for segment in segments {
        for record in &segment.records {
            if let Some((previous_seq, previous_segment)) = previous {
                let expected =
                    previous_seq
                        .checked_add(1)
                        .ok_or_else(|| MongrelError::CorruptWal {
                            offset: record.seq.0,
                            reason: "WAL sequence overflows after u64::MAX".into(),
                        })?;
                if record.seq.0 != expected {
                    let reason = if segment.segment_no == previous_segment {
                        format!(
                            "WAL segment {} sequence {} does not follow {previous_seq}",
                            segment.segment_no, record.seq.0
                        )
                    } else {
                        format!(
                            "WAL segment {} begins with sequence {}, expected {expected} after segment {previous_segment}",
                            segment.segment_no, record.seq.0
                        )
                    };
                    return Err(MongrelError::CorruptWal {
                        offset: record.seq.0,
                        reason,
                    });
                }
            }
            previous = Some((record.seq.0, segment.segment_no));
        }
    }
    Ok(())
}

fn replay_wal_layout(
    wal_root: &crate::durable_file::DurableRoot,
    layout: &WalLayout,
    wal_dek: Option<&Zeroizing<[u8; 32]>>,
) -> Result<Vec<Record>> {
    let total_bytes = layout
        .segments
        .iter()
        .try_fold(0_u64, |total, segment_no| {
            let bytes = if layout
                .head
                .is_some_and(|head| head.segment_no == *segment_no)
            {
                layout.head.unwrap().durable_len
            } else {
                wal_root
                    .open_regular(segment_filename(*segment_no))?
                    .metadata()?
                    .len()
            };
            total
                .checked_add(bytes)
                .ok_or(MongrelError::ResourceLimitExceeded {
                    resource: "WAL recovery bytes",
                    requested: usize::MAX,
                    limit: MAX_RECOVERY_WAL_BYTES as usize,
                })
        })?;
    if total_bytes > MAX_RECOVERY_WAL_BYTES {
        return Err(MongrelError::ResourceLimitExceeded {
            resource: "WAL recovery bytes",
            requested: usize::try_from(total_bytes).unwrap_or(usize::MAX),
            limit: MAX_RECOVERY_WAL_BYTES as usize,
        });
    }
    let mut segments = Vec::with_capacity(layout.segments.len());
    let mut total_records = 0_usize;
    for segment_no in layout.segments.iter().copied() {
        let mut reader = reader_for_segment(wal_root, segment_no, wal_dek)?;
        if layout
            .head
            .is_some_and(|head| head.segment_no == segment_no)
        {
            reader.constrain_to_durable_len(layout.head.unwrap().durable_len)?;
        }
        let remaining = MAX_RECOVERY_WAL_RECORDS.saturating_sub(total_records);
        // Layout validation guarantees a durable WAL head, so the head
        // segment is constrained to its authenticated prefix and every other
        // segment must parse completely: no torn tail is admissible.
        let records = reader.replay_bounded(remaining, false)?;
        total_records += records.len();
        segments.push(ReplayedSegment {
            segment_no,
            records,
        });
    }
    validate_v4_sequence_continuity(&segments)?;
    let records: Vec<Record> = segments
        .into_iter()
        .flat_map(|segment| segment.records)
        .collect();
    validate_shared_transaction_framing(&records)?;
    Ok(records)
}

/// Validate transaction semantics over the flattened replay order: system
/// transaction usage, terminal markers, commit timestamps, and commit-epoch
/// uniqueness and advancement. Record sequence continuity is validated
/// separately with segment provenance (see `validate_v4_sequence_continuity`).
pub(crate) fn validate_shared_transaction_framing(records: &[Record]) -> Result<()> {
    let mut transactions = std::collections::HashMap::<u64, ReplayTxnState>::new();
    let mut commit_epochs = std::collections::HashMap::<u64, u64>::new();
    let mut previous_commit_epoch: Option<u64> = None;
    for record in records {
        if record.txn_id == SYSTEM_TXN_ID {
            if !matches!(record.op, Op::Flush { .. }) {
                return Err(MongrelError::CorruptWal {
                    offset: record.seq.0,
                    reason: "non-system operation uses reserved transaction id 0".into(),
                });
            }
            continue;
        }
        let state = transactions.entry(record.txn_id).or_default();
        if state.terminal {
            return Err(MongrelError::CorruptWal {
                offset: record.seq.0,
                reason: format!(
                    "transaction {} has records after its terminal marker",
                    record.txn_id
                ),
            });
        }
        match record.op {
            Op::CommitTimestamp { .. } => {
                if state.timestamp_seen {
                    return Err(MongrelError::CorruptWal {
                        offset: record.seq.0,
                        reason: format!(
                            "transaction {} has duplicate commit timestamps",
                            record.txn_id
                        ),
                    });
                }
                state.timestamp_seen = true;
            }
            Op::TxnCommit { epoch, .. } => {
                if epoch == 0 {
                    return Err(MongrelError::CorruptWal {
                        offset: record.seq.0,
                        reason: format!("transaction {} commits at epoch 0", record.txn_id),
                    });
                }
                if let Some(previous) = commit_epochs.insert(epoch, record.txn_id) {
                    return Err(MongrelError::CorruptWal {
                        offset: record.seq.0,
                        reason: format!(
                            "transactions {previous} and {} share commit epoch {epoch}",
                            record.txn_id
                        ),
                    });
                }
                if previous_commit_epoch.is_some_and(|previous| epoch <= previous) {
                    return Err(MongrelError::CorruptWal {
                        offset: record.seq.0,
                        reason: format!(
                            "commit epoch {epoch} does not advance beyond {}",
                            previous_commit_epoch.unwrap_or(0)
                        ),
                    });
                }
                previous_commit_epoch = Some(epoch);
                state.terminal = true;
            }
            Op::TxnAbort => state.terminal = true,
            Op::Flush { .. } => {
                return Err(MongrelError::CorruptWal {
                    offset: record.seq.0,
                    reason: format!(
                        "transaction {} contains a system flush record",
                        record.txn_id
                    ),
                });
            }
            _ => {}
        }
    }
    Ok(())
}

impl SharedWal {
    /// Build a per-segment frame cipher from the WAL DEK.
    fn cipher_from_dek(dek: &Zeroizing<[u8; 32]>) -> Result<Box<dyn crate::encryption::Cipher>> {
        Ok(Box::new(crate::encryption::AesCipher::new(&dek[..])?))
    }

    /// Create a fresh shared WAL at `<root>/_wal/` starting at `epoch_created`.
    pub fn create(root: &Path, epoch_created: Epoch) -> Result<Self> {
        Self::create_with_dek(root, epoch_created, None)
    }

    /// Create with optional frame-level encryption (WAL DEK).
    pub fn create_with_dek(
        root: &Path,
        epoch_created: Epoch,
        wal_dek: Option<Zeroizing<[u8; 32]>>,
    ) -> Result<Self> {
        let root = Arc::new(crate::durable_file::DurableRoot::open(root)?);
        Self::create_with_durable_root(root, epoch_created, wal_dek)
    }

    pub(crate) fn create_with_durable_root(
        root: Arc<crate::durable_file::DurableRoot>,
        epoch_created: Epoch,
        wal_dek: Option<Zeroizing<[u8; 32]>>,
    ) -> Result<Self> {
        let wal_root = Arc::new(root.create_directory_all_pinned("_wal")?);
        let wal_dir = wal_root.io_path()?;
        if !list_segment_numbers(&wal_root)?.is_empty() {
            return Err(MongrelError::CorruptWal {
                offset: 0,
                reason: "refuses to create a shared WAL over existing segments".into(),
            });
        }
        let cipher = match &wal_dek {
            Some(dk) => Some(Self::cipher_from_dek(dk)?),
            None => None,
        };
        let active = Wal::create_chained_in(&wal_root, 0, epoch_created, cipher, [0; 32])?;
        if let Err(error) = write_wal_head(&root, &wal_root, 0, 0, wal_dek.as_ref()) {
            drop(active);
            let _ = wal_root.remove_file(segment_filename(0));
            return Err(error);
        }
        Ok(Self {
            root,
            wal_root,
            wal_dir,
            active,
            active_segment_no: 0,
            durable_seq: epoch_created.0,
            open_generation: 0,
            wal_dek,
            group_sync_count: 0,
        })
    }

    /// Open an existing shared WAL for append, preserving prior segments (which
    /// `replay` reads for recovery). A fresh active segment numbered one past
    /// the highest existing is created — old segments are NOT truncated (review
    /// fix #6), so a crash mid-recovery can re-replay them safely.
    pub fn open(
        root: &Path,
        epoch_created: Epoch,
        wal_dek: Option<Zeroizing<[u8; 32]>>,
    ) -> Result<Self> {
        let root = Arc::new(crate::durable_file::DurableRoot::open(root)?);
        Self::open_durable_root(root, epoch_created, wal_dek)
    }

    pub(crate) fn open_durable_root(
        root: Arc<crate::durable_file::DurableRoot>,
        epoch_created: Epoch,
        wal_dek: Option<Zeroizing<[u8; 32]>>,
    ) -> Result<Self> {
        Self::open_durable_root_validated(root, epoch_created, wal_dek, None)
    }

    pub(crate) fn open_durable_root_validated(
        root: Arc<crate::durable_file::DurableRoot>,
        epoch_created: Epoch,
        wal_dek: Option<Zeroizing<[u8; 32]>>,
        expected_records: Option<&[Record]>,
    ) -> Result<Self> {
        let wal_root =
            Arc::new(
                root.open_directory("_wal")
                    .map_err(|error| MongrelError::CorruptWal {
                        offset: 0,
                        reason: format!(
                            "existing database WAL directory cannot be opened: {error}"
                        ),
                    })?,
            );
        let wal_dir = wal_root.io_path()?;
        if let Some(expected) = expected_records {
            let layout = inspect_wal_layout(&root, &wal_root, wal_dek.as_ref())?;
            let actual = replay_wal_layout(&wal_root, &layout, wal_dek.as_ref())?;
            if bincode::serialize(&actual)? != bincode::serialize(expected)? {
                return Err(MongrelError::CorruptWal {
                    offset: 0,
                    reason: "WAL changed after recovery planning".into(),
                });
            }
        }
        remove_unpublished_header_only_segment(&root, &wal_root, wal_dek.as_ref())?;
        let layout = inspect_wal_layout(&root, &wal_root, wal_dek.as_ref())?;
        let final_segment =
            layout
                .segments
                .last()
                .copied()
                .ok_or_else(|| MongrelError::CorruptWal {
                    offset: 0,
                    reason: "existing database has no WAL segments".into(),
                })?;
        let records = replay_wal_layout(&wal_root, &layout, wal_dek.as_ref())?;
        let open_generation = layout
            .head
            .map(|head| head.open_generation)
            .unwrap_or_else(|| {
                records
                    .iter()
                    .filter(|record| record.txn_id != SYSTEM_TXN_ID)
                    .map(|record| record.txn_id >> 32)
                    .max()
                    .unwrap_or(0)
            });

        // Bytes beyond the authenticated head were never published durable.
        // Truncate that suffix before the file becomes an immutable chain
        // link. Layout validation guarantees a durable head exists.
        let durable_len = layout
            .head
            .ok_or_else(|| MongrelError::CorruptWal {
                offset: 0,
                reason: "validated WAL layout has no durable WAL head".into(),
            })?
            .durable_len;
        let final_name = segment_filename(final_segment);
        let final_file = wal_root.open_regular_read_write(&final_name)?;
        if final_file.metadata()?.len() != durable_len {
            final_file.set_len(durable_len)?;
            final_file.sync_all()?;
        }
        let previous_segment_hash = hash_segment(&wal_root, final_segment)?;
        let next_segment_no = final_segment
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("WAL segment namespace exhausted".into()))?;
        // The new session continues the globally contiguous v4 sequence. Only
        // a freshly created (record-less) WAL seeds from the creation epoch.
        let durable_seq = records
            .last()
            .map(|record| record.seq.0)
            .unwrap_or(epoch_created.0);
        let cipher = match &wal_dek {
            Some(dk) => Some(Self::cipher_from_dek(dk)?),
            None => None,
        };
        let mut active = Wal::create_chained_in(
            &wal_root,
            next_segment_no,
            epoch_created,
            cipher,
            previous_segment_hash,
        )?;
        active.next_seq = durable_seq
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("WAL sequence namespace exhausted".into()))?;
        if let Err(error) = write_wal_head(
            &root,
            &wal_root,
            next_segment_no,
            open_generation,
            wal_dek.as_ref(),
        ) {
            drop(active);
            let _ = wal_root.remove_file(segment_filename(next_segment_no));
            return Err(error);
        }
        Ok(Self {
            root,
            wal_root,
            wal_dir,
            active,
            active_segment_no: next_segment_no,
            durable_seq,
            open_generation,
            wal_dek,
            group_sync_count: 0,
        })
    }

    /// The active segment's wal_dir (test/diagnostic).
    #[allow(dead_code)]
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// Append a record for `(txn_id, table_id)`. Does not fsync.
    /// `wal.append.before`/`wal.append.after` fault hooks bracket the append
    /// (spec §9.6, FND-006).
    pub fn append(&mut self, txn_id: u64, _table_id: u64, op: Op) -> Result<u64> {
        mongreldb_fault::inject("wal.append.before").map_err(crate::commit_log::fault_as_io)?;
        let seq = self.active.append_txn(txn_id, op)?.0;
        mongreldb_fault::inject("wal.append.after").map_err(crate::commit_log::fault_as_io)?;
        Ok(seq)
    }

    /// Append a `TxnCommit` marker sealing `txn_id` at `epoch`.
    pub fn append_commit(&mut self, txn_id: u64, epoch: Epoch, added: &[AddedRun]) -> Result<u64> {
        self.append_commit_at(txn_id, epoch, added, unix_nanos_now())
    }

    pub fn append_commit_at(
        &mut self,
        txn_id: u64,
        epoch: Epoch,
        added: &[AddedRun],
        unix_nanos: u64,
    ) -> Result<u64> {
        self.active
            .append_txn(txn_id, Op::CommitTimestamp { unix_nanos })?;
        Ok(self
            .active
            .append_txn(
                txn_id,
                Op::TxnCommit {
                    epoch: epoch.0,
                    added_runs: added.to_vec(),
                },
            )?
            .0)
    }

    /// Append a `TxnAbort` marker for `txn_id`.
    pub fn append_abort(&mut self, txn_id: u64) -> Result<()> {
        self.active.append_txn(txn_id, Op::TxnAbort)?;
        Ok(())
    }

    /// Append a system record (txn_id == 0), e.g. `Flush`.
    pub fn append_system(&mut self, op: Op) -> Result<u64> {
        Ok(self.active.append_system(op)?.0)
    }

    /// Flush + fsync the active segment and return the highest durable sequence
    /// number. This is the single durability point for every concurrent
    /// appender since the last `group_sync`. `wal.fsync.before`/`wal.fsync.after`
    /// fault hooks bracket the fsync (spec §9.6, FND-006).
    pub fn group_sync(&mut self) -> Result<u64> {
        mongreldb_fault::inject("wal.fsync.before").map_err(crate::commit_log::fault_as_io)?;
        self.active.sync()?;
        mongreldb_fault::inject("wal.fsync.after").map_err(crate::commit_log::fault_as_io)?;
        write_wal_head(
            &self.root,
            &self.wal_root,
            self.active_segment_no,
            self.open_generation,
            self.wal_dek.as_ref(),
        )?;
        self.group_sync_count += 1;
        let highest = self.active.next_seq_val().saturating_sub(1);
        if highest > self.durable_seq {
            self.durable_seq = highest;
        }
        Ok(self.durable_seq)
    }

    /// Number of fsyncs issued so far (test/diagnostic — see [`group_sync`]).
    pub fn group_sync_count(&self) -> u64 {
        self.group_sync_count
    }

    /// The highest sequence number reported durable by the last `group_sync`.
    pub fn durable_seq(&self) -> u64 {
        self.durable_seq
    }

    pub(crate) fn seal_open_generation(&mut self, generation: u64) -> Result<()> {
        if generation < self.open_generation {
            return Err(MongrelError::CorruptWal {
                offset: generation,
                reason: format!(
                    "open generation {generation} precedes WAL head generation {}",
                    self.open_generation
                ),
            });
        }
        self.active.sync()?;
        let previous = self.open_generation;
        self.open_generation = generation;
        if let Err(error) = write_wal_head(
            &self.root,
            &self.wal_root,
            self.active_segment_no,
            self.open_generation,
            self.wal_dek.as_ref(),
        ) {
            self.open_generation = previous;
            return Err(error);
        }
        Ok(())
    }

    /// Rotate to a fresh segment numbered `segment_no` (which namespaces nonces
    /// under the constant WAL DEK). The current segment must already be synced.
    pub fn rotate(&mut self, segment_no: u64) -> Result<()> {
        let expected = self
            .active_segment_no
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("WAL segment namespace exhausted".into()))?;
        if segment_no != expected {
            return Err(MongrelError::InvalidArgument(format!(
                "WAL rotation segment {segment_no} does not immediately follow {}",
                self.active_segment_no
            )));
        }
        self.active.sync()?;
        write_wal_head(
            &self.root,
            &self.wal_root,
            self.active_segment_no,
            self.open_generation,
            self.wal_dek.as_ref(),
        )?;
        let highest = self.active.next_seq_val().saturating_sub(1);
        self.durable_seq = self.durable_seq.max(highest);
        let previous_segment_hash = hash_segment(&self.wal_root, self.active_segment_no)?;
        let cipher = match &self.wal_dek {
            Some(dk) => Some(Self::cipher_from_dek(dk)?),
            _ => None,
        };
        let epoch = Epoch(self.durable_seq);
        let wal = Wal::create_chained_in(
            &self.wal_root,
            segment_no,
            epoch,
            cipher,
            previous_segment_hash,
        )?;
        if let Err(error) = write_wal_head(
            &self.root,
            &self.wal_root,
            segment_no,
            self.open_generation,
            self.wal_dek.as_ref(),
        ) {
            drop(wal);
            let _ = self.wal_root.remove_file(segment_filename(segment_no));
            return Err(error);
        }
        self.active = wal;
        self.active_segment_no = segment_no;
        Ok(())
    }

    /// The active segment number.
    pub fn active_segment_no(&self) -> u64 {
        self.active_segment_no
    }

    /// After every table is checkpointed into runs, publish a fresh durable
    /// active segment before deleting any older segment. The active file handle
    /// is never unlinked while it can receive later commits.
    pub(crate) fn reset_after_checkpoint(&mut self) -> Result<usize> {
        self.group_sync()?;
        let next_segment_no = list_segment_numbers(&self.wal_root)?
            .into_iter()
            .max()
            .ok_or_else(|| MongrelError::CorruptWal {
                offset: 0,
                reason: "active WAL segment disappeared before checkpoint reset".into(),
            })?
            .checked_add(1)
            .ok_or_else(|| MongrelError::Full("WAL segment namespace exhausted".into()))?;
        self.rotate(next_segment_no)?;
        // Keep this explicit even though segment creation currently syncs its
        // header. Checkpoint safety must not depend on that constructor detail.
        self.group_sync()?;
        self.gc_segments_retain_recent(u64::MAX, 0)
    }

    /// Delete rotated (non-active) WAL segments whose records are all below
    /// `min_retained_seq` — i.e. every record in them is already durable in a
    /// run and not needed by any in-flight or committed-not-flushed txn (spec
    /// §6.4/§16). The active segment is **never** deleted. Returns the count of
    /// segment files reaped.
    ///
    /// `open()` mints a fresh active segment on every reopen without truncating
    /// the prior ones (so a crash mid-recovery can re-replay), which means old
    /// segments accumulate; this is what reaps them once their data is durable.
    pub fn gc_segments(&mut self, min_retained_seq: u64) -> Result<usize> {
        self.gc_segments_retain_recent(min_retained_seq, 0)
    }

    pub(crate) fn has_gc_segments_retain_recent(&self, retain_recent: usize) -> Result<bool> {
        let rotated = list_segment_numbers(&self.wal_root)?
            .into_iter()
            .filter(|segment| *segment != self.active_segment_no)
            .count();
        Ok(rotated > retain_recent)
    }

    /// [`Self::gc_segments`] while retaining the newest `retain_recent`
    /// rotated segments for replication followers. The active segment is
    /// retained separately and does not count toward this limit.
    pub fn gc_segments_retain_recent(
        &mut self,
        min_retained_seq: u64,
        retain_recent: usize,
    ) -> Result<usize> {
        self.group_sync()?;
        let layout = inspect_wal_layout(&self.root, &self.wal_root, self.wal_dek.as_ref())?;
        let readable = replay_wal_layout(&self.wal_root, &layout, self.wal_dek.as_ref())?;
        let segments = &layout.segments;
        let retained: Vec<u64> = segments
            .iter()
            .rev()
            .filter(|segment| **segment != self.active_segment_no)
            .take(retain_recent)
            .copied()
            .collect();
        let mut candidates = Vec::new();
        let mut prefix_open = true;
        for n in segments.iter().copied() {
            let mut reader = reader_for_segment(&self.wal_root, n, self.wal_dek.as_ref())?;
            if layout.head.is_some_and(|head| head.segment_no == n) {
                reader.constrain_to_durable_len(layout.head.unwrap().durable_len)?;
            }
            let records = reader.replay_strict()?;
            let below_floor = min_retained_seq == u64::MAX
                || records.iter().map(|record| record.seq.0).max().unwrap_or(0) < min_retained_seq;
            let reapable =
                prefix_open && n != self.active_segment_no && !retained.contains(&n) && below_floor;
            if reapable {
                candidates.push((n, records));
            } else {
                prefix_open = false;
            }
        }

        let commits = readable
            .iter()
            .filter_map(|record| match record.op {
                Op::TxnCommit { epoch, .. } => Some((record.txn_id, epoch)),
                _ => None,
            })
            .collect::<std::collections::HashMap<_, _>>();
        let removed_floor = candidates
            .iter()
            .flat_map(|(_, records)| records)
            .filter_map(|record| commits.get(&record.txn_id).copied())
            .max();
        if let Some(epoch) = removed_floor {
            crate::replication::advance_replication_wal_floor_durable(&self.root, epoch)?;
        }
        for (segment_no, _) in &candidates {
            self.wal_root.remove_file(segment_filename(*segment_no))?;
        }
        let reaped = candidates.len();
        Ok(reaped)
    }

    /// Verify the on-disk integrity of every WAL segment (spec §16): each
    /// `seg-NNNNNN.wal` file under `<root>/_wal/` must open — its header magic
    /// and version must parse, and for an encrypted WAL the frame cipher must
    /// be derivable from the WAL DEK. A segment that fails to open is corrupt
    /// or truncated and would break recovery. Returns one `(segment_no, error)`
    /// pair per failing segment. The active (in-memory) segment is trusted by
    /// construction and re-checked from disk like the others.
    pub fn verify_segments(&self) -> Vec<(u64, String)> {
        let result = inspect_wal_layout(&self.root, &self.wal_root, self.wal_dek.as_ref())
            .and_then(|layout| replay_wal_layout(&self.wal_root, &layout, self.wal_dek.as_ref()));
        match result {
            Ok(_) => Vec::new(),
            Err(error) => vec![(u64::MAX, error.to_string())],
        }
    }

    /// Replay every record across all segments in `<root>/_wal/`, in segment
    /// order. Only the newest segment may end in a crash-torn frame; every
    /// older segment is immutable and therefore must validate completely.
    pub fn replay(root: &Path) -> Result<Vec<Record>> {
        Self::replay_with_dek(root, None)
    }

    /// Replay with an optional WAL DEK (for encrypted segments).
    pub fn replay_with_dek(
        root: &Path,
        wal_dek: Option<&Zeroizing<[u8; 32]>>,
    ) -> Result<Vec<Record>> {
        let root = crate::durable_file::DurableRoot::open(root)?;
        Self::replay_durable_with_dek(&root, wal_dek)
    }

    pub(crate) fn replay_durable_with_dek(
        root: &crate::durable_file::DurableRoot,
        wal_dek: Option<&Zeroizing<[u8; 32]>>,
    ) -> Result<Vec<Record>> {
        let wal_root = root.open_directory("_wal")?;
        let layout = inspect_wal_layout(root, &wal_root, wal_dek)?;
        replay_wal_layout(&wal_root, &layout, wal_dek)
    }

    pub(crate) fn durable_open_generation(
        root: &crate::durable_file::DurableRoot,
        wal_dek: Option<&Zeroizing<[u8; 32]>>,
    ) -> Result<Option<u64>> {
        let wal_root = root.open_directory("_wal")?;
        let layout = inspect_wal_layout(root, &wal_root, wal_dek)?;
        Ok(layout.head.map(|head| head.open_generation))
    }

    /// Replay all segments cooperatively with hard total record and on-disk
    /// byte bounds.
    pub fn replay_with_dek_controlled(
        root: &Path,
        wal_dek: Option<&Zeroizing<[u8; 32]>>,
        control: &crate::ExecutionControl,
        max_records: usize,
        max_bytes: usize,
    ) -> Result<Vec<Record>> {
        let root = crate::durable_file::DurableRoot::open(root)?;
        let wal_root = root.open_directory("_wal")?;
        let layout = inspect_wal_layout(&root, &wal_root, wal_dek)?;
        let total_bytes = layout.segments.iter().try_fold(0_usize, |total, segment| {
            let bytes = if layout.head.is_some_and(|head| head.segment_no == *segment) {
                usize::try_from(layout.head.unwrap().durable_len).unwrap_or(usize::MAX)
            } else {
                usize::try_from(
                    wal_root
                        .open_regular(segment_filename(*segment))?
                        .metadata()?
                        .len(),
                )
                .unwrap_or(usize::MAX)
            };
            Ok::<_, MongrelError>(total.saturating_add(bytes))
        })?;
        if total_bytes > max_bytes {
            return Err(MongrelError::ResourceLimitExceeded {
                resource: "controlled WAL replay bytes",
                requested: total_bytes,
                limit: max_bytes,
            });
        }
        let mut segments = Vec::with_capacity(layout.segments.len());
        let mut total_records = 0_usize;
        for n in layout.segments.iter().copied() {
            control.checkpoint()?;
            let remaining = max_records.saturating_sub(total_records);
            let mut reader = reader_for_segment(&wal_root, n, wal_dek)?;
            if layout.head.is_some_and(|head| head.segment_no == n) {
                reader.constrain_to_durable_len(layout.head.unwrap().durable_len)?;
            }
            // Layout validation guarantees a durable WAL head, so every
            // segment parses strictly: the head segment is constrained to
            // its authenticated prefix and no torn tail is admissible.
            let records = reader.replay_controlled_strict(control, remaining)?;
            total_records += records.len();
            segments.push(ReplayedSegment {
                segment_no: n,
                records,
            });
        }
        validate_v4_sequence_continuity(&segments)?;
        let out: Vec<Record> = segments
            .into_iter()
            .flat_map(|segment| segment.records)
            .collect();
        validate_shared_transaction_framing(&out)?;
        Ok(out)
    }
}

fn segment_filename(segment_no: u64) -> String {
    format!("seg-{segment_no:06}.wal")
}

fn segment_number_from_path(path: &Path) -> Option<u64> {
    path.file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("seg-"))
        .and_then(|name| name.strip_suffix(".wal"))
        .and_then(|number| number.parse().ok())
}

/// List and validate the retained canonical segment sequence under `wal_dir`.
/// Garbage files are ignored, but every `seg-*` entry is authoritative and
/// therefore must be a regular file with its one canonical name. GC may remove
/// a prefix; gaps inside the retained suffix are corruption.
fn list_segment_numbers(wal_root: &crate::durable_file::DurableRoot) -> Result<Vec<u64>> {
    let mut segments = Vec::new();
    for fname in wal_root.list_regular_files(".")? {
        let s = fname.to_str().ok_or_else(|| MongrelError::CorruptWal {
            offset: 0,
            reason: "WAL directory contains a non-UTF-8 entry".into(),
        })?;
        if !s.starts_with("seg-") {
            continue;
        }
        let number = s
            .strip_prefix("seg-")
            .and_then(|value| value.strip_suffix(".wal"))
            .and_then(|value| value.parse::<u64>().ok())
            .ok_or_else(|| MongrelError::CorruptWal {
                offset: 0,
                reason: format!("malformed WAL segment filename {s:?}"),
            })?;
        if s != segment_filename(number) {
            return Err(MongrelError::CorruptWal {
                offset: 0,
                reason: format!("non-canonical WAL segment filename {s:?}"),
            });
        }
        segments.push(number);
    }
    segments.sort_unstable();
    for pair in segments.windows(2) {
        let expected = pair[0]
            .checked_add(1)
            .ok_or_else(|| MongrelError::CorruptWal {
                offset: pair[0],
                reason: "WAL segment namespace overflows after u64::MAX".into(),
            })?;
        if pair[1] != expected {
            return Err(MongrelError::CorruptWal {
                offset: pair[1],
                reason: format!(
                    "WAL segment {} does not immediately follow {}",
                    pair[1], pair[0]
                ),
            });
        }
    }
    Ok(segments)
}

#[cfg(test)]
mod shared_wal_tests {
    use super::*;
    use tempfile::tempdir;

    /// Write a file with a v3 WAL header (and no records) so tests can assert
    /// the format boundary: v3 is rejected as unsupported, never parsed.
    fn write_v3_segment(path: &Path) {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&WAL_MAGIC);
        bytes.extend_from_slice(&3_u16.to_le_bytes());
        bytes.extend_from_slice(&[ENC_PLAINTEXT, 0, 0, 0]);
        bytes.extend_from_slice(&0_u64.to_le_bytes());
        std::fs::write(path, bytes).unwrap();
    }

    #[test]
    fn v3_segments_are_rejected_as_unsupported_not_corrupt() {
        let dir = tempdir().unwrap();
        std::fs::create_dir(dir.path().join("_wal")).unwrap();
        write_v3_segment(&dir.path().join("_wal/seg-000000.wal"));

        let error = SharedWal::replay(dir.path()).unwrap_err();
        assert!(
            matches!(
                error,
                MongrelError::UnsupportedStorageVersion {
                    component: "wal",
                    found: 3,
                    supported: 4,
                }
            ),
            "expected UnsupportedStorageVersion, got {error:?}"
        );

        let dir = tempdir().unwrap();
        std::fs::create_dir(dir.path().join("_wal")).unwrap();
        write_v3_segment(&dir.path().join("_wal/seg-000000.wal"));
        let error = SharedWal::open(dir.path(), Epoch(1), None)
            .err()
            .expect("v3 WAL must be rejected");
        assert!(matches!(
            error,
            MongrelError::UnsupportedStorageVersion {
                component: "wal",
                found: 3,
                supported: 4,
            }
        ));
    }

    #[test]
    fn shared_wal_interleaves_two_tables_one_fd() {
        let dir = tempdir().unwrap();
        let mut w = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        w.append(
            1,
            10,
            Op::Put {
                table_id: 10,
                rows: vec![1],
            },
        )
        .unwrap();
        w.append(
            2,
            20,
            Op::Put {
                table_id: 20,
                rows: vec![2],
            },
        )
        .unwrap();
        w.append_commit(1, Epoch(1), &[]).unwrap();
        w.append_commit(2, Epoch(2), &[]).unwrap();
        let d = w.group_sync().unwrap();
        assert!(d >= 4);
        let recs = SharedWal::replay(dir.path()).unwrap();
        assert_eq!(
            recs.iter()
                .filter(|r| matches!(r.op, Op::Put { .. }))
                .count(),
            2
        );
        assert_eq!(
            recs.iter()
                .filter(|r| matches!(r.op, Op::TxnCommit { .. }))
                .count(),
            2
        );
    }

    #[test]
    fn controlled_shared_replay_rejects_aggregate_wal_bytes() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(1, Epoch(1), &[]).unwrap();
        wal.group_sync().unwrap();
        let control = crate::ExecutionControl::new(None);

        let error =
            SharedWal::replay_with_dek_controlled(dir.path(), None, &control, usize::MAX, 1)
                .unwrap_err();
        assert!(matches!(
            error,
            MongrelError::ResourceLimitExceeded {
                resource: "controlled WAL replay bytes",
                ..
            }
        ));
    }

    #[test]
    fn shared_wal_gc_retains_recent_rotated_segments() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        for segment in 0..4u64 {
            wal.append_commit(segment + 1, Epoch(segment + 1), &[])
                .unwrap();
            wal.group_sync().unwrap();
            if segment < 3 {
                wal.rotate(segment + 1).unwrap();
            }
        }
        assert_eq!(wal.gc_segments_retain_recent(u64::MAX, 2).unwrap(), 1);
        let count = std::fs::read_dir(dir.path().join("_wal"))
            .unwrap()
            .flatten()
            .filter(|entry| entry.path().extension().is_some_and(|ext| ext == "wal"))
            .count();
        assert_eq!(count, 3, "active plus two retained segments");
    }

    #[test]
    fn shared_replay_rejects_torn_tail_in_rotated_segment() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(1, Epoch(1), &[]).unwrap();
        wal.group_sync().unwrap();
        wal.rotate(1).unwrap();
        wal.append_commit(2, Epoch(2), &[]).unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        let old = dir.path().join("_wal/seg-000000.wal");
        let mut file = OpenOptions::new().append(true).open(old).unwrap();
        file.write_all(&[1, 2, 3]).unwrap();
        file.sync_all().unwrap();

        assert!(matches!(
            SharedWal::replay(dir.path()),
            Err(MongrelError::CorruptWal { .. })
        ));
    }

    #[test]
    fn shared_replay_rejects_records_after_commit() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(7, Epoch(1), &[]).unwrap();
        wal.append(
            7,
            1,
            Op::Delete {
                table_id: 1,
                row_ids: vec![RowId(9)],
            },
        )
        .unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        assert!(matches!(
            SharedWal::replay(dir.path()),
            Err(MongrelError::CorruptWal { .. })
        ));
    }

    #[test]
    fn rotate_without_explicit_group_sync_keeps_sequence_adjacent() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        assert_eq!(
            wal.append_system(Op::Flush {
                table_id: 1,
                flushed_epoch: 1,
            })
            .unwrap(),
            1
        );
        wal.rotate(1).unwrap();
        assert_eq!(
            wal.append_system(Op::Flush {
                table_id: 1,
                flushed_epoch: 2,
            })
            .unwrap(),
            2
        );
        wal.group_sync().unwrap();
        let records = SharedWal::replay(dir.path()).unwrap();
        assert_eq!(
            records
                .iter()
                .map(|record| record.seq.0)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
    }

    #[test]
    fn open_recovers_exact_header_only_segment_crash_window() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(1, Epoch(1), &[]).unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        let root = crate::durable_file::DurableRoot::open(dir.path()).unwrap();
        let wal_root = root.open_directory("_wal").unwrap();
        let previous_hash = hash_segment(&wal_root, 0).unwrap();
        drop(Wal::create_chained_in(&wal_root, 1, Epoch(1), None, previous_hash).unwrap());
        assert_eq!(read_wal_head(&root, None).unwrap().unwrap().segment_no, 0);

        let reopened = SharedWal::open(dir.path(), Epoch(1), None).unwrap();
        assert_eq!(reopened.active_segment_no(), 1);
        assert_eq!(list_segment_numbers(&wal_root).unwrap(), vec![0, 1]);
        assert_eq!(read_wal_head(&root, None).unwrap().unwrap().segment_no, 1);
    }

    #[test]
    fn replay_rejects_sequence_reset_across_segments() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(1, Epoch(1), &[]).unwrap();
        wal.group_sync().unwrap();
        wal.rotate(1).unwrap();
        // A session must never restart the sequence; simulate a writer bug
        // that does and prove replay still rejects it.
        wal.active.next_seq = 1;
        wal.append_commit(2, Epoch(2), &[]).unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        let error = SharedWal::replay(dir.path()).unwrap_err();
        assert!(
            matches!(error, MongrelError::CorruptWal { .. }),
            "expected CorruptWal, got {error:?}"
        );
    }

    #[test]
    fn replay_rejects_backward_commit_epoch() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(1, Epoch(2), &[]).unwrap();
        wal.append_commit(2, Epoch(1), &[]).unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        assert!(matches!(
            SharedWal::replay(dir.path()),
            Err(MongrelError::CorruptWal { .. })
        ));
    }

    #[test]
    fn multiple_sessions_continue_one_global_sequence() {
        let dir = tempdir().unwrap();
        for session in 0..3_u64 {
            let mut wal = SharedWal::open(dir.path(), Epoch(session * 2), None)
                .unwrap_or_else(|_| SharedWal::create(dir.path(), Epoch(session * 2)).unwrap());
            assert_eq!(wal.active_segment_no(), session);
            for i in 0..2_u64 {
                let txn = session * 2 + i + 1;
                wal.append_commit(txn, Epoch(txn), &[]).unwrap();
            }
            wal.group_sync().unwrap();
            drop(wal);
        }

        let records = SharedWal::replay(dir.path()).unwrap();
        let sequences: Vec<u64> = records.iter().map(|record| record.seq.0).collect();
        // Every commit writes a timestamp and a commit record: 3 sessions x 2
        // commits x 2 records, one contiguous namespace.
        assert_eq!(sequences, (1..=12).collect::<Vec<u64>>());

        // A fourth session must continue the same namespace (recovery may
        // append its own system records first — contiguity is the invariant).
        let mut wal = SharedWal::open(dir.path(), Epoch(6), None).unwrap();
        assert_eq!(wal.active_segment_no(), 3);
        wal.append_commit(7, Epoch(7), &[]).unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        let records = SharedWal::replay(dir.path()).unwrap();
        let sequences: Vec<u64> = records.iter().map(|record| record.seq.0).collect();
        assert_eq!(
            sequences,
            (1..=sequences.len() as u64).collect::<Vec<u64>>(),
            "records stay globally contiguous across sessions"
        );
    }

    #[test]
    fn head_and_strict_segment_names_detect_deletion_gaps_and_aliases() {
        let deleted = tempdir().unwrap();
        let wal = SharedWal::create(deleted.path(), Epoch(0)).unwrap();
        drop(wal);
        std::fs::remove_file(deleted.path().join("_wal/seg-000000.wal")).unwrap();
        assert!(SharedWal::replay(deleted.path()).is_err());

        let alias = tempdir().unwrap();
        let wal = SharedWal::create(alias.path(), Epoch(0)).unwrap();
        drop(wal);
        std::fs::rename(
            alias.path().join("_wal/seg-000000.wal"),
            alias.path().join("_wal/seg-0.wal"),
        )
        .unwrap();
        assert!(SharedWal::replay(alias.path()).is_err());

        let gap = tempdir().unwrap();
        let mut wal = SharedWal::create(gap.path(), Epoch(0)).unwrap();
        wal.rotate(1).unwrap();
        drop(wal);
        std::fs::rename(
            gap.path().join("_wal/seg-000001.wal"),
            gap.path().join("_wal/seg-000002.wal"),
        )
        .unwrap();
        assert!(SharedWal::replay(gap.path()).is_err());
    }

    #[test]
    fn verify_and_gc_fail_closed_on_segment_corruption() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(1, Epoch(1), &[]).unwrap();
        wal.group_sync().unwrap();
        wal.rotate(1).unwrap();
        wal.append_commit(2, Epoch(2), &[]).unwrap();
        wal.group_sync().unwrap();

        let old = dir.path().join("_wal/seg-000000.wal");
        let mut bytes = std::fs::read(&old).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x40;
        std::fs::write(&old, bytes).unwrap();
        assert!(!wal.verify_segments().is_empty());
        assert!(wal.gc_segments(u64::MAX).is_err());
        assert!(old.exists());
    }

    #[test]
    fn rotation_within_session_keeps_sequence_contiguous() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(1, Epoch(1), &[]).unwrap();
        wal.rotate(1).unwrap();
        wal.append_commit(2, Epoch(2), &[]).unwrap();
        wal.rotate(2).unwrap();
        wal.append_commit(3, Epoch(3), &[]).unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        let records = SharedWal::replay(dir.path()).unwrap();
        let sequences: Vec<u64> = records.iter().map(|record| record.seq.0).collect();
        assert_eq!(
            sequences,
            (1..=sequences.len() as u64).collect::<Vec<u64>>()
        );
        assert_eq!(sequences.len(), 6, "two records per commit");
    }

    #[test]
    fn open_truncates_garbage_beyond_the_authenticated_durable_prefix() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(1, Epoch(1), &[]).unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        let seg = dir.path().join("_wal/seg-000000.wal");
        let published_len = std::fs::metadata(&seg).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&seg).unwrap();
        file.write_all(&[0xde, 0xad, 0xbe, 0xef, 1, 2, 3, 4, 5])
            .unwrap();
        file.sync_all().unwrap();
        drop(file);

        // Bytes past the durable head were never published: open discards
        // them and keeps every durable record.
        let wal = SharedWal::open(dir.path(), Epoch(1), None).unwrap();
        drop(wal);
        assert_eq!(std::fs::metadata(&seg).unwrap().len(), published_len);
        let records = SharedWal::replay(dir.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| record.seq.0 <= 2));
    }

    #[test]
    fn open_rejects_damage_inside_the_authenticated_prefix() {
        let dir = tempdir().unwrap();
        let mut wal = SharedWal::create(dir.path(), Epoch(0)).unwrap();
        wal.append_commit(1, Epoch(1), &[]).unwrap();
        wal.group_sync().unwrap();
        wal.rotate(1).unwrap();
        wal.append_commit(2, Epoch(2), &[]).unwrap();
        wal.group_sync().unwrap();
        drop(wal);

        // Corrupt one byte inside segment 0's record payload; segment 1's
        // previous-segment hash must fail to authenticate it.
        let seg = dir.path().join("_wal/seg-000000.wal");
        let mut bytes = std::fs::read(&seg).unwrap();
        bytes[(HEADER_LEN + 12) as usize] ^= 0x01;
        std::fs::write(&seg, bytes).unwrap();

        assert!(matches!(
            SharedWal::replay(dir.path()),
            Err(MongrelError::CorruptWal { .. })
        ));
        assert!(SharedWal::open(dir.path(), Epoch(2), None).is_err());
    }

    #[test]
    fn encrypted_sessions_continue_one_global_sequence() {
        let dek = || Zeroizing::new([7_u8; 32]);
        let dir = tempdir().unwrap();
        for session in 0..3_u64 {
            let mut wal = SharedWal::open(dir.path(), Epoch(session * 2), Some(dek()))
                .unwrap_or_else(|_| {
                    SharedWal::create_with_dek(dir.path(), Epoch(session * 2), Some(dek())).unwrap()
                });
            for i in 0..2_u64 {
                let txn = session * 2 + i + 1;
                wal.append_commit(txn, Epoch(txn), &[]).unwrap();
            }
            wal.group_sync().unwrap();
            drop(wal);
        }

        let records = SharedWal::replay_with_dek(dir.path(), Some(&dek())).unwrap();
        let sequences: Vec<u64> = records.iter().map(|record| record.seq.0).collect();
        assert_eq!(
            sequences,
            (1..=sequences.len() as u64).collect::<Vec<u64>>()
        );

        // Unpublished bytes are discarded for encrypted segments as well.
        let seg = dir.path().join("_wal/seg-000002.wal");
        let published_len = std::fs::metadata(&seg).unwrap().len();
        let mut file = OpenOptions::new().append(true).open(&seg).unwrap();
        file.write_all(&[9, 9, 9, 9, 9, 9, 9]).unwrap();
        file.sync_all().unwrap();
        drop(file);
        let wal = SharedWal::open(dir.path(), Epoch(6), Some(dek())).unwrap();
        drop(wal);
        assert_eq!(std::fs::metadata(&seg).unwrap().len(), published_len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    fn frame_ranges(bytes: &[u8]) -> Vec<std::ops::Range<usize>> {
        let mut ranges = Vec::new();
        let mut offset = HEADER_LEN as usize;
        while offset < bytes.len() {
            let mut length = [0_u8; 4];
            length.copy_from_slice(&bytes[offset..offset + 4]);
            let end = offset + 24 + u32::from_le_bytes(length) as usize;
            ranges.push(offset..end);
            offset = end;
        }
        ranges
    }

    fn recompute_frame_crc(bytes: &mut [u8], start: usize) {
        let mut length = [0_u8; 4];
        length.copy_from_slice(&bytes[start..start + 4]);
        let length = u32::from_le_bytes(length) as usize;
        let seq = &bytes[start + 8..start + 16];
        let txn_id = &bytes[start + 16..start + 24];
        let payload = &bytes[start + 24..start + 24 + length];
        let mut digest = CRC32C.digest();
        digest.update(seq);
        digest.update(txn_id);
        digest.update(payload);
        bytes[start + 4..start + 8].copy_from_slice(&digest.finalize().to_le_bytes());
    }

    #[test]
    fn append_then_replay_roundtrips() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000000.wal");
        let mut wal = Wal::create(&path, Epoch(100)).unwrap();
        let s1 = wal
            .append_txn(
                7,
                Op::Put {
                    table_id: 1,
                    rows: vec![1, 2, 3],
                },
            )
            .unwrap();
        let s2 = wal
            .append_txn(
                7,
                Op::Delete {
                    table_id: 1,
                    row_ids: vec![RowId(7)],
                },
            )
            .unwrap();
        assert_eq!(s1, Epoch(101));
        assert_eq!(s2, Epoch(102));
        wal.sync().unwrap();

        let records = replay(&path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].seq, Epoch(101));
        assert_eq!(records[0].txn_id, 7);
        match &records[0].op {
            Op::Put { table_id, rows } => {
                assert_eq!(*table_id, 1);
                assert_eq!(rows, &vec![1, 2, 3]);
            }
            other => panic!("unexpected op {other:?}"),
        }
        match &records[1].op {
            Op::Delete { row_ids, .. } => {
                assert_eq!(*row_ids, vec![RowId(7)]);
            }
            other => panic!("unexpected op {other:?}"),
        }
    }

    #[test]
    fn record_roundtrips_with_txn_id_and_commit_marker() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000000.wal");
        let mut w = Wal::create(&path, Epoch(0)).unwrap();
        w.append_txn(
            7,
            Op::Put {
                table_id: 3,
                rows: vec![1, 2, 3],
            },
        )
        .unwrap();
        w.append_txn(
            7,
            Op::TxnCommit {
                epoch: 11,
                added_runs: vec![],
            },
        )
        .unwrap();
        w.sync().unwrap();
        let recs = replay(&path).unwrap();
        assert_eq!(recs[0].txn_id, 7);
        assert!(matches!(recs[0].op, Op::Put { table_id: 3, .. }));
        assert!(matches!(recs[1].op, Op::TxnCommit { epoch: 11, .. }));
        // system records carry the reserved id
        let system_path = dir.path().join("seg-000001.wal");
        let mut w2 = Wal::create(&system_path, Epoch(0)).unwrap();
        w2.append_system(Op::Flush {
            table_id: 3,
            flushed_epoch: 11,
        })
        .unwrap();
        w2.sync().unwrap();
        let recs = replay(&system_path).unwrap();
        assert_eq!(recs[0].txn_id, SYSTEM_TXN_ID);
        assert!(matches!(recs[0].op, Op::Flush { .. }));
    }

    #[test]
    fn catalog_snapshot_and_external_reset_roundtrip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-catalog.wal");
        let mut wal = Wal::create(&path, Epoch(0)).unwrap();
        wal.append_txn(
            9,
            Op::Ddl(DdlOp::CatalogSnapshot {
                catalog_json: br#"{"db_epoch":7}"#.to_vec(),
            }),
        )
        .unwrap();
        wal.append_txn(
            9,
            Op::Ddl(DdlOp::ResetExternalTableState {
                name: "ext".into(),
                generation_epoch: 7,
            }),
        )
        .unwrap();
        wal.sync().unwrap();

        let records = replay(&path).unwrap();
        assert!(matches!(
            &records[0].op,
            Op::Ddl(DdlOp::CatalogSnapshot { catalog_json })
                if catalog_json == br#"{"db_epoch":7}"#
        ));
        assert!(matches!(
            &records[1].op,
            Op::Ddl(DdlOp::ResetExternalTableState {
                name,
                generation_epoch: 7,
            }) if name == "ext"
        ));
    }

    #[test]
    fn torn_write_is_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000001.wal");
        let mut wal = Wal::create(&path, Epoch(0)).unwrap();
        wal.append_txn(
            1,
            Op::Put {
                table_id: 1,
                rows: vec![0; 10],
            },
        )
        .unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Append a garbage partial record (simulate a crash mid-write).
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        // REC_LEN claims 64 bytes but we only write a handful.
        f.write_all(&64u32.to_le_bytes()).unwrap();
        f.write_all(&[0u8; 7]).unwrap();
        f.sync_all().unwrap();
        drop(f);

        let mut reader = WalReader::open(&path).unwrap();
        // The first real record reads fine.
        assert!(reader.next_record().unwrap().is_some());
        // The partial record surfaces as a torn write.
        let err = reader.next_record().unwrap_err();
        assert!(matches!(err, MongrelError::TornWrite { .. }), "got {err:?}");
    }

    #[test]
    fn crc_corruption_is_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000002.wal");
        let mut wal = Wal::create(&path, Epoch(0)).unwrap();
        wal.append_txn(
            1,
            Op::Put {
                table_id: 9,
                rows: vec![1, 2, 3, 4],
            },
        )
        .unwrap();
        wal.sync().unwrap();
        drop(wal);

        // Flip a payload byte well past the header.
        let mut bytes = std::fs::read(&path).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path, bytes).unwrap();

        let err = WalReader::open(&path).unwrap().next_record().unwrap_err();
        assert!(
            matches!(err, MongrelError::CorruptWal { .. }),
            "got {err:?}"
        );
    }

    #[test]
    fn trailing_torn_is_eof_but_interior_corruption_errors() {
        let dir = tempdir().unwrap();

        // (a) good records then a half-written trailing frame -> replay returns
        //     the good prefix (torn tail = clean EOF).
        let path_a = dir.path().join("seg-torn.wal");
        let mut wal = Wal::create(&path_a, Epoch(0)).unwrap();
        wal.append_txn(
            1,
            Op::Put {
                table_id: 1,
                rows: vec![1],
            },
        )
        .unwrap();
        wal.append_txn(
            1,
            Op::Put {
                table_id: 1,
                rows: vec![2],
            },
        )
        .unwrap();
        wal.sync().unwrap();
        drop(wal);
        // Append a partial trailing frame (claims 64 bytes, only 7 written).
        let mut f = OpenOptions::new().append(true).open(&path_a).unwrap();
        f.write_all(&64u32.to_le_bytes()).unwrap();
        f.write_all(&[0u8; 7]).unwrap();
        f.sync_all().unwrap();
        drop(f);
        let recs = replay(&path_a).unwrap();
        assert_eq!(recs.len(), 2, "torn trailing frame must truncate cleanly");

        // (b) corrupt an INTERIOR frame's CRC and append a valid frame after ->
        //     replay errors (interior corruption, not a torn tail).
        let path_b = dir.path().join("seg-interior.wal");
        let mut wal = Wal::create(&path_b, Epoch(0)).unwrap();
        wal.append_txn(
            1,
            Op::Put {
                table_id: 1,
                rows: vec![10, 20, 30],
            },
        )
        .unwrap();
        wal.append_txn(
            1,
            Op::Put {
                table_id: 1,
                rows: vec![40],
            },
        )
        .unwrap();
        wal.sync().unwrap();
        drop(wal);
        // Flip a payload byte of the FIRST frame (interior), leaving the second
        // frame intact so a valid frame follows the corrupt one.
        let mut bytes = std::fs::read(&path_b).unwrap();
        let first_payload_byte = HEADER_LEN as usize + 4 + 4 + 8 + 8; // past len+crc+seq+txn_id
        bytes[first_payload_byte] ^= 0xFF;
        std::fs::write(&path_b, bytes).unwrap();
        let err = replay(&path_b).unwrap_err();
        assert!(
            matches!(err, MongrelError::CorruptWal { .. }),
            "interior corruption must error, got {err:?}"
        );

        // (c) a complete trailing frame with a bad CRC is corruption. Only a
        //     physically short final frame is an admissible crash-torn tail.
        let path_c = dir.path().join("seg-badtail.wal");
        let mut wal = Wal::create(&path_c, Epoch(0)).unwrap();
        wal.append_txn(
            1,
            Op::Put {
                table_id: 1,
                rows: vec![5],
            },
        )
        .unwrap();
        wal.sync().unwrap();
        drop(wal);
        let mut bytes = std::fs::read(&path_c).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xFF;
        std::fs::write(&path_c, bytes).unwrap();
        assert!(matches!(
            replay(&path_c),
            Err(MongrelError::CorruptWal { .. })
        ));
    }

    #[test]
    fn byte_threshold_auto_syncs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000003.wal");
        let mut wal = Wal::create(&path, Epoch(0)).unwrap();
        wal.sync_byte_threshold = 1; // sync after every record
        wal.append_txn(
            1,
            Op::Put {
                table_id: 1,
                rows: vec![0; 5],
            },
        )
        .unwrap();
        assert_eq!(
            wal.unflushed_bytes(),
            0,
            "threshold should have auto-synced"
        );
    }

    #[test]
    fn create_never_replaces_an_existing_segment() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000000.wal");
        drop(Wal::create(&path, Epoch(0)).unwrap());
        let before = std::fs::read(&path).unwrap();
        assert!(matches!(
            Wal::create(&path, Epoch(0)),
            Err(MongrelError::Io(error)) if error.kind() == std::io::ErrorKind::AlreadyExists
        ));
        assert_eq!(std::fs::read(path).unwrap(), before);
    }

    #[test]
    fn zero_length_frame_never_hides_a_wal_suffix() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000000.wal");
        let mut wal = Wal::create(&path, Epoch(0)).unwrap();
        wal.append_system(Op::Flush {
            table_id: 1,
            flushed_epoch: 1,
        })
        .unwrap();
        wal.sync().unwrap();
        drop(wal);
        let original = std::fs::read(&path).unwrap();
        let mut bytes = original[..HEADER_LEN as usize].to_vec();
        bytes.extend_from_slice(&0_u32.to_le_bytes());
        bytes.extend_from_slice(&original[HEADER_LEN as usize..]);
        std::fs::write(&path, bytes).unwrap();
        assert!(matches!(
            replay(&path),
            Err(MongrelError::CorruptWal { .. })
        ));
    }

    #[test]
    fn reader_rejects_outer_inner_record_identity_mismatch() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000000.wal");
        let mut wal = Wal::create(&path, Epoch(0)).unwrap();
        wal.append_system(Op::Flush {
            table_id: 1,
            flushed_epoch: 1,
        })
        .unwrap();
        wal.sync().unwrap();
        drop(wal);

        let mut bytes = std::fs::read(&path).unwrap();
        let start = HEADER_LEN as usize;
        let mut length = [0_u8; 4];
        length.copy_from_slice(&bytes[start..start + 4]);
        let length = u32::from_le_bytes(length) as usize;
        let payload = &bytes[start + 24..start + 24 + length];
        let mut record: Record = bincode::deserialize(payload).unwrap();
        record.seq = Epoch(99);
        let replacement = bincode::serialize(&record).unwrap();
        assert_eq!(replacement.len(), length);
        bytes[start + 24..start + 24 + length].copy_from_slice(&replacement);
        recompute_frame_crc(&mut bytes, start);
        std::fs::write(&path, bytes).unwrap();

        assert!(matches!(
            WalReader::open(&path).unwrap().next_record(),
            Err(MongrelError::CorruptWal { .. })
        ));
    }

    #[test]
    fn encrypted_frames_reject_reorder_replay_deletion_and_cross_segment_move() {
        fn cipher(key: &[u8; 32]) -> Box<dyn crate::encryption::Cipher> {
            Box::new(crate::encryption::AesCipher::new(key).unwrap())
        }

        let dir = tempdir().unwrap();
        let key = [0x5a; 32];
        let path = dir.path().join("seg-000009.wal");
        let mut wal = Wal::create_with_cipher(&path, Epoch(0), Some(cipher(&key)), 9).unwrap();
        for table_id in [1, 2] {
            wal.append_system(Op::Flush {
                table_id,
                flushed_epoch: table_id,
            })
            .unwrap();
        }
        wal.sync().unwrap();
        drop(wal);
        let original = std::fs::read(&path).unwrap();
        let ranges = frame_ranges(&original);
        assert_eq!(ranges.len(), 2);

        let mut reordered = original[..HEADER_LEN as usize].to_vec();
        reordered.extend_from_slice(&original[ranges[1].clone()]);
        reordered.extend_from_slice(&original[ranges[0].clone()]);
        std::fs::write(&path, reordered).unwrap();
        assert!(replay_with_cipher(&path, Some(cipher(&key))).is_err());

        let mut replayed = original[..HEADER_LEN as usize].to_vec();
        replayed.extend_from_slice(&original[ranges[0].clone()]);
        replayed.extend_from_slice(&original[ranges[0].clone()]);
        replayed.extend_from_slice(&original[ranges[1].clone()]);
        std::fs::write(&path, replayed).unwrap();
        assert!(replay_with_cipher(&path, Some(cipher(&key))).is_err());

        let mut deleted = original[..HEADER_LEN as usize].to_vec();
        deleted.extend_from_slice(&original[ranges[1].clone()]);
        std::fs::write(&path, deleted).unwrap();
        assert!(replay_with_cipher(&path, Some(cipher(&key))).is_err());

        let mut outer_tampered = original.clone();
        outer_tampered[ranges[0].start + 8] ^= 0x01;
        recompute_frame_crc(&mut outer_tampered, ranges[0].start);
        std::fs::write(&path, outer_tampered).unwrap();
        assert!(replay_with_cipher(&path, Some(cipher(&key))).is_err());

        let other = dir.path().join("seg-000010.wal");
        let mut wal = Wal::create_with_cipher(&other, Epoch(0), Some(cipher(&key)), 10).unwrap();
        wal.append_system(Op::Flush {
            table_id: 3,
            flushed_epoch: 3,
        })
        .unwrap();
        wal.sync().unwrap();
        drop(wal);
        let mut moved = std::fs::read(&other).unwrap();
        moved.truncate(HEADER_LEN as usize);
        moved.extend_from_slice(&original[ranges[0].clone()]);
        std::fs::write(&other, moved).unwrap();
        assert!(replay_with_cipher(&other, Some(cipher(&key))).is_err());
    }

    #[test]
    fn wal_nonce_is_segment_deterministic() {
        // Two segments with different segment_no must never share a frame nonce
        // base, and frames within a segment never collide.
        assert_ne!(frame_nonce_for(5, 0), frame_nonce_for(6, 0));
        assert_ne!(frame_nonce_for(5, 0), frame_nonce_for(5, 1));
        // Deterministic: identical positions produce identical nonces. Segment
        // paths are create-new and never reused.
        assert_eq!(frame_nonce_for(5, 0), frame_nonce_for(5, 0));
    }
}
