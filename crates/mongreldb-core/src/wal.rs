//! Append-only, group-commit, torn-write-safe WAL.
//!
//! Sub-ms writes come from the fact that [`Wal::append`] only copies bytes into
//! the OS file buffer (and an in-process [`BufWriter`]); it does **not** fsync.
//! A timer- or threshold-driven [`Wal::sync`] does the `flush() + sync_all()`
//! and bumps the epoch.

use crate::epoch::Epoch;
use crate::rowid::RowId;
use crate::schema::{ColumnDef, Schema};
use crate::{MongrelError, Result};
use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use zeroize::Zeroizing;

pub const WAL_MAGIC: [u8; 8] = *b"MONGRWAL";
const WAL_VERSION: u16 = 3;
const HEADER_LEN: u64 = 8 + 2 + 4 + 8; // magic + version + reserved(incl enc_flag) + epoch_created
/// Encryption flag stored in reserved[0] of the WAL header.
const ENC_PLAINTEXT: u8 = 0;
const ENC_AES_GCM: u8 = 1;

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
    /// from the catalog's monotonic `next_segment_no` (spec §7.1, review fix
    /// #23). Determinism makes a reopened segment — which truncates and rewrites
    /// the active file — reuse the SAME nonces it would have used pre-crash,
    /// which is safe because the old frames are gone (overwritten).
    segment_no: u64,
    /// Per-segment frame counter. Occupies the low 4 bytes of the nonce, so
    /// `append_record` refuses to write past `u32::MAX` frames in one segment
    /// (that would truncate the counter and reuse a nonce under the DEK).
    /// Segments rotate at flush long before this, so it is unreachable in
    /// practice — but enforced rather than assumed.
    frame_seq: u64,
}

impl Wal {
    /// Create a new WAL segment, truncating any existing file at `path`.
    pub fn create(path: impl AsRef<Path>, epoch_created: Epoch) -> Result<Self> {
        Self::create_with_cipher(path, epoch_created, None, 0)
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
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let mut wal = Self {
            file: BufWriter::with_capacity(1 << 20, file),
            path,
            next_seq: epoch_created.0 + 1,
            unflushed_bytes: 0,
            sync_byte_threshold: 64 * 1024,
            cipher,
            segment_no,
            frame_seq: 0,
        };
        wal.write_header(epoch_created)?;
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
        self.next_seq += 1;
        self.append_record(&Record::new(seq, txn_id, op))?;
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
            let ciphertext = cipher.encrypt_page(&nonce, &payload)?;
            self.frame_seq += 1;
            let mut combined = Vec::with_capacity(12 + ciphertext.len());
            combined.extend_from_slice(&nonce);
            combined.extend_from_slice(&ciphertext);
            combined
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

    fn write_header(&mut self, epoch_created: Epoch) -> Result<()> {
        let enc_flag = if self.cipher.is_some() {
            ENC_AES_GCM
        } else {
            ENC_PLAINTEXT
        };
        self.file.write_all(&WAL_MAGIC)?;
        self.file.write_all(&WAL_VERSION.to_le_bytes())?;
        self.file.write_all(&[enc_flag, 0, 0, 0])?; // enc_flag + 3 reserved
        self.file.write_all(&epoch_created.0.to_le_bytes())?;
        self.unflushed_bytes = 0;
        Ok(())
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        let _ = self.file.flush();
    }
}

/// Streaming reader used by recovery. Stops at the first torn record
/// (`REC_LEN == 0`) or CRC mismatch, returning the cleanly-committed prefix.
pub struct WalReader {
    inner: BufReader<File>,
    pos: u64,
    /// True if frames are encrypted (enc_flag in header).
    encrypted: bool,
    /// Optional cipher for decryption.
    cipher: Option<Box<dyn crate::encryption::Cipher>>,
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
        let mut file = File::open(path.as_ref())?;
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
            return Err(MongrelError::InvalidArgument(format!(
                "unsupported wal version {version}"
            )));
        }
        let mut reserved = [0u8; 4];
        file.read_exact(&mut reserved)?;
        let encrypted = reserved[0] == ENC_AES_GCM;
        let mut epoch_buf = [0u8; 8];
        file.read_exact(&mut epoch_buf)?;
        let _epoch_created = Epoch(u64::from_le_bytes(epoch_buf));
        let pos = HEADER_LEN;
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
            encrypted,
            cipher,
        })
    }

    /// Read the next record. Returns `Ok(None)` at a clean end-of-records
    /// (zero-length marker or EOF), and `Err(TornWrite)` for a partial record.
    pub fn next_record(&mut self) -> Result<Option<Record>> {
        let mut len_buf = [0u8; 4];
        match self.inner.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(e) => return Err(e.into()),
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        if len == 0 {
            return Ok(None);
        }
        // A runaway length (torn header or garbage) would trigger a huge
        // allocation; treat anything past the cap as a torn write.
        const MAX_RECORD_LEN: usize = 64 * 1024 * 1024;
        if len > MAX_RECORD_LEN {
            return Err(MongrelError::TornWrite { offset: self.pos });
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
            if payload.len() < 28 {
                // 12 (nonce) + 16 (min GCM tag) minimum
                return Err(MongrelError::CorruptWal {
                    offset: record_start,
                    reason: "encrypted frame too short".into(),
                });
            }
            let nonce: [u8; 12] = payload[..12].try_into().unwrap();
            let ciphertext = &payload[12..];
            cipher.decrypt_page(&nonce, ciphertext).map_err(|e| {
                MongrelError::Decryption(format!(
                    "WAL frame decryption failed — wrong passphrase or key? ({e})"
                ))
            })?
        } else {
            payload.to_vec()
        };

        // Trust the deserialized `seq`, not the outer frame `seq`: the outer one
        // is covered only by an unkeyed CRC32C (recomputable by anyone with
        // write access), whereas the inner `seq` rides inside the record — under
        // the CRC for plaintext frames and under AES-GCM authentication for
        // encrypted ones. They are written equal, so this changes nothing for
        // honest data while denying a tamperer the ability to renumber a frame.
        let record: Record = bincode::deserialize(&plaintext)?;
        self.pos += 4 + 4 + 8 + 8 + len as u64;
        Ok(Some(record))
    }

    /// Replay all cleanly-committed records. A torn tail (crash mid-append or a
    /// partially-flushed last frame) is treated as end-of-log and truncated —
    /// the valid prefix is returned. A CRC failure or short read that is
    /// followed by a well-formed frame is treated as **interior corruption**
    /// and surfaces as [`MongrelError::CorruptWal`] (spec §8.4, review fix #22).
    pub fn replay(&mut self) -> Result<Vec<Record>> {
        let mut out = Vec::new();
        loop {
            match self.next_record() {
                Ok(Some(rec)) => out.push(rec),
                Ok(None) => break,
                Err(MongrelError::TornWrite { offset }) => {
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
                Err(MongrelError::CorruptWal { offset, .. }) => {
                    // CRC mismatch: torn tail if nothing valid follows, else
                    // interior corruption.
                    if self.valid_frame_follows()? {
                        return Err(MongrelError::CorruptWal {
                            offset,
                            reason: "interior corruption: valid frame follows a CRC mismatch"
                                .into(),
                        });
                    }
                    break;
                }
                Err(e) => return Err(e),
            }
        }
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
    wal_dir: PathBuf,
    active: Wal,
    /// Monotonic segment number of the active segment (namespaces nonces).
    active_segment_no: u64,
    /// Highest sequence number reported durable by the last successful
    /// `group_sync`. P3's group-commit publishes only commits at or below this.
    durable_seq: u64,
    /// WAL DEK (constant across segments). None for plaintext. Kept so a
    /// `rotate` can rebuild the per-segment cipher under the same key.
    wal_dek: Option<Zeroizing<[u8; 32]>>,
    /// Count of actual fsyncs issued via [`Self::group_sync`]. With real group
    /// commit this is far below the commit count (one leader fsync serves many
    /// followers). Diagnostic / test-facing.
    group_sync_count: u64,
}

impl SharedWal {
    /// Segment filename for a given number.
    fn segment_path(wal_dir: &Path, segment_no: u64) -> PathBuf {
        wal_dir.join(format!("seg-{segment_no:06}.wal"))
    }

    /// Build a per-segment frame cipher from the WAL DEK (encryption feature).
    #[cfg(feature = "encryption")]
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
        let wal_dir = root.join("_wal");
        std::fs::create_dir_all(&wal_dir)?;
        let cipher = match &wal_dek {
            #[cfg(feature = "encryption")]
            Some(dk) => Some(Self::cipher_from_dek(dk)?),
            #[cfg(not(feature = "encryption"))]
            Some(_) => {
                return Err(MongrelError::Encryption(
                    "encryption feature disabled but a WAL DEK was supplied".into(),
                ))
            }
            None => None,
        };
        let active =
            Wal::create_with_cipher(Self::segment_path(&wal_dir, 0), epoch_created, cipher, 0)?;
        Ok(Self {
            wal_dir,
            active,
            active_segment_no: 0,
            durable_seq: epoch_created.0,
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
        let wal_dir = root.join("_wal");
        std::fs::create_dir_all(&wal_dir)?;
        let next_segment_no = list_segment_numbers(&wal_dir)?
            .into_iter()
            .max()
            .map(|m| m + 1)
            .unwrap_or(0);
        let cipher = match &wal_dek {
            #[cfg(feature = "encryption")]
            Some(dk) => Some(Self::cipher_from_dek(dk)?),
            #[cfg(not(feature = "encryption"))]
            Some(_) => {
                return Err(MongrelError::Encryption(
                    "encryption feature disabled but a WAL DEK was supplied".into(),
                ))
            }
            None => None,
        };
        let mut active = Wal::create_with_cipher(
            Self::segment_path(&wal_dir, next_segment_no),
            epoch_created,
            cipher,
            next_segment_no,
        )?;
        // Flush + fsync the fresh segment header so the recovery replay (which
        // reads every segment) never sees a half-written file.
        active.sync()?;
        Ok(Self {
            wal_dir,
            active,
            active_segment_no: next_segment_no,
            durable_seq: epoch_created.0,
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
    pub fn append(&mut self, txn_id: u64, _table_id: u64, op: Op) -> Result<u64> {
        Ok(self.active.append_txn(txn_id, op)?.0)
    }

    /// Append a `TxnCommit` marker sealing `txn_id` at `epoch`.
    pub fn append_commit(&mut self, txn_id: u64, epoch: Epoch, added: &[AddedRun]) -> Result<u64> {
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
    /// appender since the last `group_sync`.
    pub fn group_sync(&mut self) -> Result<u64> {
        self.active.sync()?;
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

    /// Rotate to a fresh segment numbered `segment_no` (which namespaces nonces
    /// under the constant WAL DEK). The current segment must already be synced.
    pub fn rotate(&mut self, segment_no: u64) -> Result<()> {
        let cipher = match &self.wal_dek {
            #[cfg(feature = "encryption")]
            Some(dk) => Some(Self::cipher_from_dek(dk)?),
            _ => None,
        };
        let path = Self::segment_path(&self.wal_dir, segment_no);
        let epoch = Epoch(self.durable_seq);
        let wal = Wal::create_with_cipher(path, epoch, cipher, segment_no)?;
        self.active = wal;
        self.active_segment_no = segment_no;
        Ok(())
    }

    /// The active segment number.
    pub fn active_segment_no(&self) -> u64 {
        self.active_segment_no
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
        let mut segments = list_segment_numbers(&self.wal_dir)?;
        segments.sort_unstable();
        let mut reaped = 0;
        for n in segments {
            if n == self.active_segment_no {
                continue; // never delete the segment we're appending to
            }
            let path = Self::segment_path(&self.wal_dir, n);
            // Fast path: an infinite floor means every non-active segment is
            // reapable, so skip the (cold-but-not-free) full replay.
            let reapable = if min_retained_seq == u64::MAX {
                true
            } else {
                // A segment is reapable when its highest seq is below the
                // retention floor. A torn/corrupt OLD segment that won't replay
                // is treated as reapable: we are GCing it anyway and its records
                // are by construction already durable in runs.
                let recs = match &self.wal_dek {
                    #[cfg(feature = "encryption")]
                    Some(dk) => {
                        let cipher = Self::cipher_from_dek(dk)?;
                        replay_with_cipher(&path, Some(cipher))
                    }
                    _ => replay(&path),
                };
                match recs {
                    Ok(recs) => recs.iter().map(|r| r.seq.0).max().unwrap_or(0) < min_retained_seq,
                    Err(_) => true,
                }
            };
            if reapable {
                std::fs::remove_file(&path)?;
                reaped += 1;
            }
        }
        if reaped > 0 {
            if let Ok(d) = std::fs::File::open(&self.wal_dir) {
                let _ = d.sync_all();
            }
        }
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
        let mut bad = Vec::new();
        let Ok(segments) = list_segment_numbers(&self.wal_dir) else {
            return bad;
        };
        for n in segments {
            let path = Self::segment_path(&self.wal_dir, n);
            // The frame cipher is constant across segments under the WAL DEK;
            // rebuild it per open (cheap key schedule, few segments).
            let res = match &self.wal_dek {
                #[cfg(feature = "encryption")]
                Some(dk) => match Self::cipher_from_dek(dk) {
                    Ok(cipher) => WalReader::open_with_cipher(&path, Some(cipher)),
                    Err(e) => Err(e),
                },
                _ => WalReader::open_with_cipher(&path, None),
            };
            if let Err(e) = res {
                bad.push((n, format!("{e}")));
            }
        }
        bad
    }

    /// Replay every record across all segments in `<root>/_wal/`, in segment
    /// order, applying the torn-tail-vs-interior-corruption rule per segment.
    pub fn replay(root: &Path) -> Result<Vec<Record>> {
        Self::replay_with_dek(root, None)
    }

    /// Replay with an optional WAL DEK (for encrypted segments).
    pub fn replay_with_dek(
        root: &Path,
        wal_dek: Option<&Zeroizing<[u8; 32]>>,
    ) -> Result<Vec<Record>> {
        let wal_dir = root.join("_wal");
        let mut segments = list_segment_numbers(&wal_dir)?;
        segments.sort_unstable();
        let mut out = Vec::new();
        for n in segments {
            let path = Self::segment_path(&wal_dir, n);
            // Replay each segment independently: a torn tail in any segment
            // truncates only that segment's prefix (interior corruption errors).
            let recs = match wal_dek {
                #[cfg(feature = "encryption")]
                Some(dk) => {
                    let cipher = Self::cipher_from_dek(dk)?;
                    replay_with_cipher(&path, Some(cipher))?
                }
                _ => replay(&path)?,
            };
            out.extend(recs);
        }
        Ok(out)
    }
}

/// List the segment numbers present under `wal_dir` (unsorted).
fn list_segment_numbers(wal_dir: &Path) -> Result<Vec<u64>> {
    let mut segments = Vec::new();
    if let Ok(rd) = std::fs::read_dir(wal_dir) {
        for entry in rd.flatten() {
            let fname = entry.file_name();
            let Some(s) = fname.to_str() else {
                continue;
            };
            let Some(stripped) = s.strip_prefix("seg-") else {
                continue;
            };
            let Some(stripped) = stripped.strip_suffix(".wal") else {
                continue;
            };
            if let Ok(n) = stripped.parse::<u64>() {
                segments.push(n);
            }
        }
    }
    Ok(segments)
}

#[cfg(test)]
mod shared_wal_tests {
    use super::*;
    use tempfile::tempdir;

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
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

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
        let mut w2 = Wal::create(&path, Epoch(0)).unwrap();
        w2.append_system(Op::Flush {
            table_id: 3,
            flushed_epoch: 11,
        })
        .unwrap();
        w2.sync().unwrap();
        let recs = replay(&path).unwrap();
        assert_eq!(recs[0].txn_id, SYSTEM_TXN_ID);
        assert!(matches!(recs[0].op, Op::Flush { .. }));
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

        // (c) a trailing frame whose CRC is bad (last frame, nothing valid
        //     after) is a torn tail -> clean truncation, no error.
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
        let recs = replay(&path_c).unwrap();
        assert_eq!(
            recs.len(),
            0,
            "trailing corrupt frame with no valid follower is a torn tail"
        );
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

    #[cfg(feature = "encryption")]
    #[test]
    fn wal_nonce_is_segment_deterministic() {
        // Two segments with different segment_no must never share a frame nonce
        // base, and frames within a segment never collide.
        assert_ne!(frame_nonce_for(5, 0), frame_nonce_for(6, 0));
        assert_ne!(frame_nonce_for(5, 0), frame_nonce_for(5, 1));
        // Deterministic: same inputs → same nonce (reopened segment reuses its
        // own nonces safely because the old frames were overwritten).
        assert_eq!(frame_nonce_for(5, 0), frame_nonce_for(5, 0));
    }
}
