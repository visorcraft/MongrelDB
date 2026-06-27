//! Append-only, group-commit, torn-write-safe WAL.
//!
//! Sub-ms writes come from the fact that [`Wal::append`] only copies bytes into
//! the OS file buffer (and an in-process [`BufWriter`]); it does **not** fsync.
//! A timer- or threshold-driven [`Wal::sync`] does the `flush() + sync_all()`
//! and bumps the epoch. See `DBPLAN.md` §6.1 for the on-disk layout.

use crate::epoch::Epoch;
use crate::rowid::RowId;
use crate::{MongrelError, Result};
use crc::{Crc, CRC_32_ISCSI};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

pub const WAL_MAGIC: [u8; 8] = *b"MONGRWAL";
const WAL_VERSION: u16 = 2;
const HEADER_LEN: u64 = 8 + 2 + 4 + 8; // magic + version + reserved(incl enc_flag) + epoch_created
/// Encryption flag stored in reserved[0] of the WAL header.
const ENC_PLAINTEXT: u8 = 0;
const ENC_AES_GCM: u8 = 1;

const CRC32C: Crc<u32> = Crc::<u32>::new(&CRC_32_ISCSI);

/// One mutation. `Put.rows` is a self-describing Arrow IPC stream (or, for tiny
/// single-row writes, a compact row batch — both are opaque bytes to the WAL).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Record {
    pub seq: Epoch,
    pub op: Op,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Op {
    Put {
        table_id: u64,
        rows: Vec<u8>,
    },
    Delete {
        table_id: u64,
        /// The MVCC epoch the tombstone was stamped with at delete time. Recovery
        /// must re-stamp the in-memory tombstone at this exact epoch (not the WAL
        /// record's monotonic `seq`, which outpaces the commit epoch) so that a
        /// tombstone committed before the last snapshot still hides the row.
        epoch: Epoch,
        row_ids: Vec<RowId>,
    },
    TruncateTable {
        table_id: u64,
    },
    /// Marker that all preceding mutations have been durably flushed to a
    /// sorted run; recovery may stop replaying after the latest `Flush`.
    Flush {
        last_seq: Epoch,
    },
}

impl Record {
    pub fn new(seq: Epoch, op: Op) -> Self {
        Self { seq, op }
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
    /// Random per-segment nonce seed (generated on creation). Ensures nonce
    /// uniqueness across restarts even with the same epoch_created.
    nonce_seed: [u8; 8],
    /// Per-segment frame counter for nonce uniqueness.
    frame_seq: u32,
}

impl Wal {
    /// Create a new WAL segment, truncating any existing file at `path`.
    pub fn create(path: impl AsRef<Path>, epoch_created: Epoch) -> Result<Self> {
        Self::create_with_cipher(path, epoch_created, None)
    }

    /// Create a new WAL segment with optional frame-level encryption.
    pub fn create_with_cipher(
        path: impl AsRef<Path>,
        epoch_created: Epoch,
        cipher: Option<Box<dyn crate::encryption::Cipher>>,
    ) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        let mut nonce_seed = [0u8; 8];
        // fill_random is always available (non-cfg-gated)
        getrandom::getrandom(&mut nonce_seed).expect("getrandom: OS CSPRNG unavailable");
        let mut wal = Self {
            file: BufWriter::with_capacity(1 << 20, file),
            path,
            next_seq: epoch_created.0 + 1,
            unflushed_bytes: 0,
            sync_byte_threshold: 64 * 1024,
            cipher,
            nonce_seed,
            frame_seq: 0,
        };
        wal.write_header(epoch_created)?;
        Ok(wal)
    }

    /// Append a record. Assigns the next monotonic sequence (the first record
    /// after a WAL created at `E` gets `E + 1`), writes it, and returns the
    /// assigned sequence. Does NOT fsync — call [`Wal::sync`] (or rely on the
    /// byte threshold). The WAL sequence is independent of the row commit
    /// epoch; the engine tracks commit epochs separately.
    pub fn append(&mut self, op: Op) -> Result<Epoch> {
        let seq = Epoch(self.next_seq);
        self.next_seq += 1;
        self.append_record(&Record::new(seq, op))?;
        Ok(seq)
    }

    fn append_record(&mut self, record: &Record) -> Result<()> {
        let payload = bincode::serialize(record)?;

        // Encrypt the payload if a cipher is present. The nonce is prepended
        // to the ciphertext so the reader can extract it from a single read.
        let frame_payload = if let Some(cipher) = &self.cipher {
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
        // CRC covers seq + (encrypted) payload.
        let mut digest = CRC32C.digest();
        digest.update(&record.seq.0.to_le_bytes());
        digest.update(&frame_payload);
        let crc_val = digest.finalize();

        self.file.write_all(&(len as u32).to_le_bytes())?;
        self.file.write_all(&crc_val.to_le_bytes())?;
        self.file.write_all(&record.seq.0.to_le_bytes())?;
        self.file.write_all(&frame_payload)?;
        self.unflushed_bytes += 4 + 4 + 8 + len as u64;
        if self.sync_byte_threshold > 0 && self.unflushed_bytes >= self.sync_byte_threshold {
            self.sync()?;
        }
        Ok(())
    }

    /// Derive a unique 12-byte AEAD nonce for the current frame:
    /// `[epoch_created: 8B LE][frame_seq: 4B LE]`. epoch_created is unique
    /// per segment (monotonically increasing), frame_seq is unique within
    /// a segment — together they guarantee nonce uniqueness across all
    /// segments under the same WAL DEK.
    fn frame_nonce(&self) -> [u8; 12] {
        let mut n = [0u8; 12];
        n[..8].copy_from_slice(&self.nonce_seed);
        n[8..].copy_from_slice(&self.frame_seq.to_le_bytes());
        n
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
                 Use Db::open_encrypted or Db::open_with_key."
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
        let mut rest = vec![0u8; 4 + 8 + len];
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
        let payload = &rest[12..];

        let mut digest = CRC32C.digest();
        digest.update(&seq.to_le_bytes());
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

        let mut record: Record = bincode::deserialize(&plaintext)?;
        record.seq = Epoch(seq);
        self.pos += 4 + 4 + 8 + len as u64;
        Ok(Some(record))
    }

    /// Replay all cleanly-committed records. A torn tail (crash mid-append) is
    /// treated as end-of-log and truncated — the valid prefix is returned.
    pub fn replay(&mut self) -> Result<Vec<Record>> {
        let mut out = Vec::new();
        loop {
            match self.next_record() {
                Ok(Some(rec)) => out.push(rec),
                Ok(None) => break,
                Err(MongrelError::TornWrite { .. }) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(out)
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
            .append(Op::Put {
                table_id: 1,
                rows: vec![1, 2, 3],
            })
            .unwrap();
        let s2 = wal
            .append(Op::Delete {
                table_id: 1,
                epoch: Epoch(101),
                row_ids: vec![RowId(7)],
            })
            .unwrap();
        assert_eq!(s1, Epoch(101));
        assert_eq!(s2, Epoch(102));
        wal.sync().unwrap();

        let records = replay(&path).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].seq, Epoch(101));
        match &records[0].op {
            Op::Put { table_id, rows } => {
                assert_eq!(*table_id, 1);
                assert_eq!(rows, &vec![1, 2, 3]);
            }
            other => panic!("unexpected op {other:?}"),
        }
        match &records[1].op {
            Op::Delete { epoch, row_ids, .. } => {
                assert_eq!(*epoch, Epoch(101));
                assert_eq!(*row_ids, vec![RowId(7)]);
            }
            other => panic!("unexpected op {other:?}"),
        }
    }

    #[test]
    fn torn_write_is_detected() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000001.wal");
        let mut wal = Wal::create(&path, Epoch(0)).unwrap();
        wal.append(Op::Put {
            table_id: 1,
            rows: vec![0; 10],
        })
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
        wal.append(Op::Put {
            table_id: 9,
            rows: vec![1, 2, 3, 4],
        })
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
    fn byte_threshold_auto_syncs() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("seg-000003.wal");
        let mut wal = Wal::create(&path, Epoch(0)).unwrap();
        wal.sync_byte_threshold = 1; // sync after every record
        wal.append(Op::Put {
            table_id: 1,
            rows: vec![0; 5],
        })
        .unwrap();
        assert_eq!(
            wal.unflushed_bytes(),
            0,
            "threshold should have auto-synced"
        );
    }
}
