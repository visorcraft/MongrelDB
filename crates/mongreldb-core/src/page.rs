use crate::epoch::Epoch;
use serde::{Deserialize, Serialize};

/// On-disk page encoding. Mirrors the strategies used by modern columnar
/// formats, plus `BinaryQuantized` for AI embedding columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[repr(u8)]
pub enum Encoding {
    Plain = 0,
    Dictionary = 1,
    Delta = 2,
    ByteStreamSplit = 3,
    RunLength = 4,
    /// 1 bit per dimension; similarity via SIMD popcount(XOR).
    BinaryQuantized = 5,
    /// Plain-encode then zstd-compress (default for fixed-width and high-card columns).
    Zstd = 6,
}

/// Per-page statistics enabling file/page-level pruning (analogous to Parquet's
/// page index, but always present and tighter). Serialized in the run's column
/// directory.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PageStat {
    pub first_row_id: u64,
    pub last_row_id: u64,
    pub null_count: u64,
    pub row_count: u32,
    pub min: Option<Vec<u8>>,
    pub max: Option<Vec<u8>>,
    /// Absolute offset of the (possibly encrypted + compressed) page payload.
    pub offset: u64,
    pub compressed_len: u32,
    pub uncompressed_len: u32,
}

/// A cached, decrypted, decompressed page tagged with the epoch at which it was
/// committed. The tag is what makes the cache self-invalidating under MVCC.
#[derive(Debug, Clone)]
pub struct CachedPage {
    pub committed_epoch: Epoch,
    pub content_hash: [u8; 32],
    pub bytes: bytes::Bytes,
}

impl CachedPage {
    /// Cache key for content-addressed, MVCC-safe storage.
    pub fn cache_key(&self) -> ([u8; 32], Epoch) {
        (self.content_hash, self.committed_epoch)
    }
}
