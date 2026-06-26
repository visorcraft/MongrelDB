use serde::{Deserialize, Serialize};

/// Logical column types. The on-disk Arrow encoding is chosen at flush based on
/// [`TypeId`] and run-time stats (e.g. low-cardinality strings → dictionary).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum TypeId {
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    TimestampNanos,
    Date32,
    /// Variable-length bytes (covers UTF-8 strings).
    Bytes,
    /// Fixed-size binary embedding of `dim` f32 components.
    Embedding {
        dim: u32,
    },
}

impl TypeId {
    /// Fixed size in bytes for fixed-width types, else `None`.
    pub const fn fixed_size(self) -> Option<usize> {
        match self {
            TypeId::Bool => Some(1),
            TypeId::Int8 | TypeId::UInt8 => Some(1),
            TypeId::Int16 | TypeId::UInt16 => Some(2),
            TypeId::Int32 | TypeId::UInt32 | TypeId::Float32 | TypeId::Date32 => Some(4),
            TypeId::Int64 | TypeId::UInt64 | TypeId::Float64 | TypeId::TimestampNanos => Some(8),
            TypeId::Bytes | TypeId::Embedding { .. } => None,
        }
    }
}

/// Per-column flags packed into a `u32`. Stored verbatim in the run header.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnFlags {
    bits: u32,
}

impl ColumnFlags {
    pub const NULLABLE: u32 = 1 << 0;
    pub const PRIMARY_KEY: u32 = 1 << 1;
    pub const ENCRYPTED: u32 = 1 << 2;
    /// Store HMAC(value) for equality or OPE for range so indexes work without
    /// decrypting.
    pub const ENCRYPTED_INDEXABLE: u32 = 1 << 3;
    /// Store 1 bit per dimension; similarity via popcount(XOR).
    pub const EMBEDDING_BINARY_QUANTIZED: u32 = 1 << 4;

    #[inline]
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    #[inline]
    pub const fn with(mut self, flag: u32) -> Self {
        self.bits |= flag;
        self
    }

    #[inline]
    pub const fn contains(&self, flag: u32) -> bool {
        self.bits & flag != 0
    }

    #[inline]
    pub const fn bits(&self) -> u32 {
        self.bits
    }
}

/// A column definition. `id` is stable, monotonic, and never reused.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: u16,
    pub name: String,
    pub ty: TypeId,
    pub flags: ColumnFlags,
}

/// The kind of secondary index to maintain for a column. The primary-key index
/// (in-memory HOT + on-disk learned PGM) is implicit and not listed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKind {
    /// Roaring bitmap (value → row-id set). Low-cardinality equality / IN.
    Bitmap,
    /// FM-index / wavelet tree for arbitrary substring + ranked access.
    FmIndex,
    /// Quantized-vector ANN (binary / PQ). For `Embedding` columns.
    Ann,
    /// Learned zonemap (PGM) for ordered range predicates.
    LearnedRange,
    /// MinHash/LSH set-similarity (AI dedup/join primitives).
    MinHash,
    /// Learned-sparse (SPLADE-style) retrieval over weighted token vectors.
    Sparse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub column_id: u16,
    pub kind: IndexKind,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Schema {
    pub schema_id: u64,
    pub columns: Vec<ColumnDef>,
    pub indexes: Vec<IndexDef>,
    /// Phase 18.2: column co-location groups. Each inner Vec lists column IDs
    /// that are always accessed together. The run writer writes their pages
    /// adjacently so a scan touching those columns benefits from sequential
    /// I/O and cache locality. Empty = no co-location (default).
    #[serde(default)]
    pub colocation: Vec<Vec<u16>>,
}

impl Schema {
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn primary_key(&self) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.flags.contains(ColumnFlags::PRIMARY_KEY))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flag_composition() {
        let f = ColumnFlags::empty()
            .with(ColumnFlags::PRIMARY_KEY)
            .with(ColumnFlags::ENCRYPTED_INDEXABLE);
        assert!(f.contains(ColumnFlags::PRIMARY_KEY));
        assert!(f.contains(ColumnFlags::ENCRYPTED_INDEXABLE));
        assert!(!f.contains(ColumnFlags::ENCRYPTED));
    }

    #[test]
    fn fixed_size() {
        assert_eq!(TypeId::Int64.fixed_size(), Some(8));
        assert_eq!(TypeId::Bytes.fixed_size(), None);
        assert_eq!(TypeId::Embedding { dim: 768 }.fixed_size(), None);
    }
}
