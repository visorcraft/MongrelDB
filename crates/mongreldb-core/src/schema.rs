use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::constraint::TableConstraints;
use crate::error::{MongrelError, Result};
use crate::memtable::Value;

/// Logical column types. The on-disk Arrow encoding is chosen at flush based on
/// [`TypeId`] and run-time stats (e.g. low-cardinality strings → dictionary).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Millisecond-precision date (days since epoch × 86400000). Same i64
    /// storage as TimestampNanos; distinct for SQL type affinity.
    Date64,
    /// Nanosecond-precision time-of-day (no date component). Stored as i64.
    Time64,
    /// SQL INTERVAL (months + days + nanoseconds). Stored as 16 bytes
    /// (i64 months, i32 days, i64 nanos).
    Interval,
    /// RFC 4122 UUID. Stored as 16-byte fixed-width (big-endian for sort order).
    Uuid,
    /// JSON value stored as UTF-8 bytes. Distinct from `Bytes` at the type level
    /// so SQL functions and clients know to parse/validate JSON.
    Json,
    /// Variable-length array of homogeneous values (e.g. `int[]`, `text[]`).
    /// Stored as JSON arrays in a Bytes column (SQL-level typed as Array).
    /// The `element_type` is advisory — the Kit layer and DataFusion handle
    /// the actual element encoding.
    Array {
        element_type: u8,
    },
    /// Variable-length bytes (covers UTF-8 strings).
    Bytes,
    /// Fixed-size binary embedding of `dim` f32 components.
    Embedding {
        dim: u32,
    },
    /// Fixed-point decimal (i128 unscaled value, precision, scale). SQL:
    /// `mongreldb_decimal(precision, scale)` or `DECIMAL(p, s)`.
    Decimal128 {
        precision: u8,
        scale: i8,
    },
    /// SQL ENUM: stored as `Value::Bytes(variant_name_utf8)`, validated against
    /// the `variants` list at write time. Dictionary-encoded on disk like
    /// `Bytes` (low-cardinality sweet spot). Membership is enforced at the
    /// write edge (SQL `coerce_value`, HTTP `json_to_value`), not at the core
    /// commit path.
    Enum {
        variants: Arc<[String]>,
    },
}

impl TypeId {
    /// Fixed size in bytes for fixed-width types, else `None`.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            TypeId::Bool => Some(1),
            TypeId::Int8 | TypeId::UInt8 => Some(1),
            TypeId::Int16 | TypeId::UInt16 => Some(2),
            TypeId::Int32 | TypeId::UInt32 | TypeId::Float32 | TypeId::Date32 => Some(4),
            TypeId::Int64
            | TypeId::UInt64
            | TypeId::Float64
            | TypeId::TimestampNanos
            | TypeId::Date64
            | TypeId::Time64 => Some(8),
            TypeId::Bytes | TypeId::Embedding { .. } | TypeId::Enum { .. } => None,
            TypeId::Decimal128 { .. } => Some(16),
            TypeId::Uuid => Some(16),
            TypeId::Json | TypeId::Array { .. } => None,
            TypeId::Interval => Some(20), // i64 months + i32 days + i64 nanos
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
    /// Engine-managed monotonic identity allocator. Valid only on a single
    /// `Int64` primary-key column per table (see [`Schema::validate_auto_increment`]).
    /// On insert, when the column is omitted or `Null`, the engine assigns the
    /// next counter value; an explicit `Int64` value is honored and advances the
    /// counter past it. Counters are 1-based, never reused, and independent of
    /// the physical [`crate::rowid::RowId`].
    pub const AUTO_INCREMENT: u32 = 1 << 5;

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
    pub const fn without(mut self, flag: u32) -> Self {
        self.bits &= !flag;
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

/// A default-value expression stored on a column definition and applied
/// authoritatively by the engine at insert stage time (before NOT NULL
/// validation) when the column is omitted or explicitly `Null`. Sequence
/// defaults are handled separately via [`ColumnFlags::AUTO_INCREMENT`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DefaultExpr {
    /// A literal value applied verbatim.
    Static(Value),
    /// Current timestamp as an ISO-8601 UTC string (`Value::Bytes`). Resolved
    /// at stage time (per-row).
    Now,
    /// A random RFC 4122 UUID (`Value::Uuid`). Resolved at stage time.
    Uuid,
}

/// A column definition. `id` is stable, monotonic, and never reused.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub id: u16,
    pub name: String,
    pub ty: TypeId,
    pub flags: ColumnFlags,
    /// Optional default expression applied at insert stage time when the column
    /// is omitted or explicitly `Null`. Serialized for catalog persistence;
    /// old catalogs without this field deserialize to `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<DefaultExpr>,
    /// How dense embedding values for this column are produced. Only meaningful
    /// when `ty` is [`TypeId::Embedding`]. Defaults to
    /// [`crate::embedding::EmbeddingSource::SuppliedByApplication`] when absent
    /// (old catalogs and application-written vectors). Storage never hard-codes
    /// an external vendor from this field — see
    /// [`crate::embedding::EmbeddingProviderRegistry`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_source: Option<crate::embedding::EmbeddingSource>,
}

/// Metadata updates supported by native ALTER COLUMN.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AlterColumn {
    pub name: Option<String>,
    pub ty: Option<TypeId>,
    pub flags: Option<ColumnFlags>,
    /// `None` = leave default unchanged, `Some(None)` = drop default,
    /// `Some(Some(expr))` = set/replace default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default_value: Option<Option<DefaultExpr>>,
    /// `None` = leave embedding source unchanged, `Some(None)` = clear to
    /// application-supplied default, `Some(Some(source))` = set/replace.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embedding_source: Option<Option<crate::embedding::EmbeddingSource>>,
}

impl AlterColumn {
    pub fn rename(name: impl Into<String>) -> Self {
        Self {
            name: Some(name.into()),
            ty: None,
            flags: None,
            default_value: None,
            embedding_source: None,
        }
    }

    pub fn set_type(ty: TypeId) -> Self {
        Self {
            name: None,
            ty: Some(ty),
            flags: None,
            default_value: None,
            embedding_source: None,
        }
    }

    pub fn set_flags(flags: ColumnFlags) -> Self {
        Self {
            name: None,
            ty: None,
            flags: Some(flags),
            default_value: None,
            embedding_source: None,
        }
    }

    pub fn set_default(expr: DefaultExpr) -> Self {
        Self {
            name: None,
            ty: None,
            flags: None,
            default_value: Some(Some(expr)),
            embedding_source: None,
        }
    }

    pub fn drop_default() -> Self {
        Self {
            name: None,
            ty: None,
            flags: None,
            default_value: Some(None),
            embedding_source: None,
        }
    }

    /// Set or replace the embedding source metadata for an embedding column.
    pub fn set_embedding_source(source: crate::embedding::EmbeddingSource) -> Self {
        Self {
            name: None,
            ty: None,
            flags: None,
            default_value: None,
            embedding_source: Some(Some(source)),
        }
    }
}

/// The kind of secondary index to maintain for a column. The primary-key index
/// (in-memory HOT + on-disk learned PGM) is implicit and not listed here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexKind {
    /// Roaring bitmap (value → row-id set). Low-cardinality equality / IN.
    Bitmap,
    /// FM-index / wavelet tree for arbitrary substring + ranked access.
    FmIndex,
    /// Binary-sign or full-precision Dense ANN for `Embedding` columns.
    Ann,
    /// Learned zonemap (PGM) for ordered range predicates.
    LearnedRange,
    /// MinHash/LSH set-similarity (AI dedup/join primitives).
    MinHash,
    /// Learned-sparse (SPLADE-style) retrieval over weighted token vectors.
    Sparse,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ann: Option<AnnOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub minhash: Option<MinHashOptions>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub learned_range: Option<LearnedRangeOptions>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnnOptions {
    #[serde(default = "default_ann_m")]
    pub m: usize,
    #[serde(default = "default_ann_ef_construction")]
    pub ef_construction: usize,
    #[serde(default = "default_ann_ef_search")]
    pub ef_search: usize,
    #[serde(default)]
    pub quantization: AnnQuantization,
}

impl Default for AnnOptions {
    fn default() -> Self {
        Self {
            m: default_ann_m(),
            ef_construction: default_ann_ef_construction(),
            ef_search: default_ann_ef_search(),
            quantization: AnnQuantization::BinarySign,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AnnQuantization {
    #[default]
    BinarySign,
    /// Full-precision f32 vectors with cosine distance (`1 - cosine_similarity`).
    Dense,
}

const fn default_ann_m() -> usize {
    16
}
const fn default_ann_ef_construction() -> usize {
    64
}
const fn default_ann_ef_search() -> usize {
    64
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MinHashOptions {
    #[serde(default = "default_minhash_permutations")]
    pub permutations: usize,
    #[serde(default = "default_minhash_bands")]
    pub bands: usize,
}

impl Default for MinHashOptions {
    fn default() -> Self {
        Self {
            permutations: default_minhash_permutations(),
            bands: default_minhash_bands(),
        }
    }
}

const fn default_minhash_permutations() -> usize {
    128
}
const fn default_minhash_bands() -> usize {
    32
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LearnedRangeOptions {
    #[serde(default = "default_learned_range_epsilon")]
    pub epsilon: usize,
}

impl Default for LearnedRangeOptions {
    fn default() -> Self {
        Self {
            epsilon: default_learned_range_epsilon(),
        }
    }
}

const fn default_learned_range_epsilon() -> usize {
    16
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IndexDef {
    pub name: String,
    pub column_id: u16,
    pub kind: IndexKind,
    /// Partial index predicate: a SQL WHERE clause expression serialized as
    /// a string (e.g. `"deleted_at IS NULL"`). Only rows matching this
    /// predicate are indexed. `None` means all rows are indexed (full index).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
    #[serde(default)]
    pub options: IndexOptions,
}

impl IndexDef {
    pub fn validate_options(&self) -> Result<()> {
        if self.options.ann.is_some() && self.kind != IndexKind::Ann
            || self.options.minhash.is_some() && self.kind != IndexKind::MinHash
            || self.options.learned_range.is_some() && self.kind != IndexKind::LearnedRange
        {
            return Err(MongrelError::Schema(format!(
                "index {} has options for a different index kind",
                self.name
            )));
        }
        if let Some(options) = &self.options.ann {
            if options.m == 0
                || options.ef_construction < options.m
                || options.ef_search == 0
                || options.m > 256
                || options.ef_construction > 65_536
                || options.ef_search > 65_536
            {
                return Err(MongrelError::Schema(format!(
                    "invalid ANN options for index {}",
                    self.name
                )));
            }
        }
        if let Some(options) = &self.options.minhash {
            if options.permutations == 0
                || options.bands == 0
                || options.permutations % options.bands != 0
                || options.permutations > 4096
                || options.bands > 1024
            {
                return Err(MongrelError::Schema(format!(
                    "invalid MinHash options for index {}",
                    self.name
                )));
            }
        }
        if self
            .options
            .learned_range
            .as_ref()
            .is_some_and(|options| options.epsilon == 0 || options.epsilon > 1_048_576)
        {
            return Err(MongrelError::Schema(format!(
                "invalid learned-range options for index {}",
                self.name
            )));
        }
        Ok(())
    }
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
    /// Engine-side declarative constraints (unique / FK / check). Empty by
    /// default — legacy and Kit-managed tables carry no engine constraints and
    /// behave exactly as before. When non-empty, the transaction layer enforces
    /// them authoritatively at commit (see [`crate::database`]).
    #[serde(default)]
    pub constraints: TableConstraints,
    /// When true, the table is clustered on its primary key: sorted runs are
    /// keyed by PK bytes rather than by `RowId`. Defaults to false.
    #[serde(default)]
    pub clustered: bool,
}

impl Schema {
    pub const MAX_EMBEDDING_DIM: u32 = 65_536;

    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|c| c.name == name)
    }

    pub fn primary_key(&self) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.flags.contains(ColumnFlags::PRIMARY_KEY))
    }

    /// Validate AI column/index representation and embedding values.
    pub fn validate_ai(&self) -> Result<()> {
        for column in &self.columns {
            let TypeId::Embedding { dim } = column.ty else {
                if column.embedding_source.is_some() {
                    return Err(MongrelError::Schema(format!(
                        "non-embedding column '{}' cannot define an embedding source",
                        column.name
                    )));
                }
                continue;
            };
            if dim == 0 || dim > Self::MAX_EMBEDDING_DIM {
                return Err(MongrelError::Schema(format!(
                    "embedding column '{}' dimension must be between 1 and {}",
                    column.name,
                    Self::MAX_EMBEDDING_DIM
                )));
            }
            match column.embedding_source.as_ref() {
                None | Some(crate::embedding::EmbeddingSource::SuppliedByApplication) => {}
                Some(crate::embedding::EmbeddingSource::LocalModel { model_id, .. }) => {
                    if model_id.is_empty() {
                        return Err(MongrelError::Schema(format!(
                            "legacy local embedding column '{}' requires a model identity",
                            column.name
                        )));
                    }
                }
                Some(crate::embedding::EmbeddingSource::GeneratedColumn { provider }) => {
                    if provider.is_empty() {
                        return Err(MongrelError::Schema(format!(
                            "legacy generated embedding column '{}' requires a provider identity",
                            column.name
                        )));
                    }
                }
                Some(crate::embedding::EmbeddingSource::ConfiguredModel {
                    provider_id,
                    model_id,
                    model_version,
                }) => {
                    if provider_id.is_empty() || model_id.is_empty() || model_version.is_empty() {
                        return Err(MongrelError::Schema(format!(
                            "embedding column '{}' requires provider, model, and version identities",
                            column.name
                        )));
                    }
                }
                Some(crate::embedding::EmbeddingSource::GeneratedColumnSpec { spec }) => {
                    if spec.provider_id.is_empty()
                        || spec.model_id.is_empty()
                        || spec.model_version.is_empty()
                        || spec.source_columns.is_empty()
                    {
                        return Err(MongrelError::Schema(format!(
                            "generated embedding column '{}' has incomplete identity or sources",
                            column.name
                        )));
                    }
                    if spec.dimension != dim {
                        return Err(MongrelError::Schema(format!(
                            "generated embedding column '{}' dimension {} does not match column dimension {}",
                            column.name, spec.dimension, dim
                        )));
                    }
                    let mut sources = std::collections::HashSet::new();
                    for source_id in &spec.source_columns {
                        if *source_id == column.id
                            || !sources.insert(*source_id)
                            || !self.columns.iter().any(|source| source.id == *source_id)
                        {
                            return Err(MongrelError::Schema(format!(
                                "generated embedding column '{}' has invalid source column {}",
                                column.name, source_id
                            )));
                        }
                    }
                }
            }
        }
        for index in &self.indexes {
            let column = self
                .columns
                .iter()
                .find(|column| column.id == index.column_id)
                .ok_or_else(|| {
                    MongrelError::Schema(format!(
                        "index '{}' references unknown column {}",
                        index.name, index.column_id
                    ))
                })?;
            let expected = match index.kind {
                IndexKind::Ann => Some("Embedding"),
                IndexKind::Sparse | IndexKind::MinHash | IndexKind::FmIndex => Some("Bytes"),
                _ => None,
            };
            if let Some(expected) = expected {
                let valid = match index.kind {
                    IndexKind::Ann => matches!(column.ty, TypeId::Embedding { .. }),
                    _ => column.ty == TypeId::Bytes,
                };
                if !valid {
                    return Err(MongrelError::Schema(format!(
                        "{:?} index '{}' requires a {expected} column",
                        index.kind, index.name
                    )));
                }
                if self
                    .indexes
                    .iter()
                    .filter(|other| {
                        other.column_id == index.column_id
                            && matches!(
                                other.kind,
                                IndexKind::Ann
                                    | IndexKind::Sparse
                                    | IndexKind::MinHash
                                    | IndexKind::FmIndex
                            )
                    })
                    .count()
                    > 1
                {
                    return Err(MongrelError::Schema(format!(
                        "column '{}' may have only one ANN, Sparse, MinHash, or FM representation index",
                        column.name
                    )));
                }
            }
        }
        Ok(())
    }

    pub fn validate_values(&self, columns: &[(u16, Value)]) -> Result<()> {
        self.validate_not_null(columns)?;
        for (column_id, value) in columns {
            let Some(column) = self.columns.iter().find(|column| column.id == *column_id) else {
                return Err(MongrelError::ColumnNotFound(column_id.to_string()));
            };
            if !value_matches_type(value, column.ty.clone()) {
                return Err(MongrelError::InvalidArgument(format!(
                    "column '{}' ({}) value {value:?} does not match type {:?}",
                    column.name, column.id, column.ty
                )));
            }
            let representation = self
                .indexes
                .iter()
                .find(|index| {
                    index.column_id == *column_id
                        && matches!(
                            index.kind,
                            IndexKind::Sparse | IndexKind::MinHash | IndexKind::FmIndex
                        )
                })
                .map(|index| index.kind);
            match representation {
                Some(IndexKind::Sparse) => match value {
                    Value::Null if column.flags.contains(ColumnFlags::NULLABLE) => {}
                    Value::Bytes(bytes) => {
                        let terms: Vec<(u32, f32)> = bincode::deserialize(bytes).map_err(|_| {
                            MongrelError::InvalidArgument(format!(
                                "sparse column '{}' requires an encoded sparse vector",
                                column.name
                            ))
                        })?;
                        if terms.is_empty() || terms.iter().any(|(_, weight)| !weight.is_finite()) {
                            return Err(MongrelError::InvalidArgument(format!(
                                "sparse column '{}' must be non-empty with finite weights",
                                column.name
                            )));
                        }
                    }
                    _ => {
                        return Err(MongrelError::InvalidArgument(format!(
                            "sparse column '{}' requires bytes or NULL",
                            column.name
                        )));
                    }
                },
                Some(IndexKind::MinHash) => match value {
                    Value::Null if column.flags.contains(ColumnFlags::NULLABLE) => {}
                    Value::Bytes(bytes) => {
                        let members: serde_json::Value =
                            serde_json::from_slice(bytes).map_err(|_| {
                                MongrelError::InvalidArgument(format!(
                                    "MinHash column '{}' requires a JSON array",
                                    column.name
                                ))
                            })?;
                        let serde_json::Value::Array(members) = members else {
                            return Err(MongrelError::InvalidArgument(format!(
                                "MinHash column '{}' requires a JSON array",
                                column.name
                            )));
                        };
                        if members.iter().any(|member| {
                            !matches!(
                                member,
                                serde_json::Value::String(_)
                                    | serde_json::Value::Number(_)
                                    | serde_json::Value::Bool(_)
                            )
                        }) {
                            return Err(MongrelError::InvalidArgument(format!(
                                "MinHash column '{}' members must be scalar",
                                column.name
                            )));
                        }
                    }
                    _ => {
                        return Err(MongrelError::InvalidArgument(format!(
                            "MinHash column '{}' requires bytes or NULL",
                            column.name
                        )));
                    }
                },
                Some(IndexKind::FmIndex) => match value {
                    Value::Null if column.flags.contains(ColumnFlags::NULLABLE) => {}
                    Value::Bytes(_) => {}
                    _ => {
                        return Err(MongrelError::InvalidArgument(format!(
                            "FM text column '{}' requires bytes or NULL",
                            column.name
                        )));
                    }
                },
                _ => {}
            }
            if let TypeId::Embedding { dim } = &column.ty {
                let Some(values) = value.as_embedding() else {
                    if matches!(value, Value::Null) {
                        continue;
                    }
                    return Err(MongrelError::InvalidArgument(format!(
                        "embedding column '{}' requires an embedding value",
                        column.name
                    )));
                };
                if values.len() != *dim as usize {
                    return Err(MongrelError::InvalidArgument(format!(
                        "embedding column '{}' dimension must be {}, got {}",
                        column.name,
                        dim,
                        values.len()
                    )));
                }
                if values.iter().any(|value| !value.is_finite()) {
                    return Err(MongrelError::InvalidArgument(format!(
                        "embedding column '{}' values must be finite",
                        column.name
                    )));
                }
            }
        }
        Ok(())
    }

    /// Validate a durable row against the current schema while honoring a
    /// later schema generation's declared default for a previously omitted or
    /// nullable cell. This is validation-only: dynamic defaults use a
    /// type-correct sentinel and are never written back during recovery.
    pub(crate) fn validate_persisted_values(&self, columns: &[(u16, Value)]) -> Result<()> {
        let mut resolved = columns.to_vec();
        for column in &self.columns {
            if column.flags.contains(ColumnFlags::NULLABLE)
                || column.flags.contains(ColumnFlags::AUTO_INCREMENT)
            {
                continue;
            }
            let position = resolved.iter().position(|(id, _)| *id == column.id);
            let missing = position
                .map(|index| matches!(resolved[index].1, Value::Null))
                .unwrap_or(true);
            if !missing {
                continue;
            }
            let Some(default) = &column.default_value else {
                continue;
            };
            let value = match default {
                DefaultExpr::Static(value) => value.clone(),
                DefaultExpr::Now => match column.ty {
                    TypeId::Bytes => Value::Bytes(Vec::new()),
                    TypeId::TimestampNanos | TypeId::Date64 => Value::Int64(0),
                    _ => unreachable!("validated NOW() default has a temporal/bytes type"),
                },
                DefaultExpr::Uuid => match column.ty {
                    TypeId::Uuid => Value::Uuid([0; 16]),
                    TypeId::Bytes => Value::Bytes(vec![0; 16]),
                    _ => unreachable!("validated UUID() default has a uuid/bytes type"),
                },
            };
            match position {
                Some(index) => resolved[index].1 = value,
                None => resolved.push((column.id, value)),
            }
        }
        self.validate_values(&resolved)
    }

    /// Validate row-level type constraints owned directly by the schema.
    /// Non-null columns must be present, and enum values must belong to their
    /// declared variant set. AUTO_INCREMENT columns may be omitted because the
    /// engine fills them before validation.
    pub fn validate_not_null(&self, columns: &[(u16, Value)]) -> Result<()> {
        // Rows are short sparse `(id, value)` lists; a linear probe beats
        // materializing a HashMap (and cloning every Value) per row.
        let at = |id: u16| columns.iter().find(|(c, _)| *c == id).map(|(_, v)| v);
        for col in &self.columns {
            if !col.flags.contains(ColumnFlags::NULLABLE) {
                // The engine supplies the AUTO_INCREMENT value, so its absence is
                // legal at this layer (filled in upstream of validation).
                if col.flags.contains(ColumnFlags::AUTO_INCREMENT) {
                    match at(col.id) {
                        None | Some(Value::Null) => continue,
                        Some(_) => {}
                    }
                }
                match at(col.id) {
                    None => {
                        return Err(MongrelError::InvalidArgument(format!(
                            "column '{}' ({}) is NOT NULL but was omitted",
                            col.name, col.id
                        )));
                    }
                    Some(Value::Null) => {
                        return Err(MongrelError::InvalidArgument(format!(
                            "column '{}' ({}) is NOT NULL but got NULL",
                            col.name, col.id
                        )));
                    }
                    Some(_) => {}
                }
            }
            if let TypeId::Enum { variants } = &col.ty {
                match at(col.id) {
                    None | Some(Value::Null) => {}
                    Some(Value::Bytes(value))
                        if variants
                            .iter()
                            .any(|variant| variant.as_bytes() == value.as_slice()) => {}
                    Some(Value::Bytes(value)) => {
                        return Err(MongrelError::InvalidArgument(format!(
                            "column '{}' ({}) enum value {:?} is not one of {:?}",
                            col.name,
                            col.id,
                            String::from_utf8_lossy(value),
                            variants
                        )));
                    }
                    Some(value) => {
                        return Err(MongrelError::InvalidArgument(format!(
                            "column '{}' ({}) enum requires a string/bytes value, got {value:?}",
                            col.name, col.id
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Enforce the `AUTO_INCREMENT` column contract: at most one such column,
    /// and it must be a non-nullable `Int64` primary key. Called at table
    /// creation time so an invalid schema never reaches the insert path.
    pub fn validate_auto_increment(&self) -> Result<()> {
        const ALLOWED_FLAGS: u32 = ColumnFlags::NULLABLE
            | ColumnFlags::PRIMARY_KEY
            | ColumnFlags::ENCRYPTED
            | ColumnFlags::ENCRYPTED_INDEXABLE
            | ColumnFlags::EMBEDDING_BINARY_QUANTIZED
            | ColumnFlags::AUTO_INCREMENT;
        const FIRST_RESERVED_COLUMN_ID: u16 = 0xFFFC;
        let mut ids = std::collections::HashSet::new();
        let mut names = std::collections::HashSet::new();
        let mut primary_keys = 0_u8;
        let mut seen: Option<&ColumnDef> = None;
        for col in &self.columns {
            if col.id >= FIRST_RESERVED_COLUMN_ID
                || col.name.is_empty()
                || col.flags.bits() & !ALLOWED_FLAGS != 0
                || !ids.insert(col.id)
                || !names.insert(col.name.as_str())
            {
                return Err(MongrelError::Schema(format!(
                    "column {:?} has a reserved/duplicate identity or unknown flags",
                    col.name
                )));
            }
            if col.flags.contains(ColumnFlags::PRIMARY_KEY) {
                primary_keys = primary_keys.saturating_add(1);
                if primary_keys > 1 {
                    return Err(MongrelError::Schema(
                        "schema may contain at most one primary key column".into(),
                    ));
                }
            }
            if !col.flags.contains(ColumnFlags::AUTO_INCREMENT) {
                continue;
            }
            if let Some(prev) = seen {
                return Err(MongrelError::Schema(format!(
                    "AUTO_INCREMENT may be set on at most one column; '{}' and '{}' both carry it",
                    prev.name, col.name
                )));
            }
            if col.ty != TypeId::Int64 {
                return Err(MongrelError::Schema(format!(
                    "AUTO_INCREMENT column '{}' must be Int64, is {:?}",
                    col.name, col.ty
                )));
            }
            if !col.flags.contains(ColumnFlags::PRIMARY_KEY) {
                return Err(MongrelError::Schema(format!(
                    "AUTO_INCREMENT column '{}' must also be the primary key",
                    col.name
                )));
            }
            if col.flags.contains(ColumnFlags::NULLABLE) {
                return Err(MongrelError::Schema(format!(
                    "AUTO_INCREMENT column '{}' must not be nullable",
                    col.name
                )));
            }
            seen = Some(col);
        }
        Ok(())
    }

    /// The single `AUTO_INCREMENT` column, if any.
    pub fn auto_increment_column(&self) -> Option<&ColumnDef> {
        self.columns
            .iter()
            .find(|c| c.flags.contains(ColumnFlags::AUTO_INCREMENT))
    }

    /// Validate that every column carrying a `default_value` has a
    /// type-compatible expression. Called at table creation and ALTER COLUMN
    /// so an invalid default never reaches the insert path.
    pub fn validate_defaults(&self) -> Result<()> {
        for col in &self.columns {
            let Some(expr) = &col.default_value else {
                continue;
            };
            match expr {
                DefaultExpr::Static(v) => {
                    if !value_matches_type(v, col.ty.clone()) {
                        return Err(MongrelError::Schema(format!(
                            "DEFAULT value for column '{}' ({:?}) does not match type {:?}",
                            col.name, v, col.ty
                        )));
                    }
                }
                DefaultExpr::Now => {
                    if !matches!(
                        col.ty,
                        TypeId::Bytes | TypeId::TimestampNanos | TypeId::Date64
                    ) {
                        return Err(MongrelError::Schema(format!(
                            "DEFAULT NOW() on column '{}' requires Bytes/TimestampNanos/Date64, is {:?}",
                            col.name, col.ty
                        )));
                    }
                }
                DefaultExpr::Uuid => {
                    if !matches!(col.ty, TypeId::Uuid | TypeId::Bytes) {
                        return Err(MongrelError::Schema(format!(
                            "DEFAULT UUID() on column '{}' requires Uuid/Bytes, is {:?}",
                            col.name, col.ty
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}

/// Check that a [`Value`] is compatible with a [`TypeId`] for default-value
/// validation. More lenient than full type-checking: `Null` is universally
/// accepted (it means "DEFAULT NULL"), and `Bytes` covers UTF-8 string types.
pub(crate) fn value_matches_type(v: &Value, ty: TypeId) -> bool {
    matches!(
        (v, ty),
        (Value::Null, _)
            | (Value::Bool(_), TypeId::Bool)
            | (
                Value::Int64(_),
                TypeId::Int8 | TypeId::Int16 | TypeId::Int32 | TypeId::Int64
            )
            | (Value::Float64(_), TypeId::Float32 | TypeId::Float64)
            | (
                Value::Bytes(_),
                TypeId::Bytes
                    | TypeId::Json
                    | TypeId::Uuid
                    | TypeId::Date64
                    | TypeId::Time64
                    | TypeId::Enum { .. }
            )
            | (
                Value::Int64(_),
                TypeId::TimestampNanos | TypeId::Date32 | TypeId::Date64 | TypeId::Time64
            )
            | (Value::Uuid(_), TypeId::Uuid)
            | (Value::Decimal(_), TypeId::Decimal128 { .. })
            | (Value::Json(_), TypeId::Json)
            | (Value::Embedding(_), TypeId::Embedding { .. })
            | (Value::GeneratedEmbedding(_), TypeId::Embedding { .. })
            | (Value::Interval { .. }, TypeId::Interval)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_options_preserve_defaults_and_validate_bounds() {
        let defaults = IndexDef {
            name: "ann".into(),
            column_id: 1,
            kind: IndexKind::Ann,
            predicate: None,
            options: IndexOptions::default(),
        };
        assert!(defaults.validate_options().is_ok());
        let json = serde_json::to_string(&defaults).unwrap();
        let restored: IndexDef = serde_json::from_str(&json).unwrap();
        assert!(restored.options.ann.is_none());
        let legacy: IndexDef = serde_json::from_value(serde_json::json!({
            "name": "legacy_ann",
            "column_id": 1,
            "kind": "Ann"
        }))
        .unwrap();
        assert!(legacy.options.ann.is_none());

        let invalid = IndexDef {
            name: "minhash".into(),
            column_id: 2,
            kind: IndexKind::MinHash,
            predicate: None,
            options: IndexOptions {
                minhash: Some(MinHashOptions {
                    permutations: 127,
                    bands: 32,
                }),
                ..Default::default()
            },
        };
        assert!(invalid.validate_options().is_err());
    }

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

    fn col(id: u16, name: &str, ty: TypeId, flags: ColumnFlags) -> ColumnDef {
        ColumnDef {
            id,
            name: name.into(),
            ty,
            flags,
            default_value: None,
            embedding_source: None,
        }
    }

    #[test]
    fn auto_increment_validation_accepts_int64_pk() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col(
                0,
                "id",
                TypeId::Int64,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT),
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_auto_increment().is_ok());
        assert_eq!(s.auto_increment_column().unwrap().id, 0);
    }

    #[test]
    fn auto_increment_validation_rejects_non_pk() {
        let s = Schema {
            schema_id: 1,
            columns: vec![
                col(
                    0,
                    "id",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                ),
                col(
                    1,
                    "seq",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::AUTO_INCREMENT),
                ),
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_auto_increment().is_err());
    }

    #[test]
    fn auto_increment_validation_rejects_non_int64() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col(
                0,
                "id",
                TypeId::Bytes,
                ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT),
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_auto_increment().is_err());
    }

    #[test]
    fn auto_increment_validation_rejects_two() {
        let s = Schema {
            schema_id: 1,
            columns: vec![
                col(
                    0,
                    "id",
                    TypeId::Int64,
                    ColumnFlags::empty()
                        .with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT),
                ),
                col(
                    1,
                    "id2",
                    TypeId::Int64,
                    ColumnFlags::empty().with(ColumnFlags::AUTO_INCREMENT),
                ),
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_auto_increment().is_err());
    }

    #[test]
    fn auto_increment_exempt_from_not_null_when_omitted() {
        let s = Schema {
            schema_id: 1,
            columns: vec![
                col(
                    0,
                    "id",
                    TypeId::Int64,
                    ColumnFlags::empty()
                        .with(ColumnFlags::PRIMARY_KEY | ColumnFlags::AUTO_INCREMENT),
                ),
                col(1, "name", TypeId::Bytes, ColumnFlags::empty()),
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        // Omitting the auto-inc column must not trip NOT NULL.
        let cols = vec![(1u16, Value::Bytes(b"x".to_vec()))];
        assert!(s.validate_not_null(&cols).is_ok());
    }

    #[test]
    fn enum_membership_is_enforced_for_nullable_and_required_columns() {
        let variants: std::sync::Arc<[String]> =
            vec!["user".to_string(), "admin".to_string()].into();
        let required = Schema {
            columns: vec![col(
                1,
                "role",
                TypeId::Enum {
                    variants: variants.clone(),
                },
                ColumnFlags::empty(),
            )],
            ..Schema::default()
        };
        assert!(required
            .validate_not_null(&[(1, Value::Bytes(b"user".to_vec()))])
            .is_ok());
        assert!(required
            .validate_not_null(&[(1, Value::Bytes(b"owner".to_vec()))])
            .is_err());

        let nullable = Schema {
            columns: vec![col(
                1,
                "role",
                TypeId::Enum { variants },
                ColumnFlags::empty().with(ColumnFlags::NULLABLE),
            )],
            ..Schema::default()
        };
        assert!(nullable.validate_not_null(&[(1, Value::Null)]).is_ok());
        assert!(nullable
            .validate_not_null(&[(1, Value::Bytes(b"owner".to_vec()))])
            .is_err());
    }

    fn col_with_default(
        id: u16,
        name: &str,
        ty: TypeId,
        flags: ColumnFlags,
        dv: DefaultExpr,
    ) -> ColumnDef {
        ColumnDef {
            id,
            name: name.into(),
            ty,
            flags,
            default_value: Some(dv),
            embedding_source: None,
        }
    }

    #[test]
    fn validate_defaults_accepts_matching_static() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "active",
                TypeId::Bool,
                ColumnFlags::empty(),
                DefaultExpr::Static(Value::Bool(true)),
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_defaults().is_ok());
    }

    #[test]
    fn validate_defaults_rejects_mismatched_static() {
        let s = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "count",
                TypeId::Int64,
                ColumnFlags::empty(),
                DefaultExpr::Static(Value::Bytes(b"oops".to_vec())),
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(s.validate_defaults().is_err());
    }

    #[test]
    fn validate_defaults_now_requires_temporal_or_bytes() {
        let ok = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "ts",
                TypeId::Bytes,
                ColumnFlags::empty(),
                DefaultExpr::Now,
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(ok.validate_defaults().is_ok());

        let bad = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "ts",
                TypeId::Int64,
                ColumnFlags::empty(),
                DefaultExpr::Now,
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(bad.validate_defaults().is_err());
    }

    #[test]
    fn validate_defaults_uuid_requires_uuid_or_bytes() {
        let ok = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "id",
                TypeId::Uuid,
                ColumnFlags::empty(),
                DefaultExpr::Uuid,
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(ok.validate_defaults().is_ok());

        let bad = Schema {
            schema_id: 1,
            columns: vec![col_with_default(
                0,
                "id",
                TypeId::Bool,
                ColumnFlags::empty(),
                DefaultExpr::Uuid,
            )],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        assert!(bad.validate_defaults().is_err());
    }

    #[test]
    fn serde_roundtrip_column_def_with_default() {
        let c = col_with_default(
            0,
            "x",
            TypeId::Bytes,
            ColumnFlags::empty(),
            DefaultExpr::Static(Value::Bytes(b"hello".to_vec())),
        );
        let json = serde_json::to_string(&c).unwrap();
        let de: ColumnDef = serde_json::from_str(&json).unwrap();
        assert_eq!(c, de);
        // ColumnDef without default deserializes to None.
        let old_json = r#"{"id":0,"name":"y","ty":{"kind":"bytes"},"flags":{"bits":0}}"#;
        let old: ColumnDef = serde_json::from_str(old_json).unwrap();
        assert!(old.default_value.is_none());
    }
}
