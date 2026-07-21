//! C-facing schema definitions and a builder that accumulates columns /
//! indexes / constraints and produces a core [`Schema`].
//!
//! The C side describes a table with:
//!  - one or more [`mongreldb_column_def`] (column id, name, type, flags),
//!  - zero or more [`mongreldb_index_def`] (secondary indexes),
//!  - zero or more [`mongreldb_unique_constraint`] / [`mongreldb_foreign_key`].
//!
//! A [`SchemaBuilder`] collects them and [`SchemaBuilder::finish`] builds the
//! engine `Schema`. The opaque handle `mongreldb_schema_builder_t` /
//! `mongreldb_schema_t` lets a caller stage a schema incrementally and then
//! hand the built schema to `mongreldb_create_table`.

use crate::error::{clear, set_error_msg, ErrorCode};
use mongreldb_core::constraint::{FkAction, ForeignKey, TableConstraints, UniqueConstraint};
use mongreldb_core::embedding::EmbeddingSource;
use mongreldb_core::schema::{
    AnnOptions, AnnQuantization, ColumnDef, ColumnFlags, IndexDef, IndexKind, IndexOptions,
    LearnedRangeOptions, MinHashOptions, Schema, TypeId,
};
use std::ffi::CString;
use std::os::raw::{c_char, c_void};
use std::sync::Arc;

// ── handle aliases ────────────────────────────────────────────────────────

/// Opaque schema-builder handle. Built up via the `mongreldb_schema_*` FFI
/// functions and finalized via `mongreldb_schema_build`.
pub type mongreldb_schema_builder_t = *mut c_void;

/// Opaque built-schema handle (returned by `mongreldb_schema_build`). Consumed
/// by `mongreldb_create_table` or freed by `mongreldb_schema_free`.
pub type mongreldb_schema_t = *mut c_void;

// ── enums ─────────────────────────────────────────────────────────────────

/// Discriminant identifying a column's logical type. The flat variants mirror
/// the common engine types; the side fields (`embedding_dim`,
/// `decimal_precision`, `decimal_scale`, `enum_variants`) carry the
/// parameterized-type metadata that `TypeId::Embedding` / `Decimal128` / `Enum`
/// need.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mongreldb_type_id {
    Bool = 0,
    Int8 = 1,
    Int16 = 2,
    Int32 = 3,
    Int64 = 4,
    UInt8 = 5,
    UInt16 = 6,
    UInt32 = 7,
    UInt64 = 8,
    Float32 = 9,
    Float64 = 10,
    TimestampNanos = 11,
    Date32 = 12,
    Date64 = 13,
    Time64 = 14,
    Interval = 15,
    Uuid = 16,
    Json = 17,
    Array = 18,
    Bytes = 19,
    Embedding = 20,
    Decimal128 = 21,
    Enum = 22,
}

/// Secondary index kind. The primary-key index is implicit and never listed.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mongreldb_index_kind {
    Bitmap = 0,
    FmIndex = 1,
    Ann = 2,
    LearnedRange = 3,
    MinHash = 4,
    Sparse = 5,
}

/// ANN vector representation and distance semantics.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mongreldb_ann_quantization {
    BinarySign = 0,
    Dense = 1,
}

/// ON DELETE action for a foreign key.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mongreldb_fk_action {
    Restrict = 0,
    Cascade = 1,
    SetNull = 2,
}

// ── structs ───────────────────────────────────────────────────────────────

/// A borrowed byte slice for column-id arrays (unique constraints / FKs).
#[repr(C)]
pub struct U16Slice {
    pub data: *const u16,
    pub len: usize,
}

impl Default for U16Slice {
    fn default() -> Self {
        Self {
            data: std::ptr::null(),
            len: 0,
        }
    }
}

/// A borrowed array of NUL-terminated C strings used for ENUM variants.
#[repr(C)]
pub struct StringArray {
    pub items: *const *const c_char,
    pub len: usize,
}

impl Default for StringArray {
    fn default() -> Self {
        Self {
            items: std::ptr::null(),
            len: 0,
        }
    }
}

/// One column definition. `flags` is a bitmask of `mongreldb_column_flags_*`
/// constants (below). The side fields (`embedding_dim`, `decimal_precision`,
/// `decimal_scale`, `enum_variants`) are read only when `ty` selects the
/// matching parameterized type.
#[repr(C)]
pub struct mongreldb_column_def {
    pub id: u16,
    pub name: *const c_char,
    /// C ABI integer. Invalid values are rejected by the builder.
    pub ty: i32,
    pub flags: u32,
    /// Required when `ty == Embedding`.
    pub embedding_dim: u32,
    /// Required when `ty == Decimal128`.
    pub decimal_precision: u8,
    /// Required when `ty == Decimal128`.
    pub decimal_scale: i8,
    /// Required when `ty == Enum`.
    pub enum_variants: StringArray,
}

// Column flag bitmask constants (mirror `ColumnFlags`).
/// Allow NULL values.
pub const MONGRELDB_COL_NULLABLE: u32 = ColumnFlags::NULLABLE;
/// This column is the (single-column) primary key.
pub const MONGRELDB_COL_PRIMARY_KEY: u32 = ColumnFlags::PRIMARY_KEY;
/// Encrypt this column's page payload at rest.
pub const MONGRELDB_COL_ENCRYPTED: u32 = ColumnFlags::ENCRYPTED;
/// Encrypt but keep queryable via deterministic/order-preserving tokens.
pub const MONGRELDB_COL_ENCRYPTED_INDEXABLE: u32 = ColumnFlags::ENCRYPTED_INDEXABLE;
/// Binary-quantize an embedding column.
pub const MONGRELDB_COL_EMBEDDING_BINARY_QUANTIZED: u32 = ColumnFlags::EMBEDDING_BINARY_QUANTIZED;
/// Engine-managed monotonic identity allocator (Int64 PK only).
pub const MONGRELDB_COL_AUTO_INCREMENT: u32 = ColumnFlags::AUTO_INCREMENT;

/// One secondary index definition.
#[repr(C)]
pub struct mongreldb_index_def {
    pub name: *const c_char,
    pub column_id: u16,
    /// C ABI integer. Invalid values are rejected by the builder.
    pub kind: i32,
}

/// Versioned secondary-index options. Zero-valued numeric fields select the
/// engine default. `predicate` may be NULL for a full index.
#[repr(C)]
pub struct mongreldb_index_options_v1 {
    pub struct_size: usize,
    pub version: u32,
    pub predicate: *const c_char,
    pub ann_m: usize,
    pub ann_ef_construction: usize,
    pub ann_ef_search: usize,
    /// 0 = BinarySign, 1 = Dense cosine.
    pub ann_quantization: i32,
    pub minhash_permutations: usize,
    pub minhash_bands: usize,
    pub learned_range_epsilon: usize,
}

/// A multi-column uniqueness constraint.
#[repr(C)]
pub struct mongreldb_unique_constraint {
    pub id: u16,
    pub name: *const c_char,
    pub columns: U16Slice,
}

/// A foreign-key reference.
#[repr(C)]
pub struct mongreldb_foreign_key {
    pub id: u16,
    pub name: *const c_char,
    pub columns: U16Slice,
    pub ref_table: *const c_char,
    pub ref_columns: U16Slice,
    /// C ABI integers. Invalid values are rejected by the builder.
    pub on_delete: i32,
    pub on_update: i32,
}

// ── type/flag resolution helpers ──────────────────────────────────────────

fn cstr_to_string_lossy(ptr: *const c_char, what: &str) -> Result<String, ErrorCode> {
    if ptr.is_null() {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("{what} must not be null"),
        ));
    }
    // SAFETY: caller guarantees a valid NUL-terminated C string.
    let bytes = unsafe { std::ffi::CStr::from_ptr(ptr).to_bytes() };
    std::str::from_utf8(bytes).map(String::from).map_err(|_| {
        set_error_msg(
            ErrorCode::InvalidArgument,
            format!("{what} was not valid UTF-8"),
        )
    })
}

/// Build a core [`TypeId`] from a C type id + side fields.
pub fn type_id_from_c(
    ty: i32,
    embedding_dim: u32,
    decimal_precision: u8,
    decimal_scale: i8,
    enum_variants: &StringArray,
) -> Result<TypeId, ErrorCode> {
    Ok(match ty {
        0 => TypeId::Bool,
        1 => TypeId::Int8,
        2 => TypeId::Int16,
        3 => TypeId::Int32,
        4 => TypeId::Int64,
        5 => TypeId::UInt8,
        6 => TypeId::UInt16,
        7 => TypeId::UInt32,
        8 => TypeId::UInt64,
        9 => TypeId::Float32,
        10 => TypeId::Float64,
        11 => TypeId::TimestampNanos,
        12 => TypeId::Date32,
        13 => TypeId::Date64,
        14 => TypeId::Time64,
        15 => TypeId::Interval,
        16 => TypeId::Uuid,
        17 => TypeId::Json,
        18 => TypeId::Array { element_type: 0 },
        19 => TypeId::Bytes,
        20 => TypeId::Embedding { dim: embedding_dim },
        21 => TypeId::Decimal128 {
            precision: decimal_precision,
            scale: decimal_scale,
        },
        22 => {
            let variants = string_array_to_vec(enum_variants)?;
            TypeId::Enum {
                variants: Arc::from(variants),
            }
        }
        _ => {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                format!("invalid type id {ty}"),
            ));
        }
    })
}

fn index_kind_from_c(kind: i32) -> Result<IndexKind, ErrorCode> {
    match kind {
        0 => Ok(IndexKind::Bitmap),
        1 => Ok(IndexKind::FmIndex),
        2 => Ok(IndexKind::Ann),
        3 => Ok(IndexKind::LearnedRange),
        4 => Ok(IndexKind::MinHash),
        5 => Ok(IndexKind::Sparse),
        _ => Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("invalid index kind {kind}"),
        )),
    }
}

fn ann_quantization_from_c(value: i32) -> Result<AnnQuantization, ErrorCode> {
    match value {
        0 => Ok(AnnQuantization::BinarySign),
        1 => Ok(AnnQuantization::Dense),
        _ => Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("invalid ANN quantization {value}"),
        )),
    }
}

fn fk_action_from_c(action: i32) -> Result<FkAction, ErrorCode> {
    match action {
        0 => Ok(FkAction::Restrict),
        1 => Ok(FkAction::Cascade),
        2 => Ok(FkAction::SetNull),
        _ => Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("invalid foreign key action {action}"),
        )),
    }
}

/// Read a [`StringArray] into an owned `Vec<String>`.
pub fn string_array_to_vec(arr: &StringArray) -> Result<Vec<String>, ErrorCode> {
    if arr.items.is_null() || arr.len == 0 {
        return Ok(Vec::new());
    }
    // SAFETY: caller guarantees `items` holds `len` valid `*const c_char`.
    let ptrs = unsafe { std::slice::from_raw_parts(arr.items, arr.len) };
    let mut out = Vec::with_capacity(arr.len);
    for (i, p) in ptrs.iter().enumerate() {
        if p.is_null() {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                format!("enum_variants[{i}] is null"),
            ));
        }
        // SAFETY: each pointer is a valid NUL-terminated C string.
        let bytes = unsafe { std::ffi::CStr::from_ptr(*p).to_bytes() };
        let s = std::str::from_utf8(bytes).map_err(|_| {
            set_error_msg(
                ErrorCode::InvalidArgument,
                format!("enum_variants[{i}] is not valid UTF-8"),
            )
        })?;
        out.push(s.to_string());
    }
    Ok(out)
}

fn u16_slice_to_vec(slice: &U16Slice) -> Result<Vec<u16>, ErrorCode> {
    if slice.data.is_null() || slice.len == 0 {
        return Ok(Vec::new());
    }
    // SAFETY: caller guarantees `data` holds `len` `u16`s.
    Ok(unsafe { std::slice::from_raw_parts(slice.data, slice.len) }.to_vec())
}

// ── builder ───────────────────────────────────────────────────────────────

/// Accumulates columns / indexes / constraints and builds a core [`Schema`].
pub struct SchemaBuilder {
    columns: Vec<ColumnDef>,
    indexes: Vec<IndexDef>,
    uniques: Vec<UniqueConstraint>,
    foreign_keys: Vec<ForeignKey>,
    clustered: bool,
}

impl SchemaBuilder {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            indexes: Vec::new(),
            uniques: Vec::new(),
            foreign_keys: Vec::new(),
            clustered: false,
        }
    }

    /// Append a column from a C `mongreldb_column_def`.
    pub fn add_column(&mut self, c: &mongreldb_column_def) -> Result<(), ErrorCode> {
        let name = cstr_to_string_lossy(c.name, "column name")?;
        let ty = type_id_from_c(
            c.ty,
            c.embedding_dim,
            c.decimal_precision,
            c.decimal_scale,
            &c.enum_variants,
        )?;
        let flags = flags_from_bits(c.flags);
        self.columns.push(ColumnDef {
            id: c.id,
            name,
            ty,
            flags,
            default_value: None,
            embedding_source: None,
        });
        Ok(())
    }

    /// Set the portable embedding source for one embedding column.
    pub fn set_embedding_source_json(
        &mut self,
        column_id: u16,
        source_json: *const c_char,
    ) -> Result<(), ErrorCode> {
        let source_json = cstr_to_string_lossy(source_json, "embedding source JSON")?;
        let source: EmbeddingSource = serde_json::from_str(&source_json).map_err(|error| {
            set_error_msg(
                ErrorCode::InvalidArgument,
                format!("invalid embedding source JSON: {error}"),
            )
        })?;
        let column = self
            .columns
            .iter_mut()
            .find(|column| column.id == column_id)
            .ok_or_else(|| {
                set_error_msg(
                    ErrorCode::InvalidArgument,
                    format!("embedding source column {column_id} does not exist"),
                )
            })?;
        if !matches!(column.ty, TypeId::Embedding { .. }) {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                format!("embedding source column {column_id} is not an embedding"),
            ));
        }
        column.embedding_source = Some(source);
        Ok(())
    }

    /// Append a secondary index.
    pub fn add_index(&mut self, i: &mongreldb_index_def) -> Result<(), ErrorCode> {
        self.add_index_with_options(i, None)
    }

    /// Append a secondary index with versioned kind-specific options.
    pub fn add_index_with_options(
        &mut self,
        i: &mongreldb_index_def,
        options: Option<&mongreldb_index_options_v1>,
    ) -> Result<(), ErrorCode> {
        let name = cstr_to_string_lossy(i.name, "index name")?;
        let kind = index_kind_from_c(i.kind)?;
        let mut index_options = IndexOptions::default();
        let predicate = if let Some(options) = options {
            if options.struct_size != std::mem::size_of::<mongreldb_index_options_v1>()
                || options.version != 1
            {
                return Err(set_error_msg(
                    ErrorCode::InvalidArgument,
                    "invalid mongreldb_index_options_v1 size or version",
                ));
            }
            match kind {
                IndexKind::Ann => {
                    let defaults = AnnOptions::default();
                    index_options.ann = Some(AnnOptions {
                        m: if options.ann_m == 0 {
                            defaults.m
                        } else {
                            options.ann_m
                        },
                        ef_construction: if options.ann_ef_construction == 0 {
                            defaults.ef_construction
                        } else {
                            options.ann_ef_construction
                        },
                        ef_search: if options.ann_ef_search == 0 {
                            defaults.ef_search
                        } else {
                            options.ann_ef_search
                        },
                        quantization: ann_quantization_from_c(options.ann_quantization)?,
                    });
                }
                IndexKind::MinHash => {
                    let defaults = MinHashOptions::default();
                    index_options.minhash = Some(MinHashOptions {
                        permutations: if options.minhash_permutations == 0 {
                            defaults.permutations
                        } else {
                            options.minhash_permutations
                        },
                        bands: if options.minhash_bands == 0 {
                            defaults.bands
                        } else {
                            options.minhash_bands
                        },
                    });
                }
                IndexKind::LearnedRange => {
                    let defaults = LearnedRangeOptions::default();
                    index_options.learned_range = Some(LearnedRangeOptions {
                        epsilon: if options.learned_range_epsilon == 0 {
                            defaults.epsilon
                        } else {
                            options.learned_range_epsilon
                        },
                    });
                }
                IndexKind::Bitmap | IndexKind::FmIndex | IndexKind::Sparse => {}
            }
            if options.predicate.is_null() {
                None
            } else {
                Some(cstr_to_string_lossy(options.predicate, "index predicate")?)
            }
        } else {
            None
        };
        let index = IndexDef {
            name,
            column_id: i.column_id,
            kind,
            predicate,
            options: index_options,
        };
        index.validate_options().map_err(|error| {
            set_error_msg(
                ErrorCode::InvalidArgument,
                format!("invalid index options: {error}"),
            )
        })?;
        self.indexes.push(index);
        Ok(())
    }

    /// Append a multi-column uniqueness constraint.
    pub fn add_unique(&mut self, u: &mongreldb_unique_constraint) -> Result<(), ErrorCode> {
        let name = cstr_to_string_lossy(u.name, "unique constraint name")?;
        let columns = u16_slice_to_vec(&u.columns)?;
        self.uniques.push(UniqueConstraint {
            id: u.id,
            name,
            columns,
        });
        Ok(())
    }

    /// Append a foreign key.
    pub fn add_foreign_key(&mut self, fk: &mongreldb_foreign_key) -> Result<(), ErrorCode> {
        let name = cstr_to_string_lossy(fk.name, "foreign key name")?;
        let ref_table = cstr_to_string_lossy(fk.ref_table, "foreign key ref_table")?;
        let columns = u16_slice_to_vec(&fk.columns)?;
        let ref_columns = u16_slice_to_vec(&fk.ref_columns)?;
        self.foreign_keys.push(ForeignKey {
            id: fk.id,
            name,
            columns,
            ref_table,
            ref_columns,
            on_delete: fk_action_from_c(fk.on_delete)?,
            on_update: fk_action_from_c(fk.on_update)?,
        });
        Ok(())
    }

    /// Mark the table as clustered on its primary key.
    pub fn set_clustered(&mut self, clustered: bool) {
        self.clustered = clustered;
    }

    /// Build the core [`Schema`] from the accumulated definitions.
    pub fn finish(&self) -> Schema {
        Schema {
            schema_id: 1,
            columns: self.columns.clone(),
            indexes: self.indexes.clone(),
            colocation: Vec::new(),
            constraints: TableConstraints {
                uniques: self.uniques.clone(),
                foreign_keys: self.foreign_keys.clone(),
                checks: Vec::new(),
            },
            clustered: self.clustered,
        }
    }
}

impl Default for SchemaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

/// Reconstruct [`ColumnFlags`] from a raw bitmask, copying only the known
/// bits (`ColumnFlags` has no `from_bits_truncate` of its own).
pub fn flags_from_bits(bits: u32) -> ColumnFlags {
    let mut f = ColumnFlags::empty();
    if bits & ColumnFlags::NULLABLE != 0 {
        f = f.with(ColumnFlags::NULLABLE);
    }
    if bits & ColumnFlags::PRIMARY_KEY != 0 {
        f = f.with(ColumnFlags::PRIMARY_KEY);
    }
    if bits & ColumnFlags::ENCRYPTED != 0 {
        f = f.with(ColumnFlags::ENCRYPTED);
    }
    if bits & ColumnFlags::ENCRYPTED_INDEXABLE != 0 {
        f = f.with(ColumnFlags::ENCRYPTED_INDEXABLE);
    }
    if bits & ColumnFlags::EMBEDDING_BINARY_QUANTIZED != 0 {
        f = f.with(ColumnFlags::EMBEDDING_BINARY_QUANTIZED);
    }
    if bits & ColumnFlags::AUTO_INCREMENT != 0 {
        f = f.with(ColumnFlags::AUTO_INCREMENT);
    }
    f
}

// ── FFI lifecycle ─────────────────────────────────────────────────────────

/// Create a fresh schema builder. Returns a handle or null on error.
#[no_mangle]
pub extern "C" fn mongreldb_schema_begin() -> mongreldb_schema_builder_t {
    clear();
    let b = Box::new(SchemaBuilder::new());
    Box::into_raw(b) as mongreldb_schema_builder_t
}

/// Add a column to a builder. Returns 0 on success, negative error code
/// otherwise.
///
/// # Safety
/// `builder` must be a valid pointer returned by [`mongreldb_schema_begin`].
/// `col` must be a valid pointer to a [`mongreldb_column_def`] whose strings
/// outlive the call.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_add_column(
    builder: mongreldb_schema_builder_t,
    col: *const mongreldb_column_def,
) -> i32 {
    clear();
    let Some(b) = as_builder_mut(builder) else {
        return set_error_msg(ErrorCode::InvalidArgument, "schema builder handle is null")
            .as_return();
    };
    if col.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "column def is null").as_return();
    }
    let c = &*col;
    match b.add_column(c) {
        Ok(()) => 0,
        Err(code) => code.as_return(),
    }
}

/// Set a column's portable embedding-source descriptor from JSON.
///
/// # Safety
/// `builder` must be valid and `source_json` must be valid NUL-terminated UTF-8.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_set_embedding_source_json(
    builder: mongreldb_schema_builder_t,
    column_id: u16,
    source_json: *const c_char,
) -> i32 {
    clear();
    let Some(builder) = as_builder_mut(builder) else {
        return set_error_msg(ErrorCode::InvalidArgument, "schema builder handle is null")
            .as_return();
    };
    match builder.set_embedding_source_json(column_id, source_json) {
        Ok(()) => 0,
        Err(error) => error.as_return(),
    }
}

/// Add a secondary index to a builder.
///
/// # Safety
/// `builder` must be valid; `idx` must point to a valid [`mongreldb_index_def`].
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_add_index(
    builder: mongreldb_schema_builder_t,
    idx: *const mongreldb_index_def,
) -> i32 {
    clear();
    let Some(b) = as_builder_mut(builder) else {
        return set_error_msg(ErrorCode::InvalidArgument, "schema builder handle is null")
            .as_return();
    };
    if idx.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "index def is null").as_return();
    }
    match b.add_index(&*idx) {
        Ok(()) => 0,
        Err(code) => code.as_return(),
    }
}

/// Add an index with versioned options without changing the legacy index ABI.
///
/// # Safety
/// `builder`, `idx`, and `options` must point to valid values. String pointers
/// must be valid NUL-terminated UTF-8 for the duration of the call.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_add_index_v2(
    builder: mongreldb_schema_builder_t,
    idx: *const mongreldb_index_def,
    options: *const mongreldb_index_options_v1,
) -> i32 {
    clear();
    let Some(builder) = as_builder_mut(builder) else {
        return set_error_msg(ErrorCode::InvalidArgument, "schema builder handle is null")
            .as_return();
    };
    if idx.is_null() || options.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "schema index argument is null")
            .as_return();
    }
    match builder.add_index_with_options(&*idx, Some(&*options)) {
        Ok(()) => 0,
        Err(error) => error.as_return(),
    }
}

/// Add a multi-column uniqueness constraint.
///
/// # Safety
/// `builder` must be valid; `u` must point to a valid
/// [`mongreldb_unique_constraint`].
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_add_unique(
    builder: mongreldb_schema_builder_t,
    u: *const mongreldb_unique_constraint,
) -> i32 {
    clear();
    let Some(b) = as_builder_mut(builder) else {
        return set_error_msg(ErrorCode::InvalidArgument, "schema builder handle is null")
            .as_return();
    };
    if u.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "unique constraint is null").as_return();
    }
    match b.add_unique(&*u) {
        Ok(()) => 0,
        Err(code) => code.as_return(),
    }
}

/// Add a foreign key.
///
/// # Safety
/// `builder` must be valid; `fk` must point to a valid [`mongreldb_foreign_key`].
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_add_foreign_key(
    builder: mongreldb_schema_builder_t,
    fk: *const mongreldb_foreign_key,
) -> i32 {
    clear();
    let Some(b) = as_builder_mut(builder) else {
        return set_error_msg(ErrorCode::InvalidArgument, "schema builder handle is null")
            .as_return();
    };
    if fk.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "foreign key is null").as_return();
    }
    match b.add_foreign_key(&*fk) {
        Ok(()) => 0,
        Err(code) => code.as_return(),
    }
}

/// Mark the table clustered on its primary key.
///
/// # Safety
/// `builder` must be valid.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_set_clustered(
    builder: mongreldb_schema_builder_t,
    clustered: u8,
) -> i32 {
    clear();
    let Some(b) = as_builder_mut(builder) else {
        return set_error_msg(ErrorCode::InvalidArgument, "schema builder handle is null")
            .as_return();
    };
    b.set_clustered(clustered != 0);
    0
}

/// Finalize a builder into a built schema handle. The caller still owns the
/// builder handle and must free it with [`mongreldb_schema_builder_free`].
///
/// # Safety
/// `builder` must be a valid pointer returned by [`mongreldb_schema_begin`].
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_build(
    builder: mongreldb_schema_builder_t,
) -> mongreldb_schema_t {
    clear();
    let Some(b) = as_builder_mut(builder) else {
        set_error_msg(ErrorCode::InvalidArgument, "schema builder handle is null");
        return std::ptr::null_mut();
    };
    let schema = b.finish();
    Box::into_raw(Box::new(schema)) as mongreldb_schema_t
}

/// Free a built schema handle. No-op on null.
///
/// # Safety
/// `schema` must be null or a pointer returned by [`mongreldb_schema_build`],
/// and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_free(schema: mongreldb_schema_t) {
    if schema.is_null() {
        return;
    }
    // SAFETY: upheld by caller.
    drop(Box::from_raw(schema as *mut Schema));
}

/// Free a schema-builder handle without building. No-op on null.
///
/// # Safety
/// `builder` must be null or a pointer returned by [`mongreldb_schema_begin`]
/// and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_schema_builder_free(builder: mongreldb_schema_builder_t) {
    if builder.is_null() {
        return;
    }
    // SAFETY: upheld by caller.
    drop(Box::from_raw(builder as *mut SchemaBuilder));
}

// SAFETY: cast a raw void pointer back to a mut SchemaBuilder reference iff
// non-null.
unsafe fn as_builder_mut(
    builder: mongreldb_schema_builder_t,
) -> Option<&'static mut SchemaBuilder> {
    if builder.is_null() {
        return None;
    }
    // SAFETY: caller guarantees the pointer originated from
    // `mongreldb_schema_begin` and is still live. The 'static lifetime is a
    // convenience lie scoped to the calling FFI function.
    Some(&mut *(builder as *mut SchemaBuilder))
}

/// Internal helper used by `database.rs` to take ownership of a built schema.
///
/// # Safety
/// `schema` must be a non-null pointer returned by [`mongreldb_schema_build`].
/// After this call the handle is consumed and must not be reused.
pub unsafe fn take_schema(schema: mongreldb_schema_t) -> Option<Schema> {
    if schema.is_null() {
        return None;
    }
    // SAFETY: upheld by caller.
    let boxed = Box::from_raw(schema as *mut Schema);
    Some(*boxed)
}

/// Re-export the CString type so other modules can construct owned names from
/// schemas without a second import.
#[allow(dead_code)]
pub(crate) fn own_cstring(s: &str) -> CString {
    CString::new(s).unwrap_or_else(|_| CString::new("invalid name").unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn versioned_index_options_and_embedding_source_round_trip() {
        let mut builder = SchemaBuilder::new();
        let column_name = CString::new("embedding").unwrap();
        builder
            .add_column(&mongreldb_column_def {
                id: 1,
                name: column_name.as_ptr(),
                ty: mongreldb_type_id::Embedding as i32,
                flags: 0,
                embedding_dim: 384,
                decimal_precision: 0,
                decimal_scale: 0,
                enum_variants: StringArray::default(),
            })
            .unwrap();

        let source = CString::new(
            r#"{"kind":"configured_model","provider_id":"local","model_id":"gte-small","model_version":"1"}"#,
        )
        .unwrap();
        builder
            .set_embedding_source_json(1, source.as_ptr())
            .unwrap();

        let index_name = CString::new("embedding_ann").unwrap();
        let predicate = CString::new("embedding IS NOT NULL").unwrap();
        builder
            .add_index_with_options(
                &mongreldb_index_def {
                    name: index_name.as_ptr(),
                    column_id: 1,
                    kind: mongreldb_index_kind::Ann as i32,
                },
                Some(&mongreldb_index_options_v1 {
                    struct_size: std::mem::size_of::<mongreldb_index_options_v1>(),
                    version: 1,
                    predicate: predicate.as_ptr(),
                    ann_m: 24,
                    ann_ef_construction: 96,
                    ann_ef_search: 48,
                    ann_quantization: 1,
                    minhash_permutations: 0,
                    minhash_bands: 0,
                    learned_range_epsilon: 0,
                }),
            )
            .unwrap();

        let schema = builder.finish();
        assert!(matches!(
            schema.columns[0].embedding_source,
            Some(EmbeddingSource::ConfiguredModel {
                ref provider_id,
                ref model_id,
                ref model_version,
            }) if provider_id == "local" && model_id == "gte-small" && model_version == "1"
        ));
        assert_eq!(
            schema.indexes[0].predicate.as_deref(),
            Some("embedding IS NOT NULL")
        );
        assert_eq!(
            schema.indexes[0].options.ann,
            Some(AnnOptions {
                m: 24,
                ef_construction: 96,
                ef_search: 48,
                quantization: AnnQuantization::Dense,
            })
        );
    }
}
