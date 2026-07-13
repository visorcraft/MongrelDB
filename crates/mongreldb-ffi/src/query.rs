//! C-facing query condition union and an `FFIQuery` builder that accumulates
//! conditions + optional projection + optional limit and produces a core
//! [`Query`].
//!
//! A `mongreldb_condition` mirrors all 13 [`Condition`] variants as a tagged
//! union. A [`FFIQuery`] is a conjunction (AND) of conditions, matching the
//! NAPI `query` semantics.

use crate::cstr::cstr_to_string;
use crate::error::{clear, set_error_msg, ErrorCode};
use crate::value::{ByteSlice, EmbeddingSlice};
use mongreldb_core::index::minhash_token_hash;
use mongreldb_core::query::{
    Condition, Query, MAX_FINAL_LIMIT, MAX_HARD_CONDITIONS, MAX_PROJECTION_COLUMNS,
    MAX_RETRIEVER_K, MAX_SET_MEMBERS, MAX_SPARSE_TERMS,
};
use std::os::raw::{c_char, c_void};

/// Opaque query handle.
pub type mongreldb_query_t = *mut c_void;

/// Discriminant for [`mongreldb_condition`]. Matches the 13 `Condition`
/// variants exposed by the engine query layer.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mongreldb_condition_kind {
    /// Primary-key exact match (encoded key bytes in `bytes`).
    Pk = 0,
    /// Low-cardinality equality via the bitmap index.
    BitmapEq = 1,
    /// Multi-value equality (bitmap union) — `IN (...)`.
    BitmapIn = 2,
    /// Anchored prefix match `LIKE 'prefix%'` on a Bytes column.
    BytesPrefix = 3,
    /// Semantic ANN over an embedding column.
    Ann = 4,
    /// Arbitrary substring via the FM index.
    FmContains = 5,
    /// Multi-segment FM intersection.
    FmContainsAll = 6,
    /// Inclusive integer range.
    RangeInt = 7,
    /// Floating-point range with per-bound inclusivity.
    RangeF64 = 8,
    /// SPLADE-style sparse retrieval.
    SparseMatch = 9,
    /// MinHash/LSH set similarity.
    MinHashSimilar = 10,
    /// `IS NULL`.
    IsNull = 11,
    /// `IS NOT NULL`.
    IsNotNull = 12,
}

/// A borrowed array of byte-slices (for `BitmapIn` / `FmContainsAll`).
#[repr(C)]
pub struct ByteSliceArray {
    pub items: *const ByteSlice,
    pub len: usize,
}

impl Default for ByteSliceArray {
    fn default() -> Self {
        Self {
            items: std::ptr::null(),
            len: 0,
        }
    }
}

/// A sparse `(token_id, weight)` pair crossing the ABI.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct SparseTerm {
    pub token: u32,
    pub weight: f32,
}

/// A borrowed array of sparse terms (for `SparseMatch`).
#[repr(C)]
pub struct SparseTermArray {
    pub items: *const SparseTerm,
    pub len: usize,
}

impl Default for SparseTermArray {
    fn default() -> Self {
        Self {
            items: std::ptr::null(),
            len: 0,
        }
    }
}

/// The C-tagged union payload for one condition. The C side sets `kind` and the
/// matching payload field(s); unused fields are ignored.
#[repr(C)]
pub struct mongreldb_condition {
    pub kind: mongreldb_condition_kind,
    /// Column id the condition applies to (ignored for `Pk`).
    pub column_id: u16,
    /// Integer range bounds (lo/hi). Used by `RangeInt` and `Pk` (int64 PK in
    /// `int64_lo`).
    pub int64_lo: i64,
    pub int64_hi: i64,
    /// Float range bounds + inclusivity flags. Used by `RangeF64`.
    pub float64_lo: f64,
    pub float64_hi: f64,
    /// 0=false/exclusive, nonzero=true/inclusive.
    pub lo_inclusive: u8,
    pub hi_inclusive: u8,
    /// `top_k` for `Ann` / `SparseMatch` / `MinHashSimilar`.
    pub k: u32,
    /// Single byte pattern (for `Pk`, `BitmapEq`, `FmContains`, `BytesPrefix`).
    pub bytes: ByteSlice,
    /// Multi-value byte patterns (for `BitmapIn`, `FmContainsAll`).
    pub byte_values: ByteSliceArray,
    /// Query embedding (for `Ann`).
    pub embedding: EmbeddingSlice,
    /// Sparse query terms (for `SparseMatch`).
    pub sparse: SparseTermArray,
    /// MinHash query members, as NUL-terminated strings (for `MinHashSimilar`).
    /// Each `*const c_char` in `minhash_members.items` is hashed via
    /// [`minhash_token_hash`].
    pub minhash_members: crate::value::MinHashMembers,
}

impl Default for mongreldb_condition {
    fn default() -> Self {
        Self {
            kind: mongreldb_condition_kind::Pk,
            column_id: 0,
            int64_lo: 0,
            int64_hi: 0,
            float64_lo: 0.0,
            float64_hi: 0.0,
            lo_inclusive: 0,
            hi_inclusive: 0,
            k: 0,
            bytes: ByteSlice::default(),
            byte_values: ByteSliceArray::default(),
            embedding: EmbeddingSlice::default(),
            sparse: SparseTermArray::default(),
            minhash_members: Default::default(),
        }
    }
}

/// Build a core [`Condition`] from a C condition. Mirrors the NAPI
/// `build_condition` logic.
///
/// # Safety
/// All pointers inside `c` must be valid for their lengths.
pub unsafe fn build_condition(c: &mongreldb_condition) -> Result<Condition, ErrorCode> {
    if matches!(
        c.kind,
        mongreldb_condition_kind::Ann
            | mongreldb_condition_kind::SparseMatch
            | mongreldb_condition_kind::MinHashSimilar
    ) && (c.k == 0 || c.k as usize > MAX_RETRIEVER_K)
    {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("condition k must be between 1 and {MAX_RETRIEVER_K}"),
        ));
    }
    if c.byte_values.len > MAX_SET_MEMBERS
        || c.minhash_members.len > MAX_SET_MEMBERS
        || c.sparse.len > MAX_SPARSE_TERMS
    {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            "condition cardinality exceeds the public query limit",
        ));
    }
    Ok(match c.kind {
        mongreldb_condition_kind::Pk => {
            let key = c.bytes.to_vec();
            Condition::Pk(key)
        }
        mongreldb_condition_kind::BitmapEq => Condition::BitmapEq {
            column_id: c.column_id,
            value: c.bytes.to_vec(),
        },
        mongreldb_condition_kind::BitmapIn => {
            let values = bytes_array_to_vec(&c.byte_values)?;
            Condition::BitmapIn {
                column_id: c.column_id,
                values,
            }
        }
        mongreldb_condition_kind::BytesPrefix => Condition::BytesPrefix {
            column_id: c.column_id,
            prefix: c.bytes.to_vec(),
        },
        mongreldb_condition_kind::Ann => {
            let query = c.embedding.to_vec();
            if query.is_empty() {
                return Err(set_error_msg(
                    ErrorCode::InvalidArgument,
                    "Ann condition requires a non-empty embedding",
                ));
            }
            Condition::Ann {
                column_id: c.column_id,
                query,
                k: c.k.max(1) as usize,
            }
        }
        mongreldb_condition_kind::FmContains => Condition::FmContains {
            column_id: c.column_id,
            pattern: c.bytes.to_vec(),
        },
        mongreldb_condition_kind::FmContainsAll => {
            let patterns = bytes_array_to_vec(&c.byte_values)?;
            Condition::FmContainsAll {
                column_id: c.column_id,
                patterns,
            }
        }
        mongreldb_condition_kind::RangeInt => Condition::Range {
            column_id: c.column_id,
            lo: c.int64_lo,
            hi: c.int64_hi,
        },
        mongreldb_condition_kind::RangeF64 => Condition::RangeF64 {
            column_id: c.column_id,
            lo: c.float64_lo,
            lo_inclusive: c.lo_inclusive != 0,
            hi: c.float64_hi,
            hi_inclusive: c.hi_inclusive != 0,
        },
        mongreldb_condition_kind::SparseMatch => {
            let query = sparse_array_to_vec(&c.sparse);
            if query.is_empty() {
                return Err(set_error_msg(
                    ErrorCode::InvalidArgument,
                    "SparseMatch requires sparse terms",
                ));
            }
            Condition::SparseMatch {
                column_id: c.column_id,
                query,
                k: c.k.max(1) as usize,
            }
        }
        mongreldb_condition_kind::MinHashSimilar => {
            let members = minhash_members_to_hashes(&c.minhash_members)?;
            Condition::MinHashSimilar {
                column_id: c.column_id,
                query: members,
                k: c.k.max(1) as usize,
            }
        }
        mongreldb_condition_kind::IsNull => Condition::IsNull {
            column_id: c.column_id,
        },
        mongreldb_condition_kind::IsNotNull => Condition::IsNotNull {
            column_id: c.column_id,
        },
    })
}

/// Read a [`ByteSliceArray`] into an owned `Vec<Vec<u8>>`.
///
/// # Safety
/// The array's pointer must be valid for `len` `ByteSlice`s, and each of those
/// must be valid for its own length.
pub unsafe fn bytes_array_to_vec(arr: &ByteSliceArray) -> Result<Vec<Vec<u8>>, ErrorCode> {
    if arr.items.is_null() || arr.len == 0 {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            "byte value array is empty",
        ));
    }
    let slices = std::slice::from_raw_parts(arr.items, arr.len);
    slices.iter().map(|s| Ok(s.to_vec())).collect()
}

/// Read a [`SparseTermArray`] into an owned `Vec<(u32, f32)>`.
///
/// # Safety
/// `items` must be valid for `len` `SparseTerm`s.
pub unsafe fn sparse_array_to_vec(arr: &SparseTermArray) -> Vec<(u32, f32)> {
    if arr.items.is_null() || arr.len == 0 {
        return Vec::new();
    }
    std::slice::from_raw_parts(arr.items, arr.len)
        .iter()
        .map(|t| (t.token, t.weight))
        .collect()
}

/// Hash the minhash member strings into `Vec<u64>`.
fn minhash_members_to_hashes(
    members: &crate::value::MinHashMembers,
) -> Result<Vec<u64>, ErrorCode> {
    if members.items.is_null() || members.len == 0 {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            "MinHashSimilar requires members",
        ));
    }
    // SAFETY: caller guarantees `items` holds `len` `*const c_char`.
    let ptrs = unsafe { std::slice::from_raw_parts(members.items, members.len) };
    let mut out = Vec::with_capacity(members.len);
    for (i, p) in ptrs.iter().enumerate() {
        if p.is_null() {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                format!("minhash_members[{i}] is null"),
            ));
        }
        let s = cstr_to_string(*p, "minhash member");
        out.push(minhash_token_hash(&s));
    }
    Ok(out)
}

// ── query builder ─────────────────────────────────────────────────────────

/// Accumulates conditions + optional projection + optional limit and builds a
/// core [`Query`]. Exposed to C as `mongreldb_query_t`.
pub struct FFIQuery {
    pub conditions: Vec<Condition>,
    pub projection: Option<Vec<u16>>,
    pub limit: Option<usize>,
}

impl FFIQuery {
    pub fn new() -> Self {
        Self {
            conditions: Vec::new(),
            projection: None,
            limit: Some(MAX_FINAL_LIMIT),
        }
    }

    /// Append a condition (conjunction).
    pub fn add(&mut self, c: Condition) {
        self.conditions.push(c);
    }

    /// Build the core query (conditions only — projection/limit are tracked
    /// separately for the table layer to apply).
    pub fn build(&self) -> Query {
        let mut q = Query::new();
        for c in &self.conditions {
            q = q.and(c.clone());
        }
        q.with_limit(self.limit.unwrap_or(MAX_FINAL_LIMIT))
    }
}

// ── FFI lifecycle ─────────────────────────────────────────────────────────

/// Hash one JSON scalar with the stable MinHash v1 contract.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_minhash_member_hash_v1_json(
    member: *const c_char,
    out_hash: *mut u64,
) -> i32 {
    clear();
    if member.is_null() || out_hash.is_null() {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            "member and out_hash must not be null",
        )
        .as_return();
    }
    let text = match std::ffi::CStr::from_ptr(member).to_str() {
        Ok(text) => text,
        Err(_) => {
            return set_error_msg(ErrorCode::InvalidArgument, "member is not valid UTF-8")
                .as_return()
        }
    };
    let value = match serde_json::from_str(text) {
        Ok(value) => value,
        Err(_) => {
            return set_error_msg(ErrorCode::InvalidArgument, "member is not valid JSON")
                .as_return()
        }
    };
    match mongreldb_core::index::minhash_member_hash_v1(&value) {
        Ok(hash) => {
            *out_hash = hash;
            0
        }
        Err(message) => set_error_msg(ErrorCode::InvalidArgument, message).as_return(),
    }
}

/// Begin a new query builder. Returns a handle.
#[no_mangle]
pub extern "C" fn mongreldb_query_begin() -> mongreldb_query_t {
    clear();
    Box::into_raw(Box::new(FFIQuery::new())) as mongreldb_query_t
}

/// Add a condition to a query builder (conjunction). Returns 0 on success.
///
/// # Safety
/// `q` must be a valid query handle; `cond` must point to a valid
/// [`mongreldb_condition`] whose internal pointers are valid for the call.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_query_add(
    q: mongreldb_query_t,
    cond: *const mongreldb_condition,
) -> i32 {
    clear();
    let Some(query) = as_query_mut(q) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if cond.is_null() {
        return set_error_msg(ErrorCode::InvalidArgument, "condition is null").as_return();
    }
    let c = &*cond;
    match build_condition(c) {
        Ok(core_c) => {
            if query.conditions.len() >= MAX_HARD_CONDITIONS {
                return set_error_msg(
                    ErrorCode::InvalidArgument,
                    format!("query exceeds {MAX_HARD_CONDITIONS} conditions"),
                )
                .as_return();
            }
            query.add(core_c);
            0
        }
        Err(code) => code.as_return(),
    }
}

/// Set the projection (column ids to return). Replaces any prior projection.
/// A null/empty slice clears the projection (return all columns).
///
/// # Safety
/// `q` must be valid; if non-null, `cols.data` must be valid for `cols.len`
/// `u16`s.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_query_set_projection(
    q: mongreldb_query_t,
    cols: *const u16,
    len: usize,
) -> i32 {
    clear();
    let Some(query) = as_query_mut(q) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if cols.is_null() || len == 0 {
        query.projection = None;
    } else {
        if len > MAX_PROJECTION_COLUMNS {
            return set_error_msg(
                ErrorCode::InvalidArgument,
                format!("projection exceeds {MAX_PROJECTION_COLUMNS} columns"),
            )
            .as_return();
        }
        // SAFETY: caller guarantees `cols` is valid for `len`.
        query.projection = Some(std::slice::from_raw_parts(cols, len).to_vec());
    }
    0
}

/// Set the result limit (max rows to return). `0` restores the safe default.
///
/// # Safety
/// `q` must be valid.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_query_set_limit(q: mongreldb_query_t, limit: u64) -> i32 {
    clear();
    let Some(query) = as_query_mut(q) else {
        return ErrorCode::InvalidArgument.as_return();
    };
    if limit > MAX_FINAL_LIMIT as u64 {
        return set_error_msg(
            ErrorCode::InvalidArgument,
            format!("limit exceeds {MAX_FINAL_LIMIT}"),
        )
        .as_return();
    }
    query.limit = Some(if limit == 0 {
        MAX_FINAL_LIMIT
    } else {
        limit as usize
    });
    0
}

/// Finalize the builder into a built query handle. The opaque handle already
/// holds the [`FFIQuery`] (conditions + projection + limit), so the same pointer
/// is returned, now semantically a built query.
///
/// # Safety
/// `q` must be a valid query handle returned by [`mongreldb_query_begin`] and
/// not yet freed.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_query_build(q: mongreldb_query_t) -> mongreldb_query_t {
    clear();
    if q.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "query handle is null");
        return std::ptr::null_mut();
    }
    // The builder is already in its final state (conditions/projection/limit
    // accumulated); just hand back the same pointer, now treated as built.
    q
}

/// Free a query handle (builder or built). No-op on null.
///
/// # Safety
/// `q` must be null or a pointer returned by [`mongreldb_query_begin`] (and
/// optionally passed through [`mongreldb_query_build`]), and not yet freed.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_query_free(q: mongreldb_query_t) {
    if q.is_null() {
        return;
    }
    // SAFETY: both builder and built-query states are `FFIQuery`, so this drop
    // is correct for either.
    drop(Box::from_raw(q as *mut FFIQuery));
}

/// Internal: borrow a query handle as a mut builder.
///
/// # Safety
/// `q` must be a valid, unconsumed builder handle.
unsafe fn as_query_mut(q: mongreldb_query_t) -> Option<&'static mut FFIQuery> {
    if q.is_null() {
        return None;
    }
    Some(&mut *(q as *mut FFIQuery))
}

/// Borrow a query handle so the table layer can read conditions + projection +
/// limit. The caller still owns the handle and must free it.
///
/// # Safety
/// `q` must be a valid query handle returned by [`mongreldb_query_begin`].
pub unsafe fn as_query(q: mongreldb_query_t) -> Option<&'static FFIQuery> {
    if q.is_null() {
        return None;
    }
    Some(&*(q as *const FFIQuery))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ffi_query_limits_fail_before_copying_input() {
        unsafe {
            let query = mongreldb_query_begin();
            assert!(!query.is_null());
            assert_ne!(
                mongreldb_query_set_limit(query, MAX_FINAL_LIMIT as u64 + 1),
                0
            );
            let column = 1u16;
            assert_ne!(
                mongreldb_query_set_projection(query, &column, MAX_PROJECTION_COLUMNS + 1,),
                0
            );
            let condition = mongreldb_condition {
                kind: mongreldb_condition_kind::Ann,
                column_id: 2,
                k: MAX_RETRIEVER_K as u32 + 1,
                embedding: EmbeddingSlice { data: &1.0, len: 1 },
                ..Default::default()
            };
            assert_ne!(mongreldb_query_add(query, &condition), 0);
            mongreldb_query_free(query);
        }
    }
}
