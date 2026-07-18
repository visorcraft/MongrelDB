//! C-facing hybrid search request API.
//!
//! Callers fill a stack-allocated [`mongreldb_search_request`] and pass it to
//! [`mongreldb_table_search`]. The request is borrowed for the duration of the
//! call (no ownership transfer).

use crate::cstr::cstr_to_string;
use crate::error::{clear, set_error, set_error_msg, ErrorCode};
use crate::query::{build_condition, mongreldb_condition};
use crate::table::{as_table, row_to_cells, FFIResult, mongreldb_result_t, mongreldb_table_t};
use crate::value::EmbeddingSlice;
use mongreldb_core::query::{
    Fusion, NamedRetriever, Rerank, Retriever, SearchRequest, VectorMetric, MAX_FINAL_LIMIT,
    MAX_HARD_CONDITIONS, MAX_PROJECTION_COLUMNS, MAX_RETRIEVER_K, MAX_RETRIEVER_NAME_BYTES,
    MAX_RETRIEVER_WEIGHT, MAX_SET_MEMBERS, MAX_SPARSE_TERMS,
};
use std::os::raw::{c_char, c_void};

/// Opaque builder handle (reserved for a future incremental builder API).
pub type mongreldb_search_request_t = *mut c_void;

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mongreldb_retriever_kind {
    Ann = 0,
    Sparse = 1,
    MinHash = 2,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mongreldb_search_metric {
    Cosine = 0,
    DotProduct = 1,
    Euclidean = 2,
}

#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum mongreldb_fusion_kind {
    ReciprocalRank = 0,
}

#[repr(C)]
pub struct mongreldb_retriever {
    pub kind: i32,
    pub column_id: u16,
    pub name: *const c_char,
    pub weight: f64,
    pub k: u32,
    pub embedding: EmbeddingSlice,
    pub sparse_terms: crate::query::SparseTermArray,
    pub minhash_members: crate::value::MinHashMembers,
}

#[repr(C)]
pub struct mongreldb_retriever_array {
    pub data: *const mongreldb_retriever,
    pub len: usize,
}

#[repr(C)]
pub struct mongreldb_fusion {
    pub kind: i32,
    pub reciprocal_rank_constant: u32,
}

#[repr(C)]
pub struct mongreldb_rerank {
    pub kind: i32,
    pub embedding_column: u16,
    pub query: EmbeddingSlice,
    pub metric: i32,
    pub candidate_limit: u32,
    pub weight: f64,
}

#[repr(C)]
pub struct mongreldb_condition_array {
    pub data: *const mongreldb_condition,
    pub len: usize,
}

#[repr(C)]
pub struct mongreldb_projection {
    pub data: *const u16,
    pub len: usize,
}

#[repr(C)]
pub struct mongreldb_search_request {
    pub must: mongreldb_condition_array,
    pub retrievers: mongreldb_retriever_array,
    pub fusion: mongreldb_fusion,
    pub rerank: *const mongreldb_rerank,
    pub limit: usize,
    pub projection: mongreldb_projection,
}

/// Begin an empty search request handle (core [`SearchRequest`]).
///
/// Preferred callers fill a stack [`mongreldb_search_request`] and pass it to
/// [`mongreldb_table_search`] directly. This builder handle is freeable with
/// [`mongreldb_search_request_free`].
#[no_mangle]
pub extern "C" fn mongreldb_search_request_begin() -> mongreldb_search_request_t {
    clear();
    let req = Box::new(SearchRequest {
        must: Vec::new(),
        retrievers: Vec::new(),
        fusion: Fusion::ReciprocalRank { constant: 60 },
        rerank: None,
        limit: 10,
        projection: None,
    });
    Box::into_raw(req) as mongreldb_search_request_t
}

/// Free a handle from [`mongreldb_search_request_begin`]. Null is a no-op.
///
/// # Safety
/// `req` must be null or a pointer returned by [`mongreldb_search_request_begin`]
/// that has not already been freed.
#[no_mangle]
pub unsafe extern "C" fn mongreldb_search_request_free(req: mongreldb_search_request_t) {
    if req.is_null() {
        return;
    }
    drop(Box::from_raw(req as *mut SearchRequest));
}

unsafe fn build_retriever(r: &mongreldb_retriever) -> Result<NamedRetriever, ErrorCode> {
    let kind = match r.kind {
        0 => mongreldb_retriever_kind::Ann,
        1 => mongreldb_retriever_kind::Sparse,
        2 => mongreldb_retriever_kind::MinHash,
        value => {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                format!("invalid retriever kind {value}"),
            ));
        }
    };
    if r.k == 0 || r.k as usize > MAX_RETRIEVER_K {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("retriever k must be between 1 and {MAX_RETRIEVER_K}"),
        ));
    }
    if !r.weight.is_finite() || r.weight < 0.0 || r.weight > MAX_RETRIEVER_WEIGHT {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            "retriever weight must be finite, non-negative, and within limit",
        ));
    }
    let name = if r.name.is_null() {
        String::new()
    } else {
        cstr_to_string(r.name, "retriever name")
    };
    if name.is_empty() || name.len() > MAX_RETRIEVER_NAME_BYTES {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            "retriever name must be non-empty and within the byte limit",
        ));
    }
    let retriever = match kind {
        mongreldb_retriever_kind::Ann => {
            let query = r.embedding.to_vec();
            if query.is_empty() {
                return Err(set_error_msg(
                    ErrorCode::InvalidArgument,
                    "Ann retriever requires a non-empty embedding",
                ));
            }
            Retriever::Ann {
                column_id: r.column_id,
                query,
                k: r.k as usize,
            }
        }
        mongreldb_retriever_kind::Sparse => {
            if r.sparse_terms.len > MAX_SPARSE_TERMS {
                return Err(set_error_msg(
                    ErrorCode::InvalidArgument,
                    "sparse retriever term count exceeds the public limit",
                ));
            }
            let terms = if r.sparse_terms.len == 0 || r.sparse_terms.items.is_null() {
                Vec::new()
            } else {
                std::slice::from_raw_parts(r.sparse_terms.items, r.sparse_terms.len)
                    .iter()
                    .map(|t| (t.token, t.weight))
                    .collect::<Vec<_>>()
            };
            Retriever::Sparse {
                column_id: r.column_id,
                query: terms,
                k: r.k as usize,
            }
        }
        mongreldb_retriever_kind::MinHash => {
            if r.minhash_members.len > MAX_SET_MEMBERS {
                return Err(set_error_msg(
                    ErrorCode::InvalidArgument,
                    "MinHash retriever member count exceeds the public limit",
                ));
            }
            let members = if r.minhash_members.len == 0 || r.minhash_members.items.is_null() {
                Vec::new()
            } else {
                std::slice::from_raw_parts(r.minhash_members.items, r.minhash_members.len)
                    .iter()
                    .map(|item| {
                        let s = std::ffi::CStr::from_ptr(*item)
                            .to_string_lossy()
                            .into_owned();
                        mongreldb_core::query::SetMember::String(s)
                    })
                    .collect::<Vec<_>>()
            };
            Retriever::MinHash {
                column_id: r.column_id,
                members,
                k: r.k as usize,
            }
        }
    };
    Ok(NamedRetriever {
        name,
        weight: r.weight,
        retriever,
    })
}

unsafe fn build_search_request(req: &mongreldb_search_request) -> Result<SearchRequest, ErrorCode> {
    if req.limit == 0 || req.limit > MAX_FINAL_LIMIT {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("search limit must be between 1 and {MAX_FINAL_LIMIT}"),
        ));
    }
    if req.must.len > MAX_HARD_CONDITIONS {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            format!("search must conditions exceed {MAX_HARD_CONDITIONS}"),
        ));
    }
    let mut must = Vec::with_capacity(req.must.len);
    if req.must.len > 0 {
        if req.must.data.is_null() {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                "search must conditions pointer is null",
            ));
        }
        for i in 0..req.must.len {
            let c = &*req.must.data.add(i);
            must.push(build_condition(c)?);
        }
    }
    if req.retrievers.len == 0 {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            "search requires at least one retriever",
        ));
    }
    if req.retrievers.data.is_null() {
        return Err(set_error_msg(
            ErrorCode::InvalidArgument,
            "search retrievers pointer is null",
        ));
    }
    let mut retrievers = Vec::with_capacity(req.retrievers.len);
    for i in 0..req.retrievers.len {
        let r = &*req.retrievers.data.add(i);
        retrievers.push(build_retriever(r)?);
    }
    let fusion = match req.fusion.kind {
        0 => Fusion::ReciprocalRank {
            constant: req.fusion.reciprocal_rank_constant.max(1),
        },
        value => {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                format!("invalid fusion kind {value}"),
            ));
        }
    };
    let rerank = if req.rerank.is_null() {
        None
    } else {
        let rr = &*req.rerank;
        if rr.kind != 0 {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                "invalid rerank kind",
            ));
        }
        if rr.candidate_limit < req.limit as u32 || rr.candidate_limit > MAX_RETRIEVER_K as u32 {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                "rerank candidate_limit is out of range",
            ));
        }
        if !rr.weight.is_finite() || rr.weight < 0.0 || rr.weight > MAX_RETRIEVER_WEIGHT {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                "rerank weight must be finite, non-negative, and within limit",
            ));
        }
        let metric = match rr.metric {
            0 => VectorMetric::Cosine,
            1 => VectorMetric::DotProduct,
            2 => VectorMetric::Euclidean,
            value => {
                return Err(set_error_msg(
                    ErrorCode::InvalidArgument,
                    format!("invalid rerank metric {value}"),
                ));
            }
        };
        Some(Rerank::ExactVector {
            embedding_column: rr.embedding_column,
            query: rr.query.to_vec(),
            metric,
            candidate_limit: rr.candidate_limit as usize,
            weight: rr.weight,
        })
    };
    let projection = if req.projection.data.is_null() || req.projection.len == 0 {
        None
    } else {
        if req.projection.len > MAX_PROJECTION_COLUMNS {
            return Err(set_error_msg(
                ErrorCode::InvalidArgument,
                format!("projection exceeds {MAX_PROJECTION_COLUMNS} columns"),
            ));
        }
        Some(std::slice::from_raw_parts(req.projection.data, req.projection.len).to_vec())
    };
    Ok(SearchRequest {
        must,
        retrievers,
        fusion,
        rerank,
        limit: req.limit,
        projection,
    })
}

/// Run a hybrid search (retrievers + fusion + optional rerank) with principal
/// authorization. Returns a result handle, or null on error.
///
/// # Safety
/// `t` must be a valid table handle. `req` must point to a valid
/// [`mongreldb_search_request`] whose nested pointers are valid for the call.
/// The returned handle must be freed with [`crate::mongreldb_result_free`].
#[no_mangle]
pub unsafe extern "C" fn mongreldb_table_search(
    t: mongreldb_table_t,
    req: *const mongreldb_search_request,
) -> mongreldb_result_t {
    clear();
    let Some(table) = as_table(t) else {
        return std::ptr::null_mut();
    };
    if req.is_null() {
        set_error_msg(ErrorCode::InvalidArgument, "search request is null");
        return std::ptr::null_mut();
    }
    let request = match build_search_request(&*req) {
        Ok(r) => r,
        Err(_) => return std::ptr::null_mut(),
    };
    let hits = match table
        .db
        .search_for_current_principal(&table.name, &request)
    {
        Ok(hits) => hits,
        Err(e) => {
            set_error(&e);
            return std::ptr::null_mut();
        }
    };

    let handle = match table.db.table(&table.name) {
        Ok(h) => h,
        Err(e) => {
            set_error(&e);
            return std::ptr::null_mut();
        }
    };
    let schema = handle.lock().schema().clone();

    let mut out_rows: Vec<Vec<(u16, mongreldb_core::Value)>> = Vec::with_capacity(hits.len());
    let mut out_ids: Vec<u64> = Vec::with_capacity(hits.len());
    for hit in hits.into_iter().take(request.limit) {
        let columns: std::collections::HashMap<u16, mongreldb_core::Value> =
            hit.cells.into_iter().collect();
        let cells = match &request.projection {
            Some(proj) => proj
                .iter()
                .map(|cid| {
                    let v = columns.get(cid).cloned().unwrap_or(mongreldb_core::Value::Null);
                    (*cid, v)
                })
                .collect(),
            None => row_to_cells(&schema, &columns),
        };
        out_rows.push(cells);
        out_ids.push(hit.row_id.0);
    }
    FFIResult {
        rows: out_rows,
        row_ids: out_ids,
        schema,
        backing: Vec::new(),
        row_cell_drops: Vec::new(),
    }
    .into_handle()
}
