//! SQL UDFs for semantic search from SQL:
//!
//! * `ann_search(<embedding-col>, '<json f32 array>', k)` — HNSW semantic search.
//! * `sparse_match(<sparse-col>, '<json [[token, weight], …]>', k)` — SPLADE-style
//!   sparse retrieval (Phase 13 deferred: SQL surface for sparse).
//!
//! Both UDFs exist so `WHERE ann_search(vec, …)` / `WHERE sparse_match(col, …)`
//! parse and the logical plan types. The real top-k is served by
//! [`crate::MongrelProvider`]'s filter pushdown, which returns `Exact` so
//! DataFusion never evaluates these UDFs at runtime. If one *is* evaluated
//! (pushdown declined), it passes every row through — correct only when an index
//! serves the query, which is the documented precondition.

use arrow::array::{
    Array, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, UInt32Array,
    UInt64Array,
};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct AnnSearchUdf {
    signature: Signature,
}

impl AnnSearchUdf {
    pub const NAME: &'static str = "ann_search";

    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for AnnSearchUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for AnnSearchUdf {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _args: &[arrow::datatypes::DataType],
    ) -> DFResult<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let arr = Arc::new(BooleanArray::from(vec![true; n])) as Arc<dyn Array>;
        Ok(ColumnarValue::Array(arr))
    }
}

/// `sparse_match(<sparse-col>, '<json [[token, weight], …]>', k)` UDF — the SQL
/// hook for SPLADE-style learned-sparse retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SparseMatchUdf {
    signature: Signature,
}

impl SparseMatchUdf {
    pub const NAME: &'static str = "sparse_match";

    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for SparseMatchUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for SparseMatchUdf {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _args: &[arrow::datatypes::DataType],
    ) -> DFResult<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        let n = args.number_rows;
        let arr = Arc::new(BooleanArray::from(vec![true; n])) as Arc<dyn Array>;
        Ok(ColumnarValue::Array(arr))
    }
}

/// `rtree_intersects(min_x, max_x, min_y, max_y, q_min_x, q_max_x, q_min_y, q_max_y)`
/// is a portable SQL spelling for the `rtree_rects` overlap pushdown. The UDF
/// also evaluates correctly if DataFusion runs it outside an external module.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RTreeIntersectsUdf {
    signature: Signature,
}

impl RTreeIntersectsUdf {
    pub const NAME: &'static str = "rtree_intersects";

    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl Default for RTreeIntersectsUdf {
    fn default() -> Self {
        Self::new()
    }
}

impl ScalarUDFImpl for RTreeIntersectsUdf {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _args: &[arrow::datatypes::DataType],
    ) -> DFResult<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 8 {
            return Err(DataFusionError::Execution(format!(
                "{} expects 8 arguments",
                Self::NAME
            )));
        }
        let values = (0..args.number_rows)
            .map(|row| {
                let min_x = f64_value(&args.args[0], row);
                let max_x = f64_value(&args.args[1], row);
                let min_y = f64_value(&args.args[2], row);
                let max_y = f64_value(&args.args[3], row);
                let q_min_x = f64_value(&args.args[4], row);
                let q_max_x = f64_value(&args.args[5], row);
                let q_min_y = f64_value(&args.args[6], row);
                let q_max_y = f64_value(&args.args[7], row);
                match (
                    min_x, max_x, min_y, max_y, q_min_x, q_max_x, q_min_y, q_max_y,
                ) {
                    (
                        Some(min_x),
                        Some(max_x),
                        Some(min_y),
                        Some(max_y),
                        Some(q_min_x),
                        Some(q_max_x),
                        Some(q_min_y),
                        Some(q_max_y),
                    ) => {
                        max_x >= q_min_x && min_x <= q_max_x && max_y >= q_min_y && min_y <= q_max_y
                    }
                    _ => false,
                }
            })
            .collect::<Vec<_>>();
        Ok(ColumnarValue::Array(Arc::new(BooleanArray::from(values))))
    }
}

fn f64_value(value: &ColumnarValue, row: usize) -> Option<f64> {
    match value {
        ColumnarValue::Scalar(scalar) => scalar_f64(scalar),
        ColumnarValue::Array(array) => array_f64(array.as_ref(), row),
    }
}

fn scalar_f64(value: &ScalarValue) -> Option<f64> {
    match value {
        ScalarValue::Float64(Some(value)) => Some(*value),
        ScalarValue::Float32(Some(value)) => Some(*value as f64),
        ScalarValue::Int64(Some(value)) => Some(*value as f64),
        ScalarValue::Int32(Some(value)) => Some(*value as f64),
        ScalarValue::UInt64(Some(value)) => Some(*value as f64),
        ScalarValue::UInt32(Some(value)) => Some(*value as f64),
        _ => None,
    }
}

fn array_f64(array: &dyn Array, row: usize) -> Option<f64> {
    if array.is_null(row) {
        return None;
    }
    if let Some(array) = array.as_any().downcast_ref::<Float64Array>() {
        Some(array.value(row))
    } else if let Some(array) = array.as_any().downcast_ref::<Float32Array>() {
        Some(array.value(row) as f64)
    } else if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        Some(array.value(row) as f64)
    } else if let Some(array) = array.as_any().downcast_ref::<Int32Array>() {
        Some(array.value(row) as f64)
    } else if let Some(array) = array.as_any().downcast_ref::<UInt64Array>() {
        Some(array.value(row) as f64)
    } else {
        array
            .as_any()
            .downcast_ref::<UInt32Array>()
            .map(|array| array.value(row) as f64)
    }
}

/// `mongreldb_fts_rank(text, query)` — compute a BM25-inspired relevance
/// score for a text column against a whitespace-tokenized query string.
///
/// The score is the sum over query terms of:
///   `tf * (1 + log2(N / (1 + df)))`
/// where `tf` is the term frequency in the document, `N` is the total number
/// of documents (approximated as a constant since we don't have global stats),
/// and `df` is the document frequency (also approximated).
///
/// For accurate BM25 with global IDF, use the `fts_docs` virtual table module
/// which maintains an inverted index. This UDF is for ad-hoc ranking of text
/// columns in regular tables.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FtsRankUdf {
    signature: Signature,
}

impl FtsRankUdf {
    pub const NAME: &'static str = "mongreldb_fts_rank";
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic(
                vec![
                    arrow::datatypes::DataType::Utf8,
                    arrow::datatypes::DataType::LargeUtf8,
                    arrow::datatypes::DataType::Utf8View,
                    arrow::datatypes::DataType::Binary,
                    arrow::datatypes::DataType::LargeBinary,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for FtsRankUdf {
    fn name(&self) -> &str {
        Self::NAME
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _args: &[arrow::datatypes::DataType],
    ) -> DFResult<arrow::datatypes::DataType> {
        Ok(arrow::datatypes::DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        if args.args.len() != 2 {
            return Err(DataFusionError::Execution(format!(
                "{} expects 2 arguments (text, query)",
                Self::NAME
            )));
        }

        // Extract the query string (second argument, expected constant).
        let query = match &args.args[1] {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => s.clone(),
            ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => s.clone(),
            _ => String::new(),
        };
        let query_terms: Vec<String> = tokenize(&query);

        // Score each row's text against the query terms.
        let values: Vec<Option<f64>> = (0..args.number_rows)
            .map(|row| {
                let text = string_value(&args.args[0], row)?;
                let doc_terms = tokenize(&text);
                if doc_terms.is_empty() || query_terms.is_empty() {
                    return Some(0.0);
                }
                // Term frequency in this document.
                let _doc_len = doc_terms.len() as f64;
                let score: f64 = query_terms
                    .iter()
                    .map(|qt| {
                        let tf = doc_terms.iter().filter(|dt| *dt == qt).count() as f64;
                        if tf == 0.0 {
                            0.0
                        } else {
                            // Simplified BM25: k1=1.2, b=0.75, avgdl≈doc_len
                            // Without global IDF, use log(1 + tf) as a proxy.
                            let k1 = 1.2;
                            let b = 0.75;
                            // Simplified BM25: avgdl≈doc_len so doc_len/avgdl = 1.
                            (tf * (k1 + 1.0)) / (tf + k1 * (1.0 - b + b))
                        }
                    })
                    .sum();
                Some(score)
            })
            .collect();

        Ok(ColumnarValue::Array(Arc::new(Float64Array::from(values))))
    }
}

/// Simple whitespace + punctuation tokenizer for FTS ranking.
fn tokenize(text: &str) -> Vec<String> {
    text.split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty())
        .map(|s| s.to_ascii_lowercase())
        .collect()
}

/// Extract a string value from a columnar value at `row`.
fn string_value(value: &ColumnarValue, row: usize) -> Option<String> {
    match value {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => Some(s.clone()),
        ColumnarValue::Scalar(ScalarValue::Utf8View(Some(s))) => Some(s.clone()),
        ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(s))) => Some(s.clone()),
        ColumnarValue::Scalar(ScalarValue::Binary(Some(b))) => String::from_utf8(b.clone()).ok(),
        ColumnarValue::Scalar(ScalarValue::LargeBinary(Some(b))) => {
            String::from_utf8(b.clone()).ok()
        }
        ColumnarValue::Array(array) => {
            if array.is_null(row) {
                return None;
            }
            if let Some(a) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
                Some(a.value(row).to_string())
            } else if let Some(a) = array
                .as_any()
                .downcast_ref::<arrow::array::LargeStringArray>()
            {
                Some(a.value(row).to_string())
            } else if let Some(a) = array.as_any().downcast_ref::<arrow::array::BinaryArray>() {
                String::from_utf8(a.value(row).to_vec()).ok()
            } else if let Some(a) = array
                .as_any()
                .downcast_ref::<arrow::array::LargeBinaryArray>()
            {
                String::from_utf8(a.value(row).to_vec()).ok()
            } else {
                None
            }
        }
        _ => None,
    }
}
