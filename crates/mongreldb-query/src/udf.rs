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

use arrow::array::{Array, BooleanArray};
use datafusion::common::Result as DFResult;
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
