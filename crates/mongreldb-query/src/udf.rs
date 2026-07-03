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
