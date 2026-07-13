use arrow::array::{ArrayRef, Float32Array, Float64Array, StringArray, UInt32Array, UInt64Array};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::Expr;
use datafusion::prelude::SessionContext;
use mongreldb_core::query::{
    Condition, Fusion, NamedRetriever, Retriever, RetrieverScore, SearchRequest,
    SetSimilarityRequest,
};
use mongreldb_core::{Database, Principal, Schema, Table, TypeId, Value};
use parking_lot::Mutex;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

pub(crate) type TableMap = Arc<Mutex<HashMap<String, Arc<Mutex<Table>>>>>;

#[derive(serde::Deserialize)]
struct HybridSpec {
    #[serde(default)]
    must: Vec<HybridCondition>,
    retrievers: Vec<HybridNamedRetriever>,
    #[serde(default = "default_rrf_constant")]
    rrf_constant: u32,
    limit: usize,
}

fn default_rrf_constant() -> u32 {
    60
}

#[derive(serde::Deserialize)]
struct HybridNamedRetriever {
    name: String,
    #[serde(default = "default_weight")]
    weight: f64,
    #[serde(flatten)]
    retriever: HybridRetriever,
}

fn default_weight() -> f64 {
    1.0
}

impl HybridNamedRetriever {
    fn to_core(&self, schema: &Schema) -> DFResult<NamedRetriever> {
        Ok(NamedRetriever {
            name: self.name.clone(),
            weight: self.weight,
            retriever: self.retriever.to_core(schema)?,
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum HybridRetriever {
    Ann {
        column: String,
        query: Vec<f32>,
        k: usize,
    },
    Sparse {
        column: String,
        query: Vec<(u32, f32)>,
        k: usize,
    },
    #[serde(rename = "minhash", alias = "min_hash")]
    MinHash {
        column: String,
        members: Vec<mongreldb_core::query::SetMember>,
        k: usize,
    },
}

impl HybridRetriever {
    fn to_core(&self, schema: &Schema) -> DFResult<Retriever> {
        Ok(match self {
            Self::Ann { column, query, k } => Retriever::Ann {
                column_id: column_id(schema, column)?,
                query: query.clone(),
                k: *k,
            },
            Self::Sparse { column, query, k } => Retriever::Sparse {
                column_id: column_id(schema, column)?,
                query: query.clone(),
                k: *k,
            },
            Self::MinHash { column, members, k } => Retriever::MinHash {
                column_id: column_id(schema, column)?,
                members: members.clone(),
                k: *k,
            },
        })
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "snake_case")]
enum HybridCondition {
    Pk {
        value: serde_json::Value,
    },
    BitmapEq {
        column: String,
        value: serde_json::Value,
    },
    BitmapIn {
        column: String,
        values: Vec<serde_json::Value>,
    },
    Range {
        column: String,
        lo: i64,
        hi: i64,
    },
    RangeF64 {
        column: String,
        lo: f64,
        lo_inclusive: bool,
        hi: f64,
        hi_inclusive: bool,
    },
    IsNull {
        column: String,
    },
    IsNotNull {
        column: String,
    },
    FmContains {
        column: String,
        pattern: String,
    },
    FmContainsAll {
        column: String,
        patterns: Vec<String>,
    },
}

impl HybridCondition {
    fn to_core(&self, schema: &Schema) -> DFResult<Condition> {
        Ok(match self {
            Self::Pk { value } => {
                let primary_key = schema
                    .primary_key()
                    .ok_or_else(|| DataFusionError::Plan("table has no primary key".into()))?;
                Condition::Pk(json_value(value, &primary_key.ty)?.encode_key())
            }
            Self::BitmapEq { column, value } => {
                let column = schema
                    .column(column)
                    .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {column}")))?;
                Condition::BitmapEq {
                    column_id: column.id,
                    value: json_value(value, &column.ty)?.encode_key(),
                }
            }
            Self::BitmapIn { column, values } => {
                let column = schema
                    .column(column)
                    .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {column}")))?;
                Condition::BitmapIn {
                    column_id: column.id,
                    values: values
                        .iter()
                        .map(|value| json_value(value, &column.ty).map(|value| value.encode_key()))
                        .collect::<DFResult<_>>()?,
                }
            }
            Self::Range { column, lo, hi } => Condition::Range {
                column_id: column_id(schema, column)?,
                lo: *lo,
                hi: *hi,
            },
            Self::RangeF64 {
                column,
                lo,
                lo_inclusive,
                hi,
                hi_inclusive,
            } => Condition::RangeF64 {
                column_id: column_id(schema, column)?,
                lo: *lo,
                lo_inclusive: *lo_inclusive,
                hi: *hi,
                hi_inclusive: *hi_inclusive,
            },
            Self::IsNull { column } => Condition::IsNull {
                column_id: column_id(schema, column)?,
            },
            Self::IsNotNull { column } => Condition::IsNotNull {
                column_id: column_id(schema, column)?,
            },
            Self::FmContains { column, pattern } => Condition::FmContains {
                column_id: column_id(schema, column)?,
                pattern: pattern.as_bytes().to_vec(),
            },
            Self::FmContainsAll { column, patterns } => Condition::FmContainsAll {
                column_id: column_id(schema, column)?,
                patterns: patterns
                    .iter()
                    .map(|pattern| pattern.as_bytes().to_vec())
                    .collect(),
            },
        })
    }
}

#[derive(Clone, Copy)]
enum Kind {
    Ann,
    Sparse,
    MinHash,
    ExactSet,
    Hybrid,
}

struct ScoredFunction {
    kind: Kind,
    tables: TableMap,
    database: Option<Arc<Database>>,
    principal: Option<Principal>,
}

impl fmt::Debug for ScoredFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScoredFunction").finish_non_exhaustive()
    }
}

pub(crate) fn register(
    ctx: &SessionContext,
    tables: TableMap,
    database: Option<Arc<Database>>,
    principal: Option<Principal>,
) {
    for (name, kind) in [
        ("ann_search_scored", Kind::Ann),
        ("sparse_search_scored", Kind::Sparse),
        ("minhash_search_scored", Kind::MinHash),
        ("set_similarity_scored", Kind::ExactSet),
        ("hybrid_search_scored", Kind::Hybrid),
    ] {
        ctx.register_udtf(
            name,
            Arc::new(ScoredFunction {
                kind,
                tables: Arc::clone(&tables),
                database: database.clone(),
                principal: principal.clone(),
            }),
        );
    }
}

impl TableFunctionImpl for ScoredFunction {
    fn call_with_args(&self, args: TableFunctionArgs) -> DFResult<Arc<dyn TableProvider>> {
        let args = args.exprs();
        if matches!(self.kind, Kind::ExactSet) {
            return self.exact_set_provider(args);
        }
        if matches!(self.kind, Kind::Hybrid) {
            return self.hybrid_provider(args);
        }
        if args.len() != 5 {
            return Err(DataFusionError::Plan(
                "scored search requires table, column, JSON query, k, projection".into(),
            ));
        }
        let table_name = string_literal(&args[0])?;
        let column_name = string_literal(&args[1])?;
        let query = string_literal(&args[2])?;
        let k = usize::try_from(integer_literal(&args[3])?)
            .ok()
            .filter(|k| *k > 0)
            .ok_or_else(|| DataFusionError::Plan("k must be > 0".into()))?;
        let projection_names: Vec<_> = string_literal(&args[4])?
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
        if projection_names.is_empty() {
            return Err(DataFusionError::Plan(
                "projection must name at least one column".into(),
            ));
        }
        let handle = self
            .tables
            .lock()
            .get(&table_name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("unknown table: {table_name}")))?;
        let schema = handle.lock().schema().clone();
        let column_id = schema
            .column(&column_name)
            .map(|column| column.id)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {column_name}")))?;
        let projection: Vec<_> = projection_names
            .iter()
            .map(|name| {
                schema
                    .column(name)
                    .map(|column| column.id)
                    .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {name}")))
            })
            .collect::<DFResult<_>>()?;
        if let Some(database) = &self.database {
            let mut required = projection.clone();
            required.push(column_id);
            database
                .require_columns_for(
                    &table_name,
                    mongreldb_core::ColumnOperation::Select,
                    &required,
                    self.principal.as_ref(),
                )
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        let retriever = parse_retriever(self.kind, column_id, &query, k)?;
        let allowed = self
            .database
            .as_ref()
            .map(|database| {
                database.authorized_candidate_ids_for(&table_name, self.principal.as_ref())
            })
            .transpose()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
            .flatten();
        let (hits, rows) = {
            let mut table = handle.lock();
            let hits = table
                .retrieve_with_allowed(&retriever, allowed.as_ref())
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let row_ids: Vec<_> = hits.iter().map(|hit| hit.row_id.0).collect();
            let snapshot = table.snapshot();
            let rows = table
                .rows_for_rids(&row_ids, snapshot)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            (hits, rows)
        };
        let rows = if let Some(database) = &self.database {
            database
                .secure_rows_for(&table_name, rows, self.principal.as_ref())
                .map_err(|error| DataFusionError::Execution(error.to_string()))?
        } else {
            rows
        };
        let scores: HashMap<_, _> = hits
            .into_iter()
            .map(|hit| (hit.row_id, (hit.rank, hit.score)))
            .collect();
        let rows: Vec<_> = rows
            .into_iter()
            .filter(|row| scores.contains_key(&row.row_id))
            .collect();
        let projected = Schema {
            schema_id: schema.schema_id,
            columns: projection
                .iter()
                .filter_map(|id| {
                    schema
                        .columns
                        .iter()
                        .find(|column| column.id == *id)
                        .cloned()
                })
                .collect(),
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        };
        let base = crate::arrow_conv::rows_to_batch(&rows, &projected)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let ranks: Vec<_> = rows
            .iter()
            .map(|row| scores[&row.row_id].0 as u64)
            .collect();
        let mut fields = base
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        let mut arrays = base.columns().to_vec();
        fields.push(Field::new("search_rank", DataType::UInt64, false));
        arrays.push(Arc::new(UInt64Array::from(ranks)) as ArrayRef);
        match self.kind {
            Kind::Ann => {
                fields.push(Field::new("ann_distance", DataType::UInt32, false));
                arrays.push(Arc::new(UInt32Array::from(
                    rows.iter()
                        .map(|row| match scores[&row.row_id].1 {
                            RetrieverScore::AnnHammingDistance(score) => score,
                            _ => unreachable!(),
                        })
                        .collect::<Vec<_>>(),
                )));
            }
            Kind::Sparse => append_float_score(
                "sparse_score",
                &rows,
                &scores,
                &mut fields,
                &mut arrays,
                |score| match score {
                    RetrieverScore::SparseDotProduct(score) => score,
                    _ => unreachable!(),
                },
            ),
            Kind::MinHash => append_float_score(
                "estimated_jaccard",
                &rows,
                &scores,
                &mut fields,
                &mut arrays,
                |score| match score {
                    RetrieverScore::MinHashEstimatedJaccard(score) => score,
                    _ => unreachable!(),
                },
            ),
            Kind::ExactSet => unreachable!(),
            Kind::Hybrid => unreachable!(),
        }
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

impl ScoredFunction {
    fn exact_set_provider(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 7 {
            return Err(DataFusionError::Plan(
                "set_similarity_scored requires table, column, members, candidate_k, min_jaccard, limit, projection".into(),
            ));
        }
        let table_name = string_literal(&args[0])?;
        let column_name = string_literal(&args[1])?;
        let members = serde_json::from_str(&string_literal(&args[2])?)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        let candidate_k = positive_usize(&args[3], "candidate_k")?;
        let min_jaccard = float_literal(&args[4])? as f32;
        let limit = positive_usize(&args[5], "limit")?;
        let projection_names: Vec<_> = string_literal(&args[6])?
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
        if projection_names.is_empty() {
            return Err(DataFusionError::Plan(
                "projection must name at least one column".into(),
            ));
        }
        let handle = self
            .tables
            .lock()
            .get(&table_name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("unknown table: {table_name}")))?;
        let schema = handle.lock().schema().clone();
        let column_id = schema
            .column(&column_name)
            .map(|column| column.id)
            .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {column_name}")))?;
        let projection: Vec<_> = projection_names
            .iter()
            .map(|name| {
                schema
                    .column(name)
                    .map(|column| column.id)
                    .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {name}")))
            })
            .collect::<DFResult<_>>()?;
        if let Some(database) = &self.database {
            let mut required = projection.clone();
            required.push(column_id);
            database
                .require_columns_for(
                    &table_name,
                    mongreldb_core::ColumnOperation::Select,
                    &required,
                    self.principal.as_ref(),
                )
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        let allowed = self
            .database
            .as_ref()
            .map(|database| {
                database.authorized_candidate_ids_for(&table_name, self.principal.as_ref())
            })
            .transpose()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
            .flatten();
        let (hits, rows) = {
            let mut table = handle.lock();
            let hits = table
                .set_similarity_with_allowed(
                    &SetSimilarityRequest {
                        column_id,
                        members,
                        candidate_k,
                        min_jaccard,
                        limit,
                    },
                    allowed.as_ref(),
                )
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let row_ids: Vec<_> = hits.iter().map(|hit| hit.row_id.0).collect();
            let snapshot = table.snapshot();
            let rows = table
                .rows_for_rids(&row_ids, snapshot)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            (hits, rows)
        };
        let rows = if let Some(database) = &self.database {
            database
                .secure_rows_for(&table_name, rows, self.principal.as_ref())
                .map_err(|error| DataFusionError::Execution(error.to_string()))?
        } else {
            rows
        };
        let scores: HashMap<_, _> = hits
            .into_iter()
            .enumerate()
            .map(|(rank, hit)| {
                (
                    hit.row_id,
                    (rank as u64 + 1, hit.estimated_jaccard, hit.exact_jaccard),
                )
            })
            .collect();
        let rows: Vec<_> = rows
            .into_iter()
            .filter(|row| scores.contains_key(&row.row_id))
            .collect();
        let projected = projected_schema(&schema, &projection);
        let base = crate::arrow_conv::rows_to_batch(&rows, &projected)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let mut fields = base
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        let mut arrays = base.columns().to_vec();
        fields.extend([
            Field::new("search_rank", DataType::UInt64, false),
            Field::new("estimated_jaccard", DataType::Float32, false),
            Field::new("exact_jaccard", DataType::Float32, false),
        ]);
        arrays.extend([
            Arc::new(UInt64Array::from(
                rows.iter()
                    .map(|row| scores[&row.row_id].0)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float32Array::from(
                rows.iter()
                    .map(|row| scores[&row.row_id].1)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
            Arc::new(Float32Array::from(
                rows.iter()
                    .map(|row| scores[&row.row_id].2)
                    .collect::<Vec<_>>(),
            )) as ArrayRef,
        ]);
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }

    fn hybrid_provider(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 3 {
            return Err(DataFusionError::Plan(
                "hybrid_search_scored requires table, request JSON, projection".into(),
            ));
        }
        let table_name = string_literal(&args[0])?;
        let spec: HybridSpec = serde_json::from_str(&string_literal(&args[1])?)
            .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        let projection_names: Vec<_> = string_literal(&args[2])?
            .split(',')
            .map(str::trim)
            .filter(|name| !name.is_empty())
            .map(str::to_owned)
            .collect();
        if projection_names.is_empty() {
            return Err(DataFusionError::Plan(
                "projection must name at least one column".into(),
            ));
        }
        let handle = self
            .tables
            .lock()
            .get(&table_name)
            .cloned()
            .ok_or_else(|| DataFusionError::Plan(format!("unknown table: {table_name}")))?;
        let schema = handle.lock().schema().clone();
        let projection: Vec<_> = projection_names
            .iter()
            .map(|name| column_id(&schema, name))
            .collect::<DFResult<_>>()?;
        let must: Vec<_> = spec
            .must
            .iter()
            .map(|condition| condition.to_core(&schema))
            .collect::<DFResult<_>>()?;
        let retrievers: Vec<_> = spec
            .retrievers
            .iter()
            .map(|retriever| retriever.to_core(&schema))
            .collect::<DFResult<_>>()?;
        if let Some(database) = &self.database {
            let mut required = projection.clone();
            required.extend(mongreldb_core::query::condition_columns(&must));
            required.extend(
                retrievers
                    .iter()
                    .map(|retriever| retriever.retriever.column_id()),
            );
            required.sort_unstable();
            required.dedup();
            database
                .require_columns_for(
                    &table_name,
                    mongreldb_core::ColumnOperation::Select,
                    &required,
                    self.principal.as_ref(),
                )
                .map_err(|error| DataFusionError::Plan(error.to_string()))?;
        }
        let allowed = self
            .database
            .as_ref()
            .map(|database| {
                database.authorized_candidate_ids_for(&table_name, self.principal.as_ref())
            })
            .transpose()
            .map_err(|error| DataFusionError::Execution(error.to_string()))?
            .flatten();
        let (hits, rows) = {
            let mut table = handle.lock();
            let hits = table
                .search_with_allowed(
                    &SearchRequest {
                        must,
                        retrievers,
                        fusion: Fusion::ReciprocalRank {
                            constant: spec.rrf_constant,
                        },
                        limit: spec.limit,
                        projection: Some(projection.clone()),
                    },
                    allowed.as_ref(),
                )
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            let row_ids: Vec<_> = hits.iter().map(|hit| hit.row_id.0).collect();
            let snapshot = table.snapshot();
            let rows = table
                .rows_for_rids(&row_ids, snapshot)
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
            (hits, rows)
        };
        let rows = if let Some(database) = &self.database {
            database
                .secure_rows_for(&table_name, rows, self.principal.as_ref())
                .map_err(|error| DataFusionError::Execution(error.to_string()))?
        } else {
            rows
        };
        let mut rows_by_id: HashMap<_, _> = rows.into_iter().map(|row| (row.row_id, row)).collect();
        let mut output_rows = Vec::new();
        let mut ranks = Vec::new();
        let mut fused_scores = Vec::new();
        let mut component_json = Vec::new();
        for (rank, hit) in hits.into_iter().enumerate() {
            let Some(row) = rows_by_id.remove(&hit.row_id) else {
                continue;
            };
            output_rows.push(row);
            ranks.push(rank as u64 + 1);
            fused_scores.push(hit.fused_score);
            component_json.push(
                serde_json::to_string(
                    &hit.components
                        .into_iter()
                        .map(|component| {
                            serde_json::json!({
                                "retriever_name": component.retriever_name,
                                "rank": component.rank,
                                "raw_score": score_json(component.raw_score),
                                "contribution": component.contribution,
                            })
                        })
                        .collect::<Vec<_>>(),
                )
                .unwrap(),
            );
        }
        let projected = projected_schema(&schema, &projection);
        let base = crate::arrow_conv::rows_to_batch(&output_rows, &projected)
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let mut fields = base
            .schema()
            .fields()
            .iter()
            .map(|field| field.as_ref().clone())
            .collect::<Vec<_>>();
        let mut arrays = base.columns().to_vec();
        fields.extend([
            Field::new("search_rank", DataType::UInt64, false),
            Field::new("fused_score", DataType::Float64, false),
            Field::new("components", DataType::Utf8, false),
        ]);
        arrays.extend([
            Arc::new(UInt64Array::from(ranks)) as ArrayRef,
            Arc::new(Float64Array::from(fused_scores)) as ArrayRef,
            Arc::new(StringArray::from(component_json)) as ArrayRef,
        ]);
        let schema = Arc::new(ArrowSchema::new(fields));
        let batch = RecordBatch::try_new(Arc::clone(&schema), arrays)?;
        Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
    }
}

fn append_float_score(
    name: &str,
    rows: &[mongreldb_core::Row],
    scores: &HashMap<mongreldb_core::RowId, (usize, RetrieverScore)>,
    fields: &mut Vec<Field>,
    arrays: &mut Vec<ArrayRef>,
    value: impl Fn(RetrieverScore) -> f32,
) {
    fields.push(Field::new(name, DataType::Float32, false));
    arrays.push(Arc::new(Float32Array::from(
        rows.iter()
            .map(|row| value(scores[&row.row_id].1))
            .collect::<Vec<_>>(),
    )));
}

fn parse_retriever(kind: Kind, column_id: u16, query: &str, k: usize) -> DFResult<Retriever> {
    Ok(match kind {
        Kind::Ann => Retriever::Ann {
            column_id,
            query: serde_json::from_str(query)
                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
            k,
        },
        Kind::Sparse => Retriever::Sparse {
            column_id,
            query: serde_json::from_str(query)
                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
            k,
        },
        Kind::MinHash => Retriever::MinHash {
            column_id,
            members: serde_json::from_str(query)
                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
            k,
        },
        Kind::ExactSet => unreachable!(),
        Kind::Hybrid => unreachable!(),
    })
}

fn projected_schema(schema: &Schema, projection: &[u16]) -> Schema {
    Schema {
        schema_id: schema.schema_id,
        columns: projection
            .iter()
            .filter_map(|id| {
                schema
                    .columns
                    .iter()
                    .find(|column| column.id == *id)
                    .cloned()
            })
            .collect(),
        indexes: vec![],
        colocation: vec![],
        constraints: Default::default(),
        clustered: false,
    }
}

fn column_id(schema: &Schema, name: &str) -> DFResult<u16> {
    schema
        .column(name)
        .map(|column| column.id)
        .ok_or_else(|| DataFusionError::Plan(format!("unknown column: {name}")))
}

fn json_value(value: &serde_json::Value, ty: &TypeId) -> DFResult<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }
    match ty {
        TypeId::Bool => value
            .as_bool()
            .map(Value::Bool)
            .ok_or_else(|| DataFusionError::Plan("expected boolean value".into())),
        TypeId::Int8
        | TypeId::Int16
        | TypeId::Int32
        | TypeId::Int64
        | TypeId::UInt8
        | TypeId::UInt16
        | TypeId::UInt32
        | TypeId::UInt64
        | TypeId::TimestampNanos
        | TypeId::Date32
        | TypeId::Date64
        | TypeId::Time64 => value
            .as_i64()
            .map(Value::Int64)
            .ok_or_else(|| DataFusionError::Plan("expected integer value".into())),
        TypeId::Float32 | TypeId::Float64 => value
            .as_f64()
            .filter(|value| value.is_finite())
            .map(Value::Float64)
            .ok_or_else(|| DataFusionError::Plan("expected finite number".into())),
        TypeId::Bytes | TypeId::Enum { .. } => value
            .as_str()
            .map(|value| Value::Bytes(value.as_bytes().to_vec()))
            .ok_or_else(|| DataFusionError::Plan("expected string value".into())),
        TypeId::Embedding { dim } => {
            let values = value
                .as_array()
                .filter(|values| values.len() == *dim as usize)
                .ok_or_else(|| {
                    DataFusionError::Plan(format!("expected embedding dimension {dim}"))
                })?;
            let values = values
                .iter()
                .map(|value| {
                    value
                        .as_f64()
                        .map(|value| value as f32)
                        .filter(|value| value.is_finite())
                        .ok_or_else(|| {
                            DataFusionError::Plan("expected finite embedding value".into())
                        })
                })
                .collect::<DFResult<_>>()?;
            Ok(Value::Embedding(values))
        }
        TypeId::Json | TypeId::Array { .. } => serde_json::to_vec(value)
            .map(Value::Json)
            .map_err(|error| DataFusionError::Plan(error.to_string())),
        _ => Err(DataFusionError::Plan(format!(
            "unsupported SQL search value type: {ty:?}"
        ))),
    }
}

fn score_json(score: RetrieverScore) -> serde_json::Value {
    match score {
        RetrieverScore::AnnHammingDistance(value) => {
            serde_json::json!({"kind":"ann_hamming_distance","value":value})
        }
        RetrieverScore::SparseDotProduct(value) => {
            serde_json::json!({"kind":"sparse_dot_product","value":value})
        }
        RetrieverScore::MinHashEstimatedJaccard(value) => {
            serde_json::json!({"kind":"minhash_estimated_jaccard","value":value})
        }
    }
}

fn positive_usize(expr: &Expr, name: &str) -> DFResult<usize> {
    usize::try_from(integer_literal(expr)?)
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| DataFusionError::Plan(format!("{name} must be > 0")))
}

fn float_literal(expr: &Expr) -> DFResult<f64> {
    match expr {
        Expr::Literal(ScalarValue::Float64(Some(value)), _) => Ok(*value),
        Expr::Literal(ScalarValue::Float32(Some(value)), _) => Ok(*value as f64),
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Ok(*value as f64),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Ok(*value as f64),
        _ => Err(DataFusionError::Plan(
            "min_jaccard must be a numeric literal".into(),
        )),
    }
}

fn string_literal(expr: &Expr) -> DFResult<String> {
    match expr {
        Expr::Literal(
            ScalarValue::Utf8(Some(value))
            | ScalarValue::LargeUtf8(Some(value))
            | ScalarValue::Utf8View(Some(value)),
            _,
        ) => Ok(value.clone()),
        _ => Err(DataFusionError::Plan(
            "scored search arguments must be literals".into(),
        )),
    }
}

fn integer_literal(expr: &Expr) -> DFResult<i64> {
    match expr {
        Expr::Literal(ScalarValue::Int64(Some(value)), _) => Ok(*value),
        Expr::Literal(ScalarValue::Int32(Some(value)), _) => Ok(*value as i64),
        Expr::Literal(ScalarValue::UInt64(Some(value)), _) => {
            i64::try_from(*value).map_err(|_| DataFusionError::Plan("k is too large".into()))
        }
        Expr::Literal(ScalarValue::UInt32(Some(value)), _) => Ok(*value as i64),
        _ => Err(DataFusionError::Plan("k must be an integer literal".into())),
    }
}
