use arrow::array::{Array, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use mongreldb_core::schema::{ColumnDef, ColumnFlags, Schema, TypeId};
use mongreldb_core::Table;
use mongreldb_query::MongrelSession;
use tempfile::tempdir;

fn schema() -> Schema {
    Schema {
        schema_id: 1,
        columns: vec![ColumnDef {
            id: 1,
            name: "id".into(),
            ty: TypeId::Int64,
            flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
    }
}

fn session() -> (tempfile::TempDir, MongrelSession) {
    let dir = tempdir().unwrap();
    let db = Table::create(dir.path(), schema(), 1).unwrap();
    (dir, MongrelSession::new(db))
}

fn string_value(batches: &[arrow::record_batch::RecordBatch], column: usize) -> String {
    batches[0]
        .column(column)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap()
        .value(0)
        .to_string()
}

fn int_value(batches: &[arrow::record_batch::RecordBatch], column: usize) -> i64 {
    batches[0]
        .column(column)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0)
}

fn float_value(batches: &[arrow::record_batch::RecordBatch], column: usize) -> f64 {
    batches[0]
        .column(column)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap()
        .value(0)
}

#[tokio::test]
async fn extended_date_time_functions() {
    let (_dir, session) = session();
    let batches = session
        .run(
            "select \
             date('2024-02-03 04:05:06') as d, \
             time('2024-02-03 04:05:06') as t, \
             datetime('2024-02-03 04:05:06', '+1 day', 'start of day') as dt, \
             unixepoch('1970-01-02 00:00:00') as ep, \
             julianday('1970-01-01 00:00:00') as jd, \
             strftime('%Y/%m/%d %H:%M:%S', '2024-02-03 04:05:06') as sf, \
             timediff('2024-02-03 04:05:08', '2024-02-03 04:05:06') as diff",
        )
        .await
        .unwrap();

    assert_eq!(string_value(&batches, 0), "2024-02-03");
    assert_eq!(string_value(&batches, 1), "04:05:06");
    assert_eq!(string_value(&batches, 2), "2024-02-04 00:00:00");
    assert_eq!(int_value(&batches, 3), 86_400);
    assert!((float_value(&batches, 4) - 2_440_587.5).abs() < f64::EPSILON);
    assert_eq!(string_value(&batches, 5), "2024/02/03 04:05:06");
    assert!((float_value(&batches, 6) - 2.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn extended_json_functions() {
    let (_dir, session) = session();
    let batches = session
        .run(
            "select \
             json_valid('{\"a\":[1,{\"b\":\"x\"}]}') as valid, \
             json_extract('{\"a\":[1,{\"b\":\"x\"}]}', '$.a[1].b') as extracted, \
             json_type('{\"a\":[1]}', '$.a') as ty, \
             json_array_length('{\"a\":[1,2,3]}', '$.a') as len, \
             json_set('{\"a\":1}', '$.b', 2) as set_value, \
             json_insert('{\"a\":1}', '$.a', 9) as inserted, \
             json_remove('{\"a\":1,\"b\":2}', '$.a') as removed, \
             json_quote('a''b') as quoted",
        )
        .await
        .unwrap();

    assert_eq!(int_value(&batches, 0), 1);
    assert_eq!(string_value(&batches, 1), "x");
    assert_eq!(string_value(&batches, 2), "array");
    assert_eq!(int_value(&batches, 3), 3);
    assert_eq!(string_value(&batches, 4), "{\"a\":1,\"b\":2}");
    assert_eq!(string_value(&batches, 5), "{\"a\":1}");
    assert_eq!(string_value(&batches, 6), "{\"b\":2}");
    assert_eq!(string_value(&batches, 7), "\"a'b\"");
}

#[tokio::test]
async fn extended_string_and_math_functions() {
    let (_dir, session) = session();
    let batches = session
        .run(
            "select \
             instr('abcdef', 'cd') as pos, \
             quote('a''b') as quoted, \
             hex('Az') as hexed, \
             unhex('417A') as unhexed, \
             printf('hi %s %d', 'x', 7) as printed, \
             format('%s-%d', 'n', 2) as formatted, \
             char(65, 66) as chars, \
             mod(7, 3) as rem, \
             pow(2, 3) as power",
        )
        .await
        .unwrap();

    assert_eq!(int_value(&batches, 0), 3);
    assert_eq!(string_value(&batches, 1), "'a''b'");
    assert_eq!(string_value(&batches, 2), "417A");
    assert_eq!(string_value(&batches, 3), "Az");
    assert_eq!(string_value(&batches, 4), "hi x 7");
    assert_eq!(string_value(&batches, 5), "n-2");
    assert_eq!(string_value(&batches, 6), "AB");
    assert!((float_value(&batches, 7) - 1.0).abs() < f64::EPSILON);
    assert!((float_value(&batches, 8) - 8.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn volatile_extended_functions_bypass_result_cache() {
    let (_dir, session) = session();
    let first = session.run("select random () as r").await.unwrap();
    let second = session.run("select random () as r").await.unwrap();
    assert_ne!(int_value(&first, 0), int_value(&second, 0));
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MeaningUdf {
    signature: Signature,
}

impl MeaningUdf {
    fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::Nullary, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MeaningUdf {
    fn name(&self) -> &str {
        "meaning_of_test"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(42))))
    }
}

#[tokio::test]
async fn custom_scalar_udfs_can_be_registered_on_session() {
    let (_dir, session) = session();
    session.register_scalar_udf(ScalarUDF::from(MeaningUdf::new()));
    let batches = session
        .run("select meaning_of_test() as value")
        .await
        .unwrap();
    assert_eq!(int_value(&batches, 0), 42);
}
