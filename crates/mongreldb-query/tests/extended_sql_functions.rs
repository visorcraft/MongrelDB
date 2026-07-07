use arrow::array::{
    Array, Float64Array, Int64Array, LargeStringArray, StringArray, StringViewArray,
};
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
            default_value: None,
        }],
        indexes: Vec::new(),
        colocation: Vec::new(),
        constraints: Default::default(),
        clustered: false,
    }
}

fn session() -> (tempfile::TempDir, MongrelSession) {
    let dir = tempdir().unwrap();
    let db = Table::create(dir.path(), schema(), 1).unwrap();
    (dir, MongrelSession::new(db))
}

fn string_value(batches: &[arrow::record_batch::RecordBatch], column: usize) -> String {
    let array = batches[0].column(column);
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        return strings.value(0).to_string();
    }
    if let Some(strings) = array.as_any().downcast_ref::<LargeStringArray>() {
        return strings.value(0).to_string();
    }
    if let Some(strings) = array.as_any().downcast_ref::<StringViewArray>() {
        return strings.value(0).to_string();
    }
    panic!(
        "column {column} is not string-like: {:?}",
        array.data_type()
    );
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

fn is_null(batches: &[arrow::record_batch::RecordBatch], column: usize) -> bool {
    batches[0].column(column).is_null(0)
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

    let batches = session
        .run(
            "select \
             json_array(1, 'x', null) as arr, \
             json_object('a', 1, 'b', 'x') as obj, \
             json_patch('{\"a\":1,\"b\":2}', '{\"b\":null,\"c\":3}') as patched, \
             json_replace('{\"a\":1}', '$.a', 9, '$.b', 2) as replaced, \
             json_error_position('{\"a\":1}') as ok_pos, \
             json_error_position('{\"a\":') as err_pos, \
             json_pretty('{\"a\":[1,2]}') as pretty",
        )
        .await
        .unwrap();

    assert_eq!(string_value(&batches, 0), "[1,\"x\",null]");
    assert_eq!(string_value(&batches, 1), "{\"a\":1,\"b\":\"x\"}");
    assert_eq!(string_value(&batches, 2), "{\"a\":1,\"c\":3}");
    assert_eq!(string_value(&batches, 3), "{\"a\":9}");
    assert_eq!(int_value(&batches, 4), 0);
    assert!(int_value(&batches, 5) > 0);
    assert!(string_value(&batches, 6).contains('\n'));

    let batches = session
        .run(
            "select \
             json_array_insert('[1,3]', '$[1]', 2) as inserted, \
             jsonb_extract('{\"a\":[1,2]}', '$.a') as extracted, \
             jsonb_set('{\"a\":1}', '$.b', 2) as set_value, \
             jsonb_patch('{\"a\":1}', '{\"b\":2}') as patched",
        )
        .await
        .unwrap();
    assert_eq!(string_value(&batches, 0), "[1,2,3]");
    assert_eq!(string_value(&batches, 1), "[1,2]");
    assert_eq!(string_value(&batches, 2), "{\"a\":1,\"b\":2}");
    assert_eq!(string_value(&batches, 3), "{\"a\":1,\"b\":2}");
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

    let batches = session
        .run(
            "select \
             glob('a*', 'abcdef') as matched, \
             length('hé') as char_len, \
             octet_length('hé') as byte_len, \
             replace('abcabc', 'ab', 'X') as replaced, \
             round(12.345, 2) as rounded, \
             sign(-9) as signed, \
             substr('abcdef', 2, 3) as sliced, \
             typeof(1.5) as ty, \
             unicode('Az') as codepoint, \
             lower('AZ') as lowered, \
             upper('az') as uppered, \
             trim('  x  ') as trimmed, \
             quote(null) as quoted_null, \
             hex(zeroblob(2)) as zeros, \
             length(randomblob(4)) as random_len, \
             ifnull(null, 'fallback') as ifnull_value, \
             iif(1, 'yes', 'no') as iif_value, \
             nullif('same', 'same') as nullif_value",
        )
        .await
        .unwrap();

    assert_eq!(int_value(&batches, 0), 1);
    assert_eq!(int_value(&batches, 1), 2);
    assert_eq!(int_value(&batches, 2), 3);
    assert_eq!(string_value(&batches, 3), "XcXc");
    assert!((float_value(&batches, 4) - 12.35).abs() < f64::EPSILON);
    assert_eq!(int_value(&batches, 5), -1);
    assert_eq!(string_value(&batches, 6), "bcd");
    assert_eq!(string_value(&batches, 7), "real");
    assert_eq!(int_value(&batches, 8), 65);
    assert_eq!(string_value(&batches, 9), "az");
    assert_eq!(string_value(&batches, 10), "AZ");
    assert_eq!(string_value(&batches, 11), "x");
    assert_eq!(string_value(&batches, 12), "NULL");
    assert_eq!(string_value(&batches, 13), "0000");
    assert_eq!(int_value(&batches, 14), 4);
    assert_eq!(string_value(&batches, 15), "fallback");
    assert_eq!(string_value(&batches, 16), "yes");
    assert!(is_null(&batches, 17));

    let batches = session
        .run(
            "select \
             glob('a[bc]d', 'abd') as class_match, \
             glob('a[^bc]d', 'aed') as negated_match, \
             glob('a[a-c]d', 'acd') as range_match, \
             printf('%04d|%-5s|%+d|%#x|%.2f|%.3s|%Q|%c', 7, 'x', 5, 26, 1.234, 'abcdef', 'a''b', 65) as printed",
        )
        .await
        .unwrap();
    assert_eq!(int_value(&batches, 0), 1);
    assert_eq!(int_value(&batches, 1), 1);
    assert_eq!(int_value(&batches, 2), 1);
    assert_eq!(
        string_value(&batches, 3),
        "0007|x    |+5|0x1a|1.23|abc|'a''b'|A"
    );

    let batches = session
        .run(
            "select \
             abs(-7) as absolute, \
             concat('a', null, 'b') as joined, \
             concat_ws('-', 'a', null, 'b') as joined_ws, \
             like('a_%', 'Abc') as like_match, \
             coalesce(null, 5, 3) as coalesced, \
             max(1, 5, 3) as scalar_max, \
             min(1, 5, 3) as scalar_min, \
             soundex('Robert') as sx, \
             unistr('A\\u0042') as uni, \
             unistr_quote('a\\b') as uni_quote, \
             mongreldb_compileoption_get(0) as compile_get, \
             mongreldb_compileoption_used('WAL') as compile_used, \
             mongreldb_offset(1) as off, \
             mongreldb_version() as compat_version, \
             mongreldb_source_id() as source_id",
        )
        .await
        .unwrap();
    assert_eq!(int_value(&batches, 0), 7);
    assert_eq!(string_value(&batches, 1), "ab");
    assert_eq!(string_value(&batches, 2), "a-b");
    assert_eq!(int_value(&batches, 3), 1);
    assert_eq!(int_value(&batches, 4), 5);
    assert_eq!(int_value(&batches, 5), 5);
    assert_eq!(int_value(&batches, 6), 1);
    assert_eq!(string_value(&batches, 7), "R163");
    assert_eq!(string_value(&batches, 8), "AB");
    assert_eq!(string_value(&batches, 9), "'a\\u005Cb'");
    assert_eq!(string_value(&batches, 10), "DATAFUSION_54");
    assert_eq!(int_value(&batches, 11), 1);
    assert!(is_null(&batches, 12));
    assert!(!string_value(&batches, 13).is_empty());
    assert!(string_value(&batches, 14).contains("mongreldb-query"));

    let batches = session
        .run(
            "select \
             acos(1) as acos_value, \
             atan2(1, 1) as atan2_value, \
             ceil(1.2) as ceil_value, \
             floor(1.8) as floor_value, \
             degrees(pi()) as deg_value, \
             radians(180) as rad_value, \
             log(10) as log10_value, \
             log(2, 8) as log_base_value, \
             sqrt(9) as sqrt_value",
        )
        .await
        .unwrap();
    assert!((float_value(&batches, 0) - 0.0).abs() < f64::EPSILON);
    assert!((float_value(&batches, 1) - std::f64::consts::FRAC_PI_4).abs() < 1e-12);
    assert!((float_value(&batches, 2) - 2.0).abs() < f64::EPSILON);
    assert!((float_value(&batches, 3) - 1.0).abs() < f64::EPSILON);
    assert!((float_value(&batches, 4) - 180.0).abs() < 1e-12);
    assert!((float_value(&batches, 5) - std::f64::consts::PI).abs() < 1e-12);
    assert!((float_value(&batches, 6) - 1.0).abs() < f64::EPSILON);
    assert!((float_value(&batches, 7) - 3.0).abs() < f64::EPSILON);
    assert!((float_value(&batches, 8) - 3.0).abs() < f64::EPSILON);
}

#[tokio::test]
async fn json_each_table_function_expands_top_level_json() {
    let (_dir, session) = session();
    let batches = session
        .run(
            "select key, value, type, atom, fullkey, path \
             from json_each('[10,{\"a\":1}]') order by id",
        )
        .await
        .unwrap();

    assert_eq!(string_value(&batches, 0), "0");
    assert_eq!(string_value(&batches, 1), "10");
    assert_eq!(string_value(&batches, 2), "integer");
    assert_eq!(string_value(&batches, 3), "10");
    assert_eq!(string_value(&batches, 4), "$[0]");
    assert_eq!(string_value(&batches, 5), "$");

    let values = batches[0]
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert_eq!(values.value(1), "{\"a\":1}");
    let atoms = batches[0]
        .column(3)
        .as_any()
        .downcast_ref::<StringArray>()
        .unwrap();
    assert!(atoms.is_null(1));
}

#[tokio::test]
async fn json_tree_table_function_expands_recursively() {
    let (_dir, session) = session();
    let batches = session
        .run(
            "select atom, parent, path \
             from json_tree('{\"a\":[1,{\"b\":2}],\"weird.key\":{\"x\":3}}') \
             where fullkey = '$.a[1].b'",
        )
        .await
        .unwrap();

    assert_eq!(string_value(&batches, 0), "2");
    assert!(int_value(&batches, 1) > 0);
    assert_eq!(string_value(&batches, 2), "$.a[1]");

    let batches = session
        .run(
            "select fullkey, path \
             from json_tree('{\"weird.key\":{\"x\":3}}', '$[\"weird.key\"]') \
             where key = 'x'",
        )
        .await
        .unwrap();
    assert_eq!(string_value(&batches, 0), "$[\"weird.key\"].x");
    assert_eq!(string_value(&batches, 1), "$[\"weird.key\"]");

    let batches = session
        .run("select atom from jsonb_tree('{\"a\":[1]}') where fullkey = '$.a[0]'")
        .await
        .unwrap();
    assert_eq!(string_value(&batches, 0), "1");
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
