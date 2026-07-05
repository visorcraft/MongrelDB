//! Extended SQL Functions: application-oriented scalar functions layered on
//! DataFusion for date/time, JSON, string, and math compatibility.

use arrow::array::{ArrayRef, BinaryArray, Float64Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use chrono::{
    DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc,
};
use datafusion::catalog::{TableFunctionArgs, TableFunctionImpl, TableProvider};
use datafusion::common::{DataFusionError, Result as DFResult, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::logical_expr::{
    ColumnarValue, Expr as DfExpr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion::prelude::SessionContext;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static RANDOM_STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

#[derive(Debug, Default)]
pub struct ExtendedSqlState {
    last_changes: AtomicU64,
    total_changes: AtomicU64,
    last_insert_rowid: AtomicU64,
}

impl ExtendedSqlState {
    pub fn record_changes(&self, changes: u64, last_insert_rowid: Option<u64>) {
        self.last_changes.store(changes, Ordering::Relaxed);
        self.total_changes.fetch_add(changes, Ordering::Relaxed);
        if let Some(rowid) = last_insert_rowid {
            self.last_insert_rowid.store(rowid, Ordering::Relaxed);
        }
    }

    fn last_changes(&self) -> u64 {
        self.last_changes.load(Ordering::Relaxed)
    }

    fn total_changes(&self) -> u64 {
        self.total_changes.load(Ordering::Relaxed)
    }

    fn last_insert_rowid(&self) -> u64 {
        self.last_insert_rowid.load(Ordering::Relaxed)
    }
}

pub fn register_extended_sql_functions(ctx: &SessionContext) {
    register_extended_sql_functions_with_state(ctx, Arc::new(ExtendedSqlState::default()));
}

pub fn register_extended_sql_functions_with_state(
    ctx: &SessionContext,
    state: Arc<ExtendedSqlState>,
) {
    for func in extended_functions() {
        ctx.register_udf(ScalarUDF::from(func.with_state(Arc::clone(&state))));
    }
    ctx.register_udtf("json_each", Arc::new(JsonEachFunc));
    ctx.register_udtf("json_tree", Arc::new(JsonTreeFunc));
    ctx.register_udtf("jsonb_each", Arc::new(JsonbEachFunc));
    ctx.register_udtf("jsonb_tree", Arc::new(JsonbTreeFunc));
    ctx.register_udtf("series", Arc::new(SeriesFunc));
}

pub fn extended_sql_function_names() -> Vec<&'static str> {
    vec![
        "date",
        "time",
        "datetime",
        "julianday",
        "unixepoch",
        "strftime",
        "timediff",
        "json",
        "jsonb",
        "json_valid",
        "json_extract",
        "jsonb_extract",
        "json_type",
        "json_array_length",
        "json_quote",
        "json_array",
        "jsonb_array",
        "json_object",
        "jsonb_object",
        "json_patch",
        "jsonb_patch",
        "json_pretty",
        "json_error_position",
        "json_array_insert",
        "jsonb_array_insert",
        "json_set",
        "jsonb_set",
        "json_insert",
        "jsonb_insert",
        "json_replace",
        "jsonb_replace",
        "json_remove",
        "jsonb_remove",
        "json_each",
        "json_tree",
        "jsonb_each",
        "jsonb_tree",
        "series",
        "abs",
        "changes",
        "instr",
        "last_insert_rowid",
        "quote",
        "hex",
        "unhex",
        "printf",
        "format",
        "char",
        "coalesce",
        "concat",
        "concat_ws",
        "like",
        "mod",
        "pow",
        "power",
        "pi",
        "random",
        "randomblob",
        "zeroblob",
        "glob",
        "regexp",
        "length",
        "octet_length",
        "replace",
        "round",
        "sign",
        "substr",
        "substring",
        "typeof",
        "unicode",
        "unistr",
        "unistr_quote",
        "lower",
        "upper",
        "ltrim",
        "rtrim",
        "trim",
        "likely",
        "unlikely",
        "likelihood",
        "ifnull",
        "iif",
        "if",
        "nullif",
        "soundex",
        "sqlite_compileoption_get",
        "sqlite_compileoption_used",
        "sqlite_offset",
        "sqlite_source_id",
        "sqlite_version",
        "total_changes",
        "load_extension",
        "acos",
        "acosh",
        "asin",
        "asinh",
        "atan",
        "atan2",
        "atanh",
        "ceil",
        "ceiling",
        "cos",
        "cosh",
        "degrees",
        "exp",
        "floor",
        "ln",
        "log",
        "log10",
        "log2",
        "radians",
        "sin",
        "sinh",
        "sqrt",
        "tan",
        "tanh",
    ]
}

pub fn contains_volatile_extended_function(sql: &str) -> bool {
    let lower = sql.to_ascii_lowercase();
    [
        "date",
        "time",
        "datetime",
        "julianday",
        "unixepoch",
        "strftime",
        "timediff",
        "random",
        "randomblob",
    ]
    .iter()
    .any(|name| contains_function_call(&lower, name))
}

fn contains_function_call(sql: &str, name: &str) -> bool {
    let mut offset = 0;
    while let Some(pos) = sql[offset..].find(name) {
        let start = offset + pos;
        let end = start + name.len();
        let before_boundary = start == 0 || !is_ident_byte(sql.as_bytes()[start - 1]);
        let after_boundary = sql
            .as_bytes()
            .get(end)
            .map(|b| !is_ident_byte(*b))
            .unwrap_or(true);
        if before_boundary && after_boundary {
            let rest = &sql[end..];
            if rest.trim_start().starts_with('(') {
                return true;
            }
        }
        offset = end;
    }
    false
}

fn is_ident_byte(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'_'
}

#[derive(Debug, Clone)]
struct ExtendedSqlFunc {
    name: &'static str,
    aliases: Vec<String>,
    kind: ExtendedFuncKind,
    signature: Signature,
    state: Arc<ExtendedSqlState>,
}

impl PartialEq for ExtendedSqlFunc {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
            && self.aliases == other.aliases
            && self.kind == other.kind
            && self.signature == other.signature
    }
}

impl Eq for ExtendedSqlFunc {}

impl Hash for ExtendedSqlFunc {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.aliases.hash(state);
        self.kind.hash(state);
        self.signature.hash(state);
    }
}

impl ExtendedSqlFunc {
    fn new(name: &'static str, kind: ExtendedFuncKind, volatility: Volatility) -> Self {
        Self {
            name,
            aliases: Vec::new(),
            kind,
            signature: Signature::variadic_any(volatility),
            state: Arc::new(ExtendedSqlState::default()),
        }
    }

    fn zero_or_more(name: &'static str, kind: ExtendedFuncKind, volatility: Volatility) -> Self {
        Self {
            name,
            aliases: Vec::new(),
            kind,
            signature: Signature::new(
                TypeSignature::OneOf(vec![TypeSignature::Nullary, TypeSignature::VariadicAny]),
                volatility,
            ),
            state: Arc::new(ExtendedSqlState::default()),
        }
    }

    fn nullary(name: &'static str, kind: ExtendedFuncKind, volatility: Volatility) -> Self {
        Self {
            name,
            aliases: Vec::new(),
            kind,
            signature: Signature::new(TypeSignature::Nullary, volatility),
            state: Arc::new(ExtendedSqlState::default()),
        }
    }

    fn with_aliases(mut self, aliases: &[&str]) -> Self {
        self.aliases = aliases.iter().map(|s| s.to_string()).collect();
        self
    }

    fn with_state(mut self, state: Arc<ExtendedSqlState>) -> Self {
        self.state = state;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum ExtendedFuncKind {
    Date,
    Time,
    DateTime,
    JulianDay,
    UnixEpoch,
    Strftime,
    TimeDiff,
    Json,
    JsonValid,
    JsonExtract,
    JsonType,
    JsonArrayLength,
    JsonQuote,
    JsonArray,
    JsonObject,
    JsonPatch,
    JsonPretty,
    JsonErrorPosition,
    JsonArrayInsert,
    JsonSet,
    JsonInsert,
    JsonReplace,
    JsonRemove,
    Abs,
    Changes,
    TotalChanges,
    LastInsertRowid,
    Instr,
    Quote,
    Hex,
    Unhex,
    Printf,
    Char,
    Coalesce,
    Concat,
    ConcatWs,
    Like,
    Mod,
    Pow,
    Pi,
    Random,
    RandomBlob,
    ZeroBlob,
    Glob,
    Regexp,
    Length,
    OctetLength,
    Replace,
    Round,
    Sign,
    Substr,
    TypeOf,
    Unicode,
    Unistr,
    UnistrQuote,
    Lower,
    Upper,
    Ltrim,
    Rtrim,
    Trim,
    Likely,
    Unlikely,
    Likelihood,
    IfNull,
    Iif,
    NullIf,
    ScalarMax,
    ScalarMin,
    Soundex,
    CompileOptionGet,
    CompileOptionUsed,
    Offset,
    SourceId,
    Version,
    LoadExtension,
    Acos,
    Acosh,
    Asin,
    Asinh,
    Atan,
    Atan2,
    Atanh,
    Ceil,
    Cos,
    Cosh,
    Degrees,
    Exp,
    Floor,
    Ln,
    Log,
    Log10,
    Log2,
    Radians,
    Sin,
    Sinh,
    Sqrt,
    Tan,
    Tanh,
}

impl ScalarUDFImpl for ExtendedSqlFunc {
    fn name(&self) -> &str {
        self.name
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _args: &[DataType]) -> DFResult<DataType> {
        Ok(match self.kind {
            ExtendedFuncKind::JsonValid
            | ExtendedFuncKind::JsonArrayLength
            | ExtendedFuncKind::Instr
            | ExtendedFuncKind::UnixEpoch
            | ExtendedFuncKind::Random
            | ExtendedFuncKind::Changes
            | ExtendedFuncKind::TotalChanges
            | ExtendedFuncKind::LastInsertRowid
            | ExtendedFuncKind::Glob
            | ExtendedFuncKind::Regexp
            | ExtendedFuncKind::Like
            | ExtendedFuncKind::Length
            | ExtendedFuncKind::OctetLength
            | ExtendedFuncKind::Sign
            | ExtendedFuncKind::Unicode
            | ExtendedFuncKind::CompileOptionUsed
            | ExtendedFuncKind::Offset
            | ExtendedFuncKind::JsonErrorPosition => DataType::Int64,
            ExtendedFuncKind::JulianDay
            | ExtendedFuncKind::TimeDiff
            | ExtendedFuncKind::Mod
            | ExtendedFuncKind::Pow
            | ExtendedFuncKind::Pi
            | ExtendedFuncKind::Round
            | ExtendedFuncKind::Acos
            | ExtendedFuncKind::Acosh
            | ExtendedFuncKind::Asin
            | ExtendedFuncKind::Asinh
            | ExtendedFuncKind::Atan
            | ExtendedFuncKind::Atan2
            | ExtendedFuncKind::Atanh
            | ExtendedFuncKind::Ceil
            | ExtendedFuncKind::Cos
            | ExtendedFuncKind::Cosh
            | ExtendedFuncKind::Degrees
            | ExtendedFuncKind::Exp
            | ExtendedFuncKind::Floor
            | ExtendedFuncKind::Ln
            | ExtendedFuncKind::Log
            | ExtendedFuncKind::Log10
            | ExtendedFuncKind::Log2
            | ExtendedFuncKind::Radians
            | ExtendedFuncKind::Sin
            | ExtendedFuncKind::Sinh
            | ExtendedFuncKind::Sqrt
            | ExtendedFuncKind::Tan
            | ExtendedFuncKind::Tanh => DataType::Float64,
            ExtendedFuncKind::RandomBlob | ExtendedFuncKind::ZeroBlob => DataType::Binary,
            ExtendedFuncKind::Abs => _args.first().cloned().unwrap_or(DataType::Float64),
            ExtendedFuncKind::Likely
            | ExtendedFuncKind::Unlikely
            | ExtendedFuncKind::Likelihood => _args.first().cloned().unwrap_or(DataType::Utf8),
            ExtendedFuncKind::Coalesce => _args
                .iter()
                .find(|ty| **ty != DataType::Null)
                .cloned()
                .unwrap_or(DataType::Utf8),
            ExtendedFuncKind::ScalarMax | ExtendedFuncKind::ScalarMin => _args
                .iter()
                .find(|ty| **ty != DataType::Null)
                .cloned()
                .unwrap_or(DataType::Utf8),
            ExtendedFuncKind::IfNull => _args
                .first()
                .filter(|ty| **ty != DataType::Null)
                .or_else(|| _args.get(1))
                .cloned()
                .unwrap_or(DataType::Utf8),
            ExtendedFuncKind::Iif => _args.get(1).cloned().unwrap_or(DataType::Utf8),
            ExtendedFuncKind::NullIf => _args.first().cloned().unwrap_or(DataType::Utf8),
            _ => DataType::Utf8,
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match self.kind {
            ExtendedFuncKind::Date => eval_string(args, |row| {
                parse_datetime_arg(row, 0).map(|dt| dt.format("%Y-%m-%d").to_string())
            }),
            ExtendedFuncKind::Time => eval_string(args, |row| {
                parse_datetime_arg(row, 0).map(|dt| dt.format("%H:%M:%S").to_string())
            }),
            ExtendedFuncKind::DateTime => eval_string(args, |row| {
                parse_datetime_arg(row, 0).map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            }),
            ExtendedFuncKind::JulianDay => eval_float(args, |row| {
                parse_datetime_arg(row, 0).map(|dt| unix_to_julian(dt.timestamp() as f64))
            }),
            ExtendedFuncKind::UnixEpoch => eval_int(args, |row| {
                parse_datetime_arg(row, 0).map(|dt| dt.timestamp())
            }),
            ExtendedFuncKind::Strftime => eval_string(args, strftime_row),
            ExtendedFuncKind::TimeDiff => eval_float(args, |row| {
                let lhs = parse_datetime_at(row, 0)?;
                let rhs = parse_datetime_at(row, 1)?;
                Some((lhs.timestamp_millis() - rhs.timestamp_millis()) as f64 / 1000.0)
            }),
            ExtendedFuncKind::Json => eval_string(args, |row| {
                let parsed = parse_json_arg(row, 0)?;
                Some(parsed.to_string())
            }),
            ExtendedFuncKind::JsonValid => eval_int(args, |row| {
                Some(
                    if row
                        .text(0)
                        .as_deref()
                        .and_then(|s| serde_json::from_str::<JsonValue>(s).ok())
                        .is_some()
                    {
                        1
                    } else {
                        0
                    },
                )
            }),
            ExtendedFuncKind::JsonExtract => eval_string(args, json_extract_row),
            ExtendedFuncKind::JsonType => eval_string(args, json_type_row),
            ExtendedFuncKind::JsonArrayLength => eval_int(args, json_array_length_row),
            ExtendedFuncKind::JsonQuote => eval_string(args, |row| {
                row.scalar_any(0)
                    .map(scalar_to_json_arg)
                    .map(|v| v.to_string())
            }),
            ExtendedFuncKind::JsonArray => eval_string(args, json_array_row),
            ExtendedFuncKind::JsonObject => eval_string(args, json_object_row),
            ExtendedFuncKind::JsonPatch => eval_string(args, json_patch_row),
            ExtendedFuncKind::JsonPretty => eval_string(args, |row| {
                let parsed = parse_json_arg(row, 0)?;
                serde_json::to_string_pretty(&parsed).ok()
            }),
            ExtendedFuncKind::JsonErrorPosition => eval_int(args, |row| {
                Some(json_error_position(row.text(0).as_deref().unwrap_or_default()) as i64)
            }),
            ExtendedFuncKind::JsonArrayInsert => eval_string(args, json_array_insert_row),
            ExtendedFuncKind::JsonSet => {
                eval_string(args, |row| json_mutate_row(row, JsonMutation::Set))
            }
            ExtendedFuncKind::JsonInsert => {
                eval_string(args, |row| json_mutate_row(row, JsonMutation::Insert))
            }
            ExtendedFuncKind::JsonReplace => {
                eval_string(args, |row| json_mutate_row(row, JsonMutation::Replace))
            }
            ExtendedFuncKind::JsonRemove => eval_string(args, json_remove_row),
            ExtendedFuncKind::Abs => eval_scalar(args, abs_scalar),
            ExtendedFuncKind::Changes => {
                let state = Arc::clone(&self.state);
                eval_int(args, move |_| Some(state.last_changes() as i64))
            }
            ExtendedFuncKind::TotalChanges => {
                let state = Arc::clone(&self.state);
                eval_int(args, move |_| Some(state.total_changes() as i64))
            }
            ExtendedFuncKind::LastInsertRowid => {
                let state = Arc::clone(&self.state);
                eval_int(args, move |_| Some(state.last_insert_rowid() as i64))
            }
            ExtendedFuncKind::Instr => eval_int(args, |row| {
                let haystack = row.text(0)?;
                let needle = row.text(1)?;
                Some(instr(&haystack, &needle))
            }),
            ExtendedFuncKind::Quote => eval_string(args, |row| row.scalar_any(0).map(sql_quote)),
            ExtendedFuncKind::Hex => eval_string(args, |row| {
                row.scalar_any(0).map(|v| hex_encode(&scalar_bytes(&v)))
            }),
            ExtendedFuncKind::Unhex => eval_string(args, |row| {
                let ignored = row.text(1).unwrap_or_default();
                let bytes = hex_decode_ignoring(&row.text(0)?, &ignored)?;
                String::from_utf8(bytes).ok()
            }),
            ExtendedFuncKind::Printf => eval_string(args, printf_row),
            ExtendedFuncKind::Char => eval_string(args, char_row),
            ExtendedFuncKind::Coalesce => eval_scalar(args, coalesce_scalar),
            ExtendedFuncKind::Concat => eval_string(args, concat_row),
            ExtendedFuncKind::ConcatWs => eval_string(args, concat_ws_row),
            ExtendedFuncKind::Like => eval_int(args, |row| {
                let pattern = row.text(0)?;
                let value = row.text(1)?;
                let escape = row.text(2).and_then(|s| s.chars().next());
                Some(i64::from(like_match_sql(&value, &pattern, escape)))
            }),
            ExtendedFuncKind::Mod => eval_float(args, |row| {
                let lhs = row.f64(0)?;
                let rhs = row.f64(1)?;
                if rhs == 0.0 {
                    None
                } else {
                    Some(lhs % rhs)
                }
            }),
            ExtendedFuncKind::Pow => eval_float(args, |row| Some(row.f64(0)?.powf(row.f64(1)?))),
            ExtendedFuncKind::Pi => eval_float(args, |_| Some(std::f64::consts::PI)),
            ExtendedFuncKind::Random => eval_int(args, |_| Some(next_random())),
            ExtendedFuncKind::RandomBlob => eval_binary(args, |row| {
                let len = row.i64(0).unwrap_or(1).max(1) as usize;
                Some(random_bytes(len))
            }),
            ExtendedFuncKind::ZeroBlob => eval_binary(args, |row| {
                let len = row.i64(0).unwrap_or(1).max(1) as usize;
                Some(vec![0; len])
            }),
            ExtendedFuncKind::Glob => eval_int(args, |row| {
                let pattern = row.text(0)?;
                let value = row.text(1)?;
                Some(if glob_match(&pattern, &value) { 1 } else { 0 })
            }),
            ExtendedFuncKind::Regexp => eval_int(args, |row| {
                let pattern = row.text(0)?;
                let value = row.text(1)?;
                // SQLite semantics: invalid regex → no match (return 0), not an error.
                match regex::Regex::new(&pattern) {
                    Ok(re) => Some(if re.is_match(&value) { 1 } else { 0 }),
                    Err(_) => Some(0),
                }
            }),
            ExtendedFuncKind::Length => eval_int(args, |row| {
                let value = row.scalar(0)?;
                Some(match value {
                    ScalarValue::Binary(Some(v))
                    | ScalarValue::LargeBinary(Some(v))
                    | ScalarValue::BinaryView(Some(v)) => v.len() as i64,
                    _ => scalar_text(&value).chars().count() as i64,
                })
            }),
            ExtendedFuncKind::OctetLength => eval_int(args, |row| {
                let value = row.scalar(0)?;
                Some(scalar_bytes(&value).len() as i64)
            }),
            ExtendedFuncKind::Replace => eval_string(args, |row| {
                let input = row.text(0)?;
                let from = row.text(1)?;
                let to = row.text(2)?;
                if from.is_empty() {
                    Some(input)
                } else {
                    Some(input.replace(&from, &to))
                }
            }),
            ExtendedFuncKind::Round => eval_float(args, |row| {
                let value = row.f64(0)?;
                let places = row.i64(1).unwrap_or(0).max(0) as i32;
                let scale = 10_f64.powi(places);
                Some((value * scale).round() / scale)
            }),
            ExtendedFuncKind::Sign => eval_int(args, |row| {
                let value = row.f64(0)?;
                Some(if value > 0.0 {
                    1
                } else if value < 0.0 {
                    -1
                } else {
                    0
                })
            }),
            ExtendedFuncKind::Substr => eval_string(args, substr_row),
            ExtendedFuncKind::TypeOf => eval_string(args, |row| {
                row.scalar_any(0).map(|v| scalar_type_name(&v).to_string())
            }),
            ExtendedFuncKind::Unicode => {
                eval_int(args, |row| row.text(0)?.chars().next().map(|ch| ch as i64))
            }
            ExtendedFuncKind::Unistr => eval_string(args, |row| unistr(&row.text(0)?)),
            ExtendedFuncKind::UnistrQuote => eval_string(args, |row| {
                row.text(0)
                    .map(|value| sql_quote(ScalarValue::Utf8(Some(unistr_quote_text(&value)))))
            }),
            ExtendedFuncKind::Lower => eval_string(args, |row| Some(row.text(0)?.to_lowercase())),
            ExtendedFuncKind::Upper => eval_string(args, |row| Some(row.text(0)?.to_uppercase())),
            ExtendedFuncKind::Ltrim => eval_string(args, |row| {
                let chars = row.text(1).unwrap_or_else(|| " ".to_string());
                Some(
                    row.text(0)?
                        .trim_start_matches(|c| chars.contains(c))
                        .to_string(),
                )
            }),
            ExtendedFuncKind::Rtrim => eval_string(args, |row| {
                let chars = row.text(1).unwrap_or_else(|| " ".to_string());
                Some(
                    row.text(0)?
                        .trim_end_matches(|c| chars.contains(c))
                        .to_string(),
                )
            }),
            ExtendedFuncKind::Trim => eval_string(args, |row| {
                let chars = row.text(1).unwrap_or_else(|| " ".to_string());
                Some(row.text(0)?.trim_matches(|c| chars.contains(c)).to_string())
            }),
            ExtendedFuncKind::Likely | ExtendedFuncKind::Unlikely => {
                eval_scalar(args, |row| row.scalar_any(0).unwrap_or(ScalarValue::Null))
            }
            ExtendedFuncKind::Likelihood => {
                eval_scalar(args, |row| row.scalar_any(0).unwrap_or(ScalarValue::Null))
            }
            ExtendedFuncKind::IfNull => eval_scalar(args, |row| {
                let first = row.scalar_any(0).unwrap_or(ScalarValue::Null);
                if first.is_null() {
                    row.scalar_any(1).unwrap_or(ScalarValue::Null)
                } else {
                    first
                }
            }),
            ExtendedFuncKind::Iif => eval_scalar(args, |row| {
                if row.truthy(0) {
                    row.scalar_any(1).unwrap_or(ScalarValue::Null)
                } else {
                    row.scalar_any(2).unwrap_or(ScalarValue::Null)
                }
            }),
            ExtendedFuncKind::NullIf => eval_scalar(args, |row| {
                let first = row.scalar_any(0).unwrap_or(ScalarValue::Null);
                let first_type = first.data_type();
                let second = row.scalar_any(1).unwrap_or(ScalarValue::Null);
                if !first.is_null()
                    && !second.is_null()
                    && scalar_text(&first) == scalar_text(&second)
                {
                    ScalarValue::try_new_null(&first_type).unwrap_or(ScalarValue::Null)
                } else {
                    first
                }
            }),
            ExtendedFuncKind::ScalarMax => eval_scalar(args, |row| scalar_extreme(row, true)),
            ExtendedFuncKind::ScalarMin => eval_scalar(args, |row| scalar_extreme(row, false)),
            ExtendedFuncKind::Soundex => eval_string(args, |row| Some(soundex(&row.text(0)?))),
            ExtendedFuncKind::CompileOptionGet => eval_string(args, |row| {
                row.i64(0)
                    .and_then(|idx| compile_options().get(idx as usize).map(|s| s.to_string()))
            }),
            ExtendedFuncKind::CompileOptionUsed => eval_int(args, |row| {
                let needle = row.text(0)?.to_ascii_uppercase();
                Some(i64::from(
                    compile_options()
                        .iter()
                        .any(|opt| opt.eq_ignore_ascii_case(&needle)),
                ))
            }),
            ExtendedFuncKind::Offset => eval_int(args, |_| None),
            ExtendedFuncKind::SourceId => eval_string(args, |_| {
                Some(format!(
                    "mongreldb-query {} extended-sql-profile",
                    env!("CARGO_PKG_VERSION")
                ))
            }),
            ExtendedFuncKind::Version => {
                eval_string(args, |_| Some(env!("CARGO_PKG_VERSION").to_string()))
            }
            ExtendedFuncKind::LoadExtension => Err(DataFusionError::Execution(
                "load_extension is disabled in MongrelDB SQL sessions".into(),
            )),
            ExtendedFuncKind::Acos => eval_float(args, |row| math_unary(row, f64::acos)),
            ExtendedFuncKind::Acosh => eval_float(args, |row| math_unary(row, f64::acosh)),
            ExtendedFuncKind::Asin => eval_float(args, |row| math_unary(row, f64::asin)),
            ExtendedFuncKind::Asinh => eval_float(args, |row| math_unary(row, f64::asinh)),
            ExtendedFuncKind::Atan => eval_float(args, |row| math_unary(row, f64::atan)),
            ExtendedFuncKind::Atan2 => eval_float(args, |row| Some(row.f64(0)?.atan2(row.f64(1)?))),
            ExtendedFuncKind::Atanh => eval_float(args, |row| math_unary(row, f64::atanh)),
            ExtendedFuncKind::Ceil => eval_float(args, |row| math_unary(row, f64::ceil)),
            ExtendedFuncKind::Cos => eval_float(args, |row| math_unary(row, f64::cos)),
            ExtendedFuncKind::Cosh => eval_float(args, |row| math_unary(row, f64::cosh)),
            ExtendedFuncKind::Degrees => eval_float(args, |row| math_unary(row, f64::to_degrees)),
            ExtendedFuncKind::Exp => eval_float(args, |row| math_unary(row, f64::exp)),
            ExtendedFuncKind::Floor => eval_float(args, |row| math_unary(row, f64::floor)),
            ExtendedFuncKind::Ln => eval_float(args, |row| math_unary(row, f64::ln)),
            ExtendedFuncKind::Log => eval_float(args, log_row),
            ExtendedFuncKind::Log10 => eval_float(args, |row| math_unary(row, f64::log10)),
            ExtendedFuncKind::Log2 => eval_float(args, |row| math_unary(row, f64::log2)),
            ExtendedFuncKind::Radians => eval_float(args, |row| math_unary(row, f64::to_radians)),
            ExtendedFuncKind::Sin => eval_float(args, |row| math_unary(row, f64::sin)),
            ExtendedFuncKind::Sinh => eval_float(args, |row| math_unary(row, f64::sinh)),
            ExtendedFuncKind::Sqrt => eval_float(args, |row| math_unary(row, f64::sqrt)),
            ExtendedFuncKind::Tan => eval_float(args, |row| math_unary(row, f64::tan)),
            ExtendedFuncKind::Tanh => eval_float(args, |row| math_unary(row, f64::tanh)),
        }
    }
}

fn extended_functions() -> Vec<ExtendedSqlFunc> {
    use ExtendedFuncKind::*;
    vec![
        ExtendedSqlFunc::zero_or_more("date", Date, Volatility::Volatile),
        ExtendedSqlFunc::zero_or_more("time", Time, Volatility::Volatile),
        ExtendedSqlFunc::zero_or_more("datetime", DateTime, Volatility::Volatile),
        ExtendedSqlFunc::zero_or_more("julianday", JulianDay, Volatility::Volatile),
        ExtendedSqlFunc::zero_or_more("unixepoch", UnixEpoch, Volatility::Volatile),
        ExtendedSqlFunc::new("strftime", Strftime, Volatility::Volatile),
        ExtendedSqlFunc::new("timediff", TimeDiff, Volatility::Volatile),
        ExtendedSqlFunc::new("json", Json, Volatility::Immutable).with_aliases(&["jsonb"]),
        ExtendedSqlFunc::new("json_valid", JsonValid, Volatility::Immutable),
        ExtendedSqlFunc::new("json_extract", JsonExtract, Volatility::Immutable)
            .with_aliases(&["jsonb_extract"]),
        ExtendedSqlFunc::new("json_type", JsonType, Volatility::Immutable),
        ExtendedSqlFunc::new("json_array_length", JsonArrayLength, Volatility::Immutable),
        ExtendedSqlFunc::new("json_quote", JsonQuote, Volatility::Immutable),
        ExtendedSqlFunc::new("json_array", JsonArray, Volatility::Immutable)
            .with_aliases(&["jsonb_array"]),
        ExtendedSqlFunc::new("json_object", JsonObject, Volatility::Immutable)
            .with_aliases(&["jsonb_object"]),
        ExtendedSqlFunc::new("json_patch", JsonPatch, Volatility::Immutable)
            .with_aliases(&["jsonb_patch"]),
        ExtendedSqlFunc::new("json_pretty", JsonPretty, Volatility::Immutable),
        ExtendedSqlFunc::new(
            "json_error_position",
            JsonErrorPosition,
            Volatility::Immutable,
        ),
        ExtendedSqlFunc::new("json_array_insert", JsonArrayInsert, Volatility::Immutable)
            .with_aliases(&["jsonb_array_insert"]),
        ExtendedSqlFunc::new("json_set", JsonSet, Volatility::Immutable)
            .with_aliases(&["jsonb_set"]),
        ExtendedSqlFunc::new("json_insert", JsonInsert, Volatility::Immutable)
            .with_aliases(&["jsonb_insert"]),
        ExtendedSqlFunc::new("json_replace", JsonReplace, Volatility::Immutable)
            .with_aliases(&["jsonb_replace"]),
        ExtendedSqlFunc::new("json_remove", JsonRemove, Volatility::Immutable)
            .with_aliases(&["jsonb_remove"]),
        ExtendedSqlFunc::new("abs", Abs, Volatility::Immutable),
        ExtendedSqlFunc::nullary("changes", Changes, Volatility::Volatile),
        ExtendedSqlFunc::nullary("total_changes", TotalChanges, Volatility::Volatile),
        ExtendedSqlFunc::nullary("last_insert_rowid", LastInsertRowid, Volatility::Volatile),
        ExtendedSqlFunc::new("instr", Instr, Volatility::Immutable),
        ExtendedSqlFunc::new("quote", Quote, Volatility::Immutable),
        ExtendedSqlFunc::new("hex", Hex, Volatility::Immutable),
        ExtendedSqlFunc::new("unhex", Unhex, Volatility::Immutable),
        ExtendedSqlFunc::new("printf", Printf, Volatility::Immutable).with_aliases(&["format"]),
        ExtendedSqlFunc::new("char", Char, Volatility::Immutable),
        ExtendedSqlFunc::new("coalesce", Coalesce, Volatility::Immutable),
        ExtendedSqlFunc::new("concat", Concat, Volatility::Immutable),
        ExtendedSqlFunc::new("concat_ws", ConcatWs, Volatility::Immutable),
        ExtendedSqlFunc::new("like", Like, Volatility::Immutable),
        ExtendedSqlFunc::new("mod", Mod, Volatility::Immutable),
        ExtendedSqlFunc::new("pow", Pow, Volatility::Immutable).with_aliases(&["power"]),
        ExtendedSqlFunc::nullary("pi", Pi, Volatility::Immutable),
        ExtendedSqlFunc::nullary("random", Random, Volatility::Volatile),
        ExtendedSqlFunc::new("randomblob", RandomBlob, Volatility::Volatile),
        ExtendedSqlFunc::new("zeroblob", ZeroBlob, Volatility::Immutable),
        ExtendedSqlFunc::new("glob", Glob, Volatility::Immutable),
        ExtendedSqlFunc::new("regexp", Regexp, Volatility::Immutable),
        ExtendedSqlFunc::new("length", Length, Volatility::Immutable),
        ExtendedSqlFunc::new("octet_length", OctetLength, Volatility::Immutable),
        ExtendedSqlFunc::new("replace", Replace, Volatility::Immutable),
        ExtendedSqlFunc::new("round", Round, Volatility::Immutable),
        ExtendedSqlFunc::new("sign", Sign, Volatility::Immutable),
        ExtendedSqlFunc::new("substr", Substr, Volatility::Immutable).with_aliases(&["substring"]),
        ExtendedSqlFunc::new("typeof", TypeOf, Volatility::Immutable),
        ExtendedSqlFunc::new("unicode", Unicode, Volatility::Immutable),
        ExtendedSqlFunc::new("unistr", Unistr, Volatility::Immutable),
        ExtendedSqlFunc::new("unistr_quote", UnistrQuote, Volatility::Immutable),
        ExtendedSqlFunc::new("lower", Lower, Volatility::Immutable),
        ExtendedSqlFunc::new("upper", Upper, Volatility::Immutable),
        ExtendedSqlFunc::new("ltrim", Ltrim, Volatility::Immutable),
        ExtendedSqlFunc::new("rtrim", Rtrim, Volatility::Immutable),
        ExtendedSqlFunc::new("trim", Trim, Volatility::Immutable),
        ExtendedSqlFunc::new("likely", Likely, Volatility::Immutable),
        ExtendedSqlFunc::new("unlikely", Unlikely, Volatility::Immutable),
        ExtendedSqlFunc::new("likelihood", Likelihood, Volatility::Immutable),
        ExtendedSqlFunc::new("ifnull", IfNull, Volatility::Immutable),
        ExtendedSqlFunc::new("iif", Iif, Volatility::Immutable).with_aliases(&["if"]),
        ExtendedSqlFunc::new("nullif", NullIf, Volatility::Immutable),
        ExtendedSqlFunc::new("__mongreldb_scalar_max", ScalarMax, Volatility::Immutable),
        ExtendedSqlFunc::new("__mongreldb_scalar_min", ScalarMin, Volatility::Immutable),
        ExtendedSqlFunc::new("soundex", Soundex, Volatility::Immutable),
        ExtendedSqlFunc::new(
            "sqlite_compileoption_get",
            CompileOptionGet,
            Volatility::Immutable,
        ),
        ExtendedSqlFunc::new(
            "sqlite_compileoption_used",
            CompileOptionUsed,
            Volatility::Immutable,
        ),
        ExtendedSqlFunc::new("sqlite_offset", Offset, Volatility::Immutable),
        ExtendedSqlFunc::nullary("sqlite_source_id", SourceId, Volatility::Immutable),
        ExtendedSqlFunc::nullary("sqlite_version", Version, Volatility::Immutable),
        ExtendedSqlFunc::new("load_extension", LoadExtension, Volatility::Volatile),
        ExtendedSqlFunc::new("acos", Acos, Volatility::Immutable),
        ExtendedSqlFunc::new("acosh", Acosh, Volatility::Immutable),
        ExtendedSqlFunc::new("asin", Asin, Volatility::Immutable),
        ExtendedSqlFunc::new("asinh", Asinh, Volatility::Immutable),
        ExtendedSqlFunc::new("atan", Atan, Volatility::Immutable),
        ExtendedSqlFunc::new("atan2", Atan2, Volatility::Immutable),
        ExtendedSqlFunc::new("atanh", Atanh, Volatility::Immutable),
        ExtendedSqlFunc::new("ceil", Ceil, Volatility::Immutable).with_aliases(&["ceiling"]),
        ExtendedSqlFunc::new("cos", Cos, Volatility::Immutable),
        ExtendedSqlFunc::new("cosh", Cosh, Volatility::Immutable),
        ExtendedSqlFunc::new("degrees", Degrees, Volatility::Immutable),
        ExtendedSqlFunc::new("exp", Exp, Volatility::Immutable),
        ExtendedSqlFunc::new("floor", Floor, Volatility::Immutable),
        ExtendedSqlFunc::new("ln", Ln, Volatility::Immutable),
        ExtendedSqlFunc::new("log", Log, Volatility::Immutable),
        ExtendedSqlFunc::new("log10", Log10, Volatility::Immutable),
        ExtendedSqlFunc::new("log2", Log2, Volatility::Immutable),
        ExtendedSqlFunc::new("radians", Radians, Volatility::Immutable),
        ExtendedSqlFunc::new("sin", Sin, Volatility::Immutable),
        ExtendedSqlFunc::new("sinh", Sinh, Volatility::Immutable),
        ExtendedSqlFunc::new("sqrt", Sqrt, Volatility::Immutable),
        ExtendedSqlFunc::new("tan", Tan, Volatility::Immutable),
        ExtendedSqlFunc::new("tanh", Tanh, Volatility::Immutable),
    ]
}

struct RowArgs {
    arrays: Vec<ArrayRef>,
    row: usize,
}

impl RowArgs {
    fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        self.scalar_any(idx).filter(|v| !v.is_null())
    }

    fn scalar_any(&self, idx: usize) -> Option<ScalarValue> {
        self.arrays
            .get(idx)
            .and_then(|arr| ScalarValue::try_from_array(arr, self.row).ok())
    }

    fn text(&self, idx: usize) -> Option<String> {
        self.scalar(idx).map(|v| scalar_text(&v))
    }

    fn i64(&self, idx: usize) -> Option<i64> {
        scalar_i64(&self.scalar(idx)?)
    }

    fn f64(&self, idx: usize) -> Option<f64> {
        scalar_f64(&self.scalar(idx)?)
    }

    fn len(&self) -> usize {
        self.arrays.len()
    }

    fn truthy(&self, idx: usize) -> bool {
        match self.scalar(idx) {
            Some(ScalarValue::Boolean(Some(v))) => v,
            Some(v) => scalar_f64(&v).is_some_and(|n| n != 0.0),
            None => false,
        }
    }
}

fn eval_string(
    args: ScalarFunctionArgs,
    mut f: impl FnMut(&RowArgs) -> Option<String>,
) -> DFResult<ColumnarValue> {
    let (arrays, rows, scalar) = expand_args(args.args, args.number_rows)?;
    let values: Vec<Option<String>> = (0..rows)
        .map(|row| {
            f(&RowArgs {
                arrays: arrays.clone(),
                row,
            })
        })
        .collect();
    finish(scalar, Arc::new(StringArray::from(values)))
}

fn eval_int(
    args: ScalarFunctionArgs,
    mut f: impl FnMut(&RowArgs) -> Option<i64>,
) -> DFResult<ColumnarValue> {
    let (arrays, rows, scalar) = expand_args(args.args, args.number_rows)?;
    let values: Vec<Option<i64>> = (0..rows)
        .map(|row| {
            f(&RowArgs {
                arrays: arrays.clone(),
                row,
            })
        })
        .collect();
    finish(scalar, Arc::new(Int64Array::from(values)))
}

fn eval_float(
    args: ScalarFunctionArgs,
    mut f: impl FnMut(&RowArgs) -> Option<f64>,
) -> DFResult<ColumnarValue> {
    let (arrays, rows, scalar) = expand_args(args.args, args.number_rows)?;
    let values: Vec<Option<f64>> = (0..rows)
        .map(|row| {
            f(&RowArgs {
                arrays: arrays.clone(),
                row,
            })
        })
        .collect();
    finish(scalar, Arc::new(Float64Array::from(values)))
}

fn eval_binary(
    args: ScalarFunctionArgs,
    mut f: impl FnMut(&RowArgs) -> Option<Vec<u8>>,
) -> DFResult<ColumnarValue> {
    let (arrays, rows, scalar) = expand_args(args.args, args.number_rows)?;
    let values: Vec<Option<Vec<u8>>> = (0..rows)
        .map(|row| {
            f(&RowArgs {
                arrays: arrays.clone(),
                row,
            })
        })
        .collect();
    let borrowed = values
        .iter()
        .map(|value| value.as_deref())
        .collect::<Vec<_>>();
    finish(scalar, Arc::new(BinaryArray::from(borrowed)))
}

fn eval_scalar(
    args: ScalarFunctionArgs,
    mut f: impl FnMut(&RowArgs) -> ScalarValue,
) -> DFResult<ColumnarValue> {
    let (arrays, rows, scalar) = expand_args(args.args, args.number_rows)?;
    let values: Vec<ScalarValue> = (0..rows)
        .map(|row| {
            f(&RowArgs {
                arrays: arrays.clone(),
                row,
            })
        })
        .collect();
    if scalar {
        Ok(ColumnarValue::Scalar(
            values.into_iter().next().unwrap_or(ScalarValue::Null),
        ))
    } else {
        Ok(ColumnarValue::Array(ScalarValue::iter_to_array(values)?))
    }
}

fn expand_args(
    args: Vec<ColumnarValue>,
    number_rows: usize,
) -> DFResult<(Vec<ArrayRef>, usize, bool)> {
    let scalar = args
        .iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)));
    let rows = if scalar {
        number_rows.max(1)
    } else {
        number_rows
    };
    let arrays = args
        .into_iter()
        .map(|arg| arg.into_array(rows))
        .collect::<DFResult<Vec<_>>>()?;
    Ok((arrays, rows, scalar))
}

fn finish(scalar: bool, arr: ArrayRef) -> DFResult<ColumnarValue> {
    if scalar {
        Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(&arr, 0)?))
    } else {
        Ok(ColumnarValue::Array(arr))
    }
}

fn scalar_text(v: &ScalarValue) -> String {
    match v {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.clone(),
        ScalarValue::Boolean(Some(v)) => {
            if *v {
                "1".into()
            } else {
                "0".into()
            }
        }
        ScalarValue::Int8(Some(v)) => v.to_string(),
        ScalarValue::Int16(Some(v)) => v.to_string(),
        ScalarValue::Int32(Some(v)) => v.to_string(),
        ScalarValue::Int64(Some(v)) => v.to_string(),
        ScalarValue::UInt8(Some(v)) => v.to_string(),
        ScalarValue::UInt16(Some(v)) => v.to_string(),
        ScalarValue::UInt32(Some(v)) => v.to_string(),
        ScalarValue::UInt64(Some(v)) => v.to_string(),
        ScalarValue::Float32(Some(v)) => v.to_string(),
        ScalarValue::Float64(Some(v)) => v.to_string(),
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v)) => String::from_utf8_lossy(v).into_owned(),
        ScalarValue::Date32(Some(days)) => {
            let date = NaiveDate::from_num_days_from_ce_opt(*days + 719_163);
            date.map(|d| d.format("%Y-%m-%d").to_string())
                .unwrap_or_default()
        }
        ScalarValue::TimestampSecond(Some(v), _) => format_unix(*v, 1),
        ScalarValue::TimestampMillisecond(Some(v), _) => format_unix(*v, 1_000),
        ScalarValue::TimestampMicrosecond(Some(v), _) => format_unix(*v, 1_000_000),
        ScalarValue::TimestampNanosecond(Some(v), _) => format_unix(*v, 1_000_000_000),
        _ => String::new(),
    }
}

fn scalar_i64(v: &ScalarValue) -> Option<i64> {
    match v {
        ScalarValue::Int8(Some(v)) => Some(*v as i64),
        ScalarValue::Int16(Some(v)) => Some(*v as i64),
        ScalarValue::Int32(Some(v)) => Some(*v as i64),
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::UInt8(Some(v)) => Some(*v as i64),
        ScalarValue::UInt16(Some(v)) => Some(*v as i64),
        ScalarValue::UInt32(Some(v)) => Some(*v as i64),
        ScalarValue::UInt64(Some(v)) => i64::try_from(*v).ok(),
        ScalarValue::Float32(Some(v)) => Some(*v as i64),
        ScalarValue::Float64(Some(v)) => Some(*v as i64),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.trim().parse().ok(),
        _ => None,
    }
}

fn scalar_f64(v: &ScalarValue) -> Option<f64> {
    match v {
        ScalarValue::Float32(Some(v)) => Some(*v as f64),
        ScalarValue::Float64(Some(v)) => Some(*v),
        ScalarValue::Int8(Some(v)) => Some(*v as f64),
        ScalarValue::Int16(Some(v)) => Some(*v as f64),
        ScalarValue::Int32(Some(v)) => Some(*v as f64),
        ScalarValue::Int64(Some(v)) => Some(*v as f64),
        ScalarValue::UInt8(Some(v)) => Some(*v as f64),
        ScalarValue::UInt16(Some(v)) => Some(*v as f64),
        ScalarValue::UInt32(Some(v)) => Some(*v as f64),
        ScalarValue::UInt64(Some(v)) => Some(*v as f64),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.trim().parse().ok(),
        _ => None,
    }
}

fn scalar_bytes(v: &ScalarValue) -> Vec<u8> {
    match v {
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v)) => v.clone(),
        _ => scalar_text(v).into_bytes(),
    }
}

fn scalar_to_json_arg(v: ScalarValue) -> JsonValue {
    match v {
        ScalarValue::Boolean(Some(v)) => JsonValue::Bool(v),
        ScalarValue::Int8(Some(v)) => JsonValue::Number(JsonNumber::from(v)),
        ScalarValue::Int16(Some(v)) => JsonValue::Number(JsonNumber::from(v)),
        ScalarValue::Int32(Some(v)) => JsonValue::Number(JsonNumber::from(v)),
        ScalarValue::Int64(Some(v)) => JsonValue::Number(JsonNumber::from(v)),
        ScalarValue::UInt8(Some(v)) => JsonValue::Number(JsonNumber::from(v)),
        ScalarValue::UInt16(Some(v)) => JsonValue::Number(JsonNumber::from(v)),
        ScalarValue::UInt32(Some(v)) => JsonValue::Number(JsonNumber::from(v)),
        ScalarValue::UInt64(Some(v)) => JsonValue::Number(JsonNumber::from(v)),
        ScalarValue::Float32(Some(v)) => JsonNumber::from_f64(v as f64)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ScalarValue::Float64(Some(v)) => JsonNumber::from_f64(v)
            .map(JsonValue::Number)
            .unwrap_or(JsonValue::Null),
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => JsonValue::String(s),
        ScalarValue::Null => JsonValue::Null,
        _ if v.is_null() => JsonValue::Null,
        _ => JsonValue::String(scalar_text(&v)),
    }
}

fn scalar_type_name(v: &ScalarValue) -> &'static str {
    if v.is_null() {
        return "null";
    }
    match v {
        ScalarValue::Boolean(_) => "integer",
        ScalarValue::Int8(_)
        | ScalarValue::Int16(_)
        | ScalarValue::Int32(_)
        | ScalarValue::Int64(_)
        | ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_) => "integer",
        ScalarValue::Float16(_) | ScalarValue::Float32(_) | ScalarValue::Float64(_) => "real",
        ScalarValue::Binary(_) | ScalarValue::LargeBinary(_) | ScalarValue::BinaryView(_) => "blob",
        _ => "text",
    }
}

fn abs_scalar(row: &RowArgs) -> ScalarValue {
    let Some(value) = row.scalar_any(0) else {
        return ScalarValue::Null;
    };
    match value {
        ScalarValue::Int8(Some(v)) => ScalarValue::Int8(v.checked_abs()),
        ScalarValue::Int16(Some(v)) => ScalarValue::Int16(v.checked_abs()),
        ScalarValue::Int32(Some(v)) => ScalarValue::Int32(v.checked_abs()),
        ScalarValue::Int64(Some(v)) => ScalarValue::Int64(v.checked_abs()),
        ScalarValue::UInt8(_)
        | ScalarValue::UInt16(_)
        | ScalarValue::UInt32(_)
        | ScalarValue::UInt64(_) => value,
        ScalarValue::Float32(Some(v)) => ScalarValue::Float32(Some(v.abs())),
        ScalarValue::Float64(Some(v)) => ScalarValue::Float64(Some(v.abs())),
        v if v.is_null() => v,
        v => scalar_f64(&v)
            .map(|n| ScalarValue::Float64(Some(n.abs())))
            .unwrap_or(ScalarValue::Float64(Some(0.0))),
    }
}

fn coalesce_scalar(row: &RowArgs) -> ScalarValue {
    for idx in 0..row.len() {
        let value = row.scalar_any(idx).unwrap_or(ScalarValue::Null);
        if !value.is_null() {
            return value;
        }
    }
    ScalarValue::Null
}

fn concat_row(row: &RowArgs) -> Option<String> {
    let mut out = String::new();
    for idx in 0..row.len() {
        if let Some(value) = row.scalar(idx) {
            out.push_str(&scalar_text(&value));
        }
    }
    Some(out)
}

fn concat_ws_row(row: &RowArgs) -> Option<String> {
    let separator = row.text(0)?;
    let parts = (1..row.len())
        .filter_map(|idx| row.scalar(idx).map(|value| scalar_text(&value)))
        .collect::<Vec<_>>();
    Some(parts.join(&separator))
}

fn scalar_extreme(row: &RowArgs, max: bool) -> ScalarValue {
    let mut best: Option<ScalarValue> = None;
    for idx in 0..row.len() {
        let value = row.scalar_any(idx).unwrap_or(ScalarValue::Null);
        if value.is_null() {
            return ScalarValue::Null;
        }
        let replace = best
            .as_ref()
            .map(|current| {
                let ordering = compare_scalar_values(&value, current);
                if max {
                    ordering == std::cmp::Ordering::Greater
                } else {
                    ordering == std::cmp::Ordering::Less
                }
            })
            .unwrap_or(true);
        if replace {
            best = Some(value);
        }
    }
    best.unwrap_or(ScalarValue::Null)
}

fn compare_scalar_values(left: &ScalarValue, right: &ScalarValue) -> std::cmp::Ordering {
    match (scalar_f64(left), scalar_f64(right)) {
        (Some(a), Some(b)) => a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal),
        _ => scalar_text(left).cmp(&scalar_text(right)),
    }
}

fn like_match_sql(value: &str, pattern: &str, escape: Option<char>) -> bool {
    fn rec(value: &[char], pattern: &[char], escape: Option<char>) -> bool {
        let Some((&head, rest)) = pattern.split_first() else {
            return value.is_empty();
        };
        if Some(head) == escape {
            let Some((&literal, rest)) = rest.split_first() else {
                return value.is_empty();
            };
            return value.first() == Some(&literal) && rec(&value[1..], rest, escape);
        }
        match head {
            '%' => {
                rec(value, rest, escape) || (!value.is_empty() && rec(&value[1..], pattern, escape))
            }
            '_' => !value.is_empty() && rec(&value[1..], rest, escape),
            ch => {
                value
                    .first()
                    .is_some_and(|value_ch| value_ch.eq_ignore_ascii_case(&ch))
                    && rec(&value[1..], rest, escape)
            }
        }
    }
    rec(
        &value.chars().collect::<Vec<_>>(),
        &pattern.chars().collect::<Vec<_>>(),
        escape,
    )
}

fn compile_options() -> &'static [&'static str] {
    &[
        "DATAFUSION_54",
        "EXTENDED_SQL_FUNCTIONS",
        "JSON_TABLE_FUNCTIONS",
        "LOG_STRUCTURED_STORAGE",
        "MVCC",
        "WAL",
    ]
}

fn math_unary(row: &RowArgs, f: impl FnOnce(f64) -> f64) -> Option<f64> {
    let out = f(row.f64(0)?);
    if out.is_nan() {
        None
    } else {
        Some(out)
    }
}

fn log_row(row: &RowArgs) -> Option<f64> {
    let out = if row.len() == 1 {
        row.f64(0)?.log10()
    } else {
        row.f64(1)?.log(row.f64(0)?)
    };
    if out.is_nan() {
        None
    } else {
        Some(out)
    }
}

fn soundex(input: &str) -> String {
    let mut letters = input.chars().filter(|ch| ch.is_ascii_alphabetic());
    let Some(first) = letters.next() else {
        return "?000".to_string();
    };
    let first = first.to_ascii_uppercase();
    let mut out = String::with_capacity(4);
    out.push(first);
    let mut last = soundex_code(first);
    for ch in letters {
        let code = soundex_code(ch.to_ascii_uppercase());
        if code != '0' && code != last {
            out.push(code);
            if out.len() == 4 {
                break;
            }
        }
        last = code;
    }
    while out.len() < 4 {
        out.push('0');
    }
    out
}

fn soundex_code(ch: char) -> char {
    match ch {
        'B' | 'F' | 'P' | 'V' => '1',
        'C' | 'G' | 'J' | 'K' | 'Q' | 'S' | 'X' | 'Z' => '2',
        'D' | 'T' => '3',
        'L' => '4',
        'M' | 'N' => '5',
        'R' => '6',
        _ => '0',
    }
}

fn unistr(input: &str) -> Option<String> {
    let chars = input.chars().collect::<Vec<_>>();
    let mut out = String::new();
    let mut idx = 0;
    while idx < chars.len() {
        let ch = chars[idx];
        idx += 1;
        if ch != '\\' {
            out.push(ch);
            continue;
        }
        let Some(marker) = chars.get(idx).copied() else {
            out.push('\\');
            break;
        };
        match marker {
            '\\' => {
                out.push('\\');
                idx += 1;
            }
            'u' => {
                idx += 1;
                out.push(read_hex_char(&chars, &mut idx, 4)?);
            }
            'U' => {
                idx += 1;
                out.push(read_hex_char(&chars, &mut idx, 8)?);
            }
            '+' => {
                idx += 1;
                out.push(read_hex_char(&chars, &mut idx, 6)?);
            }
            '0'..='9' | 'A'..='F' | 'a'..='f' => {
                out.push(read_hex_char(&chars, &mut idx, 4)?);
            }
            other => {
                out.push(other);
                idx += 1;
            }
        }
    }
    Some(out)
}

fn read_hex_char(chars: &[char], idx: &mut usize, width: usize) -> Option<char> {
    if *idx + width > chars.len() {
        return None;
    }
    let mut value = 0_u32;
    for _ in 0..width {
        value = value.checked_mul(16)?;
        value = value.checked_add(chars[*idx].to_digit(16)?)?;
        *idx += 1;
    }
    char::from_u32(value)
}

fn unistr_quote_text(input: &str) -> String {
    let mut out = String::new();
    for ch in input.chars() {
        match ch {
            '\u{0001}'..='\u{001f}' | '\\' => out.push_str(&format!("\\u{:04X}", ch as u32)),
            _ => out.push(ch),
        }
    }
    out
}

fn format_unix(value: i64, scale: i64) -> String {
    let secs = value / scale;
    Utc.timestamp_opt(secs, 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

fn parse_datetime_arg(row: &RowArgs, idx: usize) -> Option<DateTime<Utc>> {
    let mut dt = parse_datetime_at(row, idx)?;
    for i in (idx + 1)..row.len() {
        if let Some(modifier) = row.text(i) {
            dt = apply_datetime_modifier(dt, modifier.trim())?;
        }
    }
    Some(dt)
}

fn parse_datetime_at(row: &RowArgs, idx: usize) -> Option<DateTime<Utc>> {
    match row.scalar(idx) {
        None => Some(Utc::now()),
        Some(v) => parse_datetime_scalar(&v),
    }
}

fn parse_datetime_scalar(v: &ScalarValue) -> Option<DateTime<Utc>> {
    if let Some(seconds) = scalar_f64(v) {
        return unix_seconds_to_datetime(seconds);
    }
    let s = scalar_text(v);
    parse_datetime_text(s.trim())
}

fn parse_datetime_text(s: &str) -> Option<DateTime<Utc>> {
    if s.is_empty() || s.eq_ignore_ascii_case("now") {
        return Some(Utc::now());
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(s) {
        return Some(dt.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"] {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(Utc.from_utc_datetime(&dt));
        }
    }
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return date
            .and_hms_opt(0, 0, 0)
            .map(|dt| Utc.from_utc_datetime(&dt));
    }
    if let Ok(time) = NaiveTime::parse_from_str(s, "%H:%M:%S") {
        let date = NaiveDate::from_ymd_opt(1970, 1, 1)?;
        return Some(Utc.from_utc_datetime(&NaiveDateTime::new(date, time)));
    }
    s.parse::<f64>().ok().and_then(unix_seconds_to_datetime)
}

fn unix_seconds_to_datetime(seconds: f64) -> Option<DateTime<Utc>> {
    if !seconds.is_finite() {
        return None;
    }
    let secs = seconds.trunc() as i64;
    let nanos = ((seconds.fract().abs()) * 1_000_000_000.0) as u32;
    Utc.timestamp_opt(secs, nanos).single()
}

fn apply_datetime_modifier(dt: DateTime<Utc>, modifier: &str) -> Option<DateTime<Utc>> {
    if modifier.is_empty() || modifier.eq_ignore_ascii_case("unixepoch") {
        return Some(dt);
    }
    if modifier.eq_ignore_ascii_case("start of day") {
        return dt
            .date_naive()
            .and_hms_opt(0, 0, 0)
            .map(|v| Utc.from_utc_datetime(&v));
    }
    let mut parts = modifier.split_whitespace();
    let amount = parts.next()?.parse::<f64>().ok()?;
    let unit = parts.next()?.trim_end_matches('s').to_ascii_lowercase();
    let millis = match unit.as_str() {
        "day" => amount * 86_400_000.0,
        "hour" => amount * 3_600_000.0,
        "minute" => amount * 60_000.0,
        "second" => amount * 1_000.0,
        _ => return None,
    };
    Some(dt + Duration::milliseconds(millis as i64))
}

fn unix_to_julian(seconds: f64) -> f64 {
    seconds / 86_400.0 + 2_440_587.5
}

fn strftime_row(row: &RowArgs) -> Option<String> {
    let fmt = row.text(0)?;
    let dt = parse_datetime_arg(row, 1)?;
    let mut out = String::new();
    let mut chars = fmt.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        match chars.next()? {
            '%' => out.push('%'),
            'Y' => out.push_str(&format!("{:04}", dt.year())),
            'm' => out.push_str(&format!("{:02}", dt.month())),
            'd' => out.push_str(&format!("{:02}", dt.day())),
            'H' => out.push_str(&format!("{:02}", dt.hour())),
            'M' => out.push_str(&format!("{:02}", dt.minute())),
            'S' => out.push_str(&format!("{:02}", dt.second())),
            'f' => out.push_str(&format!(
                "{:02}.{:03}",
                dt.second(),
                dt.timestamp_subsec_millis()
            )),
            's' => out.push_str(&dt.timestamp().to_string()),
            'J' => out.push_str(&unix_to_julian(dt.timestamp() as f64).to_string()),
            other => {
                out.push('%');
                out.push(other);
            }
        }
    }
    Some(out)
}

fn parse_json_arg(row: &RowArgs, idx: usize) -> Option<JsonValue> {
    serde_json::from_str(&row.text(idx)?).ok()
}

fn json_extract_row(row: &RowArgs) -> Option<String> {
    let root = parse_json_arg(row, 0)?;
    if row.len() <= 1 {
        return json_scalar_result(&root);
    }
    let mut values = Vec::new();
    for idx in 1..row.len() {
        let path = row.text(idx)?;
        let found = json_path_get(&root, &parse_json_path(&path)?)
            .cloned()
            .unwrap_or(JsonValue::Null);
        if row.len() == 2 {
            return json_scalar_result(&found);
        }
        values.push(found);
    }
    Some(JsonValue::Array(values).to_string())
}

fn json_type_row(row: &RowArgs) -> Option<String> {
    let root = parse_json_arg(row, 0)?;
    let value = if row.len() > 1 {
        let path = row.text(1)?;
        json_path_get(&root, &parse_json_path(&path)?)?
    } else {
        &root
    };
    Some(json_type(value).to_string())
}

fn json_array_length_row(row: &RowArgs) -> Option<i64> {
    let root = parse_json_arg(row, 0)?;
    let value = if row.len() > 1 {
        let path = row.text(1)?;
        json_path_get(&root, &parse_json_path(&path)?)?
    } else {
        &root
    };
    Some(value.as_array().map(|v| v.len() as i64).unwrap_or(0))
}

#[derive(Clone, Copy)]
enum JsonMutation {
    Set,
    Insert,
    Replace,
}

fn json_mutate_row(row: &RowArgs, mode: JsonMutation) -> Option<String> {
    if row.len() < 3 {
        return None;
    }
    let mut root = parse_json_arg(row, 0)?;
    let mut idx = 1;
    while idx + 1 < row.len() {
        let path = parse_json_path(&row.text(idx)?)?;
        let value = row.scalar(idx + 1).map(scalar_to_json_arg)?;
        match mode {
            JsonMutation::Set => json_path_set(&mut root, &path, value, true),
            JsonMutation::Insert => json_path_set(&mut root, &path, value, false),
            JsonMutation::Replace => {
                if json_path_get(&root, &path).is_some() {
                    json_path_set(&mut root, &path, value, true);
                }
            }
        }
        idx += 2;
    }
    Some(root.to_string())
}

fn json_array_insert_row(row: &RowArgs) -> Option<String> {
    if row.len() < 3 {
        return None;
    }
    let mut root = parse_json_arg(row, 0)?;
    let mut idx = 1;
    while idx + 1 < row.len() {
        let path = parse_json_path(&row.text(idx)?)?;
        let value = row.scalar(idx + 1).map(scalar_to_json_arg)?;
        json_path_array_insert(&mut root, &path, value);
        idx += 2;
    }
    Some(root.to_string())
}

fn json_path_array_insert(value: &mut JsonValue, path: &[JsonPathToken], new_value: JsonValue) {
    let Some((last, parent_path)) = path.split_last() else {
        return;
    };
    let mut cur = value;
    for token in parent_path {
        cur = match token {
            JsonPathToken::Key(k) => match cur.as_object_mut().and_then(|obj| obj.get_mut(k)) {
                Some(value) => value,
                None => return,
            },
            JsonPathToken::Index(i) => match cur.as_array_mut().and_then(|arr| arr.get_mut(*i)) {
                Some(value) => value,
                None => return,
            },
        };
    }
    let JsonPathToken::Index(index) = last else {
        return;
    };
    if let Some(arr) = cur.as_array_mut() {
        if *index <= arr.len() {
            arr.insert(*index, new_value);
        }
    }
}

fn json_array_row(row: &RowArgs) -> Option<String> {
    let values = (0..row.len())
        .map(|idx| scalar_to_json_arg(row.scalar_any(idx).unwrap_or(ScalarValue::Null)))
        .collect::<Vec<_>>();
    Some(JsonValue::Array(values).to_string())
}

fn json_object_row(row: &RowArgs) -> Option<String> {
    if row.len() % 2 != 0 {
        return None;
    }
    let mut obj = JsonMap::new();
    let mut idx = 0;
    while idx + 1 < row.len() {
        let key = row.text(idx)?;
        let value = scalar_to_json_arg(row.scalar_any(idx + 1).unwrap_or(ScalarValue::Null));
        obj.insert(key, value);
        idx += 2;
    }
    Some(JsonValue::Object(obj).to_string())
}

fn json_patch_row(row: &RowArgs) -> Option<String> {
    let target = parse_json_arg(row, 0)?;
    let patch = parse_json_arg(row, 1)?;
    Some(json_merge_patch(target, patch).to_string())
}

fn json_merge_patch(target: JsonValue, patch: JsonValue) -> JsonValue {
    let JsonValue::Object(patch_obj) = patch else {
        return patch;
    };
    let mut target_obj = match target {
        JsonValue::Object(obj) => obj,
        _ => JsonMap::new(),
    };
    for (key, value) in patch_obj {
        if value.is_null() {
            target_obj.remove(&key);
        } else {
            let current = target_obj.remove(&key).unwrap_or(JsonValue::Null);
            target_obj.insert(key, json_merge_patch(current, value));
        }
    }
    JsonValue::Object(target_obj)
}

fn json_error_position(input: &str) -> usize {
    match serde_json::from_str::<JsonValue>(input) {
        Ok(_) => 0,
        Err(err) => {
            let line = err.line().max(1);
            let column = err.column();
            let mut current_line = 1;
            let mut line_start = 0;
            for (idx, ch) in input.char_indices() {
                if current_line == line {
                    break;
                }
                if ch == '\n' {
                    current_line += 1;
                    line_start = idx + ch.len_utf8();
                }
            }
            line_start + column.max(1)
        }
    }
}

fn json_remove_row(row: &RowArgs) -> Option<String> {
    let mut root = parse_json_arg(row, 0)?;
    for idx in 1..row.len() {
        let path = parse_json_path(&row.text(idx)?)?;
        json_path_remove(&mut root, &path);
    }
    Some(root.to_string())
}

fn json_scalar_result(value: &JsonValue) -> Option<String> {
    match value {
        JsonValue::Null => None,
        JsonValue::Bool(true) => Some("1".into()),
        JsonValue::Bool(false) => Some("0".into()),
        JsonValue::Number(n) => Some(n.to_string()),
        JsonValue::String(s) => Some(s.clone()),
        JsonValue::Array(_) | JsonValue::Object(_) => Some(value.to_string()),
    }
}

fn json_type(value: &JsonValue) -> &'static str {
    match value {
        JsonValue::Null => "null",
        JsonValue::Bool(true) => "true",
        JsonValue::Bool(false) => "false",
        JsonValue::Number(n) if n.is_i64() || n.is_u64() => "integer",
        JsonValue::Number(_) => "real",
        JsonValue::String(_) => "text",
        JsonValue::Array(_) => "array",
        JsonValue::Object(_) => "object",
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum JsonPathToken {
    Key(String),
    Index(usize),
}

fn parse_json_path(path: &str) -> Option<Vec<JsonPathToken>> {
    let chars = path.chars().collect::<Vec<_>>();
    if chars.first() != Some(&'$') {
        return None;
    }
    let mut idx = 1;
    let mut tokens = Vec::new();
    while idx < chars.len() {
        match chars[idx] {
            '.' => {
                idx += 1;
                let mut key = String::new();
                while idx < chars.len() {
                    let c = chars[idx];
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(c);
                    idx += 1;
                }
                if key.is_empty() {
                    return None;
                }
                tokens.push(JsonPathToken::Key(key));
            }
            '[' => {
                idx += 1;
                if idx >= chars.len() {
                    return None;
                }
                if matches!(chars[idx], '"' | '\'') {
                    let quote = chars[idx];
                    idx += 1;
                    let mut key = String::new();
                    while idx < chars.len() {
                        let c = chars[idx];
                        idx += 1;
                        if c == quote {
                            break;
                        }
                        if c == '\\' && idx < chars.len() {
                            key.push(chars[idx]);
                            idx += 1;
                        } else {
                            key.push(c);
                        }
                    }
                    if idx >= chars.len() || chars[idx] != ']' {
                        return None;
                    }
                    idx += 1;
                    tokens.push(JsonPathToken::Key(key));
                } else {
                    let mut n = String::new();
                    while idx < chars.len() && chars[idx] != ']' {
                        n.push(chars[idx]);
                        idx += 1;
                    }
                    if idx >= chars.len() || chars[idx] != ']' {
                        return None;
                    }
                    idx += 1;
                    tokens.push(JsonPathToken::Index(n.parse().ok()?));
                }
            }
            _ => return None,
        }
    }
    Some(tokens)
}

fn json_path_get<'a>(value: &'a JsonValue, path: &[JsonPathToken]) -> Option<&'a JsonValue> {
    let mut cur = value;
    for token in path {
        cur = match token {
            JsonPathToken::Key(k) => cur.as_object()?.get(k)?,
            JsonPathToken::Index(i) => cur.as_array()?.get(*i)?,
        };
    }
    Some(cur)
}

fn json_path_set(
    value: &mut JsonValue,
    path: &[JsonPathToken],
    new_value: JsonValue,
    overwrite: bool,
) {
    if path.is_empty() {
        if overwrite {
            *value = new_value;
        }
        return;
    }
    let mut cur = value;
    for token in &path[..path.len() - 1] {
        match token {
            JsonPathToken::Key(k) => {
                if !cur.is_object() {
                    *cur = JsonValue::Object(JsonMap::new());
                }
                cur = cur
                    .as_object_mut()
                    .unwrap()
                    .entry(k.clone())
                    .or_insert(JsonValue::Object(JsonMap::new()));
            }
            JsonPathToken::Index(i) => {
                if let Some(arr) = cur.as_array_mut() {
                    if *i >= arr.len() {
                        return;
                    }
                    cur = &mut arr[*i];
                } else {
                    return;
                }
            }
        }
    }
    match path.last().unwrap() {
        JsonPathToken::Key(k) => {
            if !cur.is_object() {
                *cur = JsonValue::Object(JsonMap::new());
            }
            let obj = cur.as_object_mut().unwrap();
            if overwrite || !obj.contains_key(k) {
                obj.insert(k.clone(), new_value);
            }
        }
        JsonPathToken::Index(i) => {
            if let Some(arr) = cur.as_array_mut() {
                if *i < arr.len() {
                    if overwrite {
                        arr[*i] = new_value;
                    }
                } else if *i == arr.len() {
                    arr.push(new_value);
                }
            }
        }
    }
}

fn json_path_remove(value: &mut JsonValue, path: &[JsonPathToken]) {
    if path.is_empty() {
        return;
    }
    let mut cur = value;
    for token in &path[..path.len() - 1] {
        cur = match token {
            JsonPathToken::Key(k) => match cur.as_object_mut().and_then(|o| o.get_mut(k)) {
                Some(v) => v,
                None => return,
            },
            JsonPathToken::Index(i) => match cur.as_array_mut().and_then(|a| a.get_mut(*i)) {
                Some(v) => v,
                None => return,
            },
        };
    }
    match path.last().unwrap() {
        JsonPathToken::Key(k) => {
            if let Some(obj) = cur.as_object_mut() {
                obj.remove(k);
            }
        }
        JsonPathToken::Index(i) => {
            if let Some(arr) = cur.as_array_mut() {
                if *i < arr.len() {
                    arr.remove(*i);
                }
            }
        }
    }
}

fn instr(haystack: &str, needle: &str) -> i64 {
    if needle.is_empty() {
        return 1;
    }
    haystack
        .find(needle)
        .map(|byte| haystack[..byte].chars().count() as i64 + 1)
        .unwrap_or(0)
}

fn sql_quote(v: ScalarValue) -> String {
    if v.is_null() {
        return "NULL".into();
    }
    match v {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => format!("'{}'", s.replace('\'', "''")),
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::BinaryView(Some(b)) => format!("X'{}'", hex_encode(&b)),
        _ => scalar_text(&v),
    }
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode_ignoring(value: &str, ignored: &str) -> Option<Vec<u8>> {
    let value = value.trim();
    let filtered = value
        .bytes()
        .filter(|b| !ignored.as_bytes().contains(b))
        .collect::<Vec<_>>();
    if filtered.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(filtered.len() / 2);
    for i in (0..filtered.len()).step_by(2) {
        let hi = hex_value(filtered[i])?;
        let lo = hex_value(filtered[i + 1])?;
        out.push((hi << 4) | lo);
    }
    Some(out)
}

fn hex_value(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn printf_row(row: &RowArgs) -> Option<String> {
    let format = row.text(0)?;
    let mut out = String::new();
    let mut arg_idx = 1;
    let chars = format.chars().collect::<Vec<_>>();
    let mut idx = 0;
    while idx < chars.len() {
        let ch = chars[idx];
        idx += 1;
        if ch != '%' {
            out.push(ch);
            continue;
        }
        if idx >= chars.len() {
            out.push('%');
            break;
        }
        if chars[idx] == '%' {
            out.push('%');
            idx += 1;
            continue;
        }
        let spec_start = idx;
        let flags = PrintfFlags::parse(&chars, &mut idx);
        let width = parse_decimal(&chars, &mut idx);
        let precision = if idx < chars.len() && chars[idx] == '.' {
            idx += 1;
            Some(parse_decimal(&chars, &mut idx).unwrap_or(0))
        } else {
            None
        };
        while idx < chars.len() && matches!(chars[idx], 'l' | 'z' | 't' | 'j') {
            idx += 1;
        }
        if idx >= chars.len() {
            out.push('%');
            for ch in &chars[spec_start..] {
                out.push(*ch);
            }
            break;
        }
        let spec = chars[idx];
        idx += 1;
        let formatted = match spec {
            's' | 'z' | 'q' | 'Q' => {
                let value = row.scalar(arg_idx).unwrap_or(ScalarValue::Null);
                arg_idx += 1;
                Some(format_printf_text(value, spec, precision))
            }
            'd' | 'i' | 'u' => {
                let value = row.i64(arg_idx).unwrap_or(0);
                arg_idx += 1;
                Some(format_printf_int(value, 10, false, &flags, precision))
            }
            'x' | 'X' => {
                let value = row.i64(arg_idx).unwrap_or(0);
                arg_idx += 1;
                Some(format_printf_int(value, 16, spec == 'X', &flags, precision))
            }
            'o' => {
                let value = row.i64(arg_idx).unwrap_or(0);
                arg_idx += 1;
                Some(format_printf_int(value, 8, false, &flags, precision))
            }
            'c' => {
                let value = row.scalar(arg_idx).unwrap_or(ScalarValue::Null);
                arg_idx += 1;
                Some(format_printf_char(value, precision.unwrap_or(1)))
            }
            'f' | 'F' | 'e' | 'E' | 'g' | 'G' => {
                let value = row.f64(arg_idx).unwrap_or(0.0);
                arg_idx += 1;
                Some(format_printf_float(value, spec, &flags, precision))
            }
            'n' => {
                arg_idx += 1;
                Some(String::new())
            }
            other => Some(format!("%{other}")),
        }
        .unwrap_or_default();
        out.push_str(&apply_printf_width(
            formatted,
            width,
            flags.left,
            flags.zero && !flags.left,
        ));
    }
    Some(out)
}

#[derive(Default)]
struct PrintfFlags {
    left: bool,
    plus: bool,
    space: bool,
    alternate: bool,
    zero: bool,
}

impl PrintfFlags {
    fn parse(chars: &[char], idx: &mut usize) -> Self {
        let mut flags = Self::default();
        while *idx < chars.len() {
            match chars[*idx] {
                '-' => flags.left = true,
                '+' => flags.plus = true,
                ' ' => flags.space = true,
                '#' => flags.alternate = true,
                '0' => flags.zero = true,
                ',' | '!' => {}
                _ => break,
            }
            *idx += 1;
        }
        flags
    }
}

fn parse_decimal(chars: &[char], idx: &mut usize) -> Option<usize> {
    let start = *idx;
    let mut value = 0_usize;
    while *idx < chars.len() {
        let Some(digit) = chars[*idx].to_digit(10) else {
            break;
        };
        value = value.saturating_mul(10).saturating_add(digit as usize);
        *idx += 1;
    }
    if *idx == start {
        None
    } else {
        Some(value)
    }
}

fn format_printf_text(value: ScalarValue, spec: char, precision: Option<usize>) -> String {
    if spec == 'Q' && value.is_null() {
        return "NULL".to_string();
    }
    let mut text = scalar_text(&value);
    if matches!(spec, 'q' | 'Q') {
        text = text.replace('\'', "''");
    }
    if let Some(limit) = precision {
        text = text.chars().take(limit).collect();
    }
    if spec == 'Q' {
        format!("'{text}'")
    } else {
        text
    }
}

fn format_printf_int(
    value: i64,
    radix: u32,
    uppercase: bool,
    flags: &PrintfFlags,
    precision: Option<usize>,
) -> String {
    let negative = value < 0 && radix == 10;
    let magnitude = if negative {
        value.unsigned_abs()
    } else {
        value as u64
    };
    let mut digits = match radix {
        8 => format!("{magnitude:o}"),
        16 if uppercase => format!("{magnitude:X}"),
        16 => format!("{magnitude:x}"),
        _ => magnitude.to_string(),
    };
    if let Some(precision) = precision {
        if precision == 0 && magnitude == 0 {
            digits.clear();
        } else if digits.len() < precision {
            digits = format!("{}{}", "0".repeat(precision - digits.len()), digits);
        }
    }
    let mut out = String::new();
    if negative {
        out.push('-');
    } else if flags.plus && radix == 10 {
        out.push('+');
    } else if flags.space && radix == 10 {
        out.push(' ');
    }
    if flags.alternate {
        match radix {
            8 if !digits.starts_with('0') => out.push('0'),
            16 if uppercase && magnitude != 0 => out.push_str("0X"),
            16 if magnitude != 0 => out.push_str("0x"),
            _ => {}
        }
    }
    out.push_str(&digits);
    out
}

fn format_printf_float(
    value: f64,
    spec: char,
    flags: &PrintfFlags,
    precision: Option<usize>,
) -> String {
    let precision = precision.unwrap_or(6);
    let mut out = match spec {
        'e' => format!("{value:.precision$e}"),
        'E' => format!("{value:.precision$E}"),
        'g' | 'G' => {
            let abs = value.abs();
            if abs != 0.0 && !(0.0001..1_000_000.0).contains(&abs) {
                if spec == 'G' {
                    format!("{value:.precision$E}")
                } else {
                    format!("{value:.precision$e}")
                }
            } else {
                let text = format!("{value:.precision$}");
                text.trim_end_matches('0').trim_end_matches('.').to_string()
            }
        }
        _ => format!("{value:.precision$}"),
    };
    if value >= 0.0 {
        if flags.plus {
            out.insert(0, '+');
        } else if flags.space {
            out.insert(0, ' ');
        }
    }
    if flags.alternate && !out.contains('.') && !out.contains('e') && !out.contains('E') {
        out.push('.');
    }
    out
}

fn format_printf_char(value: ScalarValue, repeat: usize) -> String {
    let ch = match value {
        ScalarValue::Utf8(Some(s))
        | ScalarValue::LargeUtf8(Some(s))
        | ScalarValue::Utf8View(Some(s)) => s.chars().next().unwrap_or('\0'),
        other => scalar_i64(&other)
            .and_then(|v| char::from_u32(v as u32))
            .unwrap_or('\0'),
    };
    ch.to_string().repeat(repeat.max(1))
}

fn apply_printf_width(mut text: String, width: Option<usize>, left: bool, zero: bool) -> String {
    let Some(width) = width else {
        return text;
    };
    let len = text.chars().count();
    if len >= width {
        return text;
    }
    let pad = width - len;
    let ch = if zero { '0' } else { ' ' };
    if left {
        text.extend(std::iter::repeat(ch).take(pad));
        text
    } else if zero && matches!(text.as_bytes().first(), Some(b'+' | b'-' | b' ')) {
        let sign = text.remove(0);
        format!("{sign}{}{}", ch.to_string().repeat(pad), text)
    } else if zero && (text.starts_with("0x") || text.starts_with("0X")) {
        let prefix = text[..2].to_string();
        let rest = text[2..].to_string();
        format!("{prefix}{}{}", ch.to_string().repeat(pad), rest)
    } else {
        format!("{}{}", ch.to_string().repeat(pad), text)
    }
}

fn char_row(row: &RowArgs) -> Option<String> {
    let mut out = String::new();
    for idx in 0..row.len() {
        if let Some(code) = row.i64(idx) {
            if let Some(ch) = char::from_u32(code as u32) {
                out.push(ch);
            }
        }
    }
    Some(out)
}

fn substr_row(row: &RowArgs) -> Option<String> {
    let value = row.text(0)?;
    let chars = value.chars().collect::<Vec<_>>();
    let start = row.i64(1)?;
    let len = row.i64(2);
    let start_idx = if start > 0 {
        (start - 1) as usize
    } else if start < 0 {
        chars.len().saturating_sub(start.unsigned_abs() as usize)
    } else {
        0
    };
    if start_idx >= chars.len() {
        return Some(String::new());
    }
    let end = len
        .map(|n| start_idx.saturating_add(n.max(0) as usize))
        .unwrap_or(chars.len())
        .min(chars.len());
    Some(chars[start_idx..end].iter().collect())
}

fn glob_match(pattern: &str, value: &str) -> bool {
    fn rec(pattern: &[char], value: &[char]) -> bool {
        match pattern.split_first() {
            None => value.is_empty(),
            Some(('*', rest)) => {
                rec(rest, value) || (!value.is_empty() && rec(pattern, &value[1..]))
            }
            Some(('?', rest)) => !value.is_empty() && rec(rest, &value[1..]),
            Some(('[', _)) => {
                let Some((matched, rest)) = glob_class_match(pattern, value.first().copied())
                else {
                    return value.first() == Some(&'[') && rec(&pattern[1..], &value[1..]);
                };
                matched && !value.is_empty() && rec(rest, &value[1..])
            }
            Some((ch, rest)) => value.first() == Some(ch) && rec(rest, &value[1..]),
        }
    }
    rec(
        &pattern.chars().collect::<Vec<_>>(),
        &value.chars().collect::<Vec<_>>(),
    )
}

fn glob_class_match(pattern: &[char], value: Option<char>) -> Option<(bool, &[char])> {
    if pattern.first() != Some(&'[') {
        return None;
    }
    let value = value?;
    let mut idx = 1;
    let mut negated = false;
    if matches!(pattern.get(idx), Some('^' | '!')) {
        negated = true;
        idx += 1;
    }
    let mut matched = false;
    let mut saw_entry = false;
    while idx < pattern.len() {
        if pattern[idx] == ']' && saw_entry {
            return Some((
                if negated { !matched } else { matched },
                &pattern[idx + 1..],
            ));
        }
        let start = pattern[idx];
        saw_entry = true;
        if idx + 2 < pattern.len() && pattern[idx + 1] == '-' && pattern[idx + 2] != ']' {
            let end = pattern[idx + 2];
            if start <= value && value <= end {
                matched = true;
            }
            idx += 3;
        } else {
            if value == start {
                matched = true;
            }
            idx += 1;
        }
    }
    None
}

fn random_bytes(len: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(len);
    while out.len() < len {
        out.extend_from_slice(&next_random().to_ne_bytes());
    }
    out.truncate(len);
    out
}

fn next_random() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    let old = RANDOM_STATE.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    let mut x = old ^ now;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    let value = (x.wrapping_mul(0x2545_F491_4F6C_DD1D)) as i64;
    if value == i64::MIN {
        0
    } else {
        value
    }
}

#[derive(Debug)]
struct JsonEachFunc;

impl TableFunctionImpl for JsonEachFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DFResult<Arc<dyn TableProvider>> {
        json_table_function(args, "json_each", JsonTableMode::Each)
    }
}

#[derive(Debug)]
struct JsonTreeFunc;

impl TableFunctionImpl for JsonTreeFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DFResult<Arc<dyn TableProvider>> {
        json_table_function(args, "json_tree", JsonTableMode::Tree)
    }
}

#[derive(Debug)]
struct JsonbEachFunc;

impl TableFunctionImpl for JsonbEachFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DFResult<Arc<dyn TableProvider>> {
        json_table_function(args, "jsonb_each", JsonTableMode::Each)
    }
}

#[derive(Debug)]
struct JsonbTreeFunc;

impl TableFunctionImpl for JsonbTreeFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DFResult<Arc<dyn TableProvider>> {
        json_table_function(args, "jsonb_tree", JsonTableMode::Tree)
    }
}

#[derive(Debug)]
struct SeriesFunc;

impl TableFunctionImpl for SeriesFunc {
    fn call_with_args(&self, args: TableFunctionArgs) -> DFResult<Arc<dyn TableProvider>> {
        let exprs = args.exprs();
        let mut values = Vec::with_capacity(exprs.len());
        for expr in exprs {
            values.push(literal_i64(expr).ok_or_else(|| {
                DataFusionError::Plan("series arguments must be integer literals".into())
            })?);
        }
        let (start, stop, step) = match values.as_slice() {
            [stop] => (0, *stop, 1),
            [start, stop] => (*start, *stop, 1),
            [start, stop, step] => (*start, *stop, *step),
            _ => {
                return Err(DataFusionError::Plan(
                    "series requires stop, start/stop, or start/stop/step".into(),
                ));
            }
        };
        series_provider(start, stop, step)
    }
}

fn literal_string(expr: &DfExpr) -> Option<String> {
    match expr {
        DfExpr::Literal(
            ScalarValue::Utf8(Some(s))
            | ScalarValue::LargeUtf8(Some(s))
            | ScalarValue::Utf8View(Some(s)),
            _,
        ) => Some(s.clone()),
        _ => None,
    }
}

fn literal_i64(expr: &DfExpr) -> Option<i64> {
    match expr {
        DfExpr::Literal(ScalarValue::Int64(Some(v)), _) => Some(*v),
        DfExpr::Literal(ScalarValue::Int32(Some(v)), _) => Some(*v as i64),
        DfExpr::Literal(ScalarValue::UInt64(Some(v)), _) => i64::try_from(*v).ok(),
        DfExpr::Literal(ScalarValue::UInt32(Some(v)), _) => Some(*v as i64),
        _ => None,
    }
}

pub(crate) fn series_provider(
    start: i64,
    stop: i64,
    step: i64,
) -> DFResult<Arc<dyn TableProvider>> {
    if step == 0 {
        return Err(DataFusionError::Plan("series step must not be 0".into()));
    }
    let mut values = Vec::new();
    let mut current = start;
    while if step > 0 {
        current <= stop
    } else {
        current >= stop
    } {
        if values.len() >= 1_000_000 {
            return Err(DataFusionError::Plan(
                "series output is capped at 1,000,000 rows".into(),
            ));
        }
        values.push(current);
        current = current.saturating_add(step);
        if (step > 0 && current == i64::MAX) || (step < 0 && current == i64::MIN) {
            break;
        }
    }
    let schema = Arc::new(ArrowSchema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(values)) as ArrayRef],
    )?;
    Ok(Arc::new(MemTable::try_new(schema, vec![vec![batch]])?))
}

#[derive(Clone, Copy)]
pub(crate) enum JsonTableMode {
    Each,
    Tree,
}

fn json_table_function(
    args: TableFunctionArgs,
    name: &str,
    mode: JsonTableMode,
) -> DFResult<Arc<dyn TableProvider>> {
    let exprs = args.exprs();
    if exprs.is_empty() || exprs.len() > 2 {
        return Err(DataFusionError::Plan(format!(
            "{name} requires one JSON argument and an optional root path"
        )));
    }
    let json = literal_string(&exprs[0]).ok_or_else(|| {
        DataFusionError::Plan(format!("{name} arguments must be string literals"))
    })?;
    let root_path = if exprs.len() == 2 {
        literal_string(&exprs[1]).ok_or_else(|| {
            DataFusionError::Plan(format!("{name} root path must be a string literal"))
        })?
    } else {
        "$".to_string()
    };
    json_table_provider_from_text(name, &json, Some(&root_path), mode)
}

#[derive(Default)]
struct JsonTableRows {
    key: Vec<Option<String>>,
    value: Vec<Option<String>>,
    ty: Vec<Option<String>>,
    atom: Vec<Option<String>>,
    id: Vec<i64>,
    parent: Vec<Option<i64>>,
    fullkey: Vec<Option<String>>,
    path: Vec<Option<String>>,
    next_id: i64,
}

impl JsonTableRows {
    fn push(
        &mut self,
        row_key: Option<String>,
        row_value: &JsonValue,
        fullkey: String,
        path: String,
        parent: Option<i64>,
    ) -> i64 {
        let row_id = self.next_id;
        self.next_id += 1;
        self.key.push(row_key);
        self.value
            .push(json_scalar_result(row_value).or_else(|| Some(row_value.to_string())));
        self.ty.push(Some(json_type(row_value).to_string()));
        self.atom.push(match row_value {
            JsonValue::Array(_) | JsonValue::Object(_) => None,
            _ => json_scalar_result(row_value),
        });
        self.id.push(row_id);
        self.parent.push(parent);
        self.fullkey.push(Some(fullkey));
        self.path.push(Some(path));
        row_id
    }
}

pub(crate) fn json_table_provider_from_text(
    name: &str,
    json: &str,
    root_path: Option<&str>,
    mode: JsonTableMode,
) -> DFResult<Arc<dyn TableProvider>> {
    let (schema, batches) = json_table_batches_from_text(name, json, root_path, mode)?;
    Ok(Arc::new(MemTable::try_new(schema, vec![batches])?))
}

pub(crate) fn json_table_batches_from_text(
    name: &str,
    json: &str,
    root_path: Option<&str>,
    mode: JsonTableMode,
) -> DFResult<(Arc<ArrowSchema>, Vec<RecordBatch>)> {
    let root_path = root_path.unwrap_or("$");
    let root = serde_json::from_str::<JsonValue>(json)
        .map_err(|e| DataFusionError::Plan(format!("invalid JSON for {name}: {e}")))?;
    let path = parse_json_path(root_path)
        .ok_or_else(|| DataFusionError::Plan(format!("invalid {name} root path")))?;
    let value = json_path_get(&root, &path)
        .cloned()
        .unwrap_or(JsonValue::Null);
    json_table_batches(&value, root_path, mode)
}

fn json_table_batches(
    value: &JsonValue,
    root_path: &str,
    mode: JsonTableMode,
) -> DFResult<(Arc<ArrowSchema>, Vec<RecordBatch>)> {
    let mut rows = JsonTableRows::default();

    match mode {
        JsonTableMode::Each => json_each_rows(&mut rows, value, root_path),
        JsonTableMode::Tree => {
            json_tree_rows(
                &mut rows,
                None,
                value,
                root_path.to_string(),
                root_path.to_string(),
                None,
            );
        }
    }

    let schema = Arc::new(ArrowSchema::new(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Utf8, true),
        Field::new("type", DataType::Utf8, true),
        Field::new("atom", DataType::Utf8, true),
        Field::new("id", DataType::Int64, false),
        Field::new("parent", DataType::Int64, true),
        Field::new("fullkey", DataType::Utf8, true),
        Field::new("path", DataType::Utf8, true),
    ]));
    let batch = arrow::record_batch::RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(rows.key)) as ArrayRef,
            Arc::new(StringArray::from(rows.value)),
            Arc::new(StringArray::from(rows.ty)),
            Arc::new(StringArray::from(rows.atom)),
            Arc::new(Int64Array::from(rows.id)),
            Arc::new(Int64Array::from(rows.parent)),
            Arc::new(StringArray::from(rows.fullkey)),
            Arc::new(StringArray::from(rows.path)),
        ],
    )?;
    Ok((schema, vec![batch]))
}

fn json_each_rows(rows: &mut JsonTableRows, value: &JsonValue, root_path: &str) {
    match value {
        JsonValue::Array(values) => {
            for (idx, item) in values.iter().enumerate() {
                rows.push(
                    Some(idx.to_string()),
                    item,
                    format!("{root_path}[{idx}]"),
                    root_path.to_string(),
                    None,
                );
            }
        }
        JsonValue::Object(values) => {
            for (key, item) in values {
                rows.push(
                    Some(key.clone()),
                    item,
                    json_child_path(root_path, &JsonPathToken::Key(key.clone())),
                    root_path.to_string(),
                    None,
                );
            }
        }
        JsonValue::Null => {}
        scalar => {
            rows.push(
                None,
                scalar,
                root_path.to_string(),
                root_path.to_string(),
                None,
            );
        }
    }
}

fn json_tree_rows(
    rows: &mut JsonTableRows,
    row_key: Option<String>,
    value: &JsonValue,
    fullkey: String,
    path: String,
    parent: Option<i64>,
) {
    let row_id = rows.push(row_key, value, fullkey.clone(), path, parent);
    match value {
        JsonValue::Array(values) => {
            for (idx, item) in values.iter().enumerate() {
                let child_fullkey = format!("{fullkey}[{idx}]");
                json_tree_rows(
                    rows,
                    Some(idx.to_string()),
                    item,
                    child_fullkey,
                    fullkey.clone(),
                    Some(row_id),
                );
            }
        }
        JsonValue::Object(values) => {
            for (key, item) in values {
                let token = JsonPathToken::Key(key.clone());
                let child_fullkey = json_child_path(&fullkey, &token);
                json_tree_rows(
                    rows,
                    Some(key.clone()),
                    item,
                    child_fullkey,
                    fullkey.clone(),
                    Some(row_id),
                );
            }
        }
        _ => {}
    }
}

fn json_child_path(parent: &str, token: &JsonPathToken) -> String {
    match token {
        JsonPathToken::Index(idx) => format!("{parent}[{idx}]"),
        JsonPathToken::Key(key) if is_simple_json_key(key) => format!("{parent}.{key}"),
        JsonPathToken::Key(key) => format!("{parent}[{}]", JsonValue::String(key.clone())),
    }
}

fn is_simple_json_key(key: &str) -> bool {
    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    (first == '_' || first.is_ascii_alphabetic())
        && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric())
}
