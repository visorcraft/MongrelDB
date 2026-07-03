//! Extended SQL Functions: application-oriented scalar functions layered on
//! DataFusion for date/time, JSON, string, and math compatibility.

use arrow::array::{ArrayRef, Float64Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use chrono::{
    DateTime, Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime, TimeZone, Timelike, Utc,
};
use datafusion::common::{Result as DFResult, ScalarValue};
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion::prelude::SessionContext;
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

static RANDOM_STATE: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);

pub fn register_extended_sql_functions(ctx: &SessionContext) {
    for func in extended_functions() {
        ctx.register_udf(ScalarUDF::from(func));
    }
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

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct ExtendedSqlFunc {
    name: &'static str,
    aliases: Vec<String>,
    kind: ExtendedFuncKind,
    signature: Signature,
}

impl ExtendedSqlFunc {
    fn new(name: &'static str, kind: ExtendedFuncKind, volatility: Volatility) -> Self {
        Self {
            name,
            aliases: Vec::new(),
            kind,
            signature: Signature::variadic_any(volatility),
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
        }
    }

    fn nullary(name: &'static str, kind: ExtendedFuncKind, volatility: Volatility) -> Self {
        Self {
            name,
            aliases: Vec::new(),
            kind,
            signature: Signature::new(TypeSignature::Nullary, volatility),
        }
    }

    fn with_aliases(mut self, aliases: &[&str]) -> Self {
        self.aliases = aliases.iter().map(|s| s.to_string()).collect();
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
    JsonSet,
    JsonInsert,
    JsonRemove,
    Instr,
    Quote,
    Hex,
    Unhex,
    Printf,
    Char,
    Mod,
    Pow,
    Random,
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
            | ExtendedFuncKind::Random => DataType::Int64,
            ExtendedFuncKind::JulianDay
            | ExtendedFuncKind::TimeDiff
            | ExtendedFuncKind::Mod
            | ExtendedFuncKind::Pow => DataType::Float64,
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
                row.scalar(0).map(scalar_to_json_arg).map(|v| v.to_string())
            }),
            ExtendedFuncKind::JsonSet => {
                eval_string(args, |row| json_mutate_row(row, JsonMutation::Set))
            }
            ExtendedFuncKind::JsonInsert => {
                eval_string(args, |row| json_mutate_row(row, JsonMutation::Insert))
            }
            ExtendedFuncKind::JsonRemove => eval_string(args, json_remove_row),
            ExtendedFuncKind::Instr => eval_int(args, |row| {
                let haystack = row.text(0)?;
                let needle = row.text(1)?;
                Some(instr(&haystack, &needle))
            }),
            ExtendedFuncKind::Quote => eval_string(args, |row| row.scalar(0).map(sql_quote)),
            ExtendedFuncKind::Hex => eval_string(args, |row| {
                row.scalar(0).map(|v| hex_encode(&scalar_bytes(&v)))
            }),
            ExtendedFuncKind::Unhex => eval_string(args, |row| {
                let bytes = hex_decode(&row.text(0)?)?;
                String::from_utf8(bytes).ok()
            }),
            ExtendedFuncKind::Printf => eval_string(args, printf_row),
            ExtendedFuncKind::Char => eval_string(args, char_row),
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
            ExtendedFuncKind::Random => eval_int(args, |_| Some(next_random())),
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
        ExtendedSqlFunc::new("json", Json, Volatility::Immutable),
        ExtendedSqlFunc::new("json_valid", JsonValid, Volatility::Immutable),
        ExtendedSqlFunc::new("json_extract", JsonExtract, Volatility::Immutable),
        ExtendedSqlFunc::new("json_type", JsonType, Volatility::Immutable),
        ExtendedSqlFunc::new("json_array_length", JsonArrayLength, Volatility::Immutable),
        ExtendedSqlFunc::new("json_quote", JsonQuote, Volatility::Immutable),
        ExtendedSqlFunc::new("json_set", JsonSet, Volatility::Immutable),
        ExtendedSqlFunc::new("json_insert", JsonInsert, Volatility::Immutable),
        ExtendedSqlFunc::new("json_remove", JsonRemove, Volatility::Immutable),
        ExtendedSqlFunc::new("instr", Instr, Volatility::Immutable),
        ExtendedSqlFunc::new("quote", Quote, Volatility::Immutable),
        ExtendedSqlFunc::new("hex", Hex, Volatility::Immutable),
        ExtendedSqlFunc::new("unhex", Unhex, Volatility::Immutable),
        ExtendedSqlFunc::new("printf", Printf, Volatility::Immutable).with_aliases(&["format"]),
        ExtendedSqlFunc::new("char", Char, Volatility::Immutable),
        ExtendedSqlFunc::new("mod", Mod, Volatility::Immutable),
        ExtendedSqlFunc::new("pow", Pow, Volatility::Immutable),
        ExtendedSqlFunc::nullary("random", Random, Volatility::Volatile),
    ]
}

struct RowArgs {
    arrays: Vec<ArrayRef>,
    row: usize,
}

impl RowArgs {
    fn scalar(&self, idx: usize) -> Option<ScalarValue> {
        self.arrays
            .get(idx)
            .and_then(|arr| ScalarValue::try_from_array(arr, self.row).ok())
            .filter(|v| !v.is_null())
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
        }
        idx += 2;
    }
    Some(root.to_string())
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
    let mut chars = path.chars().peekable();
    if chars.next()? != '$' {
        return None;
    }
    let mut tokens = Vec::new();
    while let Some(ch) = chars.next() {
        match ch {
            '.' => {
                let mut key = String::new();
                while let Some(&c) = chars.peek() {
                    if c == '.' || c == '[' {
                        break;
                    }
                    key.push(c);
                    chars.next();
                }
                if key.is_empty() {
                    return None;
                }
                tokens.push(JsonPathToken::Key(key));
            }
            '[' => {
                let mut n = String::new();
                for c in chars.by_ref() {
                    if c == ']' {
                        break;
                    }
                    n.push(c);
                }
                tokens.push(JsonPathToken::Index(n.parse().ok()?));
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

fn hex_decode(value: &str) -> Option<Vec<u8>> {
    let value = value.trim();
    if value.len() % 2 != 0 {
        return None;
    }
    let mut out = Vec::with_capacity(value.len() / 2);
    let bytes = value.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let hi = hex_value(bytes[i])?;
        let lo = hex_value(bytes[i + 1])?;
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
    let mut chars = format.chars();
    while let Some(ch) = chars.next() {
        if ch != '%' {
            out.push(ch);
            continue;
        }
        let Some(spec) = chars.next() else {
            out.push('%');
            break;
        };
        match spec {
            '%' => out.push('%'),
            's' | 'q' | 'Q' => {
                if let Some(v) = row.scalar(arg_idx) {
                    let text = scalar_text(&v);
                    if spec == 'Q' {
                        out.push_str(&format!("'{}'", text.replace('\'', "''")));
                    } else {
                        out.push_str(&text);
                    }
                }
                arg_idx += 1;
            }
            'd' | 'i' => {
                if let Some(v) = row.i64(arg_idx) {
                    out.push_str(&v.to_string());
                }
                arg_idx += 1;
            }
            'f' | 'g' => {
                if let Some(v) = row.f64(arg_idx) {
                    out.push_str(&v.to_string());
                }
                arg_idx += 1;
            }
            other => {
                out.push('%');
                out.push(other);
            }
        }
    }
    Some(out)
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
    (x.wrapping_mul(0x2545_F491_4F6C_DD1D)) as i64
}
