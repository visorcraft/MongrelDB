//! TSV import/export (MongrelDB-compatible format).
//!
//! Header row of column names, tab-separated values, NULL = empty field, with
//! `\t`, `\n`, `\r`, and `\` escaped (`\\t`, `\\n`, `\\r`, `\\\\`).

use crate::error::{MongrelError, Result};
use crate::memtable::Value;
use crate::schema::{Schema, TypeId};

/// Export `rows` as a TSV string under `schema`'s column order.
pub fn export_tsv(schema: &Schema, rows: &[crate::memtable::Row]) -> String {
    let mut out = String::new();
    // Header.
    let names: Vec<&str> = schema.columns.iter().map(|c| c.name.as_str()).collect();
    out.push_str(&names.join("\t"));
    out.push('\n');
    for row in rows {
        let cells: Vec<String> = schema
            .columns
            .iter()
            .map(|c| match row.columns.get(&c.id) {
                Some(Value::Null) | None => String::new(),
                Some(v) => escape(&value_to_string(v)),
            })
            .collect();
        out.push_str(&cells.join("\t"));
        out.push('\n');
    }
    out
}

/// Import a TSV string into per-row `(column_id, value)` cells, mapped by header
/// name against `schema`. Unknown headers are ignored.
pub fn import_tsv(schema: &Schema, text: &str) -> Result<Vec<Vec<(u16, Value)>>> {
    let mut lines = text.lines();
    let header = lines
        .next()
        .ok_or_else(|| MongrelError::InvalidArgument("empty tsv".into()))?;
    let header_cols: Vec<Option<u16>> = header
        .split('\t')
        .map(|name| schema.columns.iter().find(|c| c.name == name).map(|c| c.id))
        .collect();
    if header_cols.iter().any(|c| c.is_none()) {
        return Err(MongrelError::Schema(
            "tsv header references an unknown column".into(),
        ));
    }

    let mut out = Vec::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let fields: Vec<&str> = line.split('\t').collect();
        let mut row: Vec<(u16, Value)> = Vec::with_capacity(header_cols.len());
        for (i, col_id) in header_cols.iter().enumerate() {
            let col_id = col_id.unwrap();
            let raw = fields.get(i).copied().unwrap_or("");
            let value = if raw.is_empty() {
                Value::Null
            } else {
                let unescaped = unescape(raw);
                let ty = schema
                    .columns
                    .iter()
                    .find(|c| c.id == col_id)
                    .map(|c| c.ty)
                    .unwrap_or(TypeId::Bytes);
                parse_value(&unescaped, ty)?
            };
            row.push((col_id, value));
        }
        out.push(row);
    }
    Ok(out)
}

fn value_to_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int64(n) => n.to_string(),
        Value::Float64(f) => f.to_string(),
        Value::Bytes(b) => String::from_utf8_lossy(b).into_owned(),
        Value::Embedding(v) => {
            // JSON-ish array.
            let inner: Vec<String> = v.iter().map(|x| x.to_string()).collect();
            format!("[{}]", inner.join(","))
        }
        Value::Decimal(d) => d.to_string(),
        Value::Interval { months, days, nanos } => format!("{months}m {days}d {nanos}ns"),
    }
}

fn parse_value(s: &str, ty: TypeId) -> Result<Value> {
    Ok(match ty {
        TypeId::Int64 | TypeId::TimestampNanos => Value::Int64(
            s.parse()
                .map_err(|e| MongrelError::Schema(format!("int parse: {e}")))?,
        ),
        TypeId::Float64 => Value::Float64(
            s.parse()
                .map_err(|e| MongrelError::Schema(format!("float parse: {e}")))?,
        ),
        TypeId::Bool => Value::Bool(s == "true"),
        TypeId::Bytes => Value::Bytes(s.as_bytes().to_vec()),
        _ => Value::Bytes(s.as_bytes().to_vec()),
    })
}

fn escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for ch in s.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            _ => out.push(ch),
        }
    }
    out
}

fn unescape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::epoch::Epoch;
    use crate::memtable::Row;
    use crate::rowid::RowId;
    use crate::schema::{ColumnDef, ColumnFlags};

    fn schema() -> Schema {
        Schema {
            schema_id: 1,
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                },
                ColumnDef {
                    id: 2,
                    name: "note".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                },
            ],
            indexes: Vec::new(),
            colocation: vec![],
            constraints: Default::default(),
        }
    }

    #[test]
    fn round_trips_with_escapes_and_nulls() {
        let rows = vec![
            Row::new(RowId(0), Epoch(1))
                .with_column(1, Value::Int64(1))
                .with_column(2, Value::Bytes(b"hello\tworld".to_vec())),
            Row::new(RowId(1), Epoch(1))
                .with_column(1, Value::Int64(2))
                .with_column(2, Value::Null),
        ];
        let tsv = export_tsv(&schema(), &rows);
        assert!(tsv.contains("hello\\tworld"), "tabs must be escaped");
        let back = import_tsv(&schema(), &tsv).unwrap();
        assert_eq!(back.len(), 2);
        assert!(matches!(
            back[0].iter().find(|(c, _)| *c == 1),
            Some((_, Value::Int64(1)))
        ));
        // The escaped tab round-trips back to a real tab.
        assert_eq!(
            back[0]
                .iter()
                .find(|(c, _)| *c == 2)
                .map(|(_, v)| v.clone()),
            Some(Value::Bytes(b"hello\tworld".to_vec()))
        );
        assert_eq!(
            back[1]
                .iter()
                .find(|(c, _)| *c == 2)
                .map(|(_, v)| v.clone()),
            Some(Value::Null)
        );
    }
}
