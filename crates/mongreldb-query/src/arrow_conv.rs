//! MongrelDB ↔ Arrow conversions: schema mapping and `Vec<Row>` → `RecordBatch`.

use arrow::array::{
    ArrayRef, BooleanBuilder, FixedSizeListBuilder, Float32Builder, Float64Array, Float64Builder,
    Int64Array, Int64Builder, StringBuilder,
};
use arrow::buffer::{BooleanBuffer, Buffer, NullBuffer};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use mongreldb_core::columnar::NativeColumn;
use mongreldb_core::memtable::Value;
use mongreldb_core::schema::{Schema as MongrelSchema, TypeId};
use std::sync::Arc;

use crate::error::{MongrelQueryError, Result};

fn bit_set(validity: &[u8], i: usize) -> bool {
    (validity.get(i / 8).copied().unwrap_or(0) >> (i % 8)) & 1 == 1
}

/// Fast check: are all `n` positions non-null?
fn all_bits_set(validity: &[u8], n: usize) -> bool {
    if n == 0 {
        return true;
    }
    let full = n / 8;
    if !validity[..full].iter().all(|&b| b == 0xFF) {
        return false;
    }
    if n % 8 != 0 {
        let mask = (1u8 << (n % 8)) - 1;
        (validity.get(full).copied().unwrap_or(0) & mask) == mask
    } else {
        true
    }
}

/// Build an Arrow array straight from a typed [`NativeColumn`] (no `Value`).
/// For the common all-non-null case on fixed-width columns, constructs the Arrow
/// array directly from the typed buffer (one memcpy, no per-element builder).
pub fn native_to_array(ty: TypeId, col: &NativeColumn) -> Result<ArrayRef> {
    Ok(match (ty, col) {
        (TypeId::Int64 | TypeId::TimestampNanos, NativeColumn::Int64 { data, validity }) => {
            if all_bits_set(validity, data.len()) {
                Arc::new(Int64Array::new(data.clone().into(), None))
            } else {
                let mut b = Int64Builder::with_capacity(data.len());
                for (i, v) in data.iter().enumerate() {
                    if bit_set(validity, i) {
                        b.append_value(*v);
                    } else {
                        b.append_null();
                    }
                }
                Arc::new(b.finish())
            }
        }
        (TypeId::Float64, NativeColumn::Float64 { data, validity }) => {
            if all_bits_set(validity, data.len()) {
                Arc::new(Float64Array::new(data.clone().into(), None))
            } else {
                let mut b = Float64Builder::with_capacity(data.len());
                for (i, v) in data.iter().enumerate() {
                    if bit_set(validity, i) {
                        b.append_value(*v);
                    } else {
                        b.append_null();
                    }
                }
                Arc::new(b.finish())
            }
        }
        (TypeId::Bool, NativeColumn::Bool { data, validity }) => {
            let mut b = BooleanBuilder::with_capacity(data.len());
            for (i, v) in data.iter().enumerate() {
                if bit_set(validity, i) {
                    b.append_value(*v != 0);
                } else {
                    b.append_null();
                }
            }
            Arc::new(b.finish())
        }
        (
            TypeId::Bytes,
            NativeColumn::Bytes {
                offsets,
                values,
                validity,
            },
        ) => {
            let n = offsets.len().saturating_sub(1);
            let mut b = StringBuilder::with_capacity(n, values.len());
            for i in 0..n {
                if bit_set(validity, i) {
                    let lo = offsets[i] as usize;
                    let hi = offsets[i + 1] as usize;
                    b.append_value(String::from_utf8_lossy(&values[lo..hi]));
                } else {
                    b.append_null();
                }
            }
            Arc::new(b.finish())
        }
        _ => {
            return Err(MongrelQueryError::Arrow(format!(
                "native_to_array: unsupported (ty={ty:?})"
            )))
        }
    })
}

/// Zero-copy variant of [`native_to_array`] for the streaming scan path. It
/// takes ownership of the [`NativeColumn`] and, for the fixed-width `Int64` /
/// `Float64` columns, **moves** the typed data buffer (and validity buffer when
/// needed) straight into the Arrow array — no `memcpy`, no per-element builder.
/// `Bool` / `Bytes` / `Embedding` fall back to the by-reference builder.
pub fn native_to_array_owned(ty: TypeId, col: NativeColumn) -> Result<ArrayRef> {
    Ok(match (ty, col) {
        (TypeId::Int64 | TypeId::TimestampNanos, NativeColumn::Int64 { data, validity }) => {
            let n = data.len();
            Arc::new(Int64Array::new(data.into(), owned_nulls(validity, n)))
        }
        (TypeId::Float64, NativeColumn::Float64 { data, validity }) => {
            let n = data.len();
            Arc::new(Float64Array::new(data.into(), owned_nulls(validity, n)))
        }
        // Everything else: defer to the by-reference builder.
        (ty, col) => native_to_array(ty, &col)?,
    })
}

/// Build an Arrow validity (`NullBuffer`) from a MongrelDB validity byte buffer,
/// moving it without a copy. Returns `None` when every slot is non-null (Arrow
/// treats a missing validity buffer as all-non-null). `validity` is produced by
/// `validity_bitmap_from`, whose unused trailing bits are zero — Arrow-safe.
fn owned_nulls(validity: Vec<u8>, n: usize) -> Option<NullBuffer> {
    if all_bits_set(&validity, n) {
        None
    } else {
        let buffer: Buffer = validity.into();
        Some(NullBuffer::new(BooleanBuffer::new(buffer, 0, n)))
    }
}

/// Build a `RecordBatch` directly from typed columns (vectorized scan path).
pub fn native_columns_to_batch(
    columns: &[(u16, NativeColumn)],
    schema: &MongrelSchema,
) -> Result<arrow::record_batch::RecordBatch> {
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());
    for cdef in &schema.columns {
        let col = columns
            .iter()
            .find(|(id, _)| *id == cdef.id)
            .map(|(_, c)| c)
            .ok_or_else(|| MongrelQueryError::Arrow(format!("missing column {}", cdef.id)))?;
        arrays.push(native_to_array(cdef.ty, col)?);
    }
    let fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|c| Field::new(&c.name, arrow_data_type(&c.ty).unwrap(), true))
        .collect();
    arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

/// Map a MongrelDB schema to an Arrow schema over the **user** columns only
/// (system columns `_row_id`/`_epoch`/`_deleted` are hidden from SQL).
pub fn arrow_schema(schema: &MongrelSchema) -> Result<SchemaRef> {
    let fields: Result<Vec<Field>> = schema
        .columns
        .iter()
        .map(|c| arrow_data_type(&c.ty).map(|dt| Field::new(&c.name, dt, true)))
        .collect();
    Ok(Arc::new(Schema::new(fields?)) as SchemaRef)
}

pub(crate) fn arrow_data_type(ty: &TypeId) -> Result<DataType> {
    Ok(match ty {
        TypeId::Bool => DataType::Boolean,
        TypeId::Int8 => DataType::Int8,
        TypeId::Int16 => DataType::Int16,
        TypeId::Int32 | TypeId::Date32 => DataType::Int32,
        TypeId::Int64 | TypeId::TimestampNanos => DataType::Int64,
        TypeId::Date64 => DataType::Date64,
        TypeId::Time64 => DataType::Time64(arrow::datatypes::TimeUnit::Nanosecond),
        TypeId::Interval => DataType::Interval(arrow::datatypes::IntervalUnit::MonthDayNano),
        TypeId::UInt8 => DataType::UInt8,
        TypeId::UInt16 => DataType::UInt16,
        TypeId::UInt32 => DataType::UInt32,
        TypeId::UInt64 => DataType::UInt64,
        TypeId::Float32 => DataType::Float32,
        TypeId::Float64 => DataType::Float64,
        TypeId::Bytes => DataType::Utf8,
        TypeId::Embedding { dim } => DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)),
            *dim as i32,
        ),
        TypeId::Decimal128 { precision, scale } => {
            DataType::Decimal128(*precision, *scale)
        }
    })
}

/// Build a single `RecordBatch` from `rows` for the user columns of `schema`.
pub fn rows_to_batch(
    rows: &[mongreldb_core::Row],
    schema: &MongrelSchema,
) -> Result<arrow::record_batch::RecordBatch> {
    let fields: Vec<(u16, TypeId)> = schema.columns.iter().map(|c| (c.id, c.ty)).collect();
    let arrays: Vec<ArrayRef> = fields
        .iter()
        .map(|(col_id, ty)| {
            let vals: Vec<Value> = rows
                .iter()
                .map(|r| r.columns.get(col_id).cloned().unwrap_or(Value::Null))
                .collect();
            build_array(*ty, &vals)
        })
        .collect::<Result<_>>()?;
    let arrow_fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|c| Field::new(&c.name, arrow_data_type(&c.ty).unwrap(), true))
        .collect();
    arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(arrow_fields)), arrays)
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}

/// Build an Arrow array from a flat slice of values (one per row).
pub fn build_array(ty: TypeId, values: &[Value]) -> Result<ArrayRef> {
    Ok(match ty {
        TypeId::Int64 | TypeId::TimestampNanos => {
            let mut b = Int64Builder::new();
            for v in values {
                match v {
                    Value::Int64(x) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        TypeId::Float64 => {
            let mut b = Float64Builder::new();
            for v in values {
                match v {
                    Value::Float64(x) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        TypeId::Float32 => {
            let mut b = arrow::array::Float32Builder::new();
            for v in values {
                match v {
                    Value::Float64(x) => b.append_value(*x as f32),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        TypeId::Bool => {
            let mut b = BooleanBuilder::new();
            for v in values {
                match v {
                    Value::Bool(x) => b.append_value(*x),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        TypeId::Int32 | TypeId::Date32 => {
            let mut b = arrow::array::Int32Builder::new();
            for v in values {
                match v {
                    Value::Int64(x) => b.append_value(*x as i32),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        TypeId::Bytes => {
            let mut b = StringBuilder::new();
            for v in values {
                match v {
                    Value::Bytes(x) => b.append_value(String::from_utf8_lossy(x)),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        TypeId::Embedding { dim } => {
            let fbb = Float32Builder::new();
            let mut b = FixedSizeListBuilder::new(fbb, dim as i32);
            for v in values {
                match v {
                    Value::Embedding(x) if x.len() == dim as usize => {
                        for fv in x {
                            b.values().append_value(*fv);
                        }
                        b.append(true);
                    }
                    _ => {
                        for _ in 0..dim {
                            b.values().append_null();
                        }
                        b.append(false);
                    }
                }
            }
            Arc::new(b.finish())
        }
        TypeId::Decimal128 { precision, scale } => {
            let mut b = arrow::array::Decimal128Builder::new()
                .with_precision_and_scale(precision, scale)
                .map_err(|e| MongrelQueryError::Arrow(e.to_string()))?;
            for v in values {
                match v {
                    Value::Decimal(d) => b.append_value(*d),
                    _ => b.append_null(),
                }
            }
            Arc::new(b.finish())
        }
        _ => {
            return Err(MongrelQueryError::Arrow(format!(
                "unsupported column type {ty:?} for SQL projection"
            )))
        }
    })
}

/// Build a single `RecordBatch` directly from columnar `(column_id, values)`
/// pairs — the vectorized scan path (no row materialization).
pub fn columns_to_batch(
    columns: &[(u16, Vec<Value>)],
    schema: &MongrelSchema,
) -> Result<arrow::record_batch::RecordBatch> {
    // Order arrays by schema column order, mapping each to its values.
    let mut arrays: Vec<ArrayRef> = Vec::with_capacity(schema.columns.len());
    for cdef in &schema.columns {
        let vals = columns
            .iter()
            .find(|(id, _)| *id == cdef.id)
            .map(|(_, v)| v.as_slice())
            .unwrap_or(&[]);
        arrays.push(build_array(cdef.ty, vals)?);
    }
    let arrow_fields: Vec<Field> = schema
        .columns
        .iter()
        .map(|c| Field::new(&c.name, arrow_data_type(&c.ty).unwrap(), true))
        .collect();
    arrow::record_batch::RecordBatch::try_new(Arc::new(Schema::new(arrow_fields)), arrays)
        .map_err(|e| MongrelQueryError::Arrow(e.to_string()))
}
