//! Plain columnar encode/decode for the prototype type set.
//!
//! Each column is stored as a single page of the form:
//!   `[u32 validity_len][validity bytes][payload bytes]`
//! where `validity` is a bitmap (bit `i` set ⇒ row `i` non-null) and `payload`
//! is type-specific:
//! - `Int64`/`Float64`/`TimestampNanos`: N × 8 BE bytes (null rows: zeros).
//! - `Bool`/`Date32`/small ints: N × element bytes.
//! - `Bytes`: `(N+1) × 8` BE offsets then the concatenated bytes.
//! - `Embedding{dim}`: N × dim × 4 BE bytes (f32 bit patterns).
//!
//! Phase 5 will swap Plain for dictionary/delta/byte-stream-split encodings
//! chosen from run-time stats; the page framing stays identical.

use crate::error::{MongrelError, Result};
use crate::memtable::Value;
use crate::page::Encoding;
use crate::schema::TypeId;
use serde::{Deserialize, Serialize};

/// Encode a column's values into a single page.
pub fn encode_column(ty: TypeId, values: &[Value]) -> Result<Vec<u8>> {
    let n = values.len();
    let validity = validity_bitmap(values);
    let payload = match ty {
        TypeId::Int64 => fixed_encode(values, 8, |v| match v {
            Value::Int64(x) => Ok(x.to_be_bytes().to_vec()),
            Value::Null => Ok(vec![0; 8]),
            _ => Err(type_mismatch(ty, v)),
        })?,
        TypeId::Float64 => fixed_encode(values, 8, |v| match v {
            Value::Float64(f) => Ok(f.to_bits().to_be_bytes().to_vec()),
            Value::Null => Ok(vec![0; 8]),
            _ => Err(type_mismatch(ty, v)),
        })?,
        TypeId::TimestampNanos => fixed_encode(values, 8, |v| match v {
            Value::Int64(x) => Ok(x.to_be_bytes().to_vec()),
            Value::Null => Ok(vec![0; 8]),
            _ => Err(type_mismatch(ty, v)),
        })?,
        TypeId::Bool => fixed_encode(values, 1, |v| match v {
            Value::Bool(b) => Ok(vec![*b as u8]),
            Value::Null => Ok(vec![0]),
            _ => Err(type_mismatch(ty, v)),
        })?,
        TypeId::Int32 | TypeId::UInt32 | TypeId::Date32 => fixed_encode(values, 4, |v| match v {
            Value::Int64(x) => Ok((*x as i32).to_be_bytes().to_vec()),
            Value::Null => Ok(vec![0; 4]),
            _ => Err(type_mismatch(ty, v)),
        })?,
        TypeId::Bytes => bytes_encode(values)?,
        TypeId::Embedding { dim } => embedding_encode(values, dim)?,
        other => {
            return Err(MongrelError::Schema(format!(
                "encoding for type {other:?} not implemented yet"
            )))
        }
    };
    let mut page = Vec::with_capacity(4 + validity.len() + payload.len());
    page.extend_from_slice(&(validity.len() as u32).to_be_bytes());
    page.extend_from_slice(&validity);
    page.extend_from_slice(&payload);
    let _ = n;
    Ok(page)
}

/// Decode a column page back into `n` values. `le` (Phase 15.7) reads the
/// fixed-width Int64/Float64/Int32 slots and the Bytes offset table as
/// little-endian (the layout the typed native writer produces when
/// `with_native_endian` is set); big-endian otherwise (pre-15.7 runs).
pub fn decode_column(ty: TypeId, page: &[u8], n: usize, le: bool) -> Result<Vec<Value>> {
    if page.len() < 4 {
        return Err(MongrelError::InvalidArgument(
            "page too short for header".into(),
        ));
    }
    let vlen = u32::from_be_bytes([page[0], page[1], page[2], page[3]]) as usize;
    if 4 + vlen > page.len() {
        return Err(MongrelError::InvalidArgument(
            "page validity out of range".into(),
        ));
    }
    let validity = &page[4..4 + vlen];
    let payload = &page[4 + vlen..];

    let mut out = Vec::with_capacity(n);
    let mut cur = 0usize;
    let i64_at = |b: &[u8]| -> i64 {
        if le {
            i64::from_le_bytes(b.try_into().unwrap())
        } else {
            i64::from_be_bytes(b.try_into().unwrap())
        }
    };
    let u64_at = |b: &[u8]| -> u64 {
        if le {
            u64::from_le_bytes(b.try_into().unwrap())
        } else {
            u64::from_be_bytes(b.try_into().unwrap())
        }
    };
    for i in 0..n {
        let non_null = (validity.get(i / 8).copied().unwrap_or(0) >> (i % 8)) & 1 == 1;
        if !non_null {
            out.push(Value::Null);
            advance_null(&ty, payload, &mut cur)?; // consume placeholder slot
            continue;
        }
        let val = match ty {
            TypeId::Int64 | TypeId::TimestampNanos => {
                let b = take(payload, &mut cur, 8)?;
                Value::Int64(i64_at(&b))
            }
            TypeId::Float64 => {
                let b = take(payload, &mut cur, 8)?;
                Value::Float64(f64::from_bits(u64_at(&b)))
            }
            TypeId::Bool => {
                let b = take(payload, &mut cur, 1)?;
                Value::Bool(b[0] != 0)
            }
            TypeId::Int32 | TypeId::UInt32 | TypeId::Date32 => {
                let b = take(payload, &mut cur, 4)?;
                let v = if le {
                    i32::from_le_bytes(b.try_into().unwrap())
                } else {
                    i32::from_be_bytes(b.try_into().unwrap())
                };
                Value::Int64(v as i64)
            }
            TypeId::Bytes => {
                let bytes_start = (n + 1) * 8;
                let lo = read_off(payload, i, le);
                let hi = read_off(payload, i + 1, le);
                Value::Bytes(payload[bytes_start + lo..bytes_start + hi].to_vec())
            }
            TypeId::Embedding { dim } => {
                let mut acc = Vec::with_capacity(dim as usize);
                for _ in 0..dim {
                    let b = take(payload, &mut cur, 4)?;
                    acc.push(f32::from_bits(u32::from_be_bytes(b.try_into().unwrap())));
                }
                Value::Embedding(acc)
            }
            other => {
                return Err(MongrelError::Schema(format!(
                    "decoding for type {other:?} not implemented yet"
                )))
            }
        };
        out.push(val);
    }
    Ok(out)
}

// ---- helpers --------------------------------------------------------

fn validity_bitmap(values: &[Value]) -> Vec<u8> {
    let n = values.len();
    let mut bits = vec![0u8; n.div_ceil(8)];
    for (i, v) in values.iter().enumerate() {
        if !matches!(v, Value::Null) {
            bits[i / 8] |= 1 << (i % 8);
        }
    }
    bits
}

fn fixed_encode(
    values: &[Value],
    _width: usize,
    mut enc: impl FnMut(&Value) -> Result<Vec<u8>>,
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    for v in values {
        out.extend_from_slice(&enc(v)?);
    }
    Ok(out)
}

fn embedding_encode(values: &[Value], dim: u32) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(values.len() * dim as usize * 4);
    for v in values {
        match v {
            Value::Embedding(vec) => {
                if vec.len() != dim as usize {
                    return Err(MongrelError::Schema(format!(
                        "embedding dimension mismatch: expected {dim}, got {}",
                        vec.len()
                    )));
                }
                for x in vec {
                    out.extend_from_slice(&x.to_bits().to_be_bytes());
                }
            }
            Value::Null => {
                for _ in 0..dim {
                    out.extend_from_slice(&0u32.to_be_bytes());
                }
            }
            _ => return Err(type_mismatch(TypeId::Embedding { dim }, v)),
        }
    }
    Ok(out)
}

fn bytes_encode(values: &[Value]) -> Result<Vec<u8>> {
    let n = values.len();
    let mut offsets = Vec::with_capacity((n + 1) * 8);
    let mut data = Vec::new();
    let mut off = 0u64;
    offsets.extend_from_slice(&off.to_be_bytes()); // offsets[0] = 0
    for v in values {
        if let Value::Bytes(b) = v {
            data.extend_from_slice(b);
            off = off
                .checked_add(b.len() as u64)
                .ok_or_else(|| MongrelError::InvalidArgument("bytes length overflow".into()))?;
        }
        offsets.extend_from_slice(&off.to_be_bytes());
    }
    let mut out = offsets;
    out.extend_from_slice(&data);
    Ok(out)
}

fn read_off(payload: &[u8], idx: usize, le: bool) -> usize {
    let s = idx * 8;
    if le {
        u64::from_le_bytes(payload[s..s + 8].try_into().unwrap()) as usize
    } else {
        u64::from_be_bytes(payload[s..s + 8].try_into().unwrap()) as usize
    }
}

fn take(payload: &[u8], cur: &mut usize, n: usize) -> Result<Vec<u8>> {
    if *cur + n > payload.len() {
        return Err(MongrelError::InvalidArgument("payload truncated".into()));
    }
    let s = &payload[*cur..*cur + n];
    *cur += n;
    Ok(s.to_vec())
}

/// Advance the cursor past a null row's placeholder (fixed-width types only).
fn advance_null(ty: &TypeId, payload: &[u8], cur: &mut usize) -> Result<()> {
    let w = match ty {
        TypeId::Int64 | TypeId::Float64 | TypeId::TimestampNanos => 8,
        TypeId::Int32 | TypeId::UInt32 | TypeId::Date32 => 4,
        TypeId::Bool => 1,
        // Variable-length types: null rows have no payload bytes; the offsets
        // table covers them, so nothing to advance here.
        TypeId::Bytes | TypeId::Embedding { .. } => return Ok(()),
        _ => return Ok(()),
    };
    if *cur + w > payload.len() {
        return Err(MongrelError::InvalidArgument(
            "payload truncated at null".into(),
        ));
    }
    *cur += w;
    Ok(())
}

fn type_mismatch(ty: TypeId, v: &Value) -> MongrelError {
    MongrelError::Schema(format!("type mismatch: column {ty:?}, value {v:?}"))
}

// ============================ compressed pages ============================

/// Page-algorithm prefix byte stored at the start of every on-disk page.
const ALGO_PLAIN: u8 = 0; // uncompressed Plain (back-compat / tests)
const ALGO_ZSTD_PLAIN: u8 = 1; // Plain-encode then zstd
const ALGO_ZSTD_DICT: u8 = 2; // dictionary-encode then zstd (low-cardinality Bytes)
const ALGO_ZSTD_DELTA: u8 = 3; // Int64 delta-encode then zstd (sorted/sequential ints)
const ALGO_LZ4_PLAIN: u8 = 4; // Plain-encode then LZ4 (hot runs; Phase 15.3)
const ALGO_LZ4_DICT: u8 = 5; // dictionary-encode then LZ4 (low-cardinality Bytes)
const ALGO_LZ4_DELTA: u8 = 6; // Int64 delta-encode then LZ4 (sorted/sequential ints)

/// Flag bit OR'd into the algo byte when the fixed-width (Int64/Float64 /
/// Bytes-offset) payload is little-endian (Phase 15.7). Little-endian is the
/// de-facto native byte order for x86/ARM, so a LE payload decodes as a memcpy
/// (`cast_slice`) on real hardware instead of the per-element `swap_bytes` the
/// big-endian path pays. Bit 3 is free (algos 0–6 use only the low 3 bits), so
/// an old reader that doesn't know the flag sees algo > 6 and rejects the page
/// via the existing `unknown page algo` branch — backward compatible. Cleared
/// (big-endian) on every pre-15.7 page and on big-endian *writers* (which keep
/// the portable BE layout). A big-endian *reader* of a LE page swaps correctly.
const ALGO_LE_FLAG: u8 = 1 << 3;

#[inline]
fn algo_with_le(algo: u8, le: bool) -> u8 {
    if le {
        algo | ALGO_LE_FLAG
    } else {
        algo
    }
}

/// Per-page compression algorithm chosen at encode time (Phase 15.3). Mirrors
/// the on-disk algo prefix byte; the decoder reads that byte and is fully
/// self-describing, so a run may freely mix pages written under any variant.
#[derive(Debug, Clone, Copy)]
pub enum Compress {
    /// No compression (raw `ALGO_PLAIN`) — [`crate::Table::bulk_load_fast`].
    Plain,
    /// zstd at `level` (3 for compaction, 1 for the level-1 bulk path).
    Zstd(i32),
    /// LZ4 — 3–5× faster decode than zstd with ~10% worse ratio; the default for
    /// hot/mutable runs that get scanned (Phase 15.3).
    Lz4,
}

fn zstd_compress(data: &[u8]) -> Result<Vec<u8>> {
    zstd_compress_level(data, 3)
}

/// zstd at an explicit level (Phase 14.4): the bulk path uses level 1 (3–4×
/// faster, ~10% worse ratio) and background compaction upgrades cold runs back
/// to level 3. `level < 0` is the sentinel for "no compression" (raw `Plain`
/// pages) used by [`Table::bulk_load_fast`].
fn zstd_compress_level(data: &[u8], level: i32) -> Result<Vec<u8>> {
    zstd::encode_all(data, level)
        .map_err(|e| MongrelError::InvalidArgument(format!("zstd compress: {e}")))
}

/// LZ4 block compression (Phase 15.3). `lz4_flex` stores only the compressed
/// payload; the page format is `[algo][validity][payload]` and the decoder knows
/// the uncompressed payload length from the validity header + typed width, so no
/// separate length field is needed. A length-mismatch on decode surfaces as a
/// corrupt-page error.
fn lz4_compress(data: &[u8]) -> Vec<u8> {
    lz4_flex::block::compress_prepend_size(data)
}

/// Ceiling on a decompressed page payload, derived from the logical page shape
/// (`n` rows of `ty` under `algo`) — never from any on-disk length field. A
/// corrupt or maliciously-edited plaintext page (the no-`encryption` default)
/// can't drive a multi-GiB allocation before decompression is validated.
const MAX_VAR_BYTES_PER_ROW: usize = 1 << 14; // 16 KiB / value — generous; real data is far smaller

fn max_decompressed_bytes(ty: TypeId, n: usize, algo: u8) -> usize {
    // The validity section is a 4-byte big-endian length prefix + ceil(n/8) bits.
    let validity = 4 + n.div_ceil(8);
    if matches!(algo, ALGO_ZSTD_DELTA | ALGO_LZ4_DELTA) {
        return validity + n.saturating_mul(8);
    }
    if matches!(algo, ALGO_ZSTD_DICT | ALGO_LZ4_DICT) {
        // index_count + table_count (8) + n× u32 indices + table: ≤ n unique
        // entries, each a 4-byte length + its bytes.
        return validity + 8 + n.saturating_mul(4) + n.saturating_mul(4 + MAX_VAR_BYTES_PER_ROW);
    }
    let payload = match ty {
        TypeId::Bytes => (n + 1).saturating_mul(8) + n.saturating_mul(MAX_VAR_BYTES_PER_ROW),
        TypeId::Embedding { dim } => (dim as usize).saturating_mul(8).saturating_mul(n),
        _ => n.saturating_mul(ty.fixed_size().unwrap_or(8)),
    };
    validity + payload
}

fn lz4_decompress(data: &[u8], max_bytes: usize) -> Result<Vec<u8>> {
    // `decompress_size_prepended` would allocate the 4-byte LE declared size
    // verbatim — a corrupt/malicious plaintext page could claim 0xFFFFFFFF and
    // OOM the process before decompression. Validate against the page-shape
    // bound first, then decompress with the exact declared size.
    if data.len() < 4 {
        return Err(MongrelError::InvalidArgument("lz4 page truncated".into()));
    }
    let declared = u32::from_le_bytes([data[0], data[1], data[2], data[3]]) as usize;
    if declared > max_bytes {
        return Err(MongrelError::InvalidArgument(format!(
            "lz4 declared size {declared} exceeds page limit {max_bytes}"
        )));
    }
    lz4_flex::block::decompress(&data[4..], declared)
        .map_err(|e| MongrelError::InvalidArgument(format!("lz4 decompress: {e}")))
}

fn zstd_decompress(data: &[u8], max_bytes: usize) -> Result<Vec<u8>> {
    // zstd streams (grows incrementally), but a bomb can still expand far past
    // any sane page; reject anything beyond the page-shape bound.
    let out = zstd::decode_all(data)
        .map_err(|e| MongrelError::InvalidArgument(format!("zstd decompress: {e}")))?;
    if out.len() > max_bytes {
        return Err(MongrelError::InvalidArgument(format!(
            "zstd output {} exceeds page limit {max_bytes}",
            out.len()
        )));
    }
    Ok(out)
}

/// Encode a column page for the chosen [`Encoding`]. The page is self-describing
/// (a 1-byte algo prefix), so the reader needs no side metadata to decode it.
pub fn encode_page(ty: TypeId, values: &[Value], encoding: Encoding) -> Result<Vec<u8>> {
    Ok(match encoding {
        Encoding::Plain => {
            let mut out = vec![ALGO_PLAIN];
            out.extend(encode_column(ty, values)?);
            out
        }
        Encoding::Dictionary if matches!(ty, TypeId::Bytes) => {
            let dict = dict_encode_bytes(values);
            let mut out = vec![ALGO_ZSTD_DICT];
            out.extend(zstd_compress(&dict)?);
            out
        }
        _ => {
            let mut out = vec![ALGO_ZSTD_PLAIN];
            let enc = encode_column(ty, values)?;
            out.extend(zstd_compress(&enc)?);
            out
        }
    })
}

/// Decode a self-describing page back into `n` values.
pub fn decode_page(ty: TypeId, page: &[u8], n: usize) -> Result<Vec<Value>> {
    if page.is_empty() {
        return Err(MongrelError::InvalidArgument("empty page".into()));
    }
    let algo = page[0];
    // Phase 15.7: bit 3 is the little-endian flag (LE pages are written by the
    // typed native path). Strip it and route the fixed-width decode through the
    // LE-aware tight loops so the legacy Value reader is correct *and* not
    // pessimal on bulk-loaded (LE) runs.
    let le = algo & ALGO_LE_FLAG != 0;
    let base = algo & !ALGO_LE_FLAG;
    let body = &page[1..];
    // The decompressed (validity-prefixed) payload — zstd, LZ4, or raw.
    let raw_owned;
    let raw: &[u8] = match base {
        ALGO_PLAIN => body,
        ALGO_ZSTD_PLAIN | ALGO_ZSTD_DICT | ALGO_ZSTD_DELTA => {
            raw_owned = zstd_decompress(body, max_decompressed_bytes(ty, n, base))?;
            &raw_owned
        }
        ALGO_LZ4_PLAIN | ALGO_LZ4_DICT | ALGO_LZ4_DELTA => {
            raw_owned = lz4_decompress(body, max_decompressed_bytes(ty, n, base))?;
            &raw_owned
        }
        other => {
            return Err(MongrelError::InvalidArgument(format!(
                "unknown page algo {other}"
            )))
        }
    };
    match base {
        ALGO_PLAIN | ALGO_ZSTD_PLAIN | ALGO_LZ4_PLAIN => decode_column(ty, raw, n, le),
        ALGO_ZSTD_DICT | ALGO_LZ4_DICT => dict_decode_bytes(raw, n),
        ALGO_ZSTD_DELTA | ALGO_LZ4_DELTA => decode_int64_delta_values(raw, ty, n, le),
        _ => unreachable!(),
    }
}

/// Shared Int64 delta-decode → `Value`s (used by `decode_page` for both the
/// zstd and LZ4 delta algos; lets the Value read path read native-written runs).
fn decode_int64_delta_values(raw: &[u8], ty: TypeId, n: usize, le: bool) -> Result<Vec<Value>> {
    if !matches!(ty, TypeId::Int64 | TypeId::TimestampNanos) {
        return Err(MongrelError::InvalidArgument(format!(
            "delta page not valid for {ty:?}"
        )));
    }
    let (validity, p) = split_validity(raw)?;
    let deltas = if le {
        take_i64_le(p, n)?
    } else {
        take_i64_be(p, n)?
    };
    let data = delta_prefix_sum_i64(&deltas);
    let mut out = Vec::with_capacity(n);
    for (i, &v) in data.iter().enumerate() {
        let non_null = (validity.get(i / 8).copied().unwrap_or(0) >> (i % 8)) & 1 == 1;
        out.push(if non_null {
            Value::Int64(v)
        } else {
            Value::Null
        });
    }
    Ok(out)
}

/// Dictionary-encode a Bytes column: validity bitmap + per-row u32 index into a
/// unique-value table. Great for low-cardinality strings (one read of the table
/// + cheap integer indices, then zstd on top).
fn dict_encode_bytes(values: &[Value]) -> Vec<u8> {
    let validity = validity_bitmap(values);
    let mut table: Vec<Vec<u8>> = Vec::new();
    let mut index_of: std::collections::HashMap<&[u8], u32> = std::collections::HashMap::new();
    let mut indices: Vec<u32> = Vec::with_capacity(values.len());
    for v in values {
        let idx = match v {
            Value::Bytes(b) => {
                if let Some(&i) = index_of.get(b.as_slice()) {
                    i
                } else {
                    let i = table.len() as u32;
                    index_of.insert(b.as_slice(), i);
                    table.push(b.clone());
                    i
                }
            }
            _ => 0,
        };
        indices.push(idx);
    }

    let mut out = Vec::new();
    out.extend_from_slice(&(validity.len() as u32).to_be_bytes());
    out.extend_from_slice(&validity);
    out.extend_from_slice(&(indices.len() as u32).to_be_bytes());
    for i in &indices {
        out.extend_from_slice(&i.to_be_bytes());
    }
    out.extend_from_slice(&(table.len() as u32).to_be_bytes());
    for entry in &table {
        out.extend_from_slice(&(entry.len() as u32).to_be_bytes());
        out.extend_from_slice(entry);
    }
    out
}

fn dict_decode_bytes(data: &[u8], n: usize) -> Result<Vec<Value>> {
    let mut cur = 0usize;
    let vlen = read_u32_be(data, &mut cur)? as usize;
    let validity = checked_slice(data, &mut cur, vlen)?;
    let index_count = read_u32_be(data, &mut cur)? as usize;
    let mut indices = Vec::with_capacity(index_count.min(n));
    for _ in 0..index_count {
        indices.push(read_u32_be(data, &mut cur)?);
    }
    let table_count = read_u32_be(data, &mut cur)? as usize;
    let mut table: Vec<Vec<u8>> = Vec::with_capacity(table_count);
    for _ in 0..table_count {
        let len = read_u32_be(data, &mut cur)? as usize;
        table.push(checked_slice(data, &mut cur, len)?.to_vec());
    }

    let mut out = Vec::with_capacity(n);
    for (i, &idx) in indices.iter().enumerate().take(n) {
        let non_null = (validity.get(i / 8).copied().unwrap_or(0) >> (i % 8)) & 1 == 1;
        if !non_null {
            out.push(Value::Null);
        } else {
            let entry = table
                .get(idx as usize)
                .cloned()
                .ok_or_else(|| MongrelError::InvalidArgument("dict index out of range".into()))?;
            out.push(Value::Bytes(entry));
        }
    }
    Ok(out)
}

fn read_u32_be(data: &[u8], cur: &mut usize) -> Result<u32> {
    if *cur + 4 > data.len() {
        return Err(MongrelError::InvalidArgument(
            "dict payload truncated".into(),
        ));
    }
    let v = u32::from_be_bytes([data[*cur], data[*cur + 1], data[*cur + 2], data[*cur + 3]]);
    *cur += 4;
    Ok(v)
}

/// Bounds-checked `data[cur..cur+len]` that advances `cur`. Returns `Err` on a
/// truncated/corrupt dict payload instead of panicking on index OOB.
fn checked_slice<'a>(data: &'a [u8], cur: &mut usize, len: usize) -> Result<&'a [u8]> {
    if *cur + len > data.len() {
        return Err(MongrelError::InvalidArgument(
            "dict payload truncated".into(),
        ));
    }
    let s = &data[*cur..*cur + len];
    *cur += len;
    Ok(s)
}

#[cfg(test)]
mod compressed_tests {
    use super::*;

    #[test]
    fn zstd_plain_round_trip_int64() {
        let vals: Vec<Value> = (0..1000).map(Value::Int64).collect();
        let page = encode_page(TypeId::Int64, &vals, Encoding::Zstd).unwrap();
        assert!(
            page.len() < vals.len() * 8,
            "zstd must shrink sequential ints"
        );
        let back = decode_page(TypeId::Int64, &page, vals.len()).unwrap();
        assert_eq!(back, vals);
    }

    #[test]
    fn dictionary_round_trip_low_card_bytes() {
        let palette: &[&[u8]] = &[b"red", b"green", b"blue", b"red"];
        let vals: Vec<Value> = (0..500)
            .map(|i| Value::Bytes(palette[i % palette.len()].to_vec()))
            .collect();
        let page = encode_page(TypeId::Bytes, &vals, Encoding::Dictionary).unwrap();
        assert!(
            page.len() < 100,
            "4 distinct strings over 500 rows must compress to a tiny page, got {}",
            page.len()
        );
        let back = decode_page(TypeId::Bytes, &page, vals.len()).unwrap();
        assert_eq!(back, vals);
    }

    #[test]
    fn plain_page_still_round_trips() {
        let vals = vec![Value::Int64(1), Value::Null, Value::Int64(9)];
        let page = encode_page(TypeId::Int64, &vals, Encoding::Plain).unwrap();
        assert_eq!(page[0], ALGO_PLAIN);
        assert_eq!(decode_page(TypeId::Int64, &page, 3).unwrap(), vals);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_int64_with_nulls() {
        let vals = vec![
            Value::Int64(1),
            Value::Null,
            Value::Int64(-5),
            Value::Int64(1 << 40),
        ];
        let page = encode_column(TypeId::Int64, &vals).unwrap();
        let back = decode_column(TypeId::Int64, &page, vals.len(), false).unwrap();
        assert_eq!(back, vals);
    }

    #[test]
    fn round_trips_bytes() {
        let vals = vec![
            Value::Bytes(b"hello".to_vec()),
            Value::Null,
            Value::Bytes(b"".to_vec()),
            Value::Bytes(b"wide \x00 byte".to_vec()),
        ];
        let page = encode_column(TypeId::Bytes, &vals).unwrap();
        let back = decode_column(TypeId::Bytes, &page, vals.len(), false).unwrap();
        assert_eq!(back, vals);
    }

    #[test]
    fn round_trips_embedding() {
        let vals = vec![
            Value::Embedding(vec![1.0, -2.5, 3.0]),
            Value::Null,
            Value::Embedding(vec![0.0; 3]),
        ];
        let page = encode_column(TypeId::Embedding { dim: 3 }, &vals).unwrap();
        let back = decode_column(TypeId::Embedding { dim: 3 }, &page, vals.len(), false).unwrap();
        assert_eq!(back, vals);
    }

    #[test]
    fn round_trips_bool() {
        let vals = vec![
            Value::Bool(true),
            Value::Bool(false),
            Value::Null,
            Value::Bool(true),
        ];
        let page = encode_column(TypeId::Bool, &vals).unwrap();
        assert_eq!(
            decode_column(TypeId::Bool, &page, vals.len(), false).unwrap(),
            vals
        );
    }
}

// ============================ arrow-native column path ====================
//
// Typed buffers + a validity bitmap, with encode/decode that never touch the
// `Value` enum — the fast path for bulk ingest and vectorized scans. The on-disk
// page format is identical to the `Value`-based path above, so a run written by
// either path can be read by either reader.

/// A column as typed buffers (Arrow-compatible), no `Value` allocations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum NativeColumn {
    Int64 {
        data: Vec<i64>,
        validity: Vec<u8>,
    },
    Float64 {
        data: Vec<f64>,
        validity: Vec<u8>,
    },
    /// 1 byte per value (0/1).
    Bool {
        data: Vec<u8>,
        validity: Vec<u8>,
    },
    /// Arrow-style: `offsets.len() == n + 1`, `values[offsets[i]..offsets[i+1]]`.
    Bytes {
        offsets: Vec<u32>,
        values: Vec<u8>,
        validity: Vec<u8>,
    },
}

impl NativeColumn {
    pub fn len(&self) -> usize {
        match self {
            NativeColumn::Int64 { data, .. } => data.len(),
            NativeColumn::Float64 { data, .. } => data.len(),
            NativeColumn::Bool { data, .. } => data.len(),
            NativeColumn::Bytes { offsets, .. } => offsets.len().saturating_sub(1),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Verify internal invariants after deserialization (hardening (b)).
    /// Returns `false` if the column is structurally invalid (e.g. offsets
    /// length mismatch, last offset out of bounds).
    pub fn validate(&self) -> bool {
        match self {
            NativeColumn::Int64 { data, validity } => {
                validity.len() == data.len().div_ceil(8) || validity.is_empty()
            }
            NativeColumn::Float64 { data, validity } => {
                validity.len() == data.len().div_ceil(8) || validity.is_empty()
            }
            NativeColumn::Bool { data, validity } => {
                validity.len() == data.len().div_ceil(8) || validity.is_empty()
            }
            NativeColumn::Bytes {
                offsets,
                values,
                validity,
            } => {
                let n = offsets.len().saturating_sub(1);
                (validity.len() == n.div_ceil(8) || validity.is_empty())
                    && offsets
                        .last()
                        .map(|&last| (last as usize) <= values.len())
                        .unwrap_or(true)
            }
        }
    }

    /// Count null rows in the first `n` slots. An empty validity bitmap means
    /// every slot is non-null.
    pub fn null_count(&self, n: usize) -> usize {
        if n == 0 {
            return 0;
        }
        let validity = match self {
            NativeColumn::Int64 { validity, .. }
            | NativeColumn::Float64 { validity, .. }
            | NativeColumn::Bool { validity, .. }
            | NativeColumn::Bytes { validity, .. } => validity,
        };
        if validity.is_empty() {
            return 0;
        }
        (0..n).filter(|&i| !validity_bit(validity, i)).count()
    }

    /// Approximate heap size (used to bound the decoded-page cache, Phase 15.4).
    pub fn approx_bytes(&self) -> u64 {
        match self {
            NativeColumn::Int64 { data, validity } => {
                (data.len() as u64) * 8 + validity.len() as u64
            }
            NativeColumn::Float64 { data, validity } => {
                (data.len() as u64) * 8 + validity.len() as u64
            }
            NativeColumn::Bool { data, validity } => data.len() as u64 + validity.len() as u64,
            NativeColumn::Bytes {
                offsets,
                values,
                validity,
            } => values.len() as u64 + (offsets.len() as u64) * 4 + validity.len() as u64,
        }
    }

    /// A fully-non-null Int64 column of `n` sequential values `start..start+n`.
    pub fn int64_sequence(start: i64, n: usize) -> Self {
        NativeColumn::Int64 {
            data: (0..n).map(|i| start + i as i64).collect(),
            validity: full_validity(n),
        }
    }

    /// A fully-non-null Int64 column filled with `value`.
    pub fn int64_constant(value: i64, n: usize) -> Self {
        NativeColumn::Int64 {
            data: vec![value; n],
            validity: full_validity(n),
        }
    }

    /// A fully-non-null Bool column filled with `value`.
    pub fn bool_constant(value: bool, n: usize) -> Self {
        NativeColumn::Bool {
            data: vec![if value { 1 } else { 0 }; n],
            validity: full_validity(n),
        }
    }

    /// Gather the values at `indices` into a new typed column (vectorized scan
    /// merge: pick the visible versions). Skips the `Value` enum entirely.
    pub fn gather(&self, indices: &[usize]) -> NativeColumn {
        let bit = |v: &[u8], i: usize| (v.get(i / 8).copied().unwrap_or(0) >> (i % 8)) & 1 == 1;
        match self {
            NativeColumn::Int64 { data, validity } => NativeColumn::Int64 {
                data: indices.iter().map(|&i| data[i]).collect(),
                validity: validity_bitmap_from(indices.iter().map(|&i| bit(validity, i))),
            },
            NativeColumn::Float64 {
                data: fdata,
                validity: fval,
            } => NativeColumn::Float64 {
                data: indices.iter().map(|&i| fdata[i]).collect(),
                validity: validity_bitmap_from(indices.iter().map(|&i| bit(fval, i))),
            },
            NativeColumn::Bool {
                data: bdata,
                validity: bval,
            } => NativeColumn::Bool {
                data: indices.iter().map(|&i| bdata[i]).collect(),
                validity: validity_bitmap_from(indices.iter().map(|&i| bit(bval, i))),
            },
            NativeColumn::Bytes {
                offsets,
                values,
                validity,
            } => {
                let mut out_offsets = Vec::with_capacity(indices.len() + 1);
                let mut out_values = Vec::new();
                out_offsets.push(0);
                for &i in indices {
                    let lo = offsets[i] as usize;
                    let hi = offsets[i + 1] as usize;
                    out_values.extend_from_slice(&values[lo..hi]);
                    out_offsets.push(out_values.len() as u32);
                }
                NativeColumn::Bytes {
                    offsets: out_offsets,
                    values: out_values,
                    validity: validity_bitmap_from(indices.iter().map(|&i| bit(validity, i))),
                }
            }
        }
    }

    /// Typed value at `idx` as a `Value`, or `None` if null / out of range.
    /// Used by the batched row-materialization path (Phase 16.3b) to build only
    /// survivor `Row`s straight from typed buffers, avoiding the full-column
    /// `Vec<Value>` decode + per-row `.cloned()` of the legacy path.
    pub fn value_at(&self, idx: usize) -> Option<Value> {
        match self {
            NativeColumn::Int64 { data, validity } => {
                if !validity_bit(validity, idx) {
                    return None;
                }
                data.get(idx).copied().map(Value::Int64)
            }
            NativeColumn::Float64 { data, validity } => {
                if !validity_bit(validity, idx) {
                    return None;
                }
                data.get(idx).copied().map(Value::Float64)
            }
            NativeColumn::Bool { data, validity } => {
                if !validity_bit(validity, idx) {
                    return None;
                }
                data.get(idx).copied().map(|b| Value::Bool(b != 0))
            }
            NativeColumn::Bytes {
                offsets,
                values,
                validity,
            } => {
                if !validity_bit(validity, idx) {
                    return None;
                }
                if idx + 1 >= offsets.len() {
                    return None;
                }
                let lo = offsets[idx] as usize;
                let hi = offsets[idx + 1] as usize;
                Some(Value::Bytes(values[lo..hi].to_vec()))
            }
        }
    }

    /// Contiguous slice of rows `[start, end)` — used to split a column into
    /// fixed-size pages at encode time (cheap memcpy + bitmap rebuild).
    pub fn slice_range(&self, start: usize, end: usize) -> NativeColumn {
        let mk_validity = |v: &[u8]| -> Vec<bool> {
            (0..(end - start))
                .map(|i| validity_bit(v, start + i))
                .collect()
        };
        match self {
            NativeColumn::Int64 { data, validity } => NativeColumn::Int64 {
                data: data[start..end].to_vec(),
                validity: validity_bitmap_from(mk_validity(validity)),
            },
            NativeColumn::Float64 { data, validity } => NativeColumn::Float64 {
                data: data[start..end].to_vec(),
                validity: validity_bitmap_from(mk_validity(validity)),
            },
            NativeColumn::Bool { data, validity } => NativeColumn::Bool {
                data: data[start..end].to_vec(),
                validity: validity_bitmap_from(mk_validity(validity)),
            },
            NativeColumn::Bytes {
                offsets,
                values,
                validity,
            } => {
                let lo = offsets[start] as usize;
                let hi = offsets[end] as usize;
                let new_offsets: Vec<u32> = offsets[start..=end]
                    .iter()
                    .map(|o| *o - offsets[start])
                    .collect();
                NativeColumn::Bytes {
                    offsets: new_offsets,
                    values: values[lo..hi].to_vec(),
                    validity: validity_bitmap_from(mk_validity(validity)),
                }
            }
        }
    }

    /// Concatenate same-typed columns into one — used by the reader to stitch
    /// multi-page columns back into a single `NativeColumn`.
    pub fn concat(parts: &[NativeColumn]) -> NativeColumn {
        match parts.first() {
            Some(NativeColumn::Int64 { .. }) => {
                let mut data = Vec::new();
                let mut non_null: Vec<bool> = Vec::new();
                for p in parts {
                    if let NativeColumn::Int64 { data: d, validity } = p {
                        data.extend_from_slice(d);
                        non_null.extend((0..d.len()).map(|i| validity_bit(validity, i)));
                    }
                }
                NativeColumn::Int64 {
                    data,
                    validity: validity_bitmap_from(non_null),
                }
            }
            Some(NativeColumn::Float64 { .. }) => {
                let mut data = Vec::new();
                let mut non_null: Vec<bool> = Vec::new();
                for p in parts {
                    if let NativeColumn::Float64 { data: d, validity } = p {
                        data.extend_from_slice(d);
                        non_null.extend((0..d.len()).map(|i| validity_bit(validity, i)));
                    }
                }
                NativeColumn::Float64 {
                    data,
                    validity: validity_bitmap_from(non_null),
                }
            }
            Some(NativeColumn::Bool { .. }) => {
                let mut data = Vec::new();
                let mut non_null: Vec<bool> = Vec::new();
                for p in parts {
                    if let NativeColumn::Bool { data: d, validity } = p {
                        data.extend_from_slice(d);
                        non_null.extend((0..d.len()).map(|i| validity_bit(validity, i)));
                    }
                }
                NativeColumn::Bool {
                    data,
                    validity: validity_bitmap_from(non_null),
                }
            }
            Some(NativeColumn::Bytes { .. }) => {
                let mut offsets: Vec<u32> = vec![0];
                let mut values = Vec::new();
                let mut non_null: Vec<bool> = Vec::new();
                for p in parts {
                    if let NativeColumn::Bytes {
                        offsets: off,
                        values: val,
                        validity,
                    } = p
                    {
                        for w in off.windows(2) {
                            values.extend_from_slice(&val[w[0] as usize..w[1] as usize]);
                            offsets.push(values.len() as u32);
                        }
                        non_null.extend((0..off.len() - 1).map(|i| validity_bit(validity, i)));
                    }
                }
                NativeColumn::Bytes {
                    offsets,
                    values,
                    validity: validity_bitmap_from(non_null),
                }
            }
            None => NativeColumn::Bytes {
                offsets: vec![0],
                values: Vec::new(),
                validity: Vec::new(),
            },
        }
    }
}

fn full_validity(n: usize) -> Vec<u8> {
    validity_bitmap_from(std::iter::repeat(true).take(n))
}

/// An all-null typed column of length `n` (for schema-evolved columns absent
/// from an older run).
pub fn null_native(ty: TypeId, n: usize) -> NativeColumn {
    let validity = vec![0u8; n.div_ceil(8)];
    match ty {
        TypeId::Int64 | TypeId::TimestampNanos => NativeColumn::Int64 {
            data: vec![0; n],
            validity,
        },
        TypeId::Float64 => NativeColumn::Float64 {
            data: vec![0.0; n],
            validity,
        },
        TypeId::Bool => NativeColumn::Bool {
            data: vec![0; n],
            validity,
        },
        _ => NativeColumn::Bytes {
            offsets: vec![0u32; n + 1],
            values: Vec::new(),
            validity,
        },
    }
}

/// Validity bit `i` of a packed bitmap (0 → null).
#[inline]
pub fn validity_bit(validity: &[u8], i: usize) -> bool {
    (validity.get(i / 8).copied().unwrap_or(0) >> (i % 8)) & 1 == 1
}

/// Whether all `n` slots are non-null (no missing validity bit set). Used to
/// pick the branchless vectorized accumulation path for all-non-null columns.
pub fn all_non_null(validity: &[u8], n: usize) -> bool {
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

/// Per-column `(min, max, null_count)` for page-index pruning. The min/max byte
/// encodings mirror [`crate::memtable::Value::encode_key`] (big-endian ints,
/// `to_bits` for floats, raw bytes for `Bytes`); the reader decodes them back to
/// the typed value before comparing, so float byte order is not relied on.
pub fn native_min_max(ty: TypeId, col: &NativeColumn) -> (Option<Vec<u8>>, Option<Vec<u8>>, u64) {
    let _ = ty;
    match col {
        NativeColumn::Int64 { data, validity } => {
            let (mut mn, mut mx, mut nulls) = (None::<i64>, None::<i64>, 0u64);
            for (i, v) in data.iter().enumerate() {
                if !validity_bit(validity, i) {
                    nulls += 1;
                    continue;
                }
                mn = Some(mn.map_or(*v, |m| m.min(*v)));
                mx = Some(mx.map_or(*v, |m| m.max(*v)));
            }
            (
                mn.map(|v| v.to_be_bytes().to_vec()),
                mx.map(|v| v.to_be_bytes().to_vec()),
                nulls,
            )
        }
        NativeColumn::Float64 { data, validity } => {
            let (mut mn, mut mx, mut nulls) = (None::<f64>, None::<f64>, 0u64);
            for (i, v) in data.iter().enumerate() {
                if !validity_bit(validity, i) || v.is_nan() {
                    nulls += 1;
                    continue;
                }
                mn = Some(mn.map_or(*v, |m| m.min(*v)));
                mx = Some(mx.map_or(*v, |m| m.max(*v)));
            }
            (
                mn.map(|v| v.to_bits().to_be_bytes().to_vec()),
                mx.map(|v| v.to_bits().to_be_bytes().to_vec()),
                nulls,
            )
        }
        NativeColumn::Bool { data, validity } => {
            let (mut any_t, mut any_f, mut nulls) = (false, false, 0u64);
            for (i, v) in data.iter().enumerate() {
                if !validity_bit(validity, i) {
                    nulls += 1;
                    continue;
                }
                if *v != 0 {
                    any_t = true;
                } else {
                    any_f = true;
                }
            }
            let min = if any_f || any_t {
                Some(vec![if any_f { 0 } else { 1 }])
            } else {
                None
            };
            let max = if any_t || any_f {
                Some(vec![if any_t { 1 } else { 0 }])
            } else {
                None
            };
            (min, max, nulls)
        }
        NativeColumn::Bytes {
            offsets,
            values,
            validity,
        } => {
            let mut mn: Option<&[u8]> = None;
            let mut mx: Option<&[u8]> = None;
            let mut nulls = 0u64;
            for i in 0..offsets.len().saturating_sub(1) {
                if !validity_bit(validity, i) {
                    nulls += 1;
                    continue;
                }
                let s = &values[offsets[i] as usize..offsets[i + 1] as usize];
                mn = Some(match mn {
                    None => s,
                    Some(m) if s < m => s,
                    Some(m) => m,
                });
                mx = Some(match mx {
                    None => s,
                    Some(m) if s > m => s,
                    Some(m) => m,
                });
            }
            (mn.map(|s| s.to_vec()), mx.map(|s| s.to_vec()), nulls)
        }
    }
}

/// Build a value-derived [`crate::page::PageStat`] for a single page spanning
/// `[first_row_id, last_row_id]`. The offset / length slots are left zero for
/// [`write_run_with`] to fill after compression/encryption.
pub fn page_stat_for(
    ty: TypeId,
    col: &NativeColumn,
    first_row_id: u64,
    last_row_id: u64,
) -> crate::page::PageStat {
    let (min, max, null_count) = native_min_max(ty, col);
    crate::page::PageStat {
        first_row_id,
        last_row_id,
        null_count,
        row_count: col.len() as u32,
        min,
        max,
        offset: 0,
        compressed_len: 0,
        uncompressed_len: 0,
    }
}

/// Index-key encoding for element `i` of a typed column — the per-element
/// analogue of [`crate::memtable::Value::encode_key`] (big-endian ints,
/// `to_bits` for floats, raw bytes for `Bytes`). Returns `None` for null slots
/// so callers can skip them without constructing a `Value`. Used by the typed
/// bulk-index path (Phase 14.2) to build HOT/bitmap keys with no `Value` enum
/// and no per-row `HashMap`.
pub fn encode_key_native(_ty: TypeId, col: &NativeColumn, i: usize) -> Option<Vec<u8>> {
    match col {
        NativeColumn::Int64 { data, validity } if validity_bit(validity, i) => {
            Some(data[i].to_be_bytes().to_vec())
        }
        NativeColumn::Float64 { data, validity } if validity_bit(validity, i) => {
            Some(data[i].to_bits().to_be_bytes().to_vec())
        }
        NativeColumn::Bool { data, validity } if validity_bit(validity, i) => Some(vec![data[i]]),
        NativeColumn::Bytes {
            offsets,
            values,
            validity,
        } if validity_bit(validity, i) => {
            let lo = offsets[i] as usize;
            let hi = offsets[i + 1] as usize;
            Some(values[lo..hi].to_vec())
        }
        _ => None,
    }
}

/// Borrow the `i`-th value of a `Bytes` column (raw document bytes for FM/sparse
/// indexes), or `None` if null. Avoids a per-row `Vec<u8>` allocation on the
/// bulk-index path; the caller copies only when inserting.
pub fn native_bytes_at(col: &NativeColumn, i: usize) -> Option<&[u8]> {
    match col {
        NativeColumn::Bytes {
            offsets,
            values,
            validity,
        } if validity_bit(validity, i) => {
            let lo = offsets[i] as usize;
            let hi = offsets[i + 1] as usize;
            Some(&values[lo..hi])
        }
        _ => None,
    }
}

/// Build a typed column from a `Vec<Value>` (fallback path; the fast paths avoid
/// `Value` entirely).
pub fn values_to_native(ty: TypeId, values: &[Value]) -> NativeColumn {
    let n = values.len();
    let mut non_null = vec![false; n];
    match ty {
        TypeId::Int64 | TypeId::TimestampNanos => {
            let mut data = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Int64(x) => {
                        non_null[i] = true;
                        data.push(*x);
                    }
                    _ => data.push(0),
                }
            }
            NativeColumn::Int64 {
                data,
                validity: validity_bitmap_from(non_null),
            }
        }
        TypeId::Float64 => {
            let mut data = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Float64(x) => {
                        non_null[i] = true;
                        data.push(*x);
                    }
                    _ => data.push(0.0),
                }
            }
            NativeColumn::Float64 {
                data,
                validity: validity_bitmap_from(non_null),
            }
        }
        TypeId::Bool => {
            let mut data = Vec::with_capacity(n);
            for (i, v) in values.iter().enumerate() {
                match v {
                    Value::Bool(x) => {
                        non_null[i] = true;
                        data.push(if *x { 1 } else { 0 });
                    }
                    _ => data.push(0),
                }
            }
            NativeColumn::Bool {
                data,
                validity: validity_bitmap_from(non_null),
            }
        }
        _ => {
            let mut offsets = Vec::with_capacity(n + 1);
            let mut vals = Vec::new();
            offsets.push(0u32);
            for (i, v) in values.iter().enumerate() {
                if let Value::Bytes(b) = v {
                    non_null[i] = true;
                    vals.extend_from_slice(b);
                }
                offsets.push(vals.len() as u32);
            }
            NativeColumn::Bytes {
                offsets,
                values: vals,
                validity: validity_bitmap_from(non_null),
            }
        }
    }
}

fn validity_bitmap_from(non_null: impl IntoIterator<Item = bool>) -> Vec<u8> {
    let bits: Vec<bool> = non_null.into_iter().collect();
    let n = bits.len();
    let mut out = vec![0u8; n.div_ceil(8)];
    for (i, &b) in bits.iter().enumerate() {
        if b {
            out[i / 8] |= 1 << (i % 8);
        }
    }
    out
}

/// Encode a typed column straight to an algo-prefixed page (no `Value`).
///
/// `compress` selects the on-disk algorithm (Phase 14.4 / 15.3): `Plain` emits
/// raw `ALGO_PLAIN` (no compression — [`crate::Table::bulk_load_fast`]); `Zstd(lvl)`
/// writes the zstd variants at `lvl`; `Lz4` writes the LZ4 variants (hot runs,
/// faster decode). `Encoding::Plain` forces raw plain regardless of `compress`.
pub fn encode_page_native(
    ty: TypeId,
    col: &NativeColumn,
    encoding: Encoding,
    compress: Compress,
    le: bool,
) -> Result<Vec<u8>> {
    let raw = matches!(compress, Compress::Plain) || matches!(encoding, Encoding::Plain);
    Ok(match (ty, col) {
        (TypeId::Int64 | TypeId::TimestampNanos, NativeColumn::Int64 { data, validity }) => {
            if matches!(encoding, Encoding::Delta) && !raw {
                let mut payload = Vec::with_capacity(4 + validity.len() + data.len() * 8);
                payload.extend_from_slice(&(validity.len() as u32).to_be_bytes());
                payload.extend_from_slice(validity);
                let mut deltas = Vec::with_capacity(data.len());
                let mut prev = 0i64;
                for v in data {
                    deltas.push(v - prev);
                    prev = *v;
                }
                if le {
                    append_i64_le(&mut payload, &deltas);
                } else {
                    append_i64_be(&mut payload, &deltas);
                }
                compress_delta_payload(&payload, compress, le)?
            } else {
                native_plain_page(validity, compress, raw, le, |p| {
                    if le {
                        append_i64_le(p, data);
                    } else {
                        append_i64_be(p, data);
                    }
                })
            }
        }
        (
            TypeId::Float64,
            NativeColumn::Float64 {
                data: fdata,
                validity,
            },
        ) => native_plain_page(validity, compress, raw, le, |p| {
            let bits: &[u64] = bytemuck::cast_slice::<f64, u64>(fdata);
            if le {
                append_u64_le(p, bits);
            } else {
                append_u64_be(p, bits);
            }
        }),
        (
            TypeId::Bool,
            NativeColumn::Bool {
                data: bdata,
                validity,
            },
        ) => native_plain_page(validity, compress, raw, le, |p| p.extend_from_slice(bdata)),
        (
            TypeId::Bytes,
            NativeColumn::Bytes {
                offsets,
                values,
                validity,
            },
        ) => {
            if matches!(encoding, Encoding::Dictionary) && !raw {
                let dict = dict_encode_bytes_native(offsets, values, validity);
                compress_dict_payload(&dict, compress, le)?
            } else {
                native_plain_page(validity, compress, raw, le, |p| {
                    let offs: Vec<u64> = offsets.iter().map(|o| *o as u64).collect();
                    if le {
                        append_u64_le(p, &offs);
                    } else {
                        append_u64_be(p, &offs);
                    }
                    p.extend_from_slice(values);
                })
            }
        }
        _ => {
            return Err(MongrelError::InvalidArgument(format!(
                "encode_page_native: unsupported (ty={ty:?})"
            )))
        }
    })
}

/// Compress a delta-encoded Int64 payload under the chosen algorithm. `le`
/// (Phase 15.7) OR's the [`ALGO_LE_FLAG`] into the algo byte so the delta
/// carrier is decoded as little-endian.
fn compress_delta_payload(payload: &[u8], compress: Compress, le: bool) -> Result<Vec<u8>> {
    Ok(match compress {
        Compress::Plain => {
            let mut out = vec![algo_with_le(ALGO_PLAIN, le)];
            out.extend_from_slice(payload);
            out
        }
        Compress::Zstd(level) => {
            let mut out = vec![algo_with_le(ALGO_ZSTD_DELTA, le)];
            out.extend(zstd_compress_level(payload, level)?);
            out
        }
        Compress::Lz4 => {
            let mut out = vec![algo_with_le(ALGO_LZ4_DELTA, le)];
            out.extend(lz4_compress(payload));
            out
        }
    })
}

/// Compress a dictionary-encoded Bytes payload under the chosen algorithm. `le`
/// is accepted for signature symmetry but has no effect on dict payloads (the
/// dict indices/table are byte-order-agnostic u32s; the flag stays clear so a
/// reader never tries an LE int path on them).
fn compress_dict_payload(payload: &[u8], compress: Compress, _le: bool) -> Result<Vec<u8>> {
    Ok(match compress {
        Compress::Plain => {
            let mut out = vec![ALGO_PLAIN];
            out.extend_from_slice(payload);
            out
        }
        Compress::Zstd(level) => {
            let mut out = vec![ALGO_ZSTD_DICT];
            out.extend(zstd_compress_level(payload, level)?);
            out
        }
        Compress::Lz4 => {
            let mut out = vec![ALGO_LZ4_DICT];
            out.extend(lz4_compress(payload));
            out
        }
    })
}

/// Build a plain (non-delta, non-dict) page payload, then compress it under the
/// chosen algorithm — raw `ALGO_PLAIN`, `ALGO_ZSTD_PLAIN`, or `ALGO_LZ4_PLAIN`.
/// When `le` is set (Phase 15.7), the [`ALGO_LE_FLAG`] bit is OR'd into the
/// stored algo byte so the decoder picks the memcpy LE path.
fn native_plain_page(
    validity: &[u8],
    compress: Compress,
    raw: bool,
    le: bool,
    fill_payload: impl FnOnce(&mut Vec<u8>),
) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(validity.len() as u32).to_be_bytes());
    payload.extend_from_slice(validity);
    fill_payload(&mut payload);
    if raw {
        let mut out = vec![algo_with_le(ALGO_PLAIN, le)];
        out.extend(payload);
        out
    } else {
        match compress {
            Compress::Zstd(level) => {
                let mut out = vec![algo_with_le(ALGO_ZSTD_PLAIN, le)];
                out.extend(zstd_compress_level(&payload, level).expect("zstd compress"));
                out
            }
            Compress::Lz4 => {
                let mut out = vec![algo_with_le(ALGO_LZ4_PLAIN, le)];
                out.extend(lz4_compress(&payload));
                out
            }
            Compress::Plain => {
                let mut out = vec![algo_with_le(ALGO_PLAIN, le)];
                out.extend(payload);
                out
            }
        }
    }
}

/// Decode an algo-prefixed page straight to a typed column (no `Value`). The
/// algo byte is fully self-describing: it selects the compression (raw Plain,
/// zstd, or LZ4 — Phase 15.3) and the encoding family (plain / delta / dict),
/// so a reader decodes any page without side metadata.
pub fn decode_page_native(ty: TypeId, page: &[u8], n: usize) -> Result<NativeColumn> {
    use std::borrow::Cow;
    if page.is_empty() {
        return Err(MongrelError::InvalidArgument("empty page".into()));
    }
    let algo = page[0];
    let body = &page[1..];
    // Phase 15.7: bit 3 is the little-endian flag; the low 3 bits carry the
    // (compression × encoding-family) algo. Strip the flag for family dispatch
    // and remember it to pick the memcpy LE decode vs the swap BE decode.
    let le = algo & ALGO_LE_FLAG != 0;
    let base = algo & !ALGO_LE_FLAG;
    let plain_algo = matches!(base, ALGO_PLAIN | ALGO_ZSTD_PLAIN | ALGO_LZ4_PLAIN);
    let delta_algo = matches!(base, ALGO_ZSTD_DELTA | ALGO_LZ4_DELTA);
    let dict_algo = matches!(base, ALGO_ZSTD_DICT | ALGO_LZ4_DICT);
    if !plain_algo && !delta_algo && !dict_algo {
        return Err(MongrelError::InvalidArgument(format!(
            "decode_page_native: unsupported algo {algo} for ty {ty:?}"
        )));
    }
    // Step 1: obtain the uncompressed (validity-prefixed) payload.
    let raw: Cow<[u8]> = match base {
        ALGO_PLAIN => Cow::Borrowed(body),
        ALGO_ZSTD_PLAIN | ALGO_ZSTD_DELTA | ALGO_ZSTD_DICT => {
            Cow::Owned(zstd_decompress(body, max_decompressed_bytes(ty, n, base))?)
        }
        ALGO_LZ4_PLAIN | ALGO_LZ4_DELTA | ALGO_LZ4_DICT => {
            Cow::Owned(lz4_decompress(body, max_decompressed_bytes(ty, n, base))?)
        }
        _ => unreachable!(),
    };

    // Step 2: dictionary-encoded Bytes (only valid family for dict algos). Dict
    // payloads are never written LE, so `le` is ignored here.
    if dict_algo {
        return if matches!(ty, TypeId::Bytes) {
            dict_decode_bytes_native(&raw, n)
        } else {
            Err(MongrelError::InvalidArgument(format!(
                "decode_page_native: dict algo {algo} only valid for Bytes, got {ty:?}"
            )))
        };
    }

    // Int64 decode helper: memcpy LE path or swap BE path.
    let take_i64 = |p: &[u8]| -> Result<Vec<i64>> {
        if le {
            take_i64_le(p, n)
        } else {
            take_i64_be(p, n)
        }
    };
    // Step 3: delta-decoded Int64 (sequential row-id / sorted-int columns).
    if delta_algo {
        if !matches!(ty, TypeId::Int64 | TypeId::TimestampNanos) {
            return Err(MongrelError::InvalidArgument(format!(
                "decode_page_native: delta algo {algo} only valid for Int64, got {ty:?}"
            )));
        }
        let (validity, p) = split_validity(&raw)?;
        let deltas = take_i64(p)?;
        let data = delta_prefix_sum_i64(&deltas);
        return Ok(NativeColumn::Int64 { data, validity });
    }

    // Step 4: plain payload — dispatch on type.
    match ty {
        TypeId::Int64 | TypeId::TimestampNanos => {
            let (validity, p) = split_validity(&raw)?;
            Ok(NativeColumn::Int64 {
                data: take_i64(p)?,
                validity,
            })
        }
        TypeId::Float64 => {
            let (validity, p) = split_validity(&raw)?;
            let bits = if le {
                take_u64_le(p, n)?
            } else {
                take_u64_be(p, n)?
            };
            let data: Vec<f64> = bits.into_iter().map(f64::from_bits).collect();
            Ok(NativeColumn::Float64 { data, validity })
        }
        TypeId::Bool => {
            let (validity, p) = split_validity(&raw)?;
            if p.len() < n {
                return Err(MongrelError::InvalidArgument(
                    "bool payload truncated".into(),
                ));
            }
            Ok(NativeColumn::Bool {
                data: p[..n].to_vec(),
                validity,
            })
        }
        TypeId::Bytes => decode_bytes_plain_payload(&raw, n, le),
        _ => Err(MongrelError::InvalidArgument(format!(
            "decode_page_native: unsupported ty {ty:?}"
        ))),
    }
}

fn split_validity(raw: &[u8]) -> Result<(Vec<u8>, &[u8])> {
    if raw.len() < 4 {
        return Err(MongrelError::InvalidArgument("page validity header".into()));
    }
    let vlen = u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize;
    if 4 + vlen > raw.len() {
        return Err(MongrelError::InvalidArgument("page validity range".into()));
    }
    Ok((raw[4..4 + vlen].to_vec(), &raw[4 + vlen..]))
}

/// Decode the plain `Bytes` payload (`[validity][(n+1) u64 BE offsets][values]`)
/// shared by `ALGO_ZSTD_PLAIN` (post-decompress) and `ALGO_PLAIN` (raw) pages.
fn decode_bytes_plain_payload(raw: &[u8], n: usize, le: bool) -> Result<NativeColumn> {
    let (validity, p) = split_validity(raw)?;
    let table = (n + 1) * 8;
    if p.len() < table {
        return Err(MongrelError::InvalidArgument(
            "bytes offsets truncated".into(),
        ));
    }
    let offsets_be: Vec<u64> = if le {
        take_u64_le(p, n + 1)?
    } else {
        take_u64_be(p, n + 1)?
    };
    let offsets: Vec<u32> = offsets_be.into_iter().map(|o| o as u32).collect();
    let values = p[table..].to_vec();
    Ok(NativeColumn::Bytes {
        offsets,
        values,
        validity,
    })
}

fn take_i64_be(p: &[u8], n: usize) -> Result<Vec<i64>> {
    if p.len() < n * 8 {
        return Err(MongrelError::InvalidArgument(
            "int64 payload truncated".into(),
        ));
    }
    Ok(take_u64_be(p, n)?.into_iter().map(|u| u as i64).collect())
}

/// Inclusive prefix sum of i64 deltas → reconstructed values (Phase 15.6). This
/// is the hot path for every `ALGO_*_DELTA` page: the row-id and committed-epoch
/// columns of every sorted run, plus any sorted Int64 data column. Vectorized on
/// x86-64 with AVX2 (4-lane in-register block scan + a running carry broadcast
/// across blocks); a tight scalar loop elsewhere and for the <4-element tail.
/// Addition wraps (modular), matching the decoder's other integer paths;
/// row-id/epoch deltas reconstruct values well within i64 range.
fn delta_prefix_sum_i64(deltas: &[i64]) -> Vec<i64> {
    let mut out = vec![0i64; deltas.len()];
    prefix_sum_i64_into(deltas, &mut out);
    out
}

fn prefix_sum_i64_into(deltas: &[i64], out: &mut [i64]) {
    debug_assert_eq!(deltas.len(), out.len());
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && deltas.len() >= 4 {
            // SAFETY: AVX2 verified at runtime; inputs are valid shared/mutable
            // slices and the kernel uses unaligned load/store.
            unsafe {
                prefix_sum_avx2(deltas, out);
            }
            return;
        }
    }
    prefix_sum_scalar(deltas, out);
}

fn prefix_sum_scalar(deltas: &[i64], out: &mut [i64]) {
    let mut acc = 0i64;
    for (i, &d) in deltas.iter().enumerate() {
        acc = acc.wrapping_add(d);
        out[i] = acc;
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn prefix_sum_avx2(deltas: &[i64], out: &mut [i64]) {
    use std::arch::x86_64::*;
    let n = deltas.len();
    let mut running = _mm256_setzero_si256(); // [total, total, total, total] across blocks
    let mut i = 0usize;
    while i + 4 <= n {
        let mut x = _mm256_loadu_si256(deltas.as_ptr().add(i) as *const __m256i);
        // In-register inclusive scan of the 4 lanes (Harris/Goldbolt style).
        let s1 = _mm256_slli_si256(x, 8); // [0, x0, 0, x2] (per-128-bit-lane shift)
        x = _mm256_add_epi64(x, s1); // [x0, x0+x1, x2, x2+x3]
        let bc = _mm256_permute4x64_epi64(x, 0x50); // [x0, x0, x0+x1, x0+x1]
        let mask = _mm256_set_epi64x(-1, -1, 0, 0); // lanes [0,0,-1,-1]
        let carry = _mm256_and_si256(bc, mask); // [0, 0, x0+x1, x0+x1]
        x = _mm256_add_epi64(x, carry); // full block-local inclusive scan
        x = _mm256_add_epi64(x, running); // fold in the running total
        _mm256_storeu_si256(out.as_mut_ptr().add(i) as *mut __m256i, x);
        running = _mm256_permute4x64_epi64(x, 0xFF); // broadcast lane 3 → [t,t,t,t]
        i += 4;
    }
    // Scalar tail seeded with the running total (out[i-1] holds it after a full
    // block; 0 if the input was shorter than one block on this path).
    let mut acc = if i == 0 { 0 } else { out[i - 1] };
    while i < n {
        acc = acc.wrapping_add(deltas[i]);
        out[i] = acc;
        i += 1;
    }
}

/// Bulk big-endian append of `data` (one vectorized swap + zero-copy cast).
fn append_u64_be(out: &mut Vec<u8>, data: &[u64]) {
    if cfg!(target_endian = "little") {
        let swapped: Vec<u64> = data.iter().map(|v| v.swap_bytes()).collect();
        out.extend_from_slice(bytemuck::cast_slice::<u64, u8>(&swapped));
    } else {
        out.extend_from_slice(bytemuck::cast_slice::<u64, u8>(data));
    }
}

/// Bulk big-endian append of i64 data (delta-encoded columns reuse this too).
fn append_i64_be(out: &mut Vec<u8>, data: &[i64]) {
    if cfg!(target_endian = "little") {
        let swapped: Vec<i64> = data.iter().map(|v| v.swap_bytes()).collect();
        out.extend_from_slice(bytemuck::cast_slice::<i64, u8>(&swapped));
    } else {
        out.extend_from_slice(bytemuck::cast_slice::<i64, u8>(data));
    }
}

/// Read `n` big-endian u64s from `p`. Aligned slices use a zero-copy cast plus a
/// vectorized swap; unaligned (typical for decompressed buffers) fall back to a
/// tight loop. The autovectorizer turns both into SIMD loads.
fn take_u64_be(p: &[u8], n: usize) -> Result<Vec<u64>> {
    if p.len() < n * 8 {
        return Err(MongrelError::InvalidArgument(
            "u64 payload truncated".into(),
        ));
    }
    let bytes = &p[..n * 8];
    if let Ok(native) = bytemuck::try_cast_slice::<u8, u64>(bytes) {
        Ok(native.iter().map(|v| v.swap_bytes()).collect())
    } else {
        let mut out = Vec::with_capacity(n);
        for chunk in bytes.chunks_exact(8) {
            out.push(u64::from_be_bytes(chunk.try_into().unwrap()));
        }
        Ok(out)
    }
}

/// Bulk little-endian append of u64 data (Phase 15.7 native-endian pages). On a
/// little-endian target (all real hardware) this is a memcpy via `cast_slice`;
/// on a big-endian target it swaps, then memcpy. Mirrors [`append_u64_be`].
fn append_u64_le(out: &mut Vec<u8>, data: &[u64]) {
    if cfg!(target_endian = "little") {
        out.extend_from_slice(bytemuck::cast_slice::<u64, u8>(data));
    } else {
        let swapped: Vec<u64> = data.iter().map(|v| v.swap_bytes()).collect();
        out.extend_from_slice(bytemuck::cast_slice::<u64, u8>(&swapped));
    }
}

/// Bulk little-endian append of i64 data (Phase 15.7).
fn append_i64_le(out: &mut Vec<u8>, data: &[i64]) {
    if cfg!(target_endian = "little") {
        out.extend_from_slice(bytemuck::cast_slice::<i64, u8>(data));
    } else {
        let swapped: Vec<i64> = data.iter().map(|v| v.swap_bytes()).collect();
        out.extend_from_slice(bytemuck::cast_slice::<i64, u8>(&swapped));
    }
}

/// Read `n` little-endian u64s from `p` (Phase 15.7). On a little-endian target
/// the aligned fast path is a literal memcpy (`cast_slice` → `to_vec`, no
/// per-element work); unaligned buffers fall back to `from_le_bytes` per chunk,
/// which the autovectorizer turns into contiguous SIMD loads (a pure load, no
/// `bswap`, so still cheaper than the big-endian path). Big-endian readers swap.
fn take_u64_le(p: &[u8], n: usize) -> Result<Vec<u64>> {
    if p.len() < n * 8 {
        return Err(MongrelError::InvalidArgument(
            "u64 payload truncated".into(),
        ));
    }
    let bytes = &p[..n * 8];
    if cfg!(target_endian = "little") {
        if let Ok(native) = bytemuck::try_cast_slice::<u8, u64>(bytes) {
            // memcpy — the LE page is already in host order.
            return Ok(native.to_vec());
        }
        Ok(bytes
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect())
    } else {
        Ok(bytes
            .chunks_exact(8)
            .map(|c| u64::from_le_bytes(c.try_into().unwrap()))
            .collect())
    }
}

/// Read `n` little-endian i64s (Phase 15.7). Delegates to [`take_u64_le`].
fn take_i64_le(p: &[u8], n: usize) -> Result<Vec<i64>> {
    if p.len() < n * 8 {
        return Err(MongrelError::InvalidArgument(
            "int64 payload truncated".into(),
        ));
    }
    Ok(take_u64_le(p, n)?.into_iter().map(|u| u as i64).collect())
}

fn dict_encode_bytes_native(offsets: &[u32], values: &[u8], validity: &[u8]) -> Vec<u8> {
    let n = offsets.len() - 1;
    let mut table: Vec<Vec<u8>> = Vec::new();
    let mut index_of: std::collections::HashMap<&[u8], u32> = std::collections::HashMap::new();
    let mut indices: Vec<u32> = Vec::with_capacity(n);
    for i in 0..n {
        let lo = offsets[i] as usize;
        let hi = offsets[i + 1] as usize;
        let slice = &values[lo..hi];
        let idx = if let Some(&idx) = index_of.get(slice) {
            idx
        } else {
            let idx = table.len() as u32;
            index_of.insert(slice, idx);
            table.push(slice.to_vec());
            idx
        };
        indices.push(idx);
    }
    let mut out = Vec::new();
    out.extend_from_slice(&(validity.len() as u32).to_be_bytes());
    out.extend_from_slice(validity);
    out.extend_from_slice(&(indices.len() as u32).to_be_bytes());
    for i in &indices {
        out.extend_from_slice(&i.to_be_bytes());
    }
    out.extend_from_slice(&(table.len() as u32).to_be_bytes());
    for entry in &table {
        out.extend_from_slice(&(entry.len() as u32).to_be_bytes());
        out.extend_from_slice(entry);
    }
    out
}

fn dict_decode_bytes_native(data: &[u8], n: usize) -> Result<NativeColumn> {
    let mut cur = 0usize;
    let vlen = read_u32_be(data, &mut cur)? as usize;
    let validity = checked_slice(data, &mut cur, vlen)?.to_vec();
    let index_count = read_u32_be(data, &mut cur)? as usize;
    if index_count < n {
        return Err(MongrelError::InvalidArgument("dict index_count < n".into()));
    }
    let mut indices = Vec::with_capacity(index_count.min(n));
    for _ in 0..index_count {
        indices.push(read_u32_be(data, &mut cur)?);
    }
    let table_count = read_u32_be(data, &mut cur)? as usize;
    let mut table: Vec<(usize, usize)> = Vec::with_capacity(table_count); // (start, len) into values
    let mut values = Vec::new();
    for _ in 0..table_count {
        let len = read_u32_be(data, &mut cur)? as usize;
        let chunk = checked_slice(data, &mut cur, len)?;
        let start = values.len();
        values.extend_from_slice(chunk);
        table.push((start, len));
    }
    let mut merged = Vec::new();
    let mut offs = vec![0u32];
    for (i, &idx) in indices.iter().enumerate().take(n) {
        // Skip null rows: `dict_encode_bytes` writes index 0 for nulls but the
        // table may be empty (an all-null column never inserts an entry), so the
        // table lookup is only valid for non-null rows (mirrors the Value path).
        let non_null = (validity.get(i / 8).copied().unwrap_or(0) >> (i % 8)) & 1 == 1;
        if non_null {
            let (start, len) = table
                .get(idx as usize)
                .copied()
                .ok_or_else(|| MongrelError::InvalidArgument("dict index out of range".into()))?;
            merged.extend_from_slice(&values[start..start + len]);
        }
        offs.push(merged.len() as u32);
    }
    Ok(NativeColumn::Bytes {
        offsets: offs,
        values: merged,
        validity,
    })
}

#[cfg(test)]
mod native_tests {
    use super::*;

    #[test]
    fn native_int64_plain_round_trip() {
        let col = NativeColumn::Int64 {
            data: (0..1000).collect(),
            validity: full_validity(1000),
        };
        let page = encode_page_native(
            TypeId::Int64,
            &col,
            Encoding::Zstd,
            Compress::Zstd(3),
            false,
        )
        .unwrap();
        let back = decode_page_native(TypeId::Int64, &page, 1000).unwrap();
        match back {
            NativeColumn::Int64 { data, .. } => assert_eq!(data, (0..1000).collect::<Vec<_>>()),
            _ => panic!(),
        }
    }

    #[test]
    fn native_int64_delta_crushes_sequential() {
        let col = NativeColumn::int64_sequence(0, 100_000);
        let plain = encode_page_native(
            TypeId::Int64,
            &col,
            Encoding::Zstd,
            Compress::Zstd(3),
            false,
        )
        .unwrap();
        let delta = encode_page_native(
            TypeId::Int64,
            &col,
            Encoding::Delta,
            Compress::Zstd(3),
            false,
        )
        .unwrap();
        assert!(
            delta.len() < plain.len() / 5,
            "delta must crush sequential ints"
        );
        let back = decode_page_native(TypeId::Int64, &delta, 100_000).unwrap();
        match back {
            NativeColumn::Int64 { data, .. } => assert_eq!(data.len(), 100_000),
            _ => panic!(),
        }
    }

    #[test]
    fn native_bytes_dict_round_trip() {
        let n = 500;
        let mut offsets = vec![0u32];
        let mut values = Vec::new();
        for i in 0..n {
            let s = ["red", "green", "blue"][i % 3];
            values.extend_from_slice(s.as_bytes());
            offsets.push(values.len() as u32);
        }
        let col = NativeColumn::Bytes {
            offsets,
            values,
            validity: full_validity(n),
        };
        let page = encode_page_native(
            TypeId::Bytes,
            &col,
            Encoding::Dictionary,
            Compress::Zstd(3),
            false,
        )
        .unwrap();
        assert!(page.len() < 100, "dict page tiny, got {}", page.len());
        let back = decode_page_native(TypeId::Bytes, &page, n).unwrap();
        assert_eq!(back.len(), n);
    }

    #[test]
    fn native_gather_picks_indices() {
        let col = NativeColumn::Int64 {
            data: vec![10, 20, 30, 40],
            validity: full_validity(4),
        };
        let g = col.gather(&[0, 2, 3]);
        match g {
            NativeColumn::Int64 { data, .. } => assert_eq!(data, vec![10, 30, 40]),
            _ => panic!(),
        }
    }

    /// Phase 14.4: the no-zstd `bulk_load_fast` path emits `ALGO_PLAIN` pages
    /// (level < 0) that decode back identically for every native type.
    #[test]
    fn native_plain_no_zstd_round_trips_all_types() {
        let i = NativeColumn::Int64 {
            data: (0..1000).collect(),
            validity: full_validity(1000),
        };
        let p =
            encode_page_native(TypeId::Int64, &i, Encoding::Plain, Compress::Plain, false).unwrap();
        assert_eq!(p[0], ALGO_PLAIN, "Int64 plain must be ALGO_PLAIN");
        match decode_page_native(TypeId::Int64, &p, 1000).unwrap() {
            NativeColumn::Int64 { data, .. } => assert_eq!(data, (0..1000).collect::<Vec<_>>()),
            _ => panic!(),
        }

        let f = NativeColumn::Float64 {
            data: (0..500).map(|x| x as f64 * 1.5).collect(),
            validity: full_validity(500),
        };
        let p = encode_page_native(TypeId::Float64, &f, Encoding::Plain, Compress::Plain, false)
            .unwrap();
        assert_eq!(p[0], ALGO_PLAIN);
        match decode_page_native(TypeId::Float64, &p, 500).unwrap() {
            NativeColumn::Float64 { data, .. } => {
                assert_eq!(data, (0..500).map(|x| x as f64 * 1.5).collect::<Vec<_>>())
            }
            _ => panic!(),
        }

        let b = NativeColumn::Bool {
            data: (0..64).map(|i| (i % 2) as u8).collect(),
            validity: full_validity(64),
        };
        let p =
            encode_page_native(TypeId::Bool, &b, Encoding::Plain, Compress::Plain, false).unwrap();
        assert_eq!(p[0], ALGO_PLAIN);
        match decode_page_native(TypeId::Bool, &p, 64).unwrap() {
            NativeColumn::Bool { data, .. } => {
                assert_eq!(data, (0..64).map(|i| (i % 2) as u8).collect::<Vec<_>>())
            }
            _ => panic!(),
        }

        let mut offsets = vec![0u32];
        let mut values = Vec::new();
        for i in 0..200u32 {
            values.extend_from_slice(format!("v{i}").as_bytes());
            offsets.push(values.len() as u32);
        }
        let s = NativeColumn::Bytes {
            offsets,
            values,
            validity: full_validity(200),
        };
        let p =
            encode_page_native(TypeId::Bytes, &s, Encoding::Plain, Compress::Plain, false).unwrap();
        assert_eq!(p[0], ALGO_PLAIN);
        match decode_page_native(TypeId::Bytes, &p, 200).unwrap() {
            NativeColumn::Bytes {
                offsets: o,
                values: v,
                ..
            } => {
                assert_eq!(o.len(), 201);
                for i in 0..200 {
                    let lo = o[i] as usize;
                    let hi = o[i + 1] as usize;
                    assert_eq!(&v[lo..hi], format!("v{i}").as_bytes());
                }
            }
            _ => panic!(),
        }
    }

    /// Phase 15.3: LZ4 pages (`ALGO_LZ4_PLAIN`/`_DELTA`/`_DICT`) decode back
    /// identically for every native type, and the algo byte selects them.
    #[test]
    fn lz4_pages_round_trip_all_types() {
        // Int64 delta (sorted) → ALGO_LZ4_DELTA.
        let i = NativeColumn::int64_sequence(0, 1000);
        let p =
            encode_page_native(TypeId::Int64, &i, Encoding::Delta, Compress::Lz4, false).unwrap();
        assert_eq!(p[0], ALGO_LZ4_DELTA);
        match decode_page_native(TypeId::Int64, &p, 1000).unwrap() {
            NativeColumn::Int64 { data, .. } => assert_eq!(data, (0..1000).collect::<Vec<_>>()),
            _ => panic!(),
        }
        // Int64 plain → ALGO_LZ4_PLAIN.
        let p =
            encode_page_native(TypeId::Int64, &i, Encoding::Zstd, Compress::Lz4, false).unwrap();
        assert_eq!(p[0], ALGO_LZ4_PLAIN);
        match decode_page_native(TypeId::Int64, &p, 1000).unwrap() {
            NativeColumn::Int64 { data, .. } => assert_eq!(data, (0..1000).collect::<Vec<_>>()),
            _ => panic!(),
        }
        // Float64 plain.
        let f = NativeColumn::Float64 {
            data: (0..500).map(|x| x as f64 * 1.5).collect(),
            validity: full_validity(500),
        };
        let p =
            encode_page_native(TypeId::Float64, &f, Encoding::Zstd, Compress::Lz4, false).unwrap();
        assert_eq!(p[0], ALGO_LZ4_PLAIN);
        match decode_page_native(TypeId::Float64, &p, 500).unwrap() {
            NativeColumn::Float64 { data, .. } => {
                assert_eq!(data, (0..500).map(|x| x as f64 * 1.5).collect::<Vec<_>>())
            }
            _ => panic!(),
        }
        // Bytes dict (low-card) → ALGO_LZ4_DICT.
        let mut offsets = vec![0u32];
        let mut values = Vec::new();
        for i in 0..300u32 {
            values.extend_from_slice(["red", "green", "blue"][(i % 3) as usize].as_bytes());
            offsets.push(values.len() as u32);
        }
        let s = NativeColumn::Bytes {
            offsets,
            values,
            validity: full_validity(300),
        };
        let p = encode_page_native(
            TypeId::Bytes,
            &s,
            Encoding::Dictionary,
            Compress::Lz4,
            false,
        )
        .unwrap();
        assert_eq!(p[0], ALGO_LZ4_DICT);
        match decode_page_native(TypeId::Bytes, &p, 300).unwrap() {
            NativeColumn::Bytes { offsets: o, .. } => assert_eq!(o.len(), 301),
            _ => panic!(),
        }
    }

    /// Phase 15.7: little-endian pages set the `ALGO_LE_FLAG` bit and decode as
    /// a memcpy on little-endian targets. They must round-trip identically to
    /// the big-endian path for every fixed-width type, under raw / zstd / LZ4.
    #[test]
    fn le_pages_round_trip_all_types() {
        let assert_le = |page: &[u8]| assert_ne!(page[0] & ALGO_LE_FLAG, 0, "LE flag must be set");

        // Int64 plain (raw).
        let i = NativeColumn::Int64 {
            data: (0..1000).collect(),
            validity: full_validity(1000),
        };
        let p =
            encode_page_native(TypeId::Int64, &i, Encoding::Plain, Compress::Plain, true).unwrap();
        assert_eq!(p[0], ALGO_LE_FLAG, "raw LE Int64 algo = flag only");
        match decode_page_native(TypeId::Int64, &p, 1000).unwrap() {
            NativeColumn::Int64 { data, .. } => assert_eq!(data, (0..1000).collect::<Vec<_>>()),
            _ => panic!(),
        }

        // Int64 plain + zstd.
        let p =
            encode_page_native(TypeId::Int64, &i, Encoding::Zstd, Compress::Zstd(3), true).unwrap();
        assert_le(&p);
        match decode_page_native(TypeId::Int64, &p, 1000).unwrap() {
            NativeColumn::Int64 { data, .. } => assert_eq!(data, (0..1000).collect::<Vec<_>>()),
            _ => panic!(),
        }

        // Int64 delta (sequential) + LZ4 — delta carrier is LE too.
        let seq = NativeColumn::int64_sequence(0, 1000);
        let p =
            encode_page_native(TypeId::Int64, &seq, Encoding::Delta, Compress::Lz4, true).unwrap();
        assert_eq!(p[0], ALGO_LZ4_DELTA | ALGO_LE_FLAG);
        match decode_page_native(TypeId::Int64, &p, 1000).unwrap() {
            NativeColumn::Int64 { data, .. } => assert_eq!(data, (0..1000).collect::<Vec<_>>()),
            _ => panic!(),
        }

        // Float64 plain + zstd.
        let f = NativeColumn::Float64 {
            data: (0..500).map(|x| x as f64 * 1.5).collect(),
            validity: full_validity(500),
        };
        let p = encode_page_native(TypeId::Float64, &f, Encoding::Zstd, Compress::Zstd(3), true)
            .unwrap();
        assert_le(&p);
        match decode_page_native(TypeId::Float64, &p, 500).unwrap() {
            NativeColumn::Float64 { data, .. } => {
                assert_eq!(data, (0..500).map(|x| x as f64 * 1.5).collect::<Vec<_>>())
            }
            _ => panic!(),
        }
    }

    /// Peer-review fix: a corrupt/malicious plaintext LZ4 page that claims a
    /// huge decompressed size (0xFFFFFFFF) must be rejected by the page-shape
    /// bound before any multi-GiB allocation, not OOM the process.
    #[test]
    fn malicious_lz4_size_prefix_is_rejected() {
        // algo = ALGO_LZ4_PLAIN, then a 4-byte LE size prefix claiming 4 GiB.
        let mut evil = vec![ALGO_LZ4_PLAIN];
        evil.extend_from_slice(&0xFFFF_FFFFu32.to_le_bytes());
        evil.extend_from_slice(b"junk");
        let err =
            decode_page_native(TypeId::Int64, &evil, 1000).expect_err("must reject oversize lz4");
        let msg = format!("{err}");
        assert!(msg.contains("exceeds page limit"), "got: {msg}");

        // Same guard on the Value path.
        let err = decode_page(TypeId::Int64, &evil, 1000).expect_err("must reject oversize lz4");
        assert!(format!("{err}").contains("exceeds page limit"));
    }

    /// Peer-review fix: a truncated dictionary payload (validity header reads
    /// a length that runs past the buffer) must return `Err`, not panic on an
    /// out-of-bounds slice. Covers both the Value and native dict decoders.
    #[test]
    fn truncated_dict_payload_does_not_panic() {
        // ALGO_ZSTD_DICT with a decompressed body that is just a few bytes:
        // the validity length field (4 bytes BE) claims more than is present.
        let mut body = vec![ALGO_ZSTD_DICT];
        body.extend_from_slice(&zstd_compress(&[0u8; 2]).unwrap()[..]);
        decode_page(TypeId::Bytes, &body, 4).expect_err("value dict trunc must Err");

        // Native path: hand a truncated raw dict body straight to the decoder.
        let truncated: &[u8] = &[
            0x00, 0x00, 0x00, 0x10, // vlen = 16, but no validity bytes follow
        ];
        dict_decode_bytes_native(truncated, 2).expect_err("native dict trunc must Err");
        dict_decode_bytes(truncated, 2).expect_err("value dict trunc must Err");

        // Out-of-range dict index → Err rather than panic. Mark row 0 non-null
        // (validity byte 0xFF) so the decoder actually looks up index 9.
        let mut bad = vec![0u8; 0];
        bad.extend_from_slice(&1u32.to_be_bytes()); // vlen = 1
        bad.push(0xFF); // validity: bit 0 set → row 0 is non-null
        bad.extend_from_slice(&1u32.to_be_bytes()); // index_count = 1
        bad.extend_from_slice(&9u32.to_be_bytes()); // index 9 (no table)
        bad.extend_from_slice(&0u32.to_be_bytes()); // table_count = 0
        dict_decode_bytes(&bad, 1).expect_err("oob index must Err");
        dict_decode_bytes_native(&bad, 1).expect_err("oob index must Err");
    }

    /// Phase 15.6: the AVX2 delta prefix-sum must match the scalar reference for
    /// every length (including the 4-lane block boundaries) and for arbitrary
    /// deltas. Runs on every `ALGO_*_DELTA` page (row-id / epoch columns).
    #[test]
    fn delta_prefix_sum_matches_scalar_all_lengths() {
        // deterministic pseudo-random deltas (no RNG dependency in tests)
        let mut deltas: Vec<i64> = Vec::with_capacity(2000);
        let mut s: u64 = 0x9E37_79B9_7F4A_7C15;
        for _ in 0..2000 {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            deltas.push((s as i32) as i64); // small + negative deltas to exercise signs
        }
        for &len in &[
            0usize, 1, 2, 3, 4, 5, 6, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129,
            1000,
        ] {
            let input = &deltas[..len];
            let simd = delta_prefix_sum_i64(input);
            let mut ref_out = vec![0i64; len];
            prefix_sum_scalar(input, &mut ref_out);
            assert_eq!(simd, ref_out, "len {len}: SIMD prefix sum diverged");
        }

        // Monotonic row-id-like deltas (the real-world shape): 0,1,1,1,…
        let mut ids = vec![0i64; 333];
        for x in ids.iter_mut().skip(1) {
            *x = 1;
        }
        let got = delta_prefix_sum_i64(&ids);
        assert_eq!(got, (0..333).map(|x| x as i64).collect::<Vec<_>>());
    }

    /// Phase 15.6: delta pages decode identically through the vectorized path
    /// for both zstd and LZ4 carriers, including a null row mid-page.
    #[test]
    fn delta_page_decodes_with_null_via_vectorized_path() {
        let mut validity = full_validity(17);
        validity[9 / 8] &= !(1 << (9 % 8)); // clear bit 9 → row 9 null
        let col = NativeColumn::Int64 {
            data: (0..17).collect(),
            validity: validity.clone(),
        };
        for (name, comp) in [("zstd", Compress::Zstd(3)), ("lz4", Compress::Lz4)] {
            let page =
                encode_page_native(TypeId::Int64, &col, Encoding::Delta, comp, false).unwrap();
            // Delta Int64 always emits ALGO_*_DELTA.
            assert!(matches!(page[0], ALGO_ZSTD_DELTA | ALGO_LZ4_DELTA));
            let back = decode_page_native(TypeId::Int64, &page, 17).unwrap();
            match back {
                NativeColumn::Int64 { data, validity: v } => {
                    assert_eq!(data, (0..17).collect::<Vec<_>>(), "{name} data");
                    assert_eq!(v, validity, "{name} validity");
                }
                _ => panic!(),
            }
        }
    }
}
