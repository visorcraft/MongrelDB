//! C-tagged union mirror of [`mongreldb_core::Value`].
//!
//! The C side sees [`CValue`], a `#[repr(C)]` tagged union with a discriminant
//! ([`CValueTag`]) and one payload field per variant. Variable-length payloads
//! (bytes, embeddings, json) are carried as `{ data, len }` borrows that point
//! into the C caller's buffer for *inputs*, and as owned allocations for
//! *outputs* (the FFI layer copies out of core into freshly allocated buffers
//! whose lifetime is tied to the result handle).

use mongreldb_core::schema::TypeId;
use mongreldb_core::Value;
use std::os::raw::{c_char, c_void};

/// A borrowed byte slice crossing the ABI: `{ data, len }`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct ByteSlice {
    pub data: *const u8,
    pub len: usize,
}

impl Default for ByteSlice {
    fn default() -> Self {
        Self {
            data: std::ptr::null(),
            len: 0,
        }
    }
}

impl ByteSlice {
    /// Build a `ByteSlice` borrowing `bytes`.
    pub fn from_slice(bytes: &[u8]) -> Self {
        Self {
            data: bytes.as_ptr(),
            len: bytes.len(),
        }
    }

    /// Copy the borrowed bytes into an owned `Vec<u8>`. Returns an empty vec
    /// for a null `data` pointer.
    ///
    /// # Safety
    /// The caller guarantees `data` is valid for `len` bytes (or null with
    /// `len == 0`).
    pub unsafe fn to_vec(&self) -> Vec<u8> {
        if self.data.is_null() || self.len == 0 {
            return Vec::new();
        }
        // SAFETY: upheld by caller.
        std::slice::from_raw_parts(self.data, self.len).to_vec()
    }
}

/// A borrowed `f32` embedding vector crossing the ABI: `{ data, len }`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct EmbeddingSlice {
    pub data: *const f32,
    pub len: usize,
}

impl Default for EmbeddingSlice {
    fn default() -> Self {
        Self {
            data: std::ptr::null(),
            len: 0,
        }
    }
}

impl EmbeddingSlice {
    /// Copy the borrowed floats into an owned `Vec<f32>` (empty for null).
    ///
    /// # Safety
    /// The caller guarantees `data` is valid for `len` `f32`s (or null).
    pub unsafe fn to_vec(&self) -> Vec<f32> {
        if self.data.is_null() || self.len == 0 {
            return Vec::new();
        }
        // SAFETY: upheld by caller.
        std::slice::from_raw_parts(self.data, self.len).to_vec()
    }
}

/// A borrowed array of NUL-terminated C strings (used for `MinHashSimilar`
/// members and ENUM variants). Each item is a `*const c_char`.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct MinHashMembers {
    pub items: *const *const c_char,
    pub len: usize,
}

impl Default for MinHashMembers {
    fn default() -> Self {
        Self {
            items: std::ptr::null(),
            len: 0,
        }
    }
}

/// Convenience trait so query/schema code can call `.to_vec()` on a `ByteSlice`
/// without importing the inherent impl (which is fine, but the trait keeps the
/// call sites uniform with `EmbeddingSlice`).
pub trait U16SliceSafe {
    /// Copy the borrowed bytes into an owned `Vec<u8>` (empty for null).
    ///
    /// # Safety
    /// `data` must be valid for `len` bytes (or null with `len == 0`).
    unsafe fn to_vec(&self) -> Vec<u8>;
}

impl U16SliceSafe for ByteSlice {
    unsafe fn to_vec(&self) -> Vec<u8> {
        if self.data.is_null() || self.len == 0 {
            return Vec::new();
        }
        std::slice::from_raw_parts(self.data, self.len).to_vec()
    }
}

/// SQL INTERVAL: months, days, nanoseconds.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct CInterval {
    pub months: i64,
    pub days: i32,
    pub nanos: i64,
}

/// 128-bit decimal carried as low + high i64 limbs (little-endian order: low
/// first) so there is no alignment/padding surprise across ABIs.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default)]
pub struct CDecimal128 {
    pub low: i64,
    pub high: i64,
}

impl CDecimal128 {
    pub fn from_i128(v: i128) -> Self {
        let low = v as i64;
        let high = (v >> 64) as i64;
        Self { low, high }
    }

    pub fn to_i128(self) -> i128 {
        ((self.high as i128) << 64) | (self.low as u64 as i128)
    }
}

/// Discriminant for [`CValue`]. Matches the 10 `Value` variants.
#[repr(i32)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CValueTag {
    Null = 0,
    Bool = 1,
    Int64 = 2,
    Float64 = 3,
    Bytes = 4,
    Embedding = 5,
    Decimal = 6,
    Interval = 7,
    Uuid = 8,
    Json = 9,
}

/// The C-tagged union mirror of [`Value`]. The C side sets `tag` and the
/// matching payload field; the FFI layer reads exactly one field per `tag`.
///
/// Variable-length payloads (`bytes`, `embedding`, `json`) are *borrows* when
/// used as inputs to the FFI and point into the caller's memory. When produced
/// as outputs (e.g. by a query result), they point into memory owned by the
/// result handle and are freed when the handle is freed.
#[repr(C)]
#[derive(Clone, Copy)]
pub union CValuePayload {
    pub boolean: u8, // 0=false, nonzero=true
    pub int64: i64,
    pub float64: f64,
    pub bytes: ByteSlice,
    pub embedding: EmbeddingSlice,
    pub decimal: CDecimal128,
    pub interval: CInterval,
    /// 16-byte UUID (big-endian for sort order).
    pub uuid: [u8; 16],
    pub json: ByteSlice,
}

impl std::fmt::Debug for CValuePayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Unions can't be Debug-derived safely; report the raw bytes of the
        // largest field so diagnostics are still useful without reading
        // undefined memory.
        f.debug_struct("CValuePayload").finish_non_exhaustive()
    }
}

/// A complete C value: discriminant + union payload.
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CValue {
    pub tag: CValueTag,
    pub payload: CValuePayload,
}

impl Default for CValue {
    fn default() -> Self {
        Self {
            tag: CValueTag::Null,
            payload: CValuePayload { int64: 0 },
        }
    }
}

impl CValue {
    /// Build a `CValue::Null`.
    pub fn null() -> Self {
        Self::default()
    }

    pub fn boolean(b: bool) -> Self {
        Self {
            tag: CValueTag::Bool,
            payload: CValuePayload { boolean: b as u8 },
        }
    }

    pub fn int64(n: i64) -> Self {
        Self {
            tag: CValueTag::Int64,
            payload: CValuePayload { int64: n },
        }
    }

    pub fn float64(f: f64) -> Self {
        Self {
            tag: CValueTag::Float64,
            payload: CValuePayload { float64: f },
        }
    }
}

/// Marshal a core [`Value`] into a [`CValue`] whose variable-length payloads
/// borrow `backing` (used for output values whose bytes live in a result
/// handle). For simple scalar values no backing store is needed.
///
/// The returned `CValue` borrows from `backing`; the caller must keep it alive
/// for as long as the `CValue` is read.
pub fn value_to_c(v: &Value, backing: &mut Vec<Vec<u8>>) -> CValue {
    match v {
        Value::Null => CValue::null(),
        Value::Bool(b) => CValue::boolean(*b),
        Value::Int64(n) => CValue::int64(*n),
        Value::Float64(f) => CValue::float64(*f),
        Value::Bytes(b) => {
            backing.push(b.clone());
            let slice = ByteSlice::from_slice(backing.last().unwrap());
            CValue {
                tag: CValueTag::Bytes,
                payload: CValuePayload { bytes: slice },
            }
        }
        Value::Embedding(e) => {
            // Embeddings are f32; store them as raw bytes in the backing store
            // and expose a typed pointer so the C side reads f32 directly.
            let bytes: Vec<u8> = bytemuck_cast_f32(e);
            backing.push(bytes);
            let ptr = backing.last().unwrap().as_ptr() as *const f32;
            CValue {
                tag: CValueTag::Embedding,
                payload: CValuePayload {
                    embedding: EmbeddingSlice {
                        data: ptr,
                        len: e.len(),
                    },
                },
            }
        }
        Value::Decimal(d) => CValue {
            tag: CValueTag::Decimal,
            payload: CValuePayload {
                decimal: CDecimal128::from_i128(*d),
            },
        },
        Value::Interval {
            months,
            days,
            nanos,
        } => CValue {
            tag: CValueTag::Interval,
            payload: CValuePayload {
                interval: CInterval {
                    months: *months,
                    days: *days,
                    nanos: *nanos,
                },
            },
        },
        Value::Uuid(b) => CValue {
            tag: CValueTag::Uuid,
            payload: CValuePayload { uuid: *b },
        },
        Value::Json(b) => {
            backing.push(b.clone());
            let slice = ByteSlice::from_slice(backing.last().unwrap());
            CValue {
                tag: CValueTag::Json,
                payload: CValuePayload { json: slice },
            }
        }
    }
}

/// Cast a `Vec<f32>` to little-endian bytes without copying. Used so an
/// `Embedding` payload can be held in the byte-oriented backing store while
/// still exposing a typed `*const f32` to C.
fn bytemuck_cast_f32(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for f in v {
        out.extend_from_slice(&f.to_le_bytes());
    }
    out
}

/// Marshal a C-side [`CValue`] into a core [`Value`], guided by the column's
/// [`TypeId`]. This mirrors the NAPI `Cell::to_value` logic: the column type
/// (not the tag alone) decides how to interpret bytes/embeddings — e.g. an
/// `Enum` column stores its variant as `Value::Bytes`, and a `Json`/`Uuid`/
/// `TimestampNanos`/`Date32`/`Date64`/`Time64` column also maps through
/// `Value::Bytes` or `Value::Int64`.
///
/// # Safety
/// The caller guarantees that any pointer in `cv.payload` is valid for its
/// length, and that the discriminant/payload pairing is consistent.
pub unsafe fn c_to_value(cv: &CValue, ty: &TypeId) -> Value {
    use CValueTag::*;
    match (cv.tag, ty) {
        // Null is always null regardless of column type.
        (Null, _) => Value::Null,

        // Bool column reads the boolean field.
        (Bool, TypeId::Bool) => Value::Bool(cv.payload.boolean != 0),

        // Integer-typed columns read int64 (NAPI does the same: a single
        // int64 slot covers Int8..UInt64, TimestampNanos, Date32).
        (Int64, _) => Value::Int64(cv.payload.int64),

        // Float column reads float64 (covers Float32/Float64).
        (Float64, TypeId::Float32 | TypeId::Float64) => Value::Float64(cv.payload.float64),

        // Bytes-shaped payloads cover Bytes/Enum/Json/Uuid/Date64/Time64/
        // TimestampNanos(text-encoded)/Array.
        (Bytes, _) => Value::Bytes(cv.payload.bytes.to_vec()),
        (Json, _) => Value::Bytes(cv.payload.json.to_vec()),

        // Embedding column reads the f32 slice.
        (Embedding, TypeId::Embedding { .. }) => Value::Embedding(cv.payload.embedding.to_vec()),

        // Decimal column reads the 128-bit payload.
        (Decimal, TypeId::Decimal128 { .. }) => Value::Decimal(cv.payload.decimal.to_i128()),

        // Interval column reads the structured payload.
        (Interval, TypeId::Interval) => {
            let i = cv.payload.interval;
            Value::Interval {
                months: i.months,
                days: i.days,
                nanos: i.nanos,
            }
        }

        // Uuid column reads the fixed 16-byte payload.
        (Uuid, TypeId::Uuid) => Value::Uuid(cv.payload.uuid),

        // Uuid tag against a Bytes column is also acceptable (store as bytes).
        (Uuid, TypeId::Bytes) => Value::Uuid(cv.payload.uuid),

        // Fallback: anything that didn't match becomes Null. This keeps the
        // FFI total: a caller who set the wrong tag for the column type gets
        // a NULL rather than UB.
        _ => Value::Null,
    }
}

/// Re-export some raw type aliases for the generated headers / downstream
/// modules. `c_void` is used by callers that treat payloads opaquely.
#[allow(dead_code)]
pub type CVoid = c_void;

/// Ensure `c_char` is reachable for downstream bindings.
#[allow(dead_code)]
pub type CChar = c_char;
