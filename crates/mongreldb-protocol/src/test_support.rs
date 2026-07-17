//! Test-only support shared by the per-module test suites.
//!
//! This crate's dependency set is frozen (`mongreldb-types`, `serde`,
//! `thiserror`), so the serde round-trip tests cannot use `serde_json` or
//! `bincode`. Instead they round-trip through a minimal in-test serde
//! data-model format — values become a [`Value`] tree and the deserializer
//! replays it — mirroring the harnesses in `mongreldb-types` (`ids.rs` and
//! `errors.rs` tests). [`block_on`] polls the boxed futures of the service
//! traits without pulling in an async runtime.

use serde::de::value::{Error as ValueError, StrDeserializer};
use serde::de::{
    DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, SeqAccess, VariantAccess, Visitor,
};
use serde::ser::{Impossible, SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::future::Future;

/// A generic serde data-model value the in-test format round-trips through.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// `()`, `None`, and unit structs.
    Unit,
    /// A boolean.
    Bool(bool),
    /// Any unsigned integer.
    U64(u64),
    /// Any signed integer.
    I64(i64),
    /// A double-precision float.
    F64(f64),
    /// A UTF-8 string.
    Str(String),
    /// A sequence (also arrays, tuples, sets, and vectors).
    Seq(Vec<Value>),
    /// A map with arbitrary keys.
    Map(Vec<(Value, Value)>),
    /// A unit enum variant (externally tagged as the bare variant name).
    UnitVariant(&'static str),
    /// A newtype enum variant.
    NewtypeVariant(&'static str, Box<Value>),
    /// A struct.
    Struct(Vec<(&'static str, Value)>),
    /// A struct enum variant.
    StructVariant(&'static str, Vec<(&'static str, Value)>),
}

/// Serializes into the in-test [`Value`] tree.
pub struct ValueSerializer;

/// Collects sequences, tuples, and tuple structs.
pub struct SeqSerializer {
    elements: Vec<Value>,
}

impl SerializeSeq for SeqSerializer {
    type Ok = Value;
    type Error = ValueError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), ValueError> {
        self.elements.push(value.serialize(ValueSerializer)?);
        Ok(())
    }

    fn end(self) -> Result<Value, ValueError> {
        Ok(Value::Seq(self.elements))
    }
}

impl serde::ser::SerializeTuple for SeqSerializer {
    type Ok = Value;
    type Error = ValueError;

    fn serialize_element<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), ValueError> {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value, ValueError> {
        SerializeSeq::end(self)
    }
}

impl serde::ser::SerializeTupleStruct for SeqSerializer {
    type Ok = Value;
    type Error = ValueError;

    fn serialize_field<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), ValueError> {
        SerializeSeq::serialize_element(self, value)
    }

    fn end(self) -> Result<Value, ValueError> {
        SerializeSeq::end(self)
    }
}

/// Collects maps with arbitrary keys.
pub struct MapSerializer {
    entries: Vec<(Value, Value)>,
    key: Option<Value>,
}

impl SerializeMap for MapSerializer {
    type Ok = Value;
    type Error = ValueError;

    fn serialize_key<T: ?Sized + Serialize>(&mut self, key: &T) -> Result<(), ValueError> {
        self.key = Some(key.serialize(ValueSerializer)?);
        Ok(())
    }

    fn serialize_value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<(), ValueError> {
        let key = self
            .key
            .take()
            .ok_or_else(|| serde::ser::Error::custom("map value serialized before its key"))?;
        self.entries.push((key, value.serialize(ValueSerializer)?));
        Ok(())
    }

    fn end(self) -> Result<Value, ValueError> {
        Ok(Value::Map(self.entries))
    }
}

/// Collects struct fields.
pub struct StructSerializer {
    fields: Vec<(&'static str, Value)>,
}

impl SerializeStruct for StructSerializer {
    type Ok = Value;
    type Error = ValueError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), ValueError> {
        self.fields.push((key, value.serialize(ValueSerializer)?));
        Ok(())
    }

    fn end(self) -> Result<Value, ValueError> {
        Ok(Value::Struct(self.fields))
    }
}

/// Collects the fields of a struct enum variant.
pub struct StructVariantSerializer {
    variant: &'static str,
    fields: Vec<(&'static str, Value)>,
}

impl SerializeStructVariant for StructVariantSerializer {
    type Ok = Value;
    type Error = ValueError;

    fn serialize_field<T: ?Sized + Serialize>(
        &mut self,
        key: &'static str,
        value: &T,
    ) -> Result<(), ValueError> {
        self.fields.push((key, value.serialize(ValueSerializer)?));
        Ok(())
    }

    fn end(self) -> Result<Value, ValueError> {
        Ok(Value::StructVariant(self.variant, self.fields))
    }
}

macro_rules! serialize_unsupported {
    ($($method:ident ( $($param:ident : $ty:ty),* ) -> $ret:ty ;)*) => {
        $(
            fn $method(self, $($param: $ty),*) -> Result<$ret, ValueError> {
                Err(serde::ser::Error::custom(concat!(
                    stringify!($method),
                    " is not supported by the in-test serde format"
                )))
            }
        )*
    };
}

impl Serializer for ValueSerializer {
    type Ok = Value;
    type Error = ValueError;
    type SerializeSeq = SeqSerializer;
    type SerializeTuple = SeqSerializer;
    type SerializeTupleStruct = SeqSerializer;
    type SerializeTupleVariant = Impossible<Value, ValueError>;
    type SerializeMap = MapSerializer;
    type SerializeStruct = StructSerializer;
    type SerializeStructVariant = StructVariantSerializer;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn serialize_bool(self, v: bool) -> Result<Value, ValueError> {
        Ok(Value::Bool(v))
    }

    fn serialize_i64(self, v: i64) -> Result<Value, ValueError> {
        Ok(Value::I64(v))
    }

    fn serialize_i32(self, v: i32) -> Result<Value, ValueError> {
        Ok(Value::I64(i64::from(v)))
    }

    fn serialize_i16(self, v: i16) -> Result<Value, ValueError> {
        Ok(Value::I64(i64::from(v)))
    }

    fn serialize_i8(self, v: i8) -> Result<Value, ValueError> {
        Ok(Value::I64(i64::from(v)))
    }

    fn serialize_u64(self, v: u64) -> Result<Value, ValueError> {
        Ok(Value::U64(v))
    }

    fn serialize_u32(self, v: u32) -> Result<Value, ValueError> {
        Ok(Value::U64(u64::from(v)))
    }

    fn serialize_u16(self, v: u16) -> Result<Value, ValueError> {
        Ok(Value::U64(u64::from(v)))
    }

    fn serialize_u8(self, v: u8) -> Result<Value, ValueError> {
        Ok(Value::U64(u64::from(v)))
    }

    fn serialize_f64(self, v: f64) -> Result<Value, ValueError> {
        Ok(Value::F64(v))
    }

    fn serialize_f32(self, v: f32) -> Result<Value, ValueError> {
        Ok(Value::F64(f64::from(v)))
    }

    fn serialize_str(self, v: &str) -> Result<Value, ValueError> {
        Ok(Value::Str(v.to_owned()))
    }

    fn serialize_none(self) -> Result<Value, ValueError> {
        Ok(Value::Unit)
    }

    fn serialize_some<T: ?Sized + Serialize>(self, value: &T) -> Result<Value, ValueError> {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Value, ValueError> {
        Ok(Value::Unit)
    }

    fn serialize_unit_struct(self, _name: &'static str) -> Result<Value, ValueError> {
        Ok(Value::Unit)
    }

    fn serialize_unit_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
    ) -> Result<Value, ValueError> {
        Ok(Value::UnitVariant(variant))
    }

    /// Newtype structs are transparent, like bincode: the inner value is
    /// serialized directly with no wrapper.
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        value: &T,
    ) -> Result<Value, ValueError> {
        value.serialize(self)
    }

    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Value, ValueError> {
        Ok(Value::NewtypeVariant(
            variant,
            Box::new(value.serialize(ValueSerializer)?),
        ))
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<SeqSerializer, ValueError> {
        Ok(SeqSerializer {
            elements: Vec::with_capacity(len.unwrap_or(0)),
        })
    }

    fn serialize_tuple(self, len: usize) -> Result<SeqSerializer, ValueError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_tuple_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<SeqSerializer, ValueError> {
        self.serialize_seq(Some(len))
    }

    fn serialize_map(self, len: Option<usize>) -> Result<MapSerializer, ValueError> {
        Ok(MapSerializer {
            entries: Vec::with_capacity(len.unwrap_or(0)),
            key: None,
        })
    }

    fn serialize_struct(
        self,
        _name: &'static str,
        len: usize,
    ) -> Result<StructSerializer, ValueError> {
        Ok(StructSerializer {
            fields: Vec::with_capacity(len),
        })
    }

    fn serialize_struct_variant(
        self,
        _name: &'static str,
        _variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<StructVariantSerializer, ValueError> {
        Ok(StructVariantSerializer {
            variant,
            fields: Vec::with_capacity(len),
        })
    }

    serialize_unsupported! {
        serialize_char(_v: char) -> Value;
        serialize_bytes(_v: &[u8]) -> Value;
        serialize_tuple_variant(_name: &'static str, _variant_index: u32, _variant: &'static str, _len: usize) -> Self::SerializeTupleVariant;
    }
}

/// Replays a [`Value`] tree through the [`Deserializer`] interface.
pub struct ValueDeserializer(Value);

/// Replays a sequence.
struct SeqAccesser {
    iter: std::vec::IntoIter<Value>,
}

impl<'de> SeqAccess<'de> for SeqAccesser {
    type Error = ValueError;

    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, ValueError> {
        match self.iter.next() {
            Some(value) => seed.deserialize(ValueDeserializer(value)).map(Some),
            None => Ok(None),
        }
    }
}

/// Replays a map with arbitrary keys.
struct MapAccesser {
    iter: std::vec::IntoIter<(Value, Value)>,
    value: Option<Value>,
}

impl<'de> MapAccess<'de> for MapAccesser {
    type Error = ValueError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, ValueError> {
        match self.iter.next() {
            Some((key, value)) => {
                self.value = Some(value);
                seed.deserialize(ValueDeserializer(key)).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, ValueError> {
        let value = self
            .value
            .take()
            .ok_or_else(|| serde::de::Error::custom("map value missing"))?;
        seed.deserialize(ValueDeserializer(value))
    }
}

/// Replays struct fields, keyed by their static names.
struct StructAccesser {
    iter: std::vec::IntoIter<(&'static str, Value)>,
    value: Option<Value>,
}

impl<'de> MapAccess<'de> for StructAccesser {
    type Error = ValueError;

    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, ValueError> {
        match self.iter.next() {
            Some((key, value)) => {
                self.value = Some(value);
                let deserializer: StrDeserializer<'de, ValueError> = key.into_deserializer();
                seed.deserialize(deserializer).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, ValueError> {
        let value = self
            .value
            .take()
            .ok_or_else(|| serde::de::Error::custom("struct field value missing"))?;
        seed.deserialize(ValueDeserializer(value))
    }
}

/// Replays an externally tagged enum of any variant shape.
enum EnumShape {
    Unit(&'static str),
    Newtype(&'static str, Value),
    Struct(&'static str, Vec<(&'static str, Value)>),
}

impl EnumShape {
    fn variant_name(&self) -> &'static str {
        match self {
            Self::Unit(name) | Self::Newtype(name, _) | Self::Struct(name, _) => name,
        }
    }
}

impl<'de> EnumAccess<'de> for EnumShape {
    type Error = ValueError;
    type Variant = Self;

    fn variant_seed<V: DeserializeSeed<'de>>(
        self,
        seed: V,
    ) -> Result<(V::Value, Self), ValueError> {
        let deserializer: StrDeserializer<'de, ValueError> =
            self.variant_name().into_deserializer();
        Ok((seed.deserialize(deserializer)?, self))
    }
}

impl<'de> VariantAccess<'de> for EnumShape {
    type Error = ValueError;

    fn unit_variant(self) -> Result<(), ValueError> {
        match self {
            Self::Unit(_) => Ok(()),
            _ => Err(serde::de::Error::custom("expected a unit variant")),
        }
    }

    fn newtype_variant_seed<T: DeserializeSeed<'de>>(
        self,
        seed: T,
    ) -> Result<T::Value, ValueError> {
        match self {
            Self::Newtype(_, value) => seed.deserialize(ValueDeserializer(value)),
            _ => Err(serde::de::Error::custom("expected a newtype variant")),
        }
    }

    fn tuple_variant<V: Visitor<'de>>(
        self,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, ValueError> {
        Err(serde::de::Error::custom(
            "tuple variants are not supported by the in-test serde format",
        ))
    }

    fn struct_variant<V: Visitor<'de>>(
        self,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        match self {
            Self::Struct(_, fields) => visitor.visit_map(StructAccesser {
                iter: fields.into_iter(),
                value: None,
            }),
            _ => Err(serde::de::Error::custom("expected a struct variant")),
        }
    }
}

impl<'de> Deserializer<'de> for ValueDeserializer {
    type Error = ValueError;

    fn is_human_readable(&self) -> bool {
        false
    }

    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        match self.0 {
            Value::Unit => visitor.visit_unit(),
            Value::Bool(v) => visitor.visit_bool(v),
            Value::U64(v) => visitor.visit_u64(v),
            Value::I64(v) => visitor.visit_i64(v),
            Value::F64(v) => visitor.visit_f64(v),
            Value::Str(v) => visitor.visit_string(v),
            Value::Seq(elements) => visitor.visit_seq(SeqAccesser {
                iter: elements.into_iter(),
            }),
            Value::Map(entries) => visitor.visit_map(MapAccesser {
                iter: entries.into_iter(),
                value: None,
            }),
            Value::UnitVariant(variant) => visitor.visit_enum(EnumShape::Unit(variant)),
            Value::NewtypeVariant(variant, value) => {
                visitor.visit_enum(EnumShape::Newtype(variant, *value))
            }
            Value::Struct(fields) => visitor.visit_map(StructAccesser {
                iter: fields.into_iter(),
                value: None,
            }),
            Value::StructVariant(variant, fields) => {
                visitor.visit_enum(EnumShape::Struct(variant, fields))
            }
        }
    }

    fn deserialize_option<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        match self.0 {
            Value::Unit => visitor.visit_none(),
            value => visitor.visit_some(ValueDeserializer(value)),
        }
    }

    fn deserialize_unit<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        match self.0 {
            Value::Unit => visitor.visit_unit(),
            value => Err(serde::de::Error::custom(format_args!(
                "expected unit, got {value:?}"
            ))),
        }
    }

    fn deserialize_newtype_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_enum<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        self.deserialize_any(visitor)
    }

    fn deserialize_struct<V: Visitor<'de>>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        self.deserialize_any(visitor)
    }

    fn deserialize_seq<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.deserialize_any(visitor)
    }

    fn deserialize_tuple<V: Visitor<'de>>(
        self,
        _len: usize,
        visitor: V,
    ) -> Result<V::Value, ValueError> {
        self.deserialize_any(visitor)
    }

    fn deserialize_map<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
        self.deserialize_any(visitor)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
        bytes byte_buf unit_struct tuple_struct identifier ignored_any
    }
}

/// Serializes a value into the in-test [`Value`] tree.
pub fn to_value<T: Serialize>(value: &T) -> Value {
    value
        .serialize(ValueSerializer)
        .expect("test serializer failed")
}

/// Deserializes a value back out of the in-test [`Value`] tree.
pub fn from_value<T: for<'de> Deserialize<'de>>(value: &Value) -> T {
    T::deserialize(ValueDeserializer(value.clone())).expect("test deserializer failed")
}

/// Round-trips a value through the in-test serde format and asserts the
/// result compares equal to the input.
pub fn assert_serde_round_trip<T>(value: &T)
where
    T: Serialize + for<'de> Deserialize<'de> + PartialEq + std::fmt::Debug,
{
    let tree = to_value(value);
    let back: T = from_value(&tree);
    assert_eq!(&back, value, "serde round-trip through {tree:?}");
}

/// Minimal `block_on` for the service-trait tests: the crate has no async
/// runtime in its frozen dependency set, and the stub services used with it
/// always complete on the first poll.
pub fn block_on<F: Future>(future: F) -> F::Output {
    use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};

    unsafe fn clone(_: *const ()) -> RawWaker {
        raw_waker()
    }
    fn raw_waker() -> RawWaker {
        RawWaker::new(std::ptr::null(), &VTABLE)
    }
    unsafe fn noop(_: *const ()) {}
    static VTABLE: RawWakerVTable = RawWakerVTable::new(clone, noop, noop, noop);

    // Safe: the vtable functions never dereference the null data pointer.
    let waker = unsafe { Waker::from_raw(raw_waker()) };
    let mut context = Context::from_waker(&waker);
    let mut future = Box::pin(future);
    loop {
        match future.as_mut().poll(&mut context) {
            Poll::Ready(output) => return output,
            Poll::Pending => std::thread::yield_now(),
        }
    }
}
