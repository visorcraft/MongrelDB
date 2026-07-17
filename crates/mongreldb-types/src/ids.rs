//! Common cluster-wide identifiers (spec section 7).
//!
//! Two families of identifiers are defined once here:
//!
//! - Random 128-bit IDs ([`ClusterId`], [`NodeId`], [`DatabaseId`],
//!   [`TabletId`], [`RaftGroupId`], [`TransactionId`], [`QueryId`]). They are
//!   drawn from the operating-system CSPRNG via [`getrandom`], are never
//!   derived only from timestamps, and are never reused within a database.
//! - Numeric 64-bit IDs ([`TableId`], [`SchemaVersion`], [`MetadataVersion`])
//!   allocated through replicated catalog state; they are likewise never
//!   reused within a database.
//!
//! # Text form
//!
//! The canonical text form of a 128-bit ID is strict lowercase hexadecimal,
//! exactly 32 characters. Parsing is deliberately lenient: the hyphenated
//! UUID form (`8-4-4-4-12`) and uppercase hex digits are also accepted, and
//! hyphens are in fact ignored at any position. Only the canonical form is
//! ever emitted, so persist and compare canonical output rather than
//! user-supplied spellings.
//!
//! # Serde form
//!
//! For human-readable serializers (`Serializer::is_human_readable`, e.g.
//! JSON) a 128-bit ID serializes as its canonical hex string and
//! deserializes from any accepted string spelling. For binary serializers
//! (e.g. bincode) the ID keeps the compact transparent `[u8; 16]` newtype
//! form, byte-identical to the derived representation. Numeric IDs serialize
//! as plain `u64` in both worlds.

use core::fmt;
use core::str::FromStr;

/// Error returned when parsing a textual identifier fails.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdParseError {
    /// The text had the wrong number of characters.
    #[error("invalid id length: expected 32 hex digits or UUID form, got {0} chars")]
    InvalidLength(usize),
    /// The text contained a non-hexadecimal character.
    #[error("invalid hex character `{0}` in id")]
    InvalidCharacter(char),
}

fn hex_encode(bytes: &[u8; 16]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(32);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<[u8; 16], IdParseError> {
    let len = text.chars().count();
    if len != 32 {
        return Err(IdParseError::InvalidLength(len));
    }
    let mut out = [0u8; 16];
    let mut chars = text.chars();
    for byte in &mut out {
        let hi = chars.next().expect("length checked above");
        let lo = chars.next().expect("length checked above");
        let hi = hi.to_digit(16).ok_or(IdParseError::InvalidCharacter(hi))?;
        let lo = lo.to_digit(16).ok_or(IdParseError::InvalidCharacter(lo))?;
        *byte = ((hi << 4) | lo) as u8;
    }
    Ok(out)
}

macro_rules! id128 {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[doc = ""]
        #[doc = "Random 128-bit identifier drawn from the operating-system CSPRNG by"]
        #[doc = "[`new_random`](Self::new_random); never derived only from timestamps and"]
        #[doc = "never reused within a database. The all-zero value is reserved."]
        #[doc = ""]
        #[doc = "Canonical text form: strict lowercase hexadecimal, 32 characters."]
        #[doc = "Parsing is lenient and additionally accepts the hyphenated UUID form"]
        #[doc = "(`8-4-4-4-12`) and uppercase hex digits."]
        #[repr(transparent)]
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name(pub [u8; 16]);

        impl $name {
            /// The all-zero identifier (reserved; never produced by
            /// [`Self::new_random`] in practice).
            pub const ZERO: Self = Self([0u8; 16]);

            /// Draws a fresh random identifier from the operating-system
            /// CSPRNG. Never derived only from timestamps; with 128 random
            /// bits the result is never reused within a database.
            pub fn new_random() -> Self {
                let mut bytes = [0u8; 16];
                getrandom::getrandom(&mut bytes)
                    .expect("operating-system CSPRNG unavailable");
                Self(bytes)
            }

            /// Wraps raw bytes without copying.
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            /// Borrows the raw 16 bytes.
            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            /// Canonical text form: strict lowercase hexadecimal (32 chars).
            pub fn to_hex(self) -> String {
                hex_encode(&self.0)
            }
        }

        impl serde::Serialize for $name {
            /// Human-readable serializers (e.g. JSON) receive the canonical
            /// hex string; binary serializers receive the compact 16-byte
            /// newtype form, byte-identical to a derived implementation.
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                if serializer.is_human_readable() {
                    serializer.serialize_str(&hex_encode(&self.0))
                } else {
                    serializer.serialize_newtype_struct(stringify!($name), &self.0)
                }
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            /// Human-readable deserializers accept any string spelling
            /// accepted by [`FromStr`]; binary deserializers expect the
            /// compact 16-byte newtype form.
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                struct HexVisitor;

                impl serde::de::Visitor<'_> for HexVisitor {
                    type Value = $name;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str(concat!(
                            "a ",
                            stringify!($name),
                            " as 32 hex digits or hyphenated UUID form"
                        ))
                    }

                    fn visit_str<E>(self, text: &str) -> Result<Self::Value, E>
                    where
                        E: serde::de::Error,
                    {
                        text.parse().map_err(serde::de::Error::custom)
                    }
                }

                struct BytesVisitor;

                impl<'de> serde::de::Visitor<'de> for BytesVisitor {
                    type Value = $name;

                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str(concat!("a ", stringify!($name), " as 16 raw bytes"))
                    }

                    fn visit_newtype_struct<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
                    where
                        D: serde::Deserializer<'de>,
                    {
                        <[u8; 16] as serde::Deserialize>::deserialize(deserializer).map($name)
                    }

                    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                    where
                        A: serde::de::SeqAccess<'de>,
                    {
                        let bytes: [u8; 16] = match seq.next_element()? {
                            Some(value) => value,
                            None => return Err(serde::de::Error::invalid_length(0, &self)),
                        };
                        Ok($name(bytes))
                    }
                }

                if deserializer.is_human_readable() {
                    deserializer.deserialize_str(HexVisitor)
                } else {
                    deserializer.deserialize_newtype_struct(stringify!($name), BytesVisitor)
                }
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&hex_encode(&self.0))
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), hex_encode(&self.0))
            }
        }

        impl FromStr for $name {
            type Err = IdParseError;

            /// Parses the canonical 32-character hex form. Lenient by
            /// contract: hyphens are ignored at any position (so the
            /// hyphenated UUID form `8-4-4-4-12` parses) and uppercase hex
            /// digits are accepted. The canonical lowercase form emitted by
            /// [`Display`](fmt::Display) and [`to_hex`](Self::to_hex) is the
            /// only stable spelling for persistence.
            fn from_str(text: &str) -> Result<Self, Self::Err> {
                let compact: String = text.chars().filter(|c| *c != '-').collect();
                hex_decode(&compact).map(Self)
            }
        }
    };
}

id128!(
    /// Identifies one MongrelDB cluster.
    ClusterId
);
id128!(
    /// Identifies one running server node within a cluster.
    NodeId
);
id128!(
    /// Identifies one logical database.
    DatabaseId
);
id128!(
    /// Identifies one independently replicated and movable data partition.
    TabletId
);
id128!(
    /// Identifies one consensus group.
    RaftGroupId
);
id128!(
    /// Identifies one transaction.
    TransactionId
);
id128!(
    /// Identifies one query execution.
    QueryId
);

macro_rules! id64 {
    ($(#[$meta:meta])* $name:ident) => {
        $(#[$meta])*
        #[doc = ""]
        #[doc = "Numeric 64-bit identifier allocated through replicated catalog state;"]
        #[doc = "never reused within a database. The zero value is reserved."]
        #[repr(transparent)]
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize)]
        pub struct $name(pub u64);

        impl $name {
            /// The zero value (reserved).
            pub const ZERO: Self = Self(0);

            /// Wraps a raw value.
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            /// Returns the raw value.
            pub const fn get(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }

        impl FromStr for $name {
            type Err = core::num::ParseIntError;

            fn from_str(text: &str) -> Result<Self, Self::Err> {
                text.parse::<u64>().map(Self)
            }
        }
    };
}

id64!(
    /// Numeric table identifier.
    TableId
);
id64!(
    /// Monotonic schema version of one table or database.
    SchemaVersion
);
id64!(
    /// Monotonic control-plane metadata version.
    MetadataVersion
);

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use std::collections::HashSet;

    // serde_json and bincode are not dependencies of this crate and its
    // manifest cannot grow them, so the serde contract is exercised through
    // minimal handwritten harnesses: `StrSerializer` plus serde's built-in
    // `value::StrDeserializer` stand in for a human-readable format such as
    // serde_json, and `BinSerializer`/`BinDeserializer` stand in for a
    // non-human-readable format such as bincode (both treat a newtype struct
    // transparently, like bincode does).

    #[derive(Debug)]
    struct HarnessError(String);

    impl fmt::Display for HarnessError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str(&self.0)
        }
    }

    impl std::error::Error for HarnessError {}

    impl serde::ser::Error for HarnessError {
        fn custom<T: fmt::Display>(msg: T) -> Self {
            Self(msg.to_string())
        }
    }

    impl serde::de::Error for HarnessError {
        fn custom<T: fmt::Display>(msg: T) -> Self {
            Self(msg.to_string())
        }
    }

    /// Human-readable serializer that only supports strings, mirroring how a
    /// JSON serializer would carry an ID.
    struct StrSerializer;

    impl serde::Serializer for StrSerializer {
        type Ok = String;
        type Error = HarnessError;
        type SerializeSeq = serde::ser::Impossible<String, HarnessError>;
        type SerializeTuple = serde::ser::Impossible<String, HarnessError>;
        type SerializeTupleStruct = serde::ser::Impossible<String, HarnessError>;
        type SerializeTupleVariant = serde::ser::Impossible<String, HarnessError>;
        type SerializeMap = serde::ser::Impossible<String, HarnessError>;
        type SerializeStruct = serde::ser::Impossible<String, HarnessError>;
        type SerializeStructVariant = serde::ser::Impossible<String, HarnessError>;

        fn is_human_readable(&self) -> bool {
            true
        }

        fn serialize_str(self, v: &str) -> Result<String, HarnessError> {
            Ok(v.to_owned())
        }

        fn serialize_bool(self, _v: bool) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_i8(self, _v: i8) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_i16(self, _v: i16) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_i32(self, _v: i32) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_i64(self, _v: i64) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_u8(self, _v: u8) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_u16(self, _v: u16) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_u32(self, _v: u32) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_u64(self, _v: u64) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_f32(self, _v: f32) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_f64(self, _v: f64) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_char(self, _v: char) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_bytes(self, _v: &[u8]) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_none(self) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_some<T: ?Sized + serde::Serialize>(
            self,
            _value: &T,
        ) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_unit(self) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_unit_struct(self, _name: &'static str) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(
            self,
            _name: &'static str,
            _value: &T,
        ) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _value: &T,
        ) -> Result<String, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_seq(self, _len: Option<usize>) -> Result<Self::SerializeSeq, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_tuple(self, _len: usize) -> Result<Self::SerializeTuple, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
    }

    /// Non-human-readable serializer that flattens an ID to its raw bytes,
    /// treating newtype structs transparently like bincode does.
    struct BinSerializer;

    struct ByteCollector {
        buf: Vec<u8>,
    }

    impl ByteCollector {
        fn push<T: ?Sized + serde::Serialize>(&mut self, value: &T) -> Result<(), HarnessError> {
            let mut bytes = value.serialize(BinSerializer)?;
            self.buf.append(&mut bytes);
            Ok(())
        }
    }

    impl serde::ser::SerializeSeq for ByteCollector {
        type Ok = Vec<u8>;
        type Error = HarnessError;

        fn serialize_element<T: ?Sized + serde::Serialize>(
            &mut self,
            value: &T,
        ) -> Result<(), HarnessError> {
            self.push(value)
        }

        fn end(self) -> Result<Vec<u8>, HarnessError> {
            Ok(self.buf)
        }
    }

    impl serde::ser::SerializeTuple for ByteCollector {
        type Ok = Vec<u8>;
        type Error = HarnessError;

        fn serialize_element<T: ?Sized + serde::Serialize>(
            &mut self,
            value: &T,
        ) -> Result<(), HarnessError> {
            self.push(value)
        }

        fn end(self) -> Result<Vec<u8>, HarnessError> {
            Ok(self.buf)
        }
    }

    impl serde::Serializer for BinSerializer {
        type Ok = Vec<u8>;
        type Error = HarnessError;
        type SerializeSeq = ByteCollector;
        type SerializeTuple = ByteCollector;
        type SerializeTupleStruct = serde::ser::Impossible<Vec<u8>, HarnessError>;
        type SerializeTupleVariant = serde::ser::Impossible<Vec<u8>, HarnessError>;
        type SerializeMap = serde::ser::Impossible<Vec<u8>, HarnessError>;
        type SerializeStruct = serde::ser::Impossible<Vec<u8>, HarnessError>;
        type SerializeStructVariant = serde::ser::Impossible<Vec<u8>, HarnessError>;

        fn is_human_readable(&self) -> bool {
            false
        }

        fn serialize_u8(self, v: u8) -> Result<Vec<u8>, HarnessError> {
            Ok(vec![v])
        }

        fn serialize_bytes(self, v: &[u8]) -> Result<Vec<u8>, HarnessError> {
            Ok(v.to_vec())
        }

        fn serialize_newtype_struct<T: ?Sized + serde::Serialize>(
            self,
            _name: &'static str,
            value: &T,
        ) -> Result<Vec<u8>, HarnessError> {
            value.serialize(self)
        }

        fn serialize_seq(self, len: Option<usize>) -> Result<ByteCollector, HarnessError> {
            Ok(ByteCollector {
                buf: Vec::with_capacity(len.unwrap_or(0)),
            })
        }

        fn serialize_tuple(self, len: usize) -> Result<ByteCollector, HarnessError> {
            Ok(ByteCollector {
                buf: Vec::with_capacity(len),
            })
        }

        fn serialize_bool(self, _v: bool) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_i8(self, _v: i8) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_i16(self, _v: i16) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_i32(self, _v: i32) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_i64(self, _v: i64) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_u16(self, _v: u16) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_u32(self, _v: u32) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_u64(self, _v: u64) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_f32(self, _v: f32) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_f64(self, _v: f64) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_char(self, _v: char) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_str(self, _v: &str) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_none(self) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_some<T: ?Sized + serde::Serialize>(
            self,
            _value: &T,
        ) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_unit(self) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_unit_struct(self, _name: &'static str) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
        ) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_newtype_variant<T: ?Sized + serde::Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _value: &T,
        ) -> Result<Vec<u8>, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_tuple_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleStruct, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_tuple_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeTupleVariant, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_map(self, _len: Option<usize>) -> Result<Self::SerializeMap, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_struct(
            self,
            _name: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStruct, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
        fn serialize_struct_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _len: usize,
        ) -> Result<Self::SerializeStructVariant, HarnessError> {
            Err(HarnessError("unsupported".into()))
        }
    }

    /// Non-human-readable deserializer over a byte cursor, treating newtype
    /// structs transparently like bincode does. Callers request exactly as
    /// many elements as they need, so no length bookkeeping is required.
    struct BinDeserializer<'de> {
        input: &'de [u8],
    }

    impl<'de> BinDeserializer<'de> {
        fn new(input: &'de [u8]) -> Self {
            Self { input }
        }
    }

    impl<'de> serde::Deserializer<'de> for &mut BinDeserializer<'de> {
        type Error = HarnessError;

        fn is_human_readable(&self) -> bool {
            false
        }

        fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, HarnessError>
        where
            V: serde::de::Visitor<'de>,
        {
            Err(HarnessError("unsupported".into()))
        }

        fn deserialize_newtype_struct<V>(
            self,
            _name: &'static str,
            visitor: V,
        ) -> Result<V::Value, HarnessError>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_newtype_struct(self)
        }

        fn deserialize_tuple<V>(self, _len: usize, visitor: V) -> Result<V::Value, HarnessError>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_seq(ByteSeq { de: self })
        }

        fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, HarnessError>
        where
            V: serde::de::Visitor<'de>,
        {
            visitor.visit_seq(ByteSeq { de: self })
        }

        fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, HarnessError>
        where
            V: serde::de::Visitor<'de>,
        {
            let (&byte, rest) = self
                .input
                .split_first()
                .ok_or_else(|| HarnessError("unexpected end of input".into()))?;
            self.input = rest;
            visitor.visit_u8(byte)
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 u16 u32 u64 f32 f64 char str string
            bytes byte_buf option unit unit_struct
            tuple_struct map struct enum identifier ignored_any
        }
    }

    struct ByteSeq<'a, 'de> {
        de: &'a mut BinDeserializer<'de>,
    }

    impl<'de> serde::de::SeqAccess<'de> for ByteSeq<'_, 'de> {
        type Error = HarnessError;

        fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, HarnessError>
        where
            T: serde::de::DeserializeSeed<'de>,
        {
            seed.deserialize(&mut *self.de).map(Some)
        }
    }

    fn check_text_forms<T>(id: T)
    where
        T: Copy + PartialEq + fmt::Debug + fmt::Display + FromStr<Err = IdParseError>,
    {
        let text = id.to_string();
        assert_eq!(text.len(), 32, "canonical form is 32 chars");
        assert!(
            text.chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()),
            "canonical form is strict lowercase hex: {text}"
        );
        assert_eq!(text.parse::<T>().unwrap(), id, "hex round-trip");

        let uuid = format!(
            "{}-{}-{}-{}-{}",
            &text[0..8],
            &text[8..12],
            &text[12..16],
            &text[16..20],
            &text[20..32]
        );
        assert_eq!(
            uuid.parse::<T>().unwrap(),
            id,
            "hyphenated UUID form parses"
        );

        let upper = text.to_ascii_uppercase();
        assert_eq!(upper.parse::<T>().unwrap(), id, "uppercase hex parses");

        let upper_uuid = uuid.to_ascii_uppercase();
        assert_eq!(
            upper_uuid.parse::<T>().unwrap(),
            id,
            "uppercase UUID form parses"
        );
    }

    #[test]
    fn text_forms_round_trip_all_types() {
        check_text_forms(ClusterId::new_random());
        check_text_forms(NodeId::new_random());
        check_text_forms(DatabaseId::new_random());
        check_text_forms(TabletId::new_random());
        check_text_forms(RaftGroupId::new_random());
        check_text_forms(TransactionId::new_random());
        check_text_forms(QueryId::new_random());
    }

    #[test]
    fn text_form_exact_known_value() {
        let bytes = [
            0x00, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76,
            0x54, 0x32,
        ];
        let id = ClusterId::from_bytes(bytes);
        let expected = "000123456789abcdeffedcba98765432";
        assert_eq!(id.to_hex(), expected);
        assert_eq!(id.to_string(), expected);
        assert_eq!(format!("{id:?}"), format!("ClusterId({expected})"));
        assert_eq!(expected.parse::<ClusterId>().unwrap(), id);
        assert_eq!(ClusterId::from_bytes(bytes).as_bytes(), &bytes);
    }

    #[test]
    fn parse_rejects_bad_input() {
        assert_eq!(
            "".parse::<ClusterId>(),
            Err(IdParseError::InvalidLength(0)),
            "empty input"
        );
        assert_eq!(
            "abcd".parse::<ClusterId>(),
            Err(IdParseError::InvalidLength(4)),
            "too short"
        );
        assert_eq!(
            "a".repeat(33).parse::<ClusterId>(),
            Err(IdParseError::InvalidLength(33)),
            "too long"
        );
        assert_eq!(
            "--------------------------------".parse::<ClusterId>(),
            Err(IdParseError::InvalidLength(0)),
            "hyphens only"
        );
        assert_eq!(
            "g".repeat(32).parse::<ClusterId>(),
            Err(IdParseError::InvalidCharacter('g')),
            "non-hex character"
        );
        assert_eq!(
            format!("{}é", "a".repeat(31)).parse::<ClusterId>(),
            Err(IdParseError::InvalidCharacter('é')),
            "non-ASCII character"
        );
    }

    fn check_human_readable_serde<T>(id: T)
    where
        T: Copy + PartialEq + fmt::Debug + fmt::Display + FromStr<Err = IdParseError>,
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let json_like = id.serialize(StrSerializer).unwrap();
        assert_eq!(
            json_like,
            id.to_string(),
            "human-readable form is the hex string"
        );

        let back: T = T::deserialize(serde::de::value::StrDeserializer::<HarnessError>::new(
            &json_like,
        ))
        .unwrap();
        assert_eq!(back, id, "human-readable string round-trip");

        let upper: T = T::deserialize(serde::de::value::StrDeserializer::<HarnessError>::new(
            &json_like.to_ascii_uppercase(),
        ))
        .unwrap();
        assert_eq!(upper, id, "human-readable form accepts uppercase");

        let invalid = T::deserialize(serde::de::value::StrDeserializer::<HarnessError>::new(
            "not-an-id",
        ));
        assert!(
            invalid.is_err(),
            "human-readable form rejects invalid strings"
        );
    }

    #[test]
    fn human_readable_serde_round_trip_all_types() {
        check_human_readable_serde(ClusterId::new_random());
        check_human_readable_serde(NodeId::new_random());
        check_human_readable_serde(DatabaseId::new_random());
        check_human_readable_serde(TabletId::new_random());
        check_human_readable_serde(RaftGroupId::new_random());
        check_human_readable_serde(TransactionId::new_random());
        check_human_readable_serde(QueryId::new_random());
    }

    fn check_binary_serde<T>(id: T, raw: &[u8; 16])
    where
        T: Copy + PartialEq + fmt::Debug + Serialize + for<'de> Deserialize<'de>,
    {
        let bytes = id.serialize(BinSerializer).unwrap();
        assert_eq!(bytes.len(), 16, "binary form stays 16 bytes");
        assert_eq!(
            bytes.as_slice(),
            raw.as_slice(),
            "binary form is the raw bytes"
        );

        let mut de = BinDeserializer::new(&bytes);
        let back = T::deserialize(&mut de).unwrap();
        assert_eq!(back, id, "binary round-trip");
        assert!(de.input.is_empty(), "binary form consumed exactly");
    }

    #[test]
    fn binary_serde_stays_sixteen_bytes_all_types() {
        macro_rules! check {
            ($($name:ident),*) => {$(
                let id = $name::new_random();
                check_binary_serde(id, id.as_bytes());
            )*};
        }
        check!(
            ClusterId,
            NodeId,
            DatabaseId,
            TabletId,
            RaftGroupId,
            TransactionId,
            QueryId
        );
    }

    #[test]
    fn zero_constant_is_all_zero() {
        assert_eq!(ClusterId::ZERO.as_bytes(), &[0u8; 16]);
        assert_eq!(ClusterId::ZERO.to_hex(), "0".repeat(32));
        assert_eq!(
            "0".repeat(32).parse::<ClusterId>().unwrap(),
            ClusterId::ZERO
        );
        assert_eq!(TableId::ZERO.get(), 0);
        assert_eq!(SchemaVersion::ZERO.get(), 0);
        assert_eq!(MetadataVersion::ZERO.get(), 0);
    }

    #[test]
    fn ordering_follows_byte_order() {
        let low = ClusterId::from_bytes([0x00; 16]);
        let mut high_bytes = [0x00; 16];
        high_bytes[15] = 0x01;
        let high = ClusterId::from_bytes(high_bytes);
        assert!(low < high);
        assert!(ClusterId::ZERO <= low);

        let mut ids = vec![high, ClusterId::ZERO, low];
        ids.sort();
        assert_eq!(ids, vec![ClusterId::ZERO, low, high]);

        assert!(TableId::new(1) < TableId::new(2));
    }

    #[test]
    fn new_random_is_distinct_and_nonzero() {
        let mut seen = HashSet::with_capacity(1000);
        for _ in 0..1000 {
            let id = ClusterId::new_random();
            assert_ne!(
                id,
                ClusterId::ZERO,
                "random ID must not be the reserved zero value"
            );
            assert!(seen.insert(id), "random IDs must not repeat");
        }
    }

    #[test]
    fn id64_round_trips() {
        let id = TableId::new(42);
        assert_eq!(id.get(), 42);
        assert_eq!(id.to_string(), "42");
        assert_eq!("42".parse::<TableId>().unwrap(), id);
        assert!("not-a-number".parse::<TableId>().is_err());
        assert_eq!(format!("{id:?}"), "TableId(42)");
    }
}
