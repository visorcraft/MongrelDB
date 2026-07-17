//! Stable cross-language error taxonomy (spec section 9.7, FND-007).
//!
//! Implemented in the Stage 0 foundation wave: `ErrorCategory` with exactly
//! the twenty categories the spec lists, stable numeric codes that are never
//! reused, and the canonical structural form every language binding maps.
//!
//! Retry policy is expressed per category through [`ErrorCategory::retry_class`];
//! the routing rules of spec section 11.7 (leader hints, metadata refresh, and
//! the ban on replaying ambiguous writes without a durable idempotency key)
//! are layered on top of that classification by the gateway.

/// One of the twenty stable error categories of spec section 9.7.
///
/// Stability contract (spec section 4.10):
///
/// - The numeric [`ErrorCategory::code`] of a category is never reused, even
///   if a category is retired; new categories are only ever appended with
///   fresh codes.
/// - Declaration order is frozen: serde encodes enums by variant index in
///   binary formats, so variants must never be reordered.
/// - Variant names are the cross-language wire contract in text formats;
///   they never change.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize, thiserror::Error,
)]
pub enum ErrorCategory {
    /// The receiving replica is not the leader for the consensus group.
    /// Retryable with the returned leader hint (spec section 11.7).
    #[error("not leader")]
    NotLeader,
    /// No leader is currently known for the consensus group; retry after
    /// re-running leader discovery and refreshing routing metadata.
    #[error("leader unknown")]
    LeaderUnknown,
    /// The caller acted on stale routing or schema metadata; retry after a
    /// metadata refresh.
    #[error("stale metadata")]
    StaleMetadata,
    /// The target replica cannot serve the request; retry with backoff or
    /// fail over to another replica.
    #[error("replica unavailable")]
    ReplicaUnavailable,
    /// The consensus group lost quorum; retry with backoff until a quorum is
    /// restored.
    #[error("quorum unavailable")]
    QuorumUnavailable,
    /// The transaction conflicted with a concurrent transaction; retry the
    /// whole transaction.
    #[error("transaction conflict")]
    TransactionConflict,
    /// The transaction was aborted (by the system or by the caller); a fresh
    /// transaction may be retried from the beginning.
    #[error("transaction aborted")]
    TransactionAborted,
    /// A serializable transaction failed its certification; retry the whole
    /// transaction.
    #[error("serialization failure")]
    SerializationFailure,
    /// The transaction was chosen as a deadlock victim; retry the whole
    /// transaction.
    #[error("deadlock")]
    Deadlock,
    /// The caller-supplied deadline expired. Not automatically retryable:
    /// issue a new request with a fresh deadline instead of replaying.
    #[error("deadline exceeded")]
    DeadlineExceeded,
    /// The caller cancelled the operation. Never retried: cancellation is
    /// caller intent.
    #[error("cancelled")]
    Cancelled,
    /// The outcome of a commit is unknown (contact was lost after propose).
    /// NEVER blindly retried (spec section 11.7): replay only with a durable
    /// idempotency key or an unambiguous not-proposed status.
    #[error("commit outcome unknown")]
    CommitOutcomeUnknown,
    /// The commit arrived after its window closed and was not applied. The
    /// commit itself is final; only a fresh transaction may succeed.
    #[error("commit too late")]
    CommitTooLate,
    /// The tablet moved to another node; retry after refreshing routing
    /// metadata.
    #[error("tablet moved")]
    TabletMoved,
    /// The tablet is mid-split; retry after the split completes and routing
    /// metadata has been refreshed.
    #[error("tablet splitting")]
    TabletSplitting,
    /// The request was built against a different schema version; retry after
    /// refreshing the schema and re-preparing the request.
    #[error("schema version mismatch")]
    SchemaVersionMismatch,
    /// Binary, protocol, or format versions disagree (spec section 11.8).
    /// Not retryable: only upgrading or rolling back one side changes the
    /// outcome.
    #[error("cluster version mismatch")]
    ClusterVersionMismatch,
    /// A resource limit (memory, disk, budget, locks) was hit; retry with
    /// backoff once capacity frees up, or with a smaller request.
    #[error("resource exhausted")]
    ResourceExhausted,
    /// Credentials are missing or invalid. Not retryable until the caller
    /// obtains different credentials — that is a new request, not a retry.
    #[error("unauthenticated")]
    Unauthenticated,
    /// The authenticated principal lacks the required grant. Never retried:
    /// replaying the same request as the same principal re-fails.
    #[error("permission denied")]
    PermissionDenied,
}

/// The retry discipline for an [`ErrorCategory`] (spec section 11.7).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize)]
pub enum RetryClass {
    /// Never retried automatically: the same request would re-fail (or the
    /// failure is caller intent), and only changing the request, the
    /// principal, or a version changes the outcome.
    Never,
    /// Never blindly retried: the outcome is ambiguous, so a replay is only
    /// permitted with a durable idempotency key or an unambiguous
    /// not-proposed status (spec section 11.7).
    IdempotencyKeyRequired,
    /// Retryable after refreshing routing and/or schema metadata (leader
    /// hint, endpoint list, tablet map, schema version).
    AfterMetadataRefresh,
    /// Retryable after a backoff delay once the transient condition
    /// (availability, quorum, resource pressure) clears.
    AfterBackoff,
    /// Retryable by restarting the whole transaction from the beginning with
    /// a fresh snapshot.
    RetryTransaction,
}

impl ErrorCategory {
    /// All twenty categories in declaration (and code) order.
    pub const ALL: [Self; 20] = [
        Self::NotLeader,
        Self::LeaderUnknown,
        Self::StaleMetadata,
        Self::ReplicaUnavailable,
        Self::QuorumUnavailable,
        Self::TransactionConflict,
        Self::TransactionAborted,
        Self::SerializationFailure,
        Self::Deadlock,
        Self::DeadlineExceeded,
        Self::Cancelled,
        Self::CommitOutcomeUnknown,
        Self::CommitTooLate,
        Self::TabletMoved,
        Self::TabletSplitting,
        Self::SchemaVersionMismatch,
        Self::ClusterVersionMismatch,
        Self::ResourceExhausted,
        Self::Unauthenticated,
        Self::PermissionDenied,
    ];

    /// The stable numeric code of this category, in `1..=20`.
    ///
    /// Codes are never reused (spec section 4.10): a retired category keeps
    /// its code forever and new categories are only ever appended, so a code
    /// a peer emits is always interpretable.
    pub const fn code(self) -> u32 {
        match self {
            Self::NotLeader => 1,
            Self::LeaderUnknown => 2,
            Self::StaleMetadata => 3,
            Self::ReplicaUnavailable => 4,
            Self::QuorumUnavailable => 5,
            Self::TransactionConflict => 6,
            Self::TransactionAborted => 7,
            Self::SerializationFailure => 8,
            Self::Deadlock => 9,
            Self::DeadlineExceeded => 10,
            Self::Cancelled => 11,
            Self::CommitOutcomeUnknown => 12,
            Self::CommitTooLate => 13,
            Self::TabletMoved => 14,
            Self::TabletSplitting => 15,
            Self::SchemaVersionMismatch => 16,
            Self::ClusterVersionMismatch => 17,
            Self::ResourceExhausted => 18,
            Self::Unauthenticated => 19,
            Self::PermissionDenied => 20,
        }
    }

    /// Resolves a stable [`Self::code`] back to its category.
    ///
    /// Returns `None` for codes this build does not know (never emitted by
    /// this build; possibly allocated by a newer one). Callers MUST treat an
    /// unknown code as an unknown, non-retryable failure rather than
    /// guessing.
    pub const fn from_code(code: u32) -> Option<Self> {
        match code {
            1 => Some(Self::NotLeader),
            2 => Some(Self::LeaderUnknown),
            3 => Some(Self::StaleMetadata),
            4 => Some(Self::ReplicaUnavailable),
            5 => Some(Self::QuorumUnavailable),
            6 => Some(Self::TransactionConflict),
            7 => Some(Self::TransactionAborted),
            8 => Some(Self::SerializationFailure),
            9 => Some(Self::Deadlock),
            10 => Some(Self::DeadlineExceeded),
            11 => Some(Self::Cancelled),
            12 => Some(Self::CommitOutcomeUnknown),
            13 => Some(Self::CommitTooLate),
            14 => Some(Self::TabletMoved),
            15 => Some(Self::TabletSplitting),
            16 => Some(Self::SchemaVersionMismatch),
            17 => Some(Self::ClusterVersionMismatch),
            18 => Some(Self::ResourceExhausted),
            19 => Some(Self::Unauthenticated),
            20 => Some(Self::PermissionDenied),
            _ => None,
        }
    }

    /// How a failure in this category may be retried (spec section 11.7).
    pub const fn retry_class(self) -> RetryClass {
        match self {
            Self::NotLeader
            | Self::LeaderUnknown
            | Self::StaleMetadata
            | Self::TabletMoved
            | Self::TabletSplitting
            | Self::SchemaVersionMismatch => RetryClass::AfterMetadataRefresh,
            Self::ReplicaUnavailable | Self::QuorumUnavailable | Self::ResourceExhausted => {
                RetryClass::AfterBackoff
            }
            Self::TransactionConflict
            | Self::TransactionAborted
            | Self::SerializationFailure
            | Self::Deadlock
            | Self::CommitTooLate => RetryClass::RetryTransaction,
            Self::DeadlineExceeded
            | Self::Cancelled
            | Self::ClusterVersionMismatch
            | Self::Unauthenticated
            | Self::PermissionDenied => RetryClass::Never,
            Self::CommitOutcomeUnknown => RetryClass::IdempotencyKeyRequired,
        }
    }

    /// Whether a plain automatic retry — no fresh credentials and no durable
    /// idempotency key — may succeed for this category.
    ///
    /// This is deliberately `false` for [`ErrorCategory::CommitOutcomeUnknown`]
    /// even though a replay *with* a durable idempotency key is permitted
    /// (spec section 11.7): "retryable" here means safe to retry blindly.
    pub const fn is_retryable(self) -> bool {
        matches!(
            self.retry_class(),
            RetryClass::AfterMetadataRefresh
                | RetryClass::AfterBackoff
                | RetryClass::RetryTransaction
        )
    }
}

/// The canonical structural error form of the taxonomy (spec section 9.7).
///
/// Every language binding — the Node addon, the C FFI, and the JNI binding,
/// all built in later stages — maps exactly this structure: a stable
/// [`ErrorCategory`] plus a human-readable message. The message is diagnostic
/// text only; programmatic handling MUST key off `category` (or its stable
/// [`CategoryError::code`]), never off the message.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, thiserror::Error)]
#[error("{category}: {message}")]
pub struct CategoryError {
    /// The stable category of the failure.
    pub category: ErrorCategory,
    /// Human-readable detail. Not part of the stable contract.
    pub message: String,
}

impl CategoryError {
    /// Builds the structural form for `category` with diagnostic `message`.
    pub fn new(category: ErrorCategory, message: impl Into<String>) -> Self {
        Self {
            category,
            message: message.into(),
        }
    }

    /// The stable numeric code of [`Self::category`] (never reused, spec
    /// section 4.10).
    pub fn code(&self) -> u32 {
        self.category.code()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::de::value::{Error as ValueError, StrDeserializer};
    use serde::de::{
        DeserializeSeed, EnumAccess, IntoDeserializer, MapAccess, VariantAccess, Visitor,
    };
    use serde::ser::{Impossible, SerializeStruct};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    // Minimal serde data-model format used to round-trip the taxonomy in
    // tests without pulling a serialization crate into this crate's
    // (frozen) dependency set: values become a `Value` tree and the
    // deserializer replays it.
    #[derive(Debug, Clone, PartialEq, Eq)]
    enum Value {
        Str(String),
        UnitVariant(&'static str),
        Struct(Vec<(&'static str, Value)>),
    }

    struct ValueSerializer;

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
        type SerializeSeq = Impossible<Value, ValueError>;
        type SerializeTuple = Impossible<Value, ValueError>;
        type SerializeTupleStruct = Impossible<Value, ValueError>;
        type SerializeTupleVariant = Impossible<Value, ValueError>;
        type SerializeMap = Impossible<Value, ValueError>;
        type SerializeStruct = StructSerializer;
        type SerializeStructVariant = Impossible<Value, ValueError>;

        fn serialize_str(self, v: &str) -> Result<Value, ValueError> {
            Ok(Value::Str(v.to_owned()))
        }

        fn serialize_unit_variant(
            self,
            _name: &'static str,
            _variant_index: u32,
            variant: &'static str,
        ) -> Result<Value, ValueError> {
            Ok(Value::UnitVariant(variant))
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

        fn serialize_none(self) -> Result<Value, ValueError> {
            Err(serde::ser::Error::custom(
                "serialize_none is not supported by the in-test serde format",
            ))
        }

        fn serialize_some<T: ?Sized + Serialize>(self, _value: &T) -> Result<Value, ValueError> {
            Err(serde::ser::Error::custom(
                "serialize_some is not supported by the in-test serde format",
            ))
        }

        fn serialize_newtype_struct<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _value: &T,
        ) -> Result<Value, ValueError> {
            Err(serde::ser::Error::custom(
                "serialize_newtype_struct is not supported by the in-test serde format",
            ))
        }

        fn serialize_newtype_variant<T: ?Sized + Serialize>(
            self,
            _name: &'static str,
            _variant_index: u32,
            _variant: &'static str,
            _value: &T,
        ) -> Result<Value, ValueError> {
            Err(serde::ser::Error::custom(
                "serialize_newtype_variant is not supported by the in-test serde format",
            ))
        }

        serialize_unsupported! {
            serialize_bool(_v: bool) -> Value;
            serialize_i8(_v: i8) -> Value;
            serialize_i16(_v: i16) -> Value;
            serialize_i32(_v: i32) -> Value;
            serialize_i64(_v: i64) -> Value;
            serialize_u8(_v: u8) -> Value;
            serialize_u16(_v: u16) -> Value;
            serialize_u32(_v: u32) -> Value;
            serialize_u64(_v: u64) -> Value;
            serialize_f32(_v: f32) -> Value;
            serialize_f64(_v: f64) -> Value;
            serialize_char(_v: char) -> Value;
            serialize_bytes(_v: &[u8]) -> Value;
            serialize_unit() -> Value;
            serialize_unit_struct(_name: &'static str) -> Value;
            serialize_seq(_len: Option<usize>) -> Self::SerializeSeq;
            serialize_tuple(_len: usize) -> Self::SerializeTuple;
            serialize_tuple_struct(_name: &'static str, _len: usize) -> Self::SerializeTupleStruct;
            serialize_tuple_variant(_name: &'static str, _variant_index: u32, _variant: &'static str, _len: usize) -> Self::SerializeTupleVariant;
            serialize_map(_len: Option<usize>) -> Self::SerializeMap;
            serialize_struct_variant(_name: &'static str, _variant_index: u32, _variant: &'static str, _len: usize) -> Self::SerializeStructVariant;
        }
    }

    struct StructSerializer {
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

    struct ValueDeserializer(Value);

    impl<'de> Deserializer<'de> for ValueDeserializer {
        type Error = ValueError;

        fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
            match self.0 {
                Value::Str(text) => visitor.visit_string(text),
                Value::UnitVariant(variant) => visitor.visit_enum(UnitVariantAccess { variant }),
                Value::Struct(fields) => visitor.visit_map(StructAccess {
                    iter: fields.into_iter(),
                    value: None,
                }),
            }
        }

        fn deserialize_struct<V: Visitor<'de>>(
            self,
            _name: &'static str,
            _fields: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, ValueError> {
            self.deserialize_any(visitor)
        }

        fn deserialize_enum<V: Visitor<'de>>(
            self,
            _name: &'static str,
            _variants: &'static [&'static str],
            visitor: V,
        ) -> Result<V::Value, ValueError> {
            self.deserialize_any(visitor)
        }

        fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
            self.deserialize_any(visitor)
        }

        fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, ValueError> {
            self.deserialize_any(visitor)
        }

        serde::forward_to_deserialize_any! {
            bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char bytes
            byte_buf option unit unit_struct newtype_struct seq tuple
            tuple_struct map identifier ignored_any
        }
    }

    struct StructAccess {
        iter: std::vec::IntoIter<(&'static str, Value)>,
        value: Option<Value>,
    }

    impl<'de> MapAccess<'de> for StructAccess {
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

    struct UnitVariantAccess {
        variant: &'static str,
    }

    impl<'de> EnumAccess<'de> for UnitVariantAccess {
        type Error = ValueError;
        type Variant = Self;

        fn variant_seed<V: DeserializeSeed<'de>>(
            self,
            seed: V,
        ) -> Result<(V::Value, Self), ValueError> {
            let deserializer: StrDeserializer<'de, ValueError> = self.variant.into_deserializer();
            let value = seed.deserialize(deserializer)?;
            Ok((value, self))
        }
    }

    impl<'de> VariantAccess<'de> for UnitVariantAccess {
        type Error = ValueError;

        fn unit_variant(self) -> Result<(), ValueError> {
            Ok(())
        }

        fn newtype_variant_seed<T: DeserializeSeed<'de>>(
            self,
            _seed: T,
        ) -> Result<T::Value, ValueError> {
            Err(serde::de::Error::custom("expected a unit variant"))
        }

        fn tuple_variant<V: Visitor<'de>>(
            self,
            _len: usize,
            _visitor: V,
        ) -> Result<V::Value, ValueError> {
            Err(serde::de::Error::custom("expected a unit variant"))
        }

        fn struct_variant<V: Visitor<'de>>(
            self,
            _fields: &'static [&'static str],
            _visitor: V,
        ) -> Result<V::Value, ValueError> {
            Err(serde::de::Error::custom("expected a unit variant"))
        }
    }

    fn to_value<T: Serialize>(value: &T) -> Value {
        value
            .serialize(ValueSerializer)
            .expect("test serializer failed")
    }

    fn from_value<T: for<'de> Deserialize<'de>>(value: Value) -> T {
        T::deserialize(ValueDeserializer(value)).expect("test deserializer failed")
    }

    #[test]
    fn codes_are_stable_unique_and_round_trippable() {
        assert_eq!(ErrorCategory::ALL.len(), 20);
        let codes: Vec<u32> = ErrorCategory::ALL
            .iter()
            .map(|category| category.code())
            .collect();
        // Declaration order must stay exactly 1..=20: stable, ordered, unique.
        assert_eq!(codes, (1..=20).collect::<Vec<u32>>());
        for category in ErrorCategory::ALL {
            assert_eq!(ErrorCategory::from_code(category.code()), Some(category));
        }
        assert_eq!(ErrorCategory::from_code(0), None);
        assert_eq!(ErrorCategory::from_code(21), None);
    }

    #[test]
    fn serde_round_trip_preserves_every_category() {
        for category in ErrorCategory::ALL {
            let value = to_value(&category);
            // Externally tagged unit variants serialize as their bare variant
            // name; the names are the cross-language wire contract.
            match &value {
                Value::UnitVariant(name) => assert_eq!(*name, format!("{category:?}").as_str()),
                other => panic!("expected unit variant, got {other:?}"),
            }
            assert_eq!(from_value::<ErrorCategory>(value), category);
        }
    }

    #[test]
    fn category_error_serde_round_trip() {
        let original = CategoryError::new(
            ErrorCategory::CommitOutcomeUnknown,
            "commit epoch 42 lost contact after propose",
        );
        let value = to_value(&original);
        match &value {
            Value::Struct(fields) => {
                assert_eq!(fields.len(), 2);
                assert_eq!(fields[0].0, "category");
                assert_eq!(fields[1].0, "message");
            }
            other => panic!("expected struct, got {other:?}"),
        }
        assert_eq!(from_value::<CategoryError>(value), original);
    }

    #[test]
    fn display_is_human_readable() {
        assert_eq!(ErrorCategory::NotLeader.to_string(), "not leader");
        assert_eq!(
            ErrorCategory::CommitOutcomeUnknown.to_string(),
            "commit outcome unknown"
        );
        assert_eq!(
            ErrorCategory::PermissionDenied.to_string(),
            "permission denied"
        );
        let error = CategoryError::new(
            ErrorCategory::PermissionDenied,
            "principal \"alice\" lacks Admin",
        );
        assert_eq!(
            error.to_string(),
            "permission denied: principal \"alice\" lacks Admin"
        );
        assert_eq!(error.code(), ErrorCategory::PermissionDenied.code());
    }

    #[test]
    fn retry_classification_matches_spec_11_7() {
        // Routing failures resolve through refreshed metadata (leader hints).
        assert!(ErrorCategory::NotLeader.is_retryable());
        assert_eq!(
            ErrorCategory::NotLeader.retry_class(),
            RetryClass::AfterMetadataRefresh
        );
        assert!(ErrorCategory::StaleMetadata.is_retryable());
        assert_eq!(
            ErrorCategory::StaleMetadata.retry_class(),
            RetryClass::AfterMetadataRefresh
        );
        assert_eq!(
            ErrorCategory::TabletMoved.retry_class(),
            RetryClass::AfterMetadataRefresh
        );
        // Transient availability and resource pressure retry with backoff.
        assert!(ErrorCategory::ReplicaUnavailable.is_retryable());
        assert_eq!(
            ErrorCategory::QuorumUnavailable.retry_class(),
            RetryClass::AfterBackoff
        );
        assert_eq!(
            ErrorCategory::ResourceExhausted.retry_class(),
            RetryClass::AfterBackoff
        );
        // Transaction failures restart the whole transaction.
        assert!(ErrorCategory::TransactionConflict.is_retryable());
        assert_eq!(
            ErrorCategory::Deadlock.retry_class(),
            RetryClass::RetryTransaction
        );
        assert_eq!(
            ErrorCategory::SerializationFailure.retry_class(),
            RetryClass::RetryTransaction
        );
        // Section 11.7: never automatically replay an ambiguous write without
        // a durable idempotency key.
        assert!(!ErrorCategory::CommitOutcomeUnknown.is_retryable());
        assert_eq!(
            ErrorCategory::CommitOutcomeUnknown.retry_class(),
            RetryClass::IdempotencyKeyRequired
        );
        // Terminal categories are never retried.
        for category in [
            ErrorCategory::DeadlineExceeded,
            ErrorCategory::Cancelled,
            ErrorCategory::ClusterVersionMismatch,
            ErrorCategory::Unauthenticated,
            ErrorCategory::PermissionDenied,
        ] {
            assert!(
                !category.is_retryable(),
                "{category:?} must not be retryable"
            );
            assert_eq!(category.retry_class(), RetryClass::Never);
        }
    }
}
