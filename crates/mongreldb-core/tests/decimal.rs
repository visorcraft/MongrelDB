//! Decimal128 type: encode/decode round-trip.

use mongreldb_core::columnar::{decode_column, encode_column};
use mongreldb_core::memtable::Value;
use mongreldb_core::schema::TypeId;

#[test]
fn decimal_encode_decode_roundtrip() {
    let ty = TypeId::Decimal128 {
        precision: 38,
        scale: 2,
    };
    let values = vec![
        Value::Decimal(12345),  // 123.45
        Value::Decimal(-67890), // -678.90
        Value::Decimal(0),      // 0.00
        Value::Null,
        Value::Decimal(i128::MAX),
    ];
    let page = encode_column(ty.clone(), &values).unwrap();
    let decoded = decode_column(ty, &page, values.len(), false).unwrap();
    assert_eq!(decoded, values);
}

#[test]
fn decimal_encode_key_is_comparable() {
    // Big-endian encoding means byte order matches value order.
    let a = Value::Decimal(100).encode_key();
    let b = Value::Decimal(200).encode_key();
    assert!(a < b, "decimal keys should be byte-comparable");
}
