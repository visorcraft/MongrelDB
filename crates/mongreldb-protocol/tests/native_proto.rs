use mongreldb_protocol::native::{ApiVersion, HealthRequest, RequestContext};
use mongreldb_protocol::{validate_native_context, NATIVE_API_MAJOR};
use prost::Message;

fn context(major: u32) -> RequestContext {
    RequestContext {
        version: Some(ApiVersion { major, minor: 0 }),
        request_id: "request-1".into(),
        deadline_unix_micros: 0,
        idempotency_key: String::new(),
    }
}

#[test]
fn native_version_is_mandatory_and_unknown_major_fails_closed() {
    assert!(validate_native_context(None).is_err());
    assert!(validate_native_context(Some(&context(NATIVE_API_MAJOR))).is_ok());
    let error = validate_native_context(Some(&context(99))).unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
}

#[test]
fn protobuf_unknown_field_is_tolerated() {
    let request = HealthRequest {
        context: Some(context(NATIVE_API_MAJOR)),
    };
    let mut bytes = request.encode_to_vec();
    // Unknown field 99, varint wire type, value 7.
    bytes.extend_from_slice(&[0x98, 0x06, 0x07]);
    let decoded = HealthRequest::decode(bytes.as_slice()).unwrap();
    assert_eq!(
        decoded.context.unwrap().version.unwrap().major,
        NATIVE_API_MAJOR
    );
}

#[test]
fn malformed_protobuf_frame_is_rejected() {
    // Length-delimited field claims five bytes but carries one.
    assert!(HealthRequest::decode([0x0a, 0x05, 0x01].as_slice()).is_err());
}
