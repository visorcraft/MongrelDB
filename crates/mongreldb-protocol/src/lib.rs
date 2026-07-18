//! MongrelDB versioned protocol messages and service definitions (spec
//! section 6.7, Stage 1D).
//!
//! Every protocol adapter (native RPC, HTTP/JSON, Kit, MySQL wire) converts
//! into the canonical request model defined here. All network formats carry a
//! versioned envelope and fail closed on incompatible versions (spec 4.10).
//!
//! - [`request`]: the canonical request model every adapter converts into
//!   (S1D-001).
//! - [`envelope`]: the versioned, checksummed wire envelope with fail-closed
//!   decode (spec section 4.10).
//! - [`services`]: the seven service traits adapters dispatch against
//!   (S1D-003).
//! - [`session`]: the server-side session model (S1D-004).
//! - [`prepared`]: the prepared-statement binding record and its
//!   invalidation check (S1D-005).

pub mod envelope;
pub mod native_transport;
pub mod prepared;
pub mod request;
pub mod services;
pub mod session;

/// Generated Protobuf messages and tonic client/server stubs for the native
/// HTTP/2 transport.
pub mod native {
    tonic::include_proto!("mongreldb.v1");
}

pub const NATIVE_API_MAJOR: u32 = 1;
pub const NATIVE_API_MINOR: u32 = 0;

/// Fail closed when a native request omits its version or uses another major
/// protocol generation. Newer minor versions remain forward-compatible
/// through Protobuf unknown-field handling.
pub fn validate_native_context(
    context: Option<&native::RequestContext>,
) -> Result<(), tonic::Status> {
    let version = context
        .and_then(|context| context.version.as_ref())
        .ok_or_else(|| tonic::Status::invalid_argument("native API version is required"))?;
    if version.major != NATIVE_API_MAJOR {
        return Err(tonic::Status::failed_precondition(format!(
            "unsupported native API major {}; supported major is {}",
            version.major, NATIVE_API_MAJOR
        )));
    }
    Ok(())
}

#[cfg(test)]
pub(crate) mod test_support;
