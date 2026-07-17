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
pub mod prepared;
pub mod request;
pub mod services;
pub mod session;

#[cfg(test)]
pub(crate) mod test_support;
