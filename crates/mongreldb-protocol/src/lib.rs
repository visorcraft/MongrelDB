//! MongrelDB versioned protocol messages and service definitions (spec
//! section 6.7, Stage 1D).
//!
//! Every protocol adapter (native RPC, HTTP/JSON, Kit, MySQL wire) converts
//! into the canonical request model defined here. All network formats carry a
//! versioned envelope and fail closed on incompatible versions (spec 4.10).

pub mod request;
pub mod services;
