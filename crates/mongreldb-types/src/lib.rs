//! MongrelDB shared durable and network types.
//!
//! This crate sits at the bottom of the dependency graph (spec section 6.10):
//! every other crate may depend on it, and it depends on nothing first-party.
//! It owns the common identifiers (section 7), the MVCC timestamp model
//! (section 8), and the stable cross-language error taxonomy (section 9.7).

pub mod errors;
pub mod hlc;
pub mod ids;
