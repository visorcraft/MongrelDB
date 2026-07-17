//! Canonical request model (spec section 10.4, S1D-001).
//!
//! Every protocol adapter (native RPC, HTTP/JSON, Kit, MySQL wire) converts
//! its wire form into the canonical [`ExecuteRequest`] defined here, so
//! admission, authorization, resource governance, and execution see exactly
//! one request shape regardless of how the request arrived.
//!
//! # Deadline representation
//!
//! The spec model carries `deadline: Option<Instant>`, but
//! [`std::time::Instant`] is opaque, monotonic, and not serde-able, so the
//! canonical model instead stores `deadline_unix_micros`: wall-clock
//! microseconds since the Unix epoch — the same time base as
//! [`mongreldb_types::hlc::HlcTimestamp::physical_micros`]. This form is
//! serde-stable in every format, comparable across processes, and converts
//! to a monotonic `Instant` once at server admission; queue wait, planning,
//! execution, serialization, and network backpressure all count toward it
//! (S1D-006). `None` means the server applies its configured default
//! deadline: a request is never unbounded (spec section 4.9).
//!
//! # Identity mapping
//!
//! [`AuthenticatedIdentity`] is the protocol-side canonical form of the
//! engine-side identity (`mongreldb_core::auth::Principal`, the handle
//! identity carried by every open database handle):
//!
//! - [`AuthenticatedIdentity::CatalogUser`] pins the immutable catalog
//!   identity: `username`, `user_id`, and `created_version` map to
//!   `Principal::username`, `Principal::user_id`, and
//!   `Principal::created_epoch`. Roles, permissions, and the admin flag are
//!   deliberately NOT part of the wire identity: the server re-resolves them
//!   from the catalog at session open, so username reuse cannot revive a
//!   stale principal.
//! - [`AuthenticatedIdentity::Credentialless`] corresponds to deployments
//!   with catalog auth disabled (embedded mode, or a daemon without
//!   `--auth-users`): there is no `Principal` at all.
//! - [`AuthenticatedIdentity::ServicePrincipal`] corresponds to internal
//!   server components (replication, CDC, maintenance jobs) that act without
//!   catalog credentials. External adapters MUST never mint one; the
//!   authorization boundary fails closed on unrecognized principals.

use core::fmt;
use core::str::FromStr;

use mongreldb_types::ids::{DatabaseId, QueryId};

use crate::prepared::StatementId;

/// Identifies one client session.
///
/// Session IDs are allocated by the server at session open (128 bits, drawn
/// from a CSPRNG by the allocating service; this crate is dependency-frozen
/// and deliberately does not mint them). The all-zero value is reserved.
///
/// Canonical text form: strict lowercase hexadecimal, 32 characters.
#[repr(transparent)]
#[derive(
    Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct SessionId(pub [u8; 16]);

impl SessionId {
    /// The all-zero session identifier (reserved).
    pub const ZERO: Self = Self([0u8; 16]);

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
        let mut out = String::with_capacity(32);
        for byte in self.0 {
            out.push(char::from_digit((byte >> 4) as u32, 16).expect("nibble"));
            out.push(char::from_digit((byte & 0x0f) as u32, 16).expect("nibble"));
        }
        out
    }
}

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SessionId({})", self.to_hex())
    }
}

/// Error returned when parsing a textual [`SessionId`] fails.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SessionIdParseError {
    /// The text had the wrong number of characters.
    #[error("invalid session id length: expected 32 hex digits, got {0} chars")]
    InvalidLength(usize),
    /// The text contained a non-hexadecimal character.
    #[error("invalid hex character `{0}` in session id")]
    InvalidCharacter(char),
}

impl FromStr for SessionId {
    type Err = SessionIdParseError;

    /// Parses the canonical 32-character hex form. Lenient by contract, like
    /// the `mongreldb-types` identifiers: hyphens are ignored at any position
    /// (so the hyphenated UUID form `8-4-4-4-12` parses) and uppercase hex
    /// digits are accepted.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let compact: String = text.chars().filter(|c| *c != '-').collect();
        if compact.chars().count() != 32 {
            return Err(SessionIdParseError::InvalidLength(compact.chars().count()));
        }
        let mut bytes = [0u8; 16];
        let mut chars = compact.chars();
        for byte in &mut bytes {
            let hi = chars.next().expect("length checked above");
            let lo = chars.next().expect("length checked above");
            let hi = hi
                .to_digit(16)
                .ok_or(SessionIdParseError::InvalidCharacter(hi))?;
            let lo = lo
                .to_digit(16)
                .ok_or(SessionIdParseError::InvalidCharacter(lo))?;
            *byte = ((hi << 4) | lo) as u8;
        }
        Ok(Self(bytes))
    }
}

/// The authenticated identity attached to a request (S1D-001).
///
/// See the module-level documentation for the mapping to the engine-side
/// identity (`mongreldb_core::auth::Principal`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AuthenticatedIdentity {
    /// No catalog credentials: embedded mode or a daemon running without
    /// catalog auth. Carries no identity; the storage authorization layer
    /// applies its no-auth policy.
    Credentialless,
    /// A catalog-authenticated user. Pins the immutable identity
    /// (`user_id` + `created_version`); grants are re-resolved by the server
    /// at session open so a recreated username cannot inherit stale rights.
    CatalogUser {
        /// Case-sensitive username.
        username: String,
        /// Immutable catalog user id (`Principal::user_id`).
        user_id: u64,
        /// User generation paired with `user_id` (`Principal::created_epoch`).
        created_version: u64,
    },
    /// An internal server component (replication, CDC, maintenance) acting
    /// without catalog credentials. Never accepted from external adapters.
    ServicePrincipal {
        /// Stable name of the internal component, e.g. `"replication"`.
        name: String,
    },
}

/// MVCC isolation level requested by [`ExecuteCommand::Begin`].
///
/// Mirrors `mongreldb_core::txn::IsolationLevel` one-to-one (`Snapshot`,
/// `ReadCommitted`, `Serializable`). The protocol crate cannot depend on the
/// core crate (the server wave depends on both), so the enum is duplicated
/// here and converted at the service boundary.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub enum IsolationLevel {
    /// Snapshot isolation: the transaction reads one consistent snapshot.
    #[default]
    Snapshot,
    /// Read committed: each statement reads the latest committed state.
    ReadCommitted,
    /// Serializable: snapshot reads plus certification at commit.
    Serializable,
}

/// One bound parameter value of a [`ExecuteCommand::Sql`] or
/// [`ExecuteCommand::ExecutePrepared`] request.
///
/// The canonical model carries typed scalar values; composite Arrow-native
/// parameters land with the server wave. [`ParameterValue::type_name`]
/// provides the canonical type names recorded in
/// [`crate::prepared::PreparedStatementBinding::parameter_types`].
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ParameterValue {
    /// SQL `NULL`.
    Null,
    /// A boolean.
    Bool(bool),
    /// A 64-bit signed integer.
    Integer(i64),
    /// A double-precision float.
    Float(f64),
    /// A UTF-8 string.
    Text(String),
    /// Opaque binary data.
    Bytes(Vec<u8>),
}

impl ParameterValue {
    /// The canonical type name of this value, as recorded in a prepared
    /// statement's `parameter_types`.
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Null => "NULL",
            Self::Bool(_) => "BOOL",
            Self::Integer(_) => "INT64",
            Self::Float(_) => "FLOAT64",
            Self::Text(_) => "TEXT",
            Self::Bytes(_) => "BYTES",
        }
    }
}

/// Administrative operations carried by [`ExecuteCommand::Admin`].
///
/// Intentionally lean: user/role management, the operations gated by the
/// catalog's admin permission today. Further admin verbs are appended in
/// later stages; discriminants are never reordered or reused (spec
/// section 4.10).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AdminCommand {
    /// Create a catalog user.
    CreateUser {
        /// Case-sensitive username of the new user.
        username: String,
    },
    /// Drop a catalog user.
    DropUser {
        /// Case-sensitive username to drop.
        username: String,
    },
    /// Grant a role to a user.
    GrantRole {
        /// Case-sensitive username receiving the role.
        username: String,
        /// Role to grant.
        role: String,
    },
    /// Revoke a role from a user.
    RevokeRole {
        /// Case-sensitive username losing the role.
        username: String,
        /// Role to revoke.
        role: String,
    },
}

/// The operation an [`ExecuteRequest`] asks the server to perform (S1D-001).
///
/// Variants are never reordered and discriminants never reused (spec
/// section 4.10); new commands are only appended.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ExecuteCommand {
    /// Parse, plan, and execute a SQL statement.
    Sql {
        /// The SQL text.
        text: String,
        /// Bound parameter values, in statement order.
        params: Vec<ParameterValue>,
    },
    /// Execute a previously prepared statement.
    ExecutePrepared {
        /// Session-scoped prepared statement handle from
        /// [`crate::services::QueryService::prepare`].
        statement_id: StatementId,
        /// Bound parameter values, in statement order.
        params: Vec<ParameterValue>,
    },
    /// Begin a transaction on the session.
    Begin {
        /// Requested MVCC isolation level.
        isolation: IsolationLevel,
    },
    /// Commit the session's active transaction.
    Commit,
    /// Roll back the session's active transaction.
    Rollback,
    /// Cancel a running query (S1D-006).
    Cancel {
        /// The query to cancel.
        query_id: QueryId,
    },
    /// Fetch the schema of one table.
    GetSchema {
        /// Table name.
        table: String,
    },
    /// An administrative operation.
    Admin(AdminCommand),
}

/// Conservative default bounds a server applies when a request does not
/// state one explicitly (spec section 4.9): 100 000 rows.
pub const DEFAULT_MAX_ROWS: u64 = 100_000;
/// Conservative default result-size bound: 64 MiB of Arrow IPC payload.
pub const DEFAULT_MAX_BYTES: u64 = 64 * 1024 * 1024;
/// Conservative default candidate-count bound: one million candidates.
pub const DEFAULT_MAX_CANDIDATE_COUNT: u64 = 1_000_000;

/// Per-request result bounds (spec sections 4.9 and 10.4, S1D-001).
///
/// Every request is bounded: `None` does NOT mean unbounded, it means the
/// server applies its configured default (see the `DEFAULT_*` constants for
/// the conservative shipped defaults) and may clamp explicit values down to
/// configured ceilings. The effective limits are what the executor
/// enforces; exceeding them fails the request with
/// [`mongreldb_types::errors::ErrorCategory::ResourceExhausted`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct ResultLimits {
    /// Maximum result rows; `None` applies the server default.
    pub max_rows: Option<u64>,
    /// Maximum result bytes; `None` applies the server default.
    pub max_bytes: Option<u64>,
    /// Maximum candidate rows scanned during retrieval; `None` applies the
    /// server default.
    pub max_candidate_count: Option<u64>,
}

impl ResultLimits {
    /// Effective row bound after applying server-enforceable defaults.
    pub fn effective_max_rows(&self) -> u64 {
        self.max_rows.unwrap_or(DEFAULT_MAX_ROWS)
    }

    /// Effective byte bound after applying server-enforceable defaults.
    pub fn effective_max_bytes(&self) -> u64 {
        self.max_bytes.unwrap_or(DEFAULT_MAX_BYTES)
    }

    /// Effective candidate-count bound after applying server-enforceable
    /// defaults.
    pub fn effective_max_candidate_count(&self) -> u64 {
        self.max_candidate_count
            .unwrap_or(DEFAULT_MAX_CANDIDATE_COUNT)
    }
}

/// The canonical request every protocol adapter converts into (S1D-001).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ExecuteRequest {
    /// Unique identifier of this request, chosen by the caller; never reused,
    /// so retries with the same id can be deduplicated.
    pub request_id: [u8; 16],
    /// Identifier of the query execution this request drives.
    pub query_id: QueryId,
    /// Session the request runs on, if sessionful.
    pub session_id: Option<SessionId>,
    /// Logical database the request targets.
    pub database_id: DatabaseId,
    /// Authenticated identity the request acts as.
    pub principal: AuthenticatedIdentity,
    /// The operation to perform.
    pub command: ExecuteCommand,
    /// Wall-clock deadline in microseconds since the Unix epoch; see the
    /// module-level documentation for why this is not an `Instant`.
    pub deadline_unix_micros: Option<u64>,
    /// Per-request result bounds; always effective, never unbounded.
    pub result_limits: ResultLimits,
    /// Optional resource group the request is admitted into (spec
    /// section 10.5, workload classes and resource governance).
    pub resource_group: Option<String>,
    /// Optional durable idempotency key: present iff the caller may safely
    /// replay the request after an ambiguous outcome (spec section 11.7).
    pub idempotency_key: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::assert_serde_round_trip;

    fn sample_principal() -> AuthenticatedIdentity {
        AuthenticatedIdentity::CatalogUser {
            username: "alice".to_owned(),
            user_id: 42,
            created_version: 7,
        }
    }

    fn sample_params() -> Vec<ParameterValue> {
        vec![
            ParameterValue::Null,
            ParameterValue::Bool(true),
            ParameterValue::Integer(-42),
            ParameterValue::Float(2.5),
            ParameterValue::Text("hello".to_owned()),
            ParameterValue::Bytes(vec![0xde, 0xad, 0xbe, 0xef]),
        ]
    }

    #[test]
    fn session_id_text_form_round_trips() {
        let bytes = [
            0x00, 0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76,
            0x54, 0x32,
        ];
        let id = SessionId::from_bytes(bytes);
        assert_eq!(id.to_hex(), "000123456789abcdeffedcba98765432");
        assert_eq!(id.to_string(), id.to_hex());
        assert_eq!(
            format!("{id:?}"),
            "SessionId(000123456789abcdeffedcba98765432)"
        );
        assert_eq!(id.to_hex().parse::<SessionId>().unwrap(), id);
        assert_eq!(
            "00012345-6789-abcd-effe-dcba98765432"
                .parse::<SessionId>()
                .unwrap(),
            id,
            "hyphenated UUID form parses"
        );
        assert_eq!(
            id.to_hex()
                .to_ascii_uppercase()
                .parse::<SessionId>()
                .unwrap(),
            id,
            "uppercase hex parses"
        );
        assert_eq!(id.as_bytes(), &bytes);
        assert_eq!(SessionId::ZERO.as_bytes(), &[0u8; 16]);
    }

    #[test]
    fn session_id_parse_rejects_bad_input() {
        assert_eq!(
            "".parse::<SessionId>(),
            Err(SessionIdParseError::InvalidLength(0))
        );
        assert_eq!(
            "abcd".parse::<SessionId>(),
            Err(SessionIdParseError::InvalidLength(4))
        );
        assert_eq!(
            "g".repeat(32).parse::<SessionId>(),
            Err(SessionIdParseError::InvalidCharacter('g'))
        );
    }

    #[test]
    fn session_id_serde_round_trip() {
        assert_serde_round_trip(&SessionId::from_bytes([0x5a; 16]));
        assert_serde_round_trip(&SessionId::ZERO);
    }

    #[test]
    fn authenticated_identity_serde_round_trip() {
        assert_serde_round_trip(&AuthenticatedIdentity::Credentialless);
        assert_serde_round_trip(&sample_principal());
        assert_serde_round_trip(&AuthenticatedIdentity::ServicePrincipal {
            name: "replication".to_owned(),
        });
    }

    #[test]
    fn isolation_level_serde_round_trip_and_default() {
        assert_eq!(IsolationLevel::default(), IsolationLevel::Snapshot);
        for level in [
            IsolationLevel::Snapshot,
            IsolationLevel::ReadCommitted,
            IsolationLevel::Serializable,
        ] {
            assert_serde_round_trip(&level);
        }
    }

    #[test]
    fn parameter_value_serde_round_trip_and_type_names() {
        for value in sample_params() {
            assert_serde_round_trip(&value);
        }
        let names: Vec<&'static str> = sample_params()
            .iter()
            .map(ParameterValue::type_name)
            .collect();
        assert_eq!(names, ["NULL", "BOOL", "INT64", "FLOAT64", "TEXT", "BYTES"]);
    }

    #[test]
    fn admin_command_serde_round_trip() {
        for command in [
            AdminCommand::CreateUser {
                username: "alice".to_owned(),
            },
            AdminCommand::DropUser {
                username: "alice".to_owned(),
            },
            AdminCommand::GrantRole {
                username: "alice".to_owned(),
                role: "analyst".to_owned(),
            },
            AdminCommand::RevokeRole {
                username: "alice".to_owned(),
                role: "analyst".to_owned(),
            },
        ] {
            assert_serde_round_trip(&command);
        }
    }

    #[test]
    fn execute_command_serde_round_trip_every_variant() {
        for command in [
            ExecuteCommand::Sql {
                text: "SELECT * FROM t WHERE a = ?".to_owned(),
                params: sample_params(),
            },
            ExecuteCommand::ExecutePrepared {
                statement_id: StatementId::new(9),
                params: sample_params(),
            },
            ExecuteCommand::Begin {
                isolation: IsolationLevel::Serializable,
            },
            ExecuteCommand::Commit,
            ExecuteCommand::Rollback,
            ExecuteCommand::Cancel {
                query_id: QueryId::new_random(),
            },
            ExecuteCommand::GetSchema {
                table: "events".to_owned(),
            },
            ExecuteCommand::Admin(AdminCommand::GrantRole {
                username: "alice".to_owned(),
                role: "analyst".to_owned(),
            }),
        ] {
            assert_serde_round_trip(&command);
        }
    }

    #[test]
    fn result_limits_defaults_are_bounded() {
        let limits = ResultLimits::default();
        assert_eq!(limits.max_rows, None);
        assert_eq!(limits.effective_max_rows(), DEFAULT_MAX_ROWS);
        assert_eq!(limits.effective_max_bytes(), DEFAULT_MAX_BYTES);
        assert_eq!(
            limits.effective_max_candidate_count(),
            DEFAULT_MAX_CANDIDATE_COUNT
        );
        let explicit = ResultLimits {
            max_rows: Some(10),
            max_bytes: Some(1024),
            max_candidate_count: Some(50),
        };
        assert_eq!(explicit.effective_max_rows(), 10);
        assert_eq!(explicit.effective_max_bytes(), 1024);
        assert_eq!(explicit.effective_max_candidate_count(), 50);
        assert_serde_round_trip(&limits);
        assert_serde_round_trip(&explicit);
    }

    #[test]
    fn execute_request_serde_round_trip() {
        let full = ExecuteRequest {
            request_id: [0x11; 16],
            query_id: QueryId::new_random(),
            session_id: Some(SessionId::from_bytes([0x22; 16])),
            database_id: DatabaseId::new_random(),
            principal: sample_principal(),
            command: ExecuteCommand::Sql {
                text: "SELECT 1".to_owned(),
                params: sample_params(),
            },
            deadline_unix_micros: Some(1_758_000_000_000_000),
            result_limits: ResultLimits {
                max_rows: Some(500),
                max_bytes: None,
                max_candidate_count: Some(10_000),
            },
            resource_group: Some("interactive".to_owned()),
            idempotency_key: Some("req-42".to_owned()),
        };
        assert_serde_round_trip(&full);
        let minimal = ExecuteRequest {
            session_id: None,
            deadline_unix_micros: None,
            result_limits: ResultLimits::default(),
            resource_group: None,
            idempotency_key: None,
            principal: AuthenticatedIdentity::Credentialless,
            command: ExecuteCommand::Commit,
            ..full.clone()
        };
        assert_serde_round_trip(&minimal);
    }
}
