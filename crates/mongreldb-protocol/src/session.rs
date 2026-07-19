//! Session model (spec section 10.4, S1D-004).
//!
//! A [`Session`] is the server-side state of one client session: who is
//! connected, which database they are on, what transaction is active, which
//! statements are prepared, the session settings, the read-your-writes
//! token, and when the session was last active. Sessions are lightweight and
//! do not own storage (S1D-004): the type here is data only — storage
//! handles, execution state, and admission slots are owned by the server and
//! referenced by id. Idle-time bounds (S1D-007) are enforced against
//! [`Session::last_activity_unix_micros`].

use std::collections::BTreeMap;

use mongreldb_types::hlc::HlcTimestamp;
use mongreldb_types::ids::{DatabaseId, TransactionId};

use crate::prepared::{PreparedStatementBinding, StatementId};
use crate::request::{AuthenticatedIdentity, IsolationLevel, SessionId};

/// The transaction state of a session (S1D-004).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TransactionState {
    /// No transaction is active; statements execute in autocommit mode.
    Idle,
    /// A transaction is active on this session.
    Active {
        /// The active transaction.
        transaction_id: TransactionId,
        /// Isolation level the transaction was begun with.
        isolation: IsolationLevel,
    },
}

impl TransactionState {
    /// Whether a transaction is currently active.
    pub fn is_active(&self) -> bool {
        matches!(self, Self::Active { .. })
    }
}

/// The server-side state of one client session (S1D-004).
///
/// Data only: a session owns no storage, no executor state, and no admission
/// slots — it is the lightweight record those subsystems key off.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Session {
    /// Server-allocated identifier of this session.
    pub session_id: SessionId,
    /// Authenticated identity the session acts as; fixed at session open.
    pub principal: AuthenticatedIdentity,
    /// Database the session's statements resolve against.
    pub current_database: DatabaseId,
    /// Active transaction, if any.
    pub transaction_state: TransactionState,
    /// Prepared statements live on this session, by handle.
    pub prepared_statements: BTreeMap<StatementId, PreparedStatementBinding>,
    /// Session settings (e.g. `timezone`, `statement_timeout`); keys and
    /// values are adapter-defined, unknown keys are ignored by the server.
    pub settings: BTreeMap<String, String>,
    /// Read-your-writes token: the highest commit timestamp this session has
    /// durably observed; subsequent reads wait for visibility up to it.
    pub read_your_writes_token: Option<HlcTimestamp>,
    /// Last activity, wall-clock microseconds since the Unix epoch (same
    /// time base as [`crate::request::ExecuteRequest::deadline_unix_micros`]);
    /// idle reaping (S1D-007) keys off this.
    pub last_activity_unix_micros: u64,
}

impl Session {
    /// Opens a fresh session: no active transaction, no prepared statements,
    /// default settings, no read-your-writes token yet.
    pub fn new(
        session_id: SessionId,
        principal: AuthenticatedIdentity,
        current_database: DatabaseId,
        now_unix_micros: u64,
    ) -> Self {
        Self {
            session_id,
            principal,
            current_database,
            transaction_state: TransactionState::Idle,
            prepared_statements: BTreeMap::new(),
            settings: BTreeMap::new(),
            read_your_writes_token: None,
            last_activity_unix_micros: now_unix_micros,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::assert_serde_round_trip;

    fn sample_session() -> Session {
        let mut session = Session::new(
            SessionId::from_bytes([0x33; 16]),
            AuthenticatedIdentity::CatalogUser {
                username: "alice".to_owned(),
                user_id: 42,
                created_version: 7,
            },
            DatabaseId::new_random(),
            1_758_000_000_000_000,
        );
        session.transaction_state = TransactionState::Active {
            transaction_id: TransactionId::new_random(),
            isolation: IsolationLevel::Snapshot,
        };
        session.prepared_statements.insert(
            StatementId::new(1),
            PreparedStatementBinding {
                statement_id: StatementId::new(1),
                sql: "SELECT 1".to_owned(),
                parameter_types: vec![],
                catalog_version: mongreldb_types::ids::MetadataVersion::new(100),
                schema_versions: BTreeMap::new(),
                feature_set: std::collections::BTreeSet::new(),
            },
        );
        session
            .settings
            .insert("timezone".to_owned(), "UTC".to_owned());
        session.read_your_writes_token = Some(HlcTimestamp {
            physical_micros: 1_758_000_000_000_001,
            logical: 3,
            node_tiebreaker: 1,
        });
        session
    }

    #[test]
    fn new_session_starts_idle_and_empty() {
        let session = Session::new(
            SessionId::ZERO,
            AuthenticatedIdentity::Credentialless,
            DatabaseId::new_random(),
            1,
        );
        assert_eq!(session.transaction_state, TransactionState::Idle);
        assert!(!session.transaction_state.is_active());
        assert!(session.prepared_statements.is_empty());
        assert!(session.settings.is_empty());
        assert_eq!(session.read_your_writes_token, None);
        assert_eq!(session.last_activity_unix_micros, 1);
    }

    #[test]
    fn transaction_state_serde_round_trip() {
        assert_serde_round_trip(&TransactionState::Idle);
        assert_serde_round_trip(&TransactionState::Active {
            transaction_id: TransactionId::new_random(),
            isolation: IsolationLevel::Serializable,
        });
    }

    #[test]
    fn session_serde_round_trip() {
        assert_serde_round_trip(&sample_session());
        assert_serde_round_trip(&Session::new(
            SessionId::from_bytes([0x44; 16]),
            AuthenticatedIdentity::ServicePrincipal {
                name: "cdc".to_owned(),
            },
            DatabaseId::new_random(),
            42,
        ));
        assert_serde_round_trip(&Session::new(
            SessionId::from_bytes([0x45; 16]),
            AuthenticatedIdentity::ExternalPrincipal {
                provider: "oidc".to_owned(),
                subject: "alice".to_owned(),
                username: "alice".to_owned(),
                user_id: 7,
                created_version: 8,
                scopes: vec!["query".to_owned()],
            },
            DatabaseId::new_random(),
            43,
        ));
    }
}
