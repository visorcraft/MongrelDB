//! `DatabaseHandle`: a lightweight caller-specific object referencing one
//! `DatabaseCore` (spec §10.1, S1A-001).
//!
//! Handles are issued by [`crate::manager::DatabaseManager::open_shared`].
//! Every handle carries its own [`HandleIdentity`] and [`HandleAccess`], while
//! recovery, WAL opening, open-generation advancement, and table mounting all
//! happened exactly once on the shared core. Dropping one handle never closes
//! storage; the core closes when the last reference drops or when
//! [`DatabaseHandle::shutdown`] drains it (S1A-004).
//!
//! Handles expose only principal-aware operations. They never dereference to
//! the raw [`Database`] facade or return mutable table handles.

use std::sync::Arc;
use std::time::Duration;

use crate::core::{LifecycleState, OperationGuard};
use crate::database::Database;
use crate::error::{MongrelError, Result};
use crate::service_principal::ServicePrincipalDefinition;

/// A password that zeroizes its allocation and never reveals itself through
/// `Debug` output.
pub struct SecretString(zeroize::Zeroizing<String>);

impl SecretString {
    pub fn new(secret: impl Into<String>) -> Self {
        Self(zeroize::Zeroizing::new(secret.into()))
    }

    pub(crate) fn expose(&self) -> &str {
        self.0.as_str()
    }
}

impl std::fmt::Debug for SecretString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretString([REDACTED])")
    }
}

/// The per-caller identity bound to one handle (spec §10.1, S1A-001).
///
/// Catalog identities pin one user generation and are re-resolved against the
/// live catalog on each authorized operation. Service identities pin a
/// registered token generation (`token_id` + `principal_id` +
/// `creation_version`) and re-resolve live scopes from the shared core's
/// service-principal store on each authorize (P0.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HandleIdentity {
    /// No credentials were supplied for this handle.
    Credentialless,
    /// A user resolved from the database catalog. `user_id` +
    /// `created_version` pin the exact catalog generation so username reuse
    /// cannot revive a stale identity.
    CatalogUser {
        username: String,
        user_id: u64,
        created_version: u64,
    },
    /// An authenticated service principal. Authority is re-resolved live from
    /// the shared core; the handle never freezes a caller-supplied permission
    /// vector.
    ServicePrincipal {
        token_id: String,
        principal_id: [u8; 16],
        creation_version: u64,
    },
}

/// The identity requested when attaching a shared handle (spec §2.2).
///
/// Catalog credentials are verified against the live shared catalog before a
/// handle is issued. Service credentials are verified against the core's
/// service-principal store (P0.1). The secret allocation is zeroized when
/// attach returns.
///
/// Callers cannot supply a permission vector on open — that was the P0.1
/// authority gap (`ScopedServicePrincipal`). Service authority is assigned
/// only by admin registration on the shared core.
#[derive(Debug)]
pub enum OpenIdentity {
    /// Attach without credentials. Rejected (fail closed) when the database
    /// catalog has `require_auth` enabled.
    Credentialless,
    /// Authenticate one registered service principal by token id + secret.
    ServiceCredentials {
        token_id: String,
        secret: SecretString,
    },
    /// Authenticate one catalog user on the existing shared core.
    CatalogCredentials {
        username: String,
        password: SecretString,
    },
}

/// Crate-private service capability for trusted internal actors (P0.1-T6).
///
/// Not part of [`OpenIdentity`], not FFI/Kit/serializable from untrusted
/// input, and not constructible outside `mongreldb-core`. Use this only for
/// in-process engine paths that must act as a service principal without
/// going through public credentials.
#[derive(Debug, Clone)]
pub(crate) struct InternalServiceCapability {
    pub principal_id: [u8; 16],
    pub permissions: Vec<crate::auth::Permission>,
}

/// Per-handle access restriction (spec §2.2: "each handle may have its own
/// principal and read-only restriction").
///
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct HandleAccess {
    read_only: bool,
}

impl HandleAccess {
    /// A read-write handle (the Stage 1A default).
    pub fn read_write() -> Self {
        Self { read_only: false }
    }

    /// A read-only handle.
    pub fn read_only() -> Self {
        Self { read_only: true }
    }

    /// Whether this handle is restricted to reads.
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }
}

/// A lightweight caller-specific handle onto one shared [`DatabaseCore`]
/// (spec §10.1, S1A-001).
///
/// Obtained from [`crate::manager::DatabaseManager::open_shared`]. Cloning or
/// dropping a handle has no storage side effects; storage closes when the
/// last core reference drops or [`Self::shutdown`] runs.
pub struct DatabaseHandle {
    /// The facade carrying this handle's live authorization context.
    database: Database,
    identity: HandleIdentity,
    access: HandleAccess,
    /// Frozen scopes for crate-private [`InternalServiceCapability`] only.
    /// Authenticated service principals re-resolve live and leave this `None`.
    internal_capability: Option<InternalServiceCapability>,
}

impl DatabaseHandle {
    pub(crate) fn new(
        database: Database,
        identity: HandleIdentity,
        access: HandleAccess,
        internal_capability: Option<InternalServiceCapability>,
    ) -> Self {
        Self {
            database,
            identity,
            access,
            internal_capability,
        }
    }

    /// Issue a handle bound to a crate-private internal service capability.
    /// Not reachable from public attach paths.
    #[allow(dead_code)] // used by trusted in-crate callers / future wiring
    pub(crate) fn with_internal_capability(
        database: Database,
        capability: InternalServiceCapability,
        access: HandleAccess,
    ) -> Self {
        let identity = HandleIdentity::ServicePrincipal {
            token_id: format!("internal:{:02x?}", capability.principal_id),
            principal_id: capability.principal_id,
            creation_version: 0,
        };
        Self::new(database, identity, access, Some(capability))
    }

    /// The identity bound to this handle.
    pub fn identity(&self) -> &HandleIdentity {
        &self.identity
    }

    /// The access restriction bound to this handle.
    pub fn access(&self) -> HandleAccess {
        self.access
    }

    /// Whether two handles reference the exact same process-local core.
    pub fn shares_core_with(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.database.core(), &other.database.core())
    }

    /// The current lifecycle state of the shared core (S1A-004).
    pub fn lifecycle_state(&self) -> LifecycleState {
        self.database.lifecycle_state()
    }

    /// Admit one operation against the shared core (S1A-004). The RAII guard
    /// releases the operation slot on drop; new operations are rejected once
    /// the core leaves [`LifecycleState::Open`].
    pub fn operation_guard(&self) -> Result<OperationGuard> {
        self.database.operation_guard()
    }

    fn authorize_write(&self, operation: &'static str) -> Result<()> {
        if self.access.is_read_only() {
            Err(MongrelError::ReadOnlyHandle { operation })
        } else {
            Ok(())
        }
    }

    /// Re-resolve authenticated service authority from the shared core and
    /// rebind the facade principal so subsequent require/txn checks see live
    /// scopes (P0.1-T4/T5).
    fn resolve_service_authority(&self) -> Result<()> {
        if self.internal_capability.is_some() {
            return Ok(());
        }
        let HandleIdentity::ServicePrincipal {
            token_id,
            principal_id,
            creation_version,
        } = &self.identity
        else {
            return Ok(());
        };
        let def = self.database.core().service_principals.resolve_live(
            token_id,
            *principal_id,
            *creation_version,
        )?;
        self.database.rebind_service_principal(&def);
        Ok(())
    }

    fn authorize_permission(&self, permission: &crate::auth::Permission) -> Result<()> {
        if let Some(capability) = &self.internal_capability {
            if !capability
                .permissions
                .iter()
                .any(|granted| granted.satisfies(permission))
            {
                return Err(MongrelError::PermissionDenied {
                    required: permission.clone(),
                    principal: format!("service:{:02x?}", capability.principal_id),
                });
            }
            return Ok(());
        }
        if let HandleIdentity::ServicePrincipal {
            token_id,
            principal_id,
            creation_version,
        } = &self.identity
        {
            let def = self.database.core().service_principals.resolve_live(
                token_id,
                *principal_id,
                *creation_version,
            )?;
            self.database.rebind_service_principal(&def);
            if !def
                .permissions
                .iter()
                .any(|granted| granted.satisfies(permission))
            {
                return Err(MongrelError::PermissionDenied {
                    required: permission.clone(),
                    principal: format!("service:{token_id}"),
                });
            }
            return Ok(());
        }
        self.database.require(permission)
    }

    /// Execute an authorized native query with live roles, RLS, masks, and
    /// column grants.
    pub fn query(
        &self,
        table: &str,
        query: &crate::query::Query,
        projection: Option<&[u16]>,
    ) -> Result<Vec<crate::memtable::Row>> {
        self.resolve_service_authority()?;
        self.database
            .query_for_current_principal(table, query, projection)
    }

    /// Count rows visible to this handle after live authorization and RLS.
    pub fn count(&self, table: &str) -> Result<u64> {
        self.resolve_service_authority()?;
        self.database
            .count_for(table, self.database.principal().as_ref())
    }

    /// Return all rows visible to this handle after RLS and masks.
    pub fn rows(&self, table: &str) -> Result<Vec<crate::memtable::Row>> {
        self.resolve_service_authority()?;
        self.database
            .rows_for(table, self.database.principal().as_ref())
    }

    /// Create a table through this handle's live principal.
    pub fn create_table(&self, name: &str, schema: crate::schema::Schema) -> Result<u64> {
        self.authorize_write("create table")?;
        self.authorize_permission(&crate::auth::Permission::Ddl)?;
        self.database.create_table(name, schema)
    }

    /// Atomically insert one row through this handle's live principal.
    pub fn put(
        &self,
        table: &str,
        cells: Vec<(u16, crate::memtable::Value)>,
    ) -> Result<Option<i64>> {
        self.authorize_write("put")?;
        self.resolve_service_authority()?;
        self.database
            .transaction_for_current_principal(|transaction| transaction.put(table, cells))
    }

    /// Atomically insert many rows through this handle's live principal (P1.4).
    pub fn put_batch(
        &self,
        table: &str,
        rows: Vec<Vec<(u16, crate::memtable::Value)>>,
    ) -> Result<Vec<Option<i64>>> {
        self.authorize_write("put_batch")?;
        self.resolve_service_authority()?;
        self.database
            .transaction_for_current_principal(|transaction| transaction.put_batch(table, rows))
    }

    /// Atomically update one row through this handle's live principal (P1.4).
    pub fn update(
        &self,
        table: &str,
        row_id: crate::RowId,
        cells: Vec<(u16, crate::memtable::Value)>,
    ) -> Result<crate::txn::OwnedRow> {
        self.authorize_write("update")?;
        self.resolve_service_authority()?;
        self.database
            .transaction_for_current_principal(|transaction| {
                let mut images = transaction.update_many(table, vec![(row_id, cells)])?;
                images.pop().ok_or_else(|| {
                    MongrelError::NotFound(format!("row {row_id:?} not found for update"))
                })
            })
    }

    /// Open an authorized session bound to this handle's principal (P1.4-T1).
    ///
    /// Full SQL `MongrelSession` lives in the query crate (avoids core→query
    /// dependency). This session is the authorized CRUD/transaction wrapper
    /// without raw core escape.
    pub fn session(&self) -> Result<AuthorizedMongrelSession<'_>> {
        self.resolve_service_authority()?;
        Ok(AuthorizedMongrelSession { handle: self })
    }

    /// Begin an authorized multi-statement transaction (P1.4-T2).
    ///
    /// Writes through the returned [`AuthorizedTransaction`] re-check the
    /// handle's read-only restriction. Commit/rollback are explicit.
    pub fn begin(&self) -> Result<AuthorizedTransaction<'_>> {
        self.authorize_write("begin")?;
        self.resolve_service_authority()?;
        let principal = self.database.principal();
        let txn = self.database.begin_as(principal);
        Ok(AuthorizedTransaction {
            handle: self,
            txn: Some(txn),
        })
    }

    /// Atomically delete one row through this handle's live principal.
    pub fn delete(&self, table: &str, row_id: crate::RowId) -> Result<()> {
        self.authorize_write("delete")?;
        self.resolve_service_authority()?;
        self.database
            .transaction_for_current_principal(|transaction| transaction.delete(table, row_id))
    }

    /// Create a secondary index through this handle's live principal (P1.4-T3).
    ///
    /// Requires DDL permission (and fails on a read-only handle). Uses the
    /// same product path as SQL `CREATE INDEX` (`Database::create_index`).
    pub fn create_index(&self, table: &str, definition: crate::schema::IndexDef) -> Result<u64> {
        self.authorize_write("create index")?;
        self.authorize_permission(&crate::auth::Permission::Ddl)?;
        self.database.create_index(table, definition)
    }

    /// Drop a secondary index by name through this handle's live principal (P1.4-T3).
    pub fn drop_index(&self, table: &str, name: &str) -> Result<()> {
        self.authorize_write("drop index")?;
        self.authorize_permission(&crate::auth::Permission::Ddl)?;
        self.database.drop_index(table, name)
    }

    /// Create a stored procedure through this handle's live principal (P1.4-X6).
    pub fn create_procedure(
        &self,
        procedure: crate::procedure::StoredProcedure,
    ) -> Result<crate::procedure::StoredProcedure> {
        self.authorize_write("create procedure")?;
        self.authorize_permission(&crate::auth::Permission::Ddl)?;
        self.resolve_service_authority()?;
        self.database.create_procedure(procedure)
    }

    /// Drop a stored procedure by name (P1.4-X6).
    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        self.authorize_write("drop procedure")?;
        self.authorize_permission(&crate::auth::Permission::Ddl)?;
        self.resolve_service_authority()?;
        self.database.drop_procedure(name)
    }

    /// Call a stored procedure through this handle's live principal (P1.4-X6).
    pub fn call_procedure(
        &self,
        name: &str,
        args: std::collections::HashMap<String, crate::memtable::Value>,
    ) -> Result<crate::procedure::ProcedureCallResult> {
        // Mode-dependent write authorization is enforced inside Database when
        // the procedure body mutates; the handle still re-resolves authority.
        self.resolve_service_authority()?;
        self.database.call_procedure(name, args)
    }

    /// Create a trigger through this handle's live principal (P1.4-X6).
    pub fn create_trigger(
        &self,
        trigger: crate::trigger::StoredTrigger,
    ) -> Result<crate::trigger::StoredTrigger> {
        self.authorize_write("create trigger")?;
        self.authorize_permission(&crate::auth::Permission::Ddl)?;
        self.resolve_service_authority()?;
        self.database.create_trigger(trigger)
    }

    /// Drop a trigger by name (P1.4-X6).
    pub fn drop_trigger(&self, name: &str) -> Result<()> {
        self.authorize_write("drop trigger")?;
        self.authorize_permission(&crate::auth::Permission::Ddl)?;
        self.resolve_service_authority()?;
        self.database.drop_trigger(name)
    }

    /// Historical rows visible to this principal at `snapshot` (P1.4-X7).
    pub fn rows_at_epoch(
        &self,
        table: &str,
        snapshot: crate::epoch::Snapshot,
    ) -> Result<Vec<crate::memtable::Row>> {
        self.resolve_service_authority()?;
        self.database
            .rows_at_epoch_for_current_principal(table, snapshot)
    }

    /// Create a catalog user. Admin only.
    pub fn create_user(&self, username: &str, password: &str) -> Result<crate::auth::UserEntry> {
        self.authorize_write("create user")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.create_user(username, password)
    }

    /// Drop a catalog user. Admin only.
    pub fn drop_user(&self, username: &str) -> Result<()> {
        self.authorize_write("drop user")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.drop_user(username)
    }

    /// Create a role. Admin only.
    pub fn create_role(&self, role: &str) -> Result<crate::auth::RoleEntry> {
        self.authorize_write("create role")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.create_role(role)
    }

    /// Grant a role to a catalog user. Admin only.
    pub fn grant_role(&self, username: &str, role: &str) -> Result<()> {
        self.authorize_write("grant role")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.grant_role(username, role)
    }

    /// Revoke a role from a catalog user. Admin only.
    pub fn revoke_role(&self, username: &str, role: &str) -> Result<()> {
        self.authorize_write("revoke role")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.revoke_role(username, role)
    }

    /// Grant one permission to a role. Admin only.
    pub fn grant_permission(&self, role: &str, permission: crate::auth::Permission) -> Result<()> {
        self.authorize_write("grant permission")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.grant_permission(role, permission)
    }

    /// Revoke one permission from a role. Admin only.
    pub fn revoke_permission(&self, role: &str, permission: crate::auth::Permission) -> Result<()> {
        self.authorize_write("revoke permission")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.revoke_permission(role, permission)
    }

    /// Register an authenticated service principal on the shared core (P0.1).
    /// Admin only. The returned definition's secret is never re-exported;
    /// callers must retain the raw secret they supplied.
    pub fn register_service_principal(
        &self,
        token_id: impl Into<String>,
        principal_id: [u8; 16],
        permissions: Vec<crate::auth::Permission>,
        raw_secret: &str,
        expires_unix: u64,
    ) -> Result<ServicePrincipalDefinition> {
        self.authorize_write("register service principal")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.core().service_principals.register(
            token_id,
            principal_id,
            permissions,
            raw_secret,
            expires_unix,
        )
    }

    /// Revoke a registered service principal. Admin only. Existing handles
    /// bound to the token fail on the next authorized operation.
    pub fn revoke_service_principal(&self, token_id: &str) -> Result<()> {
        self.authorize_write("revoke service principal")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.core().service_principals.revoke(token_id)
    }

    /// Replace the live permission set for a registered service principal.
    /// Admin only. Scope reduction takes effect on the next authorize without
    /// reopening handles.
    pub fn set_service_principal_permissions(
        &self,
        token_id: &str,
        permissions: Vec<crate::auth::Permission>,
    ) -> Result<()> {
        self.authorize_write("set service principal permissions")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database
            .core()
            .service_principals
            .set_permissions(token_id, permissions)
    }

    /// Shut the shared core down (spec §10.1, S1A-004): drain in-flight
    /// operations within `drain_deadline`, sync durable state, release the
    /// file lock, and mark the core `Closed`. Every handle — including this
    /// one — then rejects further operations. Dropping a handle never closes
    /// storage; only this method or the last core drop does.
    pub fn shutdown(&self, drain_deadline: Duration) -> Result<()> {
        self.authorize_write("shutdown")?;
        self.authorize_permission(&crate::auth::Permission::Admin)?;
        self.database.core().shutdown(drain_deadline)
    }
}

/// Principal-bound authorized session without raw core escape (P1.4-T1).
///
/// Obtained from [`DatabaseHandle::session`]. Delegates to the handle's
/// authorized surface; never exposes raw [`crate::database::Database`] /
/// table handles.
pub struct AuthorizedMongrelSession<'a> {
    handle: &'a DatabaseHandle,
}

/// Alias used by some call sites.
pub type AuthorizedSession<'a> = AuthorizedMongrelSession<'a>;

impl<'a> AuthorizedMongrelSession<'a> {
    pub fn begin(&self) -> Result<AuthorizedTransaction<'a>> {
        self.handle.begin()
    }

    pub fn put(
        &self,
        table: &str,
        cells: Vec<(u16, crate::memtable::Value)>,
    ) -> Result<Option<i64>> {
        self.handle.put(table, cells)
    }

    pub fn update(
        &self,
        table: &str,
        row_id: crate::RowId,
        cells: Vec<(u16, crate::memtable::Value)>,
    ) -> Result<crate::txn::OwnedRow> {
        self.handle.update(table, row_id, cells)
    }

    pub fn delete(&self, table: &str, row_id: crate::RowId) -> Result<()> {
        self.handle.delete(table, row_id)
    }

    pub fn query(
        &self,
        table: &str,
        query: &crate::query::Query,
        projection: Option<&[u16]>,
    ) -> Result<Vec<crate::memtable::Row>> {
        self.handle.query(table, query, projection)
    }

    pub fn count(&self, table: &str) -> Result<u64> {
        self.handle.count(table)
    }

    pub fn put_batch(
        &self,
        table: &str,
        rows: Vec<Vec<(u16, crate::memtable::Value)>>,
    ) -> Result<Vec<Option<i64>>> {
        self.handle.put_batch(table, rows)
    }

    /// Create a secondary index (P1.4-X5).
    pub fn create_index(&self, table: &str, definition: crate::schema::IndexDef) -> Result<u64> {
        self.handle.create_index(table, definition)
    }

    /// Drop a secondary index (P1.4-X5).
    pub fn drop_index(&self, table: &str, name: &str) -> Result<()> {
        self.handle.drop_index(table, name)
    }

    /// Create a stored procedure through the authorized surface (P1.4-X6).
    pub fn create_procedure(
        &self,
        procedure: crate::procedure::StoredProcedure,
    ) -> Result<crate::procedure::StoredProcedure> {
        self.handle.create_procedure(procedure)
    }

    /// Drop a stored procedure by name (P1.4-X6).
    pub fn drop_procedure(&self, name: &str) -> Result<()> {
        self.handle.drop_procedure(name)
    }

    /// Execute a stored procedure with live principal authorization (P1.4-X6).
    pub fn call_procedure(
        &self,
        name: &str,
        args: std::collections::HashMap<String, crate::memtable::Value>,
    ) -> Result<crate::procedure::ProcedureCallResult> {
        self.handle.call_procedure(name, args)
    }

    /// Create a trigger through the authorized surface (P1.4-X6).
    pub fn create_trigger(
        &self,
        trigger: crate::trigger::StoredTrigger,
    ) -> Result<crate::trigger::StoredTrigger> {
        self.handle.create_trigger(trigger)
    }

    /// Drop a trigger by name (P1.4-X6).
    pub fn drop_trigger(&self, name: &str) -> Result<()> {
        self.handle.drop_trigger(name)
    }

    /// Historical rows at a snapshot under the session principal (P1.4-X7).
    pub fn rows_at_epoch(
        &self,
        table: &str,
        snapshot: crate::epoch::Snapshot,
    ) -> Result<Vec<crate::memtable::Row>> {
        self.handle.rows_at_epoch(table, snapshot)
    }

    /// Authorized aggregate count (P1.4-X7).
    pub fn aggregate_count(&self, table: &str) -> Result<u64> {
        self.handle.count(table)
    }
}

/// Authorized multi-statement transaction bound to one [`DatabaseHandle`] (P1.4).
///
/// Obtained from [`DatabaseHandle::begin`]. Mutations re-check the handle's
/// read-only restriction; principal authorization is enforced by the underlying
/// [`crate::txn::Transaction`].
pub struct AuthorizedTransaction<'a> {
    handle: &'a DatabaseHandle,
    txn: Option<crate::txn::Transaction<'a>>,
}

impl<'a> AuthorizedTransaction<'a> {
    fn txn_mut(&mut self) -> Result<&mut crate::txn::Transaction<'a>> {
        self.txn
            .as_mut()
            .ok_or_else(|| MongrelError::InvalidArgument("transaction already finished".into()))
    }

    /// Stage an insert.
    pub fn put(
        &mut self,
        table: &str,
        cells: Vec<(u16, crate::memtable::Value)>,
    ) -> Result<Option<i64>> {
        self.handle.authorize_write("put")?;
        self.handle.resolve_service_authority()?;
        self.txn_mut()?.put(table, cells)
    }

    /// Stage many inserts.
    pub fn put_batch(
        &mut self,
        table: &str,
        rows: Vec<Vec<(u16, crate::memtable::Value)>>,
    ) -> Result<Vec<Option<i64>>> {
        self.handle.authorize_write("put_batch")?;
        self.handle.resolve_service_authority()?;
        self.txn_mut()?.put_batch(table, rows)
    }

    /// Stage an update of one row.
    pub fn update(
        &mut self,
        table: &str,
        row_id: crate::RowId,
        cells: Vec<(u16, crate::memtable::Value)>,
    ) -> Result<crate::txn::OwnedRow> {
        self.handle.authorize_write("update")?;
        self.handle.resolve_service_authority()?;
        let mut images = self.txn_mut()?.update_many(table, vec![(row_id, cells)])?;
        images
            .pop()
            .ok_or_else(|| MongrelError::NotFound(format!("row {row_id:?} not found for update")))
    }

    /// Stage a delete of one row.
    pub fn delete(&mut self, table: &str, row_id: crate::RowId) -> Result<()> {
        self.handle.authorize_write("delete")?;
        self.handle.resolve_service_authority()?;
        self.txn_mut()?.delete(table, row_id)
    }

    /// Commit the transaction.
    pub fn commit(mut self) -> Result<crate::epoch::Epoch> {
        self.handle.authorize_write("commit")?;
        let txn = self
            .txn
            .take()
            .ok_or_else(|| MongrelError::InvalidArgument("transaction already finished".into()))?;
        txn.commit()
    }

    /// Roll back the transaction.
    pub fn rollback(mut self) {
        if let Some(txn) = self.txn.take() {
            txn.rollback();
        }
    }
}

impl Drop for AuthorizedTransaction<'_> {
    fn drop(&mut self) {
        if let Some(txn) = self.txn.take() {
            txn.rollback();
        }
    }
}

impl std::fmt::Debug for DatabaseHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatabaseHandle")
            .field("identity", &self.identity)
            .field("access", &self.access)
            .field("database", &self.database)
            .finish()
    }
}
