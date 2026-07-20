//! Versioned catalog commands (spec §10.6, S1F-001).
//!
//! Every logical catalog mutation is expressed as a versioned
//! [`CatalogCommand`] wrapped in a [`CatalogCommandRecord`]. The record carries
//! an explicit encoding `version` (spec §4.10 — decode fails closed on an
//! unknown version, unknown variant, or unknown field) and a monotonic
//! `catalog_version` assigned by the catalog the command is applied to.
//!
//! # Durability model
//!
//! The CATALOG file is demoted from sole authority to a checkpoint. Command
//! records ride inside the existing persistence mechanics with **no on-disk
//! format change**:
//!
//! - `Catalog` retains a bounded in-struct history (`command_log`), so the
//!   existing `DdlOp::CatalogSnapshot` WAL payload (`DdlOp::encode_catalog`)
//!   and the `<root>/CATALOG` checkpoint both carry the applied command
//!   records automatically.
//! - [`Catalog::apply_command`] validates, applies, bumps `catalog_version`,
//!   and appends the record to the bounded history;
//!   [`Catalog::apply_command_and_checkpoint`] then rewrites the checkpoint
//!   through the existing atomic write path.
//!
//! Application is deterministic: [`apply`] is a pure function from
//! `(&Catalog, &CatalogCommand)` to a resolved [`CatalogDelta`], and
//! `CatalogDelta::apply_to` replays the resolved change verbatim. Replaying
//! the same record against the same catalog version is an idempotent no-op
//! (see [`Catalog::apply_command`]).
//!
//! # Authorization boundary (spec §4.6) — landed design
//!
//! [`required_permission`] is the **permission map** for every command:
//!
//! - table/column/index, trigger/procedure, and materialized-view commands
//!   require [`Permission::Ddl`];
//! - user/role/grant/revoke, RLS-policy/column-mask, resource-group, and
//!   job-definition commands require [`Permission::Admin`].
//!
//! **Enforcement sits on the emitter, not in pure apply.** `Database`
//! mutation entry points call `require` / `require_for` against the caller's
//! principal (using the same map) before proposing or applying catalog work.
//! [`apply`] / [`Catalog::apply_command`] remain deterministic and
//! principal-free so a replica can replay an already-authorized record
//! without re-evaluating session identity. Emitters that propose
//! `CatalogCommand`s MUST check `required_permission` against the current
//! principal so revocation stays effective without reopening the core.
//!
//! # Canonical type aliases
//!
//! - [`ResourceGroupDef`] is a compatibility alias for
//!   [`crate::resource::ResourceGroup`]. `SetResourceGroup` keeps name-only
//!   validation; full group invariants live in
//!   [`crate::resource::ResourceGroup::validate`].
//! - [`JobDefinition`] is the catalog-level job-definition record; `kind` /
//!   `state` re-export the S1F-002 types in [`crate::jobs`]. `SetJobState`
//!   only guards terminal states; the full transition machine belongs to the
//!   `JOBS` registry.
//!
//! Released CATALOG files carry none of these keys (serde-defaulted fields),
//! so they open unchanged.
//!
//! # Surfaces not expressed as catalog commands
//!
//! Databases (the catalog is single-database; `db_epoch`/id counters remain
//! engine-managed), external tables, hidden CTAS building-table states,
//! SQLite pragmas (`user_version`/`application_id`), and `require_auth`
//! bootstrap stay on direct `Database`/`Catalog` fields. `db_epoch` is NOT
//! bumped by command application — epochs stay the commit sequencer's
//! authority; `catalog_version` orders commands.

use crate::auth::{Permission, RoleEntry, UserEntry};
use crate::catalog::{Catalog, CatalogEntry, MaterializedViewEntry, TableState};
use crate::error::{MongrelError, Result};
use crate::procedure::{ProcedureEntry, StoredProcedure};
use crate::schema::{ColumnDef, IndexDef, Schema, TypeId};
use crate::security::{ColumnMask, MaskStrategy, RowPolicy, SecurityCatalog, SecurityExpr};
use crate::trigger::{StoredTrigger, TriggerEntry, TriggerTarget};
use serde::{Deserialize, Serialize};

/// Encoding format version of [`CatalogCommandRecord`] (spec §4.10). Unknown
/// versions fail closed on decode.
pub const CATALOG_COMMAND_FORMAT_VERSION: u16 = 1;

/// Bound on the retained command-history tail in [`Catalog::command_log`].
/// Keeps CATALOG checkpoints and `CatalogSnapshot` WAL payloads bounded while
/// retaining ample `commands_since` history for single-node operation.
pub const COMMAND_HISTORY_LIMIT: usize = 256;

/// The catalog-level resource-group payload: the canonical S1E-002
/// [`crate::resource::ResourceGroup`], aliased for the catalog code that named
/// the forward-reference placeholder. Its serde shape is the durable contract;
/// released CATALOG files carry no `resource_groups` key and open with the
/// field serde-defaulted to empty.
pub type ResourceGroupDef = crate::resource::ResourceGroup;

pub use crate::jobs::{JobKind, JobState};

/// Persistent job definition recorded in the catalog (spec §10.6, S1F-001).
/// `kind`/`state` are the canonical S1F-002 types re-exported above; the live
/// state machine and per-job progress live in the `JOBS` registry
/// ([`crate::jobs::JobRegistry`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct JobDefinition {
    /// Unique, caller-allocated job id. Never reused.
    pub job_id: u64,
    pub kind: JobKind,
    pub state: JobState,
    /// Primary target (table, index, or materialized-view name), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    pub created_epoch: u64,
    pub updated_epoch: u64,
}

/// One logical catalog mutation (spec §10.6, S1F-001).
///
/// Variants carry already-validated payloads (mirroring how `DdlOp` records
/// carry resolved `ColumnDef`/schema JSON): deep semantic validation
/// (expression trees, SQL typing) happens at the emitting layer, while
/// [`apply`] enforces the catalog-level structural invariants fail-closed
/// (existence, uniqueness, id allocation, reference integrity).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CatalogCommand {
    // ── Tables and columns ────────────────────────────────────────────
    /// Create a live table. `table_id` is allocated deterministically from
    /// `next_table_id` at apply time and stamped as `schema.schema_id`.
    CreateTable {
        name: String,
        schema: Schema,
        created_epoch: u64,
    },
    /// Logically drop a live table. Cascades to triggers targeting the
    /// table, the same-named materialized-view definition, RLS state,
    /// policies, masks, and table-scoped role permissions (mirrors
    /// `Database::drop_table`).
    DropTable {
        name: String,
        at_epoch: u64,
    },
    /// Rename a live table. Retargets triggers and renames the same-named
    /// materialized-view definition. `name == new_name` is a recorded no-op
    /// (mirrors `Database::rename_table`).
    RenameTable {
        name: String,
        new_name: String,
        at_epoch: u64,
    },
    /// Replace one existing column definition (same `id`) with an
    /// already-validated one. Mirrors `DdlOp::AlterTable`.
    AlterColumn {
        table: String,
        column: ColumnDef,
    },
    /// Add a column with a caller-allocated, unused `id`.
    AddColumn {
        table: String,
        column: ColumnDef,
    },
    /// Drop a column by name; indexes referencing it are dropped too
    /// (mirrors the SQL `ALTER TABLE ... DROP COLUMN` rebuild path).
    DropColumn {
        table: String,
        column: String,
    },
    // ── Indexes ───────────────────────────────────────────────────────
    /// Add a secondary index definition to a live table.
    AddIndex {
        table: String,
        index: IndexDef,
    },
    /// Remove one index by exact name. Compound SQL index names expand to
    /// one command per [`IndexDef`] at the emitting layer.
    RemoveIndex {
        table: String,
        name: String,
    },
    /// Atomically replace one index definition with another. Publication is
    /// compare-and-swap on `expected_schema_sequence` (`Schema::schema_id`):
    /// concurrent DDL that advanced the sequence fails closed with
    /// [`MongrelError::Conflict`] and leaves the newer schema untouched.
    /// This is a single durable command — never compose from
    /// [`Self::AddIndex`] + [`Self::RemoveIndex`].
    ReplaceIndex {
        table: String,
        expected_schema_sequence: u64,
        expected_old_name: String,
        new_definition: IndexDef,
    },
    // ── Users, roles, grants ──────────────────────────────────────────
    /// Create a user. `id` is allocated deterministically from
    /// `next_user_id` (minimum 1) at apply time.
    CreateUser {
        username: String,
        password_hash: String,
        is_admin: bool,
        created_epoch: u64,
    },
    DropUser {
        username: String,
    },
    AlterUserPassword {
        username: String,
        password_hash: String,
    },
    SetUserAdmin {
        username: String,
        is_admin: bool,
    },
    CreateRole {
        name: String,
        created_epoch: u64,
    },
    /// Drop a role and strip it from every user (mirrors
    /// `Database::drop_role`).
    DropRole {
        name: String,
    },
    /// Grant a role to a user. Idempotent: granting an already-held role is
    /// a recorded no-op (mirrors `Database::grant_role`).
    GrantRole {
        username: String,
        role: String,
    },
    /// Revoke a role from a user. Idempotent no-op when not held.
    RevokeRole {
        username: String,
        role: String,
    },
    /// Grant a permission to a role, merging column lists for column-scoped
    /// grants. Idempotent no-op when the merged set is unchanged.
    GrantPermission {
        role: String,
        permission: Permission,
    },
    /// Revoke a permission from a role. Idempotent no-op when unchanged.
    RevokePermission {
        role: String,
        permission: Permission,
    },
    // ── Row-level security and masks ──────────────────────────────────
    EnableRls {
        table: String,
    },
    DisableRls {
        table: String,
    },
    /// Create or replace a row policy keyed by `(table, name)`.
    SetRowPolicy {
        policy: RowPolicy,
    },
    DropRowPolicy {
        table: String,
        name: String,
    },
    /// Create or replace a column mask keyed by `(table, name)`.
    SetColumnMask {
        mask: ColumnMask,
    },
    DropColumnMask {
        table: String,
        name: String,
    },
    /// Wholesale security-catalog replacement (RLS tables, policies, masks),
    /// mirroring `Database::set_security_catalog`. Validated against the
    /// candidate catalog exactly like the legacy path.
    SetSecurityCatalog {
        security: SecurityCatalog,
    },
    // ── Triggers and procedures ───────────────────────────────────────
    CreateTrigger {
        trigger: StoredTrigger,
    },
    DropTrigger {
        name: String,
    },
    CreateProcedure {
        procedure: StoredProcedure,
    },
    DropProcedure {
        name: String,
    },
    // ── Materialized views ────────────────────────────────────────────
    /// Create or replace a materialized-view definition. The backing live
    /// table must already exist (mirrors `Database::set_materialized_view`).
    CreateMaterializedView {
        definition: MaterializedViewEntry,
    },
    /// Drop only the definition; the physical table is dropped separately
    /// via [`CatalogCommand::DropTable`] (which also cascades definitions).
    DropMaterializedView {
        name: String,
    },
    /// Record refresh bookkeeping: bump `last_refresh_epoch` and, for
    /// incremental views, advance the CDC checkpoint when provided.
    RefreshMaterializedView {
        name: String,
        at_epoch: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        checkpoint_event_id: Option<String>,
    },
    // ── Resource groups (forward reference) ───────────────────────────
    /// Create or replace a resource group.
    SetResourceGroup {
        group: ResourceGroupDef,
    },
    RemoveResourceGroup {
        name: String,
    },
    // ── Job definitions (forward reference) ───────────────────────────
    /// Submit a new persistent job. `job_id` must be unused.
    SubmitJob {
        job: JobDefinition,
    },
    /// Record a job state change. Transitions out of terminal states
    /// (`Succeeded`/`Failed`) fail closed; S1F-002 owns the full machine.
    SetJobState {
        job_id: u64,
        state: JobState,
        at_epoch: u64,
    },
    // ── Serde-appended replacements (decode of older records unaffected) ──
    /// Create or replace a trigger, keyed by name. `trigger` is the resolved
    /// image: the emitter stamps `created_epoch`/`updated_epoch`/`version`
    /// from the commit epoch (and bumps `version` on replacement) before
    /// proposing, so replay is verbatim (mirrors
    /// `Database::create_or_replace_trigger`).
    ReplaceTrigger {
        trigger: StoredTrigger,
    },
    /// Create or replace a stored procedure, keyed by name; the payload is
    /// the epoch-resolved image (mirrors
    /// `Database::create_or_replace_procedure`).
    ReplaceProcedure {
        procedure: StoredProcedure,
    },
    /// Create a user with both Argon2id and SCRAM-SHA-256 credentials.
    CreateUserWithScram {
        username: String,
        password_hash: String,
        scram_sha_256: crate::security_hardening::ScramVerifier,
        is_admin: bool,
        created_epoch: u64,
    },
    /// Replace both password credential forms atomically.
    AlterUserPasswordWithScram {
        username: String,
        password_hash: String,
        scram_sha_256: crate::security_hardening::ScramVerifier,
    },
    /// Create a user with native SCRAM and MySQL 8 compatibility verifiers.
    CreateUserWithAuthVerifiers {
        username: String,
        password_hash: String,
        scram_sha_256: crate::security_hardening::ScramVerifier,
        mysql_caching_sha2: crate::auth::MysqlCachingSha2Verifier,
        is_admin: bool,
        created_epoch: u64,
    },
    /// Replace all password verifier forms atomically.
    AlterUserPasswordWithAuthVerifiers {
        username: String,
        password_hash: String,
        scram_sha_256: crate::security_hardening::ScramVerifier,
        mysql_caching_sha2: crate::auth::MysqlCachingSha2Verifier,
    },
}

/// A [`CatalogCommand`] plus its versioning envelope (spec §4.10).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogCommandRecord {
    /// Encoding format version; must equal [`CATALOG_COMMAND_FORMAT_VERSION`].
    pub version: u16,
    /// Monotonic catalog version this record assigns (`previous + 1`).
    pub catalog_version: u64,
    /// The mutation itself.
    pub command: CatalogCommand,
}

impl CatalogCommandRecord {
    /// Build the next record for `catalog` carrying `command`.
    pub fn next(catalog: &Catalog, command: CatalogCommand) -> Self {
        Self {
            version: CATALOG_COMMAND_FORMAT_VERSION,
            catalog_version: catalog.catalog_version.saturating_add(1),
            command,
        }
    }
}

/// The resolved, deterministic effect of [`apply`].
///
/// Every variant carries fully-resolved values (allocated ids, complete
/// replacement schemas/security catalogs) so `apply_to` is a mechanical
/// replay with no further decisions.
#[derive(Debug, Clone)]
pub enum CatalogDelta {
    /// Nothing changed: idempotent re-application or an inherently no-op
    /// command (e.g. granting an already-held role).
    NoOp,
    TableCreated {
        entry: CatalogEntry,
    },
    TableDropped {
        table_id: u64,
        name: String,
        at_epoch: u64,
    },
    TableRenamed {
        table_id: u64,
        old_name: String,
        new_name: String,
        at_epoch: u64,
    },
    /// Column and index mutations resolve to a whole-schema replacement.
    SchemaReplaced {
        table_id: u64,
        schema: Schema,
    },
    /// Create, password change, admin flag, and grant/revoke-role all
    /// resolve to a user upsert keyed by username.
    UserUpserted(UserEntry),
    UserRemoved {
        username: String,
    },
    /// Create and grant/revoke-permission resolve to a role upsert by name.
    RoleUpserted(RoleEntry),
    RoleRemoved {
        name: String,
    },
    /// RLS/policy/mask mutations resolve to a wholesale security-catalog
    /// replacement (mirrors `Database::set_security_catalog`).
    SecurityReplaced {
        security: SecurityCatalog,
    },
    TriggerUpserted(TriggerEntry),
    TriggerRemoved {
        name: String,
    },
    ProcedureUpserted(ProcedureEntry),
    ProcedureRemoved {
        name: String,
    },
    MaterializedViewUpserted(MaterializedViewEntry),
    MaterializedViewRemoved {
        name: String,
    },
    ResourceGroupUpserted(ResourceGroupDef),
    ResourceGroupRemoved {
        name: String,
    },
    JobUpserted(JobDefinition),
}

impl CatalogDelta {
    /// Replay the resolved change onto `catalog`. Deterministic: every
    /// decision was made by [`apply`].
    pub(crate) fn apply_to(&self, catalog: &mut Catalog) -> Result<()> {
        match self {
            CatalogDelta::NoOp => {}
            CatalogDelta::TableCreated { entry } => {
                catalog.next_table_id = entry.table_id.checked_add(1).ok_or_else(|| {
                    MongrelError::InvalidArgument("table id space exhausted".into())
                })?;
                catalog.tables.push(entry.clone());
            }
            CatalogDelta::TableDropped {
                table_id,
                name,
                at_epoch,
            } => {
                let entry = catalog
                    .tables
                    .iter_mut()
                    .find(|table| table.table_id == *table_id)
                    .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
                entry.state = TableState::Dropped {
                    at_epoch: *at_epoch,
                };
                catalog.triggers.retain(|trigger| {
                    !matches!(
                        &trigger.trigger.target,
                        TriggerTarget::Table(target) if target == name
                    )
                });
                catalog
                    .materialized_views
                    .retain(|definition| definition.name != *name);
                catalog.security.rls_tables.retain(|table| table != name);
                catalog
                    .security
                    .policies
                    .retain(|policy| policy.table != *name);
                catalog.security.masks.retain(|mask| mask.table != *name);
                for role in &mut catalog.roles {
                    role.permissions
                        .retain(|permission| permission_table(permission) != Some(name.as_str()));
                }
                advance_security_version(catalog)?;
            }
            CatalogDelta::TableRenamed {
                table_id,
                old_name,
                new_name,
                at_epoch,
            } => {
                let entry = catalog
                    .tables
                    .iter_mut()
                    .find(|table| table.table_id == *table_id)
                    .ok_or_else(|| {
                        MongrelError::NotFound(format!("table {old_name:?} not found"))
                    })?;
                entry.name = new_name.clone();
                for trigger in &mut catalog.triggers {
                    if matches!(
                        &trigger.trigger.target,
                        TriggerTarget::Table(target) if target == old_name
                    ) {
                        trigger.trigger = trigger.trigger.retarget_table(new_name, *at_epoch)?;
                    }
                }
                if let Some(definition) = catalog
                    .materialized_views
                    .iter_mut()
                    .find(|definition| definition.name == *old_name)
                {
                    definition.name = new_name.clone();
                }
                // Mirrors `Database::rename_table`: table-scoped security
                // state and role permissions follow the rename, and the
                // security version advances.
                for table in &mut catalog.security.rls_tables {
                    if table == old_name {
                        *table = new_name.clone();
                    }
                }
                for policy in &mut catalog.security.policies {
                    if policy.table == *old_name {
                        policy.table = new_name.clone();
                    }
                }
                for mask in &mut catalog.security.masks {
                    if mask.table == *old_name {
                        mask.table = new_name.clone();
                    }
                }
                for role in &mut catalog.roles {
                    for permission in &mut role.permissions {
                        rename_permission_table(permission, old_name, new_name);
                    }
                }
                advance_security_version(catalog)?;
            }
            CatalogDelta::SchemaReplaced { table_id, schema } => {
                let entry = catalog
                    .tables
                    .iter_mut()
                    .find(|table| table.table_id == *table_id)
                    .ok_or_else(|| {
                        MongrelError::NotFound(format!("table id {table_id} not found"))
                    })?;
                entry.schema = schema.clone();
            }
            CatalogDelta::UserUpserted(entry) => {
                match catalog
                    .users
                    .iter_mut()
                    .find(|user| user.username == entry.username)
                {
                    Some(existing) => *existing = entry.clone(),
                    None => {
                        let next = entry.id.checked_add(1).ok_or_else(|| {
                            MongrelError::Full("user-id namespace exhausted".into())
                        })?;
                        catalog.users.push(entry.clone());
                        catalog.next_user_id = catalog.next_user_id.max(next);
                    }
                }
                advance_security_version(catalog)?;
            }
            CatalogDelta::UserRemoved { username } => {
                catalog.users.retain(|user| user.username != *username);
                advance_security_version(catalog)?;
            }
            CatalogDelta::RoleUpserted(entry) => {
                match catalog
                    .roles
                    .iter_mut()
                    .find(|role| role.name == entry.name)
                {
                    Some(existing) => *existing = entry.clone(),
                    None => catalog.roles.push(entry.clone()),
                }
                advance_security_version(catalog)?;
            }
            CatalogDelta::RoleRemoved { name } => {
                catalog.roles.retain(|role| role.name != *name);
                for user in &mut catalog.users {
                    user.roles.retain(|role| role != name);
                }
                advance_security_version(catalog)?;
            }
            CatalogDelta::SecurityReplaced { security } => {
                catalog.security = security.clone();
                advance_security_version(catalog)?;
            }
            CatalogDelta::TriggerUpserted(entry) => {
                match catalog
                    .triggers
                    .iter_mut()
                    .find(|trigger| trigger.trigger.name == entry.trigger.name)
                {
                    Some(existing) => *existing = entry.clone(),
                    None => catalog.triggers.push(entry.clone()),
                }
            }
            CatalogDelta::TriggerRemoved { name } => {
                catalog
                    .triggers
                    .retain(|trigger| trigger.trigger.name != *name);
            }
            CatalogDelta::ProcedureUpserted(entry) => {
                match catalog
                    .procedures
                    .iter_mut()
                    .find(|procedure| procedure.procedure.name == entry.procedure.name)
                {
                    Some(existing) => *existing = entry.clone(),
                    None => catalog.procedures.push(entry.clone()),
                }
            }
            CatalogDelta::ProcedureRemoved { name } => {
                catalog
                    .procedures
                    .retain(|procedure| procedure.procedure.name != *name);
            }
            CatalogDelta::MaterializedViewUpserted(definition) => {
                match catalog
                    .materialized_views
                    .iter_mut()
                    .find(|existing| existing.name == definition.name)
                {
                    Some(existing) => *existing = definition.clone(),
                    None => catalog.materialized_views.push(definition.clone()),
                }
            }
            CatalogDelta::MaterializedViewRemoved { name } => {
                catalog
                    .materialized_views
                    .retain(|definition| definition.name != *name);
            }
            CatalogDelta::ResourceGroupUpserted(group) => {
                match catalog
                    .resource_groups
                    .iter_mut()
                    .find(|existing| existing.name == group.name)
                {
                    Some(existing) => *existing = group.clone(),
                    None => catalog.resource_groups.push(group.clone()),
                }
            }
            CatalogDelta::ResourceGroupRemoved { name } => {
                catalog.resource_groups.retain(|group| group.name != *name);
            }
            CatalogDelta::JobUpserted(job) => {
                match catalog
                    .job_definitions
                    .iter_mut()
                    .find(|existing| existing.job_id == job.job_id)
                {
                    Some(existing) => *existing = job.clone(),
                    None => catalog.job_definitions.push(job.clone()),
                }
            }
        }
        Ok(())
    }
}

/// The permission the emitting layer must hold to propose `command`
/// (spec §4.6). Pure map: `Database` enforces via `require` / `require_for`
/// before mutation; apply paths do not re-check principals (see module docs).
pub fn required_permission(command: &CatalogCommand) -> Permission {
    match command {
        CatalogCommand::CreateTable { .. }
        | CatalogCommand::DropTable { .. }
        | CatalogCommand::RenameTable { .. }
        | CatalogCommand::AlterColumn { .. }
        | CatalogCommand::AddColumn { .. }
        | CatalogCommand::DropColumn { .. }
        | CatalogCommand::AddIndex { .. }
        | CatalogCommand::RemoveIndex { .. }
        | CatalogCommand::ReplaceIndex { .. }
        | CatalogCommand::CreateTrigger { .. }
        | CatalogCommand::DropTrigger { .. }
        | CatalogCommand::CreateProcedure { .. }
        | CatalogCommand::DropProcedure { .. }
        | CatalogCommand::ReplaceTrigger { .. }
        | CatalogCommand::ReplaceProcedure { .. }
        | CatalogCommand::CreateMaterializedView { .. }
        | CatalogCommand::DropMaterializedView { .. }
        | CatalogCommand::RefreshMaterializedView { .. } => Permission::Ddl,
        CatalogCommand::CreateUser { .. }
        | CatalogCommand::CreateUserWithScram { .. }
        | CatalogCommand::CreateUserWithAuthVerifiers { .. }
        | CatalogCommand::DropUser { .. }
        | CatalogCommand::AlterUserPassword { .. }
        | CatalogCommand::AlterUserPasswordWithScram { .. }
        | CatalogCommand::AlterUserPasswordWithAuthVerifiers { .. }
        | CatalogCommand::SetUserAdmin { .. }
        | CatalogCommand::CreateRole { .. }
        | CatalogCommand::DropRole { .. }
        | CatalogCommand::GrantRole { .. }
        | CatalogCommand::RevokeRole { .. }
        | CatalogCommand::GrantPermission { .. }
        | CatalogCommand::RevokePermission { .. }
        | CatalogCommand::EnableRls { .. }
        | CatalogCommand::DisableRls { .. }
        | CatalogCommand::SetRowPolicy { .. }
        | CatalogCommand::DropRowPolicy { .. }
        | CatalogCommand::SetColumnMask { .. }
        | CatalogCommand::DropColumnMask { .. }
        | CatalogCommand::SetSecurityCatalog { .. }
        | CatalogCommand::SetResourceGroup { .. }
        | CatalogCommand::RemoveResourceGroup { .. }
        | CatalogCommand::SubmitJob { .. }
        | CatalogCommand::SetJobState { .. } => Permission::Admin,
    }
}

/// Validate `command` against `catalog` and resolve its deterministic effect.
/// Pure: `catalog` is never mutated; idempotent commands resolve to
/// [`CatalogDelta::NoOp`].
pub fn apply(catalog: &Catalog, command: &CatalogCommand) -> Result<CatalogDelta> {
    match command {
        CatalogCommand::CreateTable {
            name,
            schema,
            created_epoch,
        } => {
            if name.is_empty() || name.starts_with(crate::database::CTAS_BUILD_TABLE_PREFIX) {
                return Err(MongrelError::InvalidArgument(format!(
                    "invalid table name {name:?}"
                )));
            }
            if catalog.live(name).is_some() || catalog.building_for(name).is_some() {
                return Err(MongrelError::InvalidArgument(format!(
                    "table {name:?} already exists or is being built"
                )));
            }
            let mut schema = schema.clone();
            // Schema validation runs before the command can be recorded so a
            // replayed command stream never carries an unopenable schema
            // (mirrors `create_table_with_state`).
            schema.validate_auto_increment()?;
            schema.validate_defaults()?;
            schema.validate_ai()?;
            for index in &schema.indexes {
                index.validate_options()?;
            }
            for constraint in &schema.constraints.checks {
                constraint.expr.validate()?;
            }
            let table_id = catalog.next_table_id;
            schema.schema_id = table_id;
            Ok(CatalogDelta::TableCreated {
                entry: CatalogEntry {
                    table_id,
                    name: name.clone(),
                    schema,
                    state: TableState::Live,
                    created_epoch: *created_epoch,
                },
            })
        }
        CatalogCommand::DropTable { name, at_epoch } => {
            let entry = catalog
                .live(name)
                .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
            Ok(CatalogDelta::TableDropped {
                table_id: entry.table_id,
                name: name.clone(),
                at_epoch: *at_epoch,
            })
        }
        CatalogCommand::RenameTable {
            name,
            new_name,
            at_epoch,
        } => {
            if name == new_name {
                return Ok(CatalogDelta::NoOp);
            }
            if new_name.is_empty()
                || name.starts_with(crate::database::CTAS_BUILD_TABLE_PREFIX)
                || new_name.starts_with(crate::database::CTAS_BUILD_TABLE_PREFIX)
            {
                return Err(MongrelError::InvalidArgument(
                    "invalid table rename identity".into(),
                ));
            }
            let entry = catalog
                .live(name)
                .ok_or_else(|| MongrelError::NotFound(format!("table {name:?} not found")))?;
            if catalog.live(new_name).is_some() || catalog.building_for(new_name).is_some() {
                return Err(MongrelError::InvalidArgument(format!(
                    "a table named {new_name:?} already exists"
                )));
            }
            Ok(CatalogDelta::TableRenamed {
                table_id: entry.table_id,
                old_name: name.clone(),
                new_name: new_name.clone(),
                at_epoch: *at_epoch,
            })
        }
        CatalogCommand::AlterColumn { table, column } => {
            let entry = live_entry(catalog, table)?;
            let mut schema = entry.schema.clone();
            let position = schema
                .columns
                .iter()
                .position(|existing| existing.id == column.id)
                .ok_or_else(|| {
                    MongrelError::ColumnNotFound(format!("column id {} on {table}", column.id))
                })?;
            schema.columns[position] = column.clone();
            // The engine (`Table::prepare_alter_column`) bumps `schema_id` on
            // every applied alteration; the resolved delta mirrors it so a
            // replayed command stream reproduces the same schema image.
            schema.schema_id = schema
                .schema_id
                .checked_add(1)
                .ok_or_else(|| MongrelError::Schema("schema id space exhausted".into()))?;
            Ok(CatalogDelta::SchemaReplaced {
                table_id: entry.table_id,
                schema,
            })
        }
        CatalogCommand::AddColumn { table, column } => {
            let entry = live_entry(catalog, table)?;
            if entry.schema.column(&column.name).is_some() {
                return Err(MongrelError::InvalidArgument(format!(
                    "column {} already exists on {table}",
                    column.name
                )));
            }
            if entry
                .schema
                .columns
                .iter()
                .any(|existing| existing.id == column.id)
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "column id {} already used on {table}",
                    column.id
                )));
            }
            let mut schema = entry.schema.clone();
            schema.columns.push(column.clone());
            schema.validate_auto_increment()?;
            schema.validate_defaults()?;
            schema.validate_ai()?;
            Ok(CatalogDelta::SchemaReplaced {
                table_id: entry.table_id,
                schema,
            })
        }
        CatalogCommand::DropColumn { table, column } => {
            let entry = live_entry(catalog, table)?;
            let schema = &entry.schema;
            let target = schema.column(column).ok_or_else(|| {
                MongrelError::ColumnNotFound(format!("column {column} on {table}"))
            })?;
            let dropped_id = target.id;
            let mut schema = schema.clone();
            schema.columns.retain(|existing| existing.id != dropped_id);
            schema.indexes.retain(|index| index.column_id != dropped_id);
            Ok(CatalogDelta::SchemaReplaced {
                table_id: entry.table_id,
                schema,
            })
        }
        CatalogCommand::AddIndex { table, index } => {
            let entry = live_entry(catalog, table)?;
            let schema = &entry.schema;
            if schema
                .indexes
                .iter()
                .any(|existing| existing.name == index.name)
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "index {} already exists on {table}",
                    index.name
                )));
            }
            index.validate_options()?;
            let mut schema = schema.clone();
            schema.indexes.push(index.clone());
            bump_schema_sequence(&mut schema)?;
            // validate_ai enforces column existence + kind/type compatibility.
            schema.validate_ai()?;
            Ok(CatalogDelta::SchemaReplaced {
                table_id: entry.table_id,
                schema,
            })
        }
        CatalogCommand::RemoveIndex { table, name } => {
            let entry = live_entry(catalog, table)?;
            let schema = &entry.schema;
            if !schema.indexes.iter().any(|index| index.name == *name) {
                return Err(MongrelError::NotFound(format!(
                    "index {name} does not exist on {table}"
                )));
            }
            let mut schema = schema.clone();
            schema.indexes.retain(|index| index.name != *name);
            bump_schema_sequence(&mut schema)?;
            Ok(CatalogDelta::SchemaReplaced {
                table_id: entry.table_id,
                schema,
            })
        }
        CatalogCommand::ReplaceIndex {
            table,
            expected_schema_sequence,
            expected_old_name,
            new_definition,
        } => {
            let entry = live_entry(catalog, table)?;
            let schema = &entry.schema;
            if schema.schema_id != *expected_schema_sequence {
                return Err(MongrelError::Conflict(format!(
                    "index replace on {table}: expected schema sequence {expected_schema_sequence}, found {}",
                    schema.schema_id
                )));
            }
            if !schema
                .indexes
                .iter()
                .any(|index| index.name == *expected_old_name)
            {
                return Err(MongrelError::NotFound(format!(
                    "index {expected_old_name} does not exist on {table}"
                )));
            }
            if new_definition.name != *expected_old_name
                && schema
                    .indexes
                    .iter()
                    .any(|index| index.name == new_definition.name)
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "index {} already exists on {table}",
                    new_definition.name
                )));
            }
            new_definition.validate_options()?;
            let mut schema = schema.clone();
            let position = schema
                .indexes
                .iter()
                .position(|index| index.name == *expected_old_name)
                .expect("presence checked above");
            schema.indexes[position] = new_definition.clone();
            bump_schema_sequence(&mut schema)?;
            schema.validate_ai()?;
            Ok(CatalogDelta::SchemaReplaced {
                table_id: entry.table_id,
                schema,
            })
        }
        CatalogCommand::CreateUser {
            username,
            password_hash,
            is_admin,
            created_epoch,
        } => {
            if username.is_empty() {
                return Err(MongrelError::InvalidArgument(
                    "username must not be empty".into(),
                ));
            }
            if catalog.users.iter().any(|user| user.username == *username) {
                return Err(MongrelError::InvalidArgument(format!(
                    "user {username:?} already exists"
                )));
            }
            let id = catalog.next_user_id.max(1);
            Ok(CatalogDelta::UserUpserted(UserEntry {
                id,
                username: username.clone(),
                password_hash: password_hash.clone(),
                scram_sha_256: None,
                mysql_caching_sha2: None,
                roles: Vec::new(),
                is_admin: *is_admin,
                created_epoch: *created_epoch,
            }))
        }
        CatalogCommand::DropUser { username } => {
            if !catalog.users.iter().any(|user| user.username == *username) {
                return Err(MongrelError::NotFound(format!(
                    "user {username:?} not found"
                )));
            }
            Ok(CatalogDelta::UserRemoved {
                username: username.clone(),
            })
        }
        CatalogCommand::AlterUserPassword {
            username,
            password_hash,
        } => {
            let user = find_user(catalog, username)?;
            let mut user = user.clone();
            user.password_hash = password_hash.clone();
            user.scram_sha_256 = None;
            user.mysql_caching_sha2 = None;
            Ok(CatalogDelta::UserUpserted(user))
        }
        CatalogCommand::CreateUserWithScram {
            username,
            password_hash,
            scram_sha_256,
            is_admin,
            created_epoch,
        } => {
            if username.is_empty() {
                return Err(MongrelError::InvalidArgument(
                    "username must not be empty".into(),
                ));
            }
            if catalog.users.iter().any(|user| user.username == *username) {
                return Err(MongrelError::InvalidArgument(format!(
                    "user {username:?} already exists"
                )));
            }
            Ok(CatalogDelta::UserUpserted(UserEntry {
                id: catalog.next_user_id.max(1),
                username: username.clone(),
                password_hash: password_hash.clone(),
                scram_sha_256: Some(scram_sha_256.clone()),
                mysql_caching_sha2: None,
                roles: Vec::new(),
                is_admin: *is_admin,
                created_epoch: *created_epoch,
            }))
        }
        CatalogCommand::AlterUserPasswordWithScram {
            username,
            password_hash,
            scram_sha_256,
        } => {
            let mut user = find_user(catalog, username)?.clone();
            user.password_hash = password_hash.clone();
            user.scram_sha_256 = Some(scram_sha_256.clone());
            user.mysql_caching_sha2 = None;
            Ok(CatalogDelta::UserUpserted(user))
        }
        CatalogCommand::CreateUserWithAuthVerifiers {
            username,
            password_hash,
            scram_sha_256,
            mysql_caching_sha2,
            is_admin,
            created_epoch,
        } => {
            if username.is_empty() {
                return Err(MongrelError::InvalidArgument(
                    "username must not be empty".into(),
                ));
            }
            if catalog.users.iter().any(|user| user.username == *username) {
                return Err(MongrelError::InvalidArgument(format!(
                    "user {username:?} already exists"
                )));
            }
            Ok(CatalogDelta::UserUpserted(UserEntry {
                id: catalog.next_user_id.max(1),
                username: username.clone(),
                password_hash: password_hash.clone(),
                scram_sha_256: Some(scram_sha_256.clone()),
                mysql_caching_sha2: Some(mysql_caching_sha2.clone()),
                roles: Vec::new(),
                is_admin: *is_admin,
                created_epoch: *created_epoch,
            }))
        }
        CatalogCommand::AlterUserPasswordWithAuthVerifiers {
            username,
            password_hash,
            scram_sha_256,
            mysql_caching_sha2,
        } => {
            let mut user = find_user(catalog, username)?.clone();
            user.password_hash = password_hash.clone();
            user.scram_sha_256 = Some(scram_sha_256.clone());
            user.mysql_caching_sha2 = Some(mysql_caching_sha2.clone());
            Ok(CatalogDelta::UserUpserted(user))
        }
        CatalogCommand::SetUserAdmin { username, is_admin } => {
            let user = find_user(catalog, username)?;
            if user.is_admin == *is_admin {
                return Ok(CatalogDelta::NoOp);
            }
            let mut user = user.clone();
            user.is_admin = *is_admin;
            Ok(CatalogDelta::UserUpserted(user))
        }
        CatalogCommand::CreateRole {
            name,
            created_epoch,
        } => {
            if name.is_empty() {
                return Err(MongrelError::InvalidArgument(
                    "role name must not be empty".into(),
                ));
            }
            if catalog.roles.iter().any(|role| role.name == *name) {
                return Err(MongrelError::InvalidArgument(format!(
                    "role {name:?} already exists"
                )));
            }
            Ok(CatalogDelta::RoleUpserted(RoleEntry {
                name: name.clone(),
                permissions: Vec::new(),
                created_epoch: *created_epoch,
            }))
        }
        CatalogCommand::DropRole { name } => {
            if !catalog.roles.iter().any(|role| role.name == *name) {
                return Err(MongrelError::NotFound(format!("role {name:?} not found")));
            }
            Ok(CatalogDelta::RoleRemoved { name: name.clone() })
        }
        CatalogCommand::GrantRole { username, role } => {
            if !catalog.roles.iter().any(|entry| entry.name == *role) {
                return Err(MongrelError::NotFound(format!("role {role:?} not found")));
            }
            let user = find_user(catalog, username)?;
            if user.roles.iter().any(|held| held == role) {
                return Ok(CatalogDelta::NoOp);
            }
            let mut user = user.clone();
            user.roles.push(role.clone());
            Ok(CatalogDelta::UserUpserted(user))
        }
        CatalogCommand::RevokeRole { username, role } => {
            let user = find_user(catalog, username)?;
            if !user.roles.iter().any(|held| held == role) {
                return Ok(CatalogDelta::NoOp);
            }
            let mut user = user.clone();
            user.roles.retain(|held| held != role);
            Ok(CatalogDelta::UserUpserted(user))
        }
        CatalogCommand::GrantPermission { role, permission } => {
            let role = find_role(catalog, role)?;
            let mut resolved = role.clone();
            merge_permission(&mut resolved.permissions, permission.clone());
            if resolved.permissions == role.permissions {
                return Ok(CatalogDelta::NoOp);
            }
            Ok(CatalogDelta::RoleUpserted(resolved))
        }
        CatalogCommand::RevokePermission { role, permission } => {
            let role = find_role(catalog, role)?;
            let mut resolved = role.clone();
            revoke_permission_from(&mut resolved.permissions, permission);
            if resolved.permissions == role.permissions {
                return Ok(CatalogDelta::NoOp);
            }
            Ok(CatalogDelta::RoleUpserted(resolved))
        }
        CatalogCommand::EnableRls { table } => {
            live_entry(catalog, table)?;
            if catalog.security.rls_enabled(table) {
                return Ok(CatalogDelta::NoOp);
            }
            let mut security = catalog.security.clone();
            security.rls_tables.push(table.clone());
            Ok(CatalogDelta::SecurityReplaced { security })
        }
        CatalogCommand::DisableRls { table } => {
            if !catalog.security.rls_enabled(table) {
                return Ok(CatalogDelta::NoOp);
            }
            let mut security = catalog.security.clone();
            security.rls_tables.retain(|name| name != table);
            Ok(CatalogDelta::SecurityReplaced { security })
        }
        CatalogCommand::SetRowPolicy { policy } => {
            let entry = live_entry(catalog, &policy.table)?;
            if let Some(expression) = &policy.using {
                validate_policy_columns(expression, &entry.schema)?;
            }
            if let Some(expression) = &policy.with_check {
                validate_policy_columns(expression, &entry.schema)?;
            }
            let mut security = catalog.security.clone();
            match security
                .policies
                .iter_mut()
                .find(|existing| existing.table == policy.table && existing.name == policy.name)
            {
                Some(existing) => *existing = policy.clone(),
                None => security.policies.push(policy.clone()),
            }
            Ok(CatalogDelta::SecurityReplaced { security })
        }
        CatalogCommand::DropRowPolicy { table, name } => {
            if !catalog
                .security
                .policies
                .iter()
                .any(|policy| policy.table == *table && policy.name == *name)
            {
                return Err(MongrelError::NotFound(format!(
                    "policy {name:?} on {table:?} not found"
                )));
            }
            let mut security = catalog.security.clone();
            security
                .policies
                .retain(|policy| !(policy.table == *table && policy.name == *name));
            Ok(CatalogDelta::SecurityReplaced { security })
        }
        CatalogCommand::SetColumnMask { mask } => {
            let entry = live_entry(catalog, &mask.table)?;
            let column = entry
                .schema
                .columns
                .iter()
                .find(|column| column.id == mask.column)
                .ok_or_else(|| {
                    MongrelError::NotFound(format!(
                        "mask column {} on {:?} not found",
                        mask.column, mask.table
                    ))
                })?;
            if matches!(
                mask.strategy,
                MaskStrategy::Redact { .. } | MaskStrategy::Sha256
            ) && !matches!(column.ty, TypeId::Bytes | TypeId::Enum { .. })
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "mask {:?} requires a string/bytes column",
                    mask.name
                )));
            }
            let mut security = catalog.security.clone();
            match security
                .masks
                .iter_mut()
                .find(|existing| existing.table == mask.table && existing.name == mask.name)
            {
                Some(existing) => *existing = mask.clone(),
                None => security.masks.push(mask.clone()),
            }
            Ok(CatalogDelta::SecurityReplaced { security })
        }
        CatalogCommand::DropColumnMask { table, name } => {
            if !catalog
                .security
                .masks
                .iter()
                .any(|mask| mask.table == *table && mask.name == *name)
            {
                return Err(MongrelError::NotFound(format!(
                    "mask {name:?} on {table:?} not found"
                )));
            }
            let mut security = catalog.security.clone();
            security
                .masks
                .retain(|mask| !(mask.table == *table && mask.name == *name));
            Ok(CatalogDelta::SecurityReplaced { security })
        }
        CatalogCommand::SetSecurityCatalog { security } => {
            // Wholesale replacement: validated against the candidate catalog
            // with the same validator the legacy `set_security_catalog` path
            // runs, so a replayed command stream never carries an unopenable
            // security catalog.
            crate::database::validate_security_catalog(catalog, security)?;
            Ok(CatalogDelta::SecurityReplaced {
                security: security.clone(),
            })
        }
        CatalogCommand::CreateTrigger { trigger } => {
            trigger.validate()?;
            if let TriggerTarget::Table(target) = &trigger.target {
                if catalog.live(target).is_none() {
                    return Err(MongrelError::InvalidArgument(format!(
                        "trigger {:?} references unknown target table {target:?}",
                        trigger.name
                    )));
                }
            }
            if catalog
                .triggers
                .iter()
                .any(|entry| entry.trigger.name == trigger.name)
            {
                return Err(MongrelError::TriggerValidation(format!(
                    "trigger {:?} already exists",
                    trigger.name
                )));
            }
            Ok(CatalogDelta::TriggerUpserted(TriggerEntry::from(
                trigger.clone(),
            )))
        }
        CatalogCommand::DropTrigger { name } => {
            if !catalog
                .triggers
                .iter()
                .any(|entry| entry.trigger.name == *name)
            {
                return Err(MongrelError::NotFound(format!(
                    "trigger {name:?} not found"
                )));
            }
            Ok(CatalogDelta::TriggerRemoved { name: name.clone() })
        }
        CatalogCommand::CreateProcedure { procedure } => {
            procedure.validate()?;
            for step in &procedure.body.steps {
                if let Some(table) = step.table() {
                    if catalog.live(table).is_none() {
                        return Err(MongrelError::InvalidArgument(format!(
                            "procedure {:?} references unknown table {table:?}",
                            procedure.name
                        )));
                    }
                }
            }
            if catalog
                .procedures
                .iter()
                .any(|entry| entry.procedure.name == procedure.name)
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "procedure {:?} already exists",
                    procedure.name
                )));
            }
            Ok(CatalogDelta::ProcedureUpserted(ProcedureEntry::from(
                procedure.clone(),
            )))
        }
        CatalogCommand::DropProcedure { name } => {
            if !catalog
                .procedures
                .iter()
                .any(|entry| entry.procedure.name == *name)
            {
                return Err(MongrelError::NotFound(format!(
                    "procedure {name:?} not found"
                )));
            }
            Ok(CatalogDelta::ProcedureRemoved { name: name.clone() })
        }
        CatalogCommand::ReplaceTrigger { trigger } => {
            // CreateTrigger's structural validation without the must-not-exist
            // rule: an existing entry is replaced by name. The payload is the
            // emitter-resolved image (epochs/version already stamped), so the
            // delta replays it verbatim.
            trigger.validate()?;
            if let TriggerTarget::Table(target) = &trigger.target {
                if catalog.live(target).is_none() {
                    return Err(MongrelError::InvalidArgument(format!(
                        "trigger {:?} references unknown target table {target:?}",
                        trigger.name
                    )));
                }
            }
            Ok(CatalogDelta::TriggerUpserted(TriggerEntry::from(
                trigger.clone(),
            )))
        }
        CatalogCommand::ReplaceProcedure { procedure } => {
            procedure.validate()?;
            for step in &procedure.body.steps {
                if let Some(table) = step.table() {
                    if catalog.live(table).is_none() {
                        return Err(MongrelError::InvalidArgument(format!(
                            "procedure {:?} references unknown table {table:?}",
                            procedure.name
                        )));
                    }
                }
            }
            Ok(CatalogDelta::ProcedureUpserted(ProcedureEntry::from(
                procedure.clone(),
            )))
        }
        CatalogCommand::CreateMaterializedView { definition } => {
            if definition.name.is_empty() || definition.query.trim().is_empty() {
                return Err(MongrelError::InvalidArgument(
                    "materialized view name and query must not be empty".into(),
                ));
            }
            if catalog.live(&definition.name).is_none() {
                return Err(MongrelError::NotFound(format!(
                    "materialized view table {:?} not found",
                    definition.name
                )));
            }
            Ok(CatalogDelta::MaterializedViewUpserted(definition.clone()))
        }
        CatalogCommand::DropMaterializedView { name } => {
            if !catalog
                .materialized_views
                .iter()
                .any(|definition| definition.name == *name)
            {
                return Err(MongrelError::NotFound(format!(
                    "materialized view {name:?} not found"
                )));
            }
            Ok(CatalogDelta::MaterializedViewRemoved { name: name.clone() })
        }
        CatalogCommand::RefreshMaterializedView {
            name,
            at_epoch,
            checkpoint_event_id,
        } => {
            let definition = catalog
                .materialized_views
                .iter()
                .find(|definition| definition.name == *name)
                .ok_or_else(|| {
                    MongrelError::NotFound(format!("materialized view {name:?} not found"))
                })?;
            let mut definition = definition.clone();
            definition.last_refresh_epoch = *at_epoch;
            if let Some(checkpoint) = checkpoint_event_id {
                let plan = definition.incremental.as_mut().ok_or_else(|| {
                    MongrelError::InvalidArgument(format!(
                        "materialized view {name:?} has no incremental plan"
                    ))
                })?;
                plan.checkpoint_event_id = checkpoint.clone();
            }
            Ok(CatalogDelta::MaterializedViewUpserted(definition))
        }
        CatalogCommand::SetResourceGroup { group } => {
            if group.name.is_empty() {
                return Err(MongrelError::InvalidArgument(
                    "resource group name must not be empty".into(),
                ));
            }
            Ok(CatalogDelta::ResourceGroupUpserted(group.clone()))
        }
        CatalogCommand::RemoveResourceGroup { name } => {
            if !catalog
                .resource_groups
                .iter()
                .any(|group| group.name == *name)
            {
                return Err(MongrelError::NotFound(format!(
                    "resource group {name:?} not found"
                )));
            }
            Ok(CatalogDelta::ResourceGroupRemoved { name: name.clone() })
        }
        CatalogCommand::SubmitJob { job } => {
            if catalog
                .job_definitions
                .iter()
                .any(|existing| existing.job_id == job.job_id)
            {
                return Err(MongrelError::InvalidArgument(format!(
                    "job id {} already exists",
                    job.job_id
                )));
            }
            Ok(CatalogDelta::JobUpserted(job.clone()))
        }
        CatalogCommand::SetJobState {
            job_id,
            state,
            at_epoch,
        } => {
            let job = catalog
                .job_definitions
                .iter()
                .find(|job| job.job_id == *job_id)
                .ok_or_else(|| MongrelError::NotFound(format!("job id {job_id} not found")))?;
            if job.state.is_terminal() {
                return Err(MongrelError::InvalidArgument(format!(
                    "job id {job_id} is in terminal state {:?}",
                    job.state
                )));
            }
            let mut job = job.clone();
            job.state = *state;
            job.updated_epoch = *at_epoch;
            Ok(CatalogDelta::JobUpserted(job))
        }
    }
}

/// Encode a command record (JSON; deterministic field order).
pub fn encode_command(record: &CatalogCommandRecord) -> Result<Vec<u8>> {
    serde_json::to_vec(record)
        .map_err(|error| MongrelError::Other(format!("catalog command serialize: {error}")))
}

/// Decode a command record, failing closed (spec §4.10) on malformed JSON,
/// unknown variants/fields, or an unsupported encoding version.
pub fn decode_command(bytes: &[u8]) -> Result<CatalogCommandRecord> {
    let record: CatalogCommandRecord = serde_json::from_slice(bytes)
        .map_err(|error| MongrelError::Other(format!("catalog command deserialize: {error}")))?;
    if record.version != CATALOG_COMMAND_FORMAT_VERSION {
        return Err(MongrelError::UnsupportedStorageVersion {
            component: "catalog command",
            found: record.version,
            supported: CATALOG_COMMAND_FORMAT_VERSION,
        });
    }
    Ok(record)
}

fn live_entry<'a>(catalog: &'a Catalog, table: &str) -> Result<&'a CatalogEntry> {
    catalog
        .live(table)
        .ok_or_else(|| MongrelError::NotFound(format!("table {table:?} not found")))
}

/// Advance `Schema::schema_id` so concurrent index DDL can CAS against a
/// single sequence number at publication.
fn bump_schema_sequence(schema: &mut Schema) -> Result<()> {
    schema.schema_id = schema
        .schema_id
        .checked_add(1)
        .ok_or_else(|| MongrelError::Schema("schema id space exhausted".into()))?;
    Ok(())
}

fn find_user<'a>(catalog: &'a Catalog, username: &str) -> Result<&'a UserEntry> {
    catalog
        .users
        .iter()
        .find(|user| user.username == username)
        .ok_or_else(|| MongrelError::NotFound(format!("user {username:?} not found")))
}

fn find_role<'a>(catalog: &'a Catalog, name: &str) -> Result<&'a RoleEntry> {
    catalog
        .roles
        .iter()
        .find(|role| role.name == name)
        .ok_or_else(|| MongrelError::NotFound(format!("role {name:?} not found")))
}

/// Bump the optimistic authorization snapshot counter (mirrors
/// `advance_security_version` in `database.rs`; unified when command routing
/// lands).
fn advance_security_version(catalog: &mut Catalog) -> Result<()> {
    catalog.security_version = catalog.security_version.checked_add(1).ok_or_else(|| {
        MongrelError::Conflict("security catalog version space is exhausted".into())
    })?;
    Ok(())
}

/// Table a permission references (mirrors `permission_table` in
/// `database.rs`; unified when command routing lands).
fn permission_table(permission: &Permission) -> Option<&str> {
    match permission {
        Permission::Select { table }
        | Permission::Insert { table }
        | Permission::Update { table }
        | Permission::Delete { table }
        | Permission::SelectColumns { table, .. }
        | Permission::InsertColumns { table, .. }
        | Permission::UpdateColumns { table, .. } => Some(table),
        Permission::All | Permission::Ddl | Permission::Admin => None,
    }
}

/// Retarget a table-scoped permission on rename (mirrors
/// `rename_permission_table` in `database.rs`).
fn rename_permission_table(permission: &mut Permission, old: &str, new: &str) {
    let table = match permission {
        Permission::Select { table }
        | Permission::Insert { table }
        | Permission::Update { table }
        | Permission::Delete { table }
        | Permission::SelectColumns { table, .. }
        | Permission::InsertColumns { table, .. }
        | Permission::UpdateColumns { table, .. } => Some(table),
        Permission::All | Permission::Ddl | Permission::Admin => None,
    };
    if let Some(table) = table.filter(|table| table.as_str() == old) {
        *table = new.to_string();
    }
}

/// Merge a granted permission, coalescing column-scoped grants on the same
/// table (mirrors `merge_permission` in `database.rs`).
fn merge_permission(permissions: &mut Vec<Permission>, permission: Permission) {
    let (kind, table, mut columns) = match permission {
        Permission::SelectColumns { table, columns } => (0, table, columns),
        Permission::InsertColumns { table, columns } => (1, table, columns),
        Permission::UpdateColumns { table, columns } => (2, table, columns),
        permission if !permissions.contains(&permission) => {
            permissions.push(permission);
            return;
        }
        _ => return,
    };
    for permission in permissions.iter_mut() {
        let existing = match permission {
            Permission::SelectColumns {
                table: existing_table,
                columns,
            } if kind == 0 && existing_table == &table => Some(columns),
            Permission::InsertColumns {
                table: existing_table,
                columns,
            } if kind == 1 && existing_table == &table => Some(columns),
            Permission::UpdateColumns {
                table: existing_table,
                columns,
            } if kind == 2 && existing_table == &table => Some(columns),
            _ => None,
        };
        if let Some(existing) = existing {
            existing.append(&mut columns);
            existing.sort();
            existing.dedup();
            return;
        }
    }
    columns.sort();
    columns.dedup();
    let permission = if kind == 0 {
        Permission::SelectColumns { table, columns }
    } else if kind == 1 {
        Permission::InsertColumns { table, columns }
    } else {
        Permission::UpdateColumns { table, columns }
    };
    permissions.push(permission);
}

/// Remove a revoked permission, subtracting column-scoped grants (mirrors
/// `revoke_permission_from` in `database.rs`).
fn revoke_permission_from(permissions: &mut Vec<Permission>, revoked: &Permission) {
    let revoked_columns = match revoked {
        Permission::SelectColumns { table, columns } => Some((0, table, columns)),
        Permission::InsertColumns { table, columns } => Some((1, table, columns)),
        Permission::UpdateColumns { table, columns } => Some((2, table, columns)),
        _ => None,
    };
    let Some((kind, table, columns)) = revoked_columns else {
        permissions.retain(|permission| permission != revoked);
        return;
    };
    for permission in permissions.iter_mut() {
        let current = match permission {
            Permission::SelectColumns {
                table: current_table,
                columns,
            } if kind == 0 && current_table == table => Some(columns),
            Permission::InsertColumns {
                table: current_table,
                columns,
            } if kind == 1 && current_table == table => Some(columns),
            Permission::UpdateColumns {
                table: current_table,
                columns,
            } if kind == 2 && current_table == table => Some(columns),
            _ => None,
        };
        if let Some(current) = current {
            current.retain(|column| !columns.contains(column));
        }
    }
    permissions.retain(|permission| match permission {
        Permission::SelectColumns { columns, .. }
        | Permission::InsertColumns { columns, .. }
        | Permission::UpdateColumns { columns, .. } => !columns.is_empty(),
        _ => true,
    });
}

/// Fail closed when a policy expression references a column id the table
/// schema does not have (mirrors `validate_security_expression` in
/// `database.rs`).
fn validate_policy_columns(expression: &SecurityExpr, schema: &Schema) -> Result<()> {
    match expression {
        SecurityExpr::True => Ok(()),
        SecurityExpr::ColumnEqCurrentUser { column }
        | SecurityExpr::ColumnEqValue { column, .. } => {
            if schema
                .columns
                .iter()
                .any(|candidate| candidate.id == *column)
            {
                Ok(())
            } else {
                Err(MongrelError::InvalidArgument(format!(
                    "security expression references unknown column id {column}"
                )))
            }
        }
        SecurityExpr::And { left, right } | SecurityExpr::Or { left, right } => {
            validate_policy_columns(left, schema)?;
            validate_policy_columns(right, schema)
        }
        SecurityExpr::Not { expression } => validate_policy_columns(expression, schema),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::ColumnFlags;

    fn test_schema() -> Schema {
        Schema {
            schema_id: 0,
            columns: vec![
                ColumnDef {
                    id: 1,
                    name: "id".into(),
                    ty: TypeId::Int64,
                    flags: ColumnFlags::empty().with(ColumnFlags::PRIMARY_KEY),
                    default_value: None,
                    embedding_source: None,
                },
                ColumnDef {
                    id: 2,
                    name: "secret".into(),
                    ty: TypeId::Bytes,
                    flags: ColumnFlags::empty(),
                    default_value: None,
                    embedding_source: None,
                },
            ],
            indexes: vec![],
            colocation: vec![],
            constraints: Default::default(),
            clustered: false,
        }
    }

    fn catalog_with_table() -> Catalog {
        let mut catalog = Catalog::empty();
        let record = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::CreateTable {
                name: "t".into(),
                schema: test_schema(),
                created_epoch: 1,
            },
        );
        catalog.apply_command(&record).unwrap();
        catalog
    }

    #[test]
    fn create_table_allocates_id_and_stamps_schema() {
        let catalog = catalog_with_table();
        assert_eq!(catalog.catalog_version(), 1);
        let entry = catalog.live("t").unwrap();
        assert_eq!(entry.table_id, 0);
        assert_eq!(entry.schema.schema_id, 0);
        assert_eq!(catalog.next_table_id, 1);
        // The same schema id must not collide with a second table.
        let mut catalog = catalog;
        let record = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::CreateTable {
                name: "u".into(),
                schema: test_schema(),
                created_epoch: 2,
            },
        );
        catalog.apply_command(&record).unwrap();
        assert_eq!(catalog.live("u").unwrap().table_id, 1);
        assert_eq!(catalog.live("u").unwrap().schema.schema_id, 1);
    }

    #[test]
    fn duplicate_create_table_fails_closed() {
        let catalog = catalog_with_table();
        let delta = apply(
            &catalog,
            &CatalogCommand::CreateTable {
                name: "t".into(),
                schema: test_schema(),
                created_epoch: 9,
            },
        );
        assert!(matches!(delta, Err(MongrelError::InvalidArgument(_))));
    }

    #[test]
    fn version_gap_and_replay_guard() {
        let mut catalog = catalog_with_table();
        // Gap: version 3 when the next expected is 2.
        let gap = CatalogCommandRecord {
            version: CATALOG_COMMAND_FORMAT_VERSION,
            catalog_version: 3,
            command: CatalogCommand::DisableRls { table: "t".into() },
        };
        assert!(matches!(
            catalog.apply_command(&gap),
            Err(MongrelError::Conflict(_))
        ));
        // Replay of the exact recorded command is an idempotent no-op.
        let recorded = catalog.commands_since(0)[0].clone();
        let delta = catalog.apply_command(&recorded).unwrap();
        assert!(matches!(delta, CatalogDelta::NoOp));
        assert_eq!(catalog.catalog_version(), 1);
        // A different command claiming an already-applied version conflicts.
        let conflicting = CatalogCommandRecord {
            version: CATALOG_COMMAND_FORMAT_VERSION,
            catalog_version: 1,
            command: CatalogCommand::CreateTable {
                name: "other".into(),
                schema: test_schema(),
                created_epoch: 5,
            },
        };
        assert!(matches!(
            catalog.apply_command(&conflicting),
            Err(MongrelError::Conflict(_))
        ));
    }

    #[test]
    fn unsupported_encoding_version_fails_closed() {
        let mut catalog = catalog_with_table();
        let record = CatalogCommandRecord {
            version: CATALOG_COMMAND_FORMAT_VERSION + 1,
            catalog_version: 2,
            command: CatalogCommand::DisableRls { table: "t".into() },
        };
        assert!(matches!(
            catalog.apply_command(&record),
            Err(MongrelError::UnsupportedStorageVersion { .. })
        ));
    }

    #[test]
    fn encode_decode_round_trip() {
        let catalog = catalog_with_table();
        let record = &catalog.commands_since(0)[0];
        let bytes = encode_command(record).unwrap();
        let decoded = decode_command(&bytes).unwrap();
        assert_eq!(decoded.catalog_version, record.catalog_version);
        assert_eq!(decoded.version, CATALOG_COMMAND_FORMAT_VERSION);
        assert_eq!(encode_command(&decoded).unwrap(), bytes);
    }

    #[test]
    fn decode_rejects_unknown_version_and_variant() {
        let catalog = catalog_with_table();
        let bytes = encode_command(&catalog.commands_since(0)[0]).unwrap();
        let mut json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        json["version"] = serde_json::json!(99);
        assert!(matches!(
            decode_command(serde_json::to_string(&json).unwrap().as_bytes()),
            Err(MongrelError::UnsupportedStorageVersion { .. })
        ));
        let unknown = br#"{"version":1,"catalog_version":1,"command":{"NotACommand":{}}}"#;
        assert!(decode_command(unknown).is_err());
        let unknown_field =
            br#"{"version":1,"catalog_version":1,"command":{"DisableRls":{"table":"t"}},"x":1}"#;
        assert!(decode_command(unknown_field).is_err());
    }

    #[test]
    fn drop_table_cascades_security_and_roles() {
        let mut catalog = catalog_with_table();
        for command in [
            CatalogCommand::EnableRls { table: "t".into() },
            CatalogCommand::SetRowPolicy {
                policy: RowPolicy {
                    name: "p".into(),
                    table: "t".into(),
                    command: crate::security::PolicyCommand::Select,
                    subjects: vec![],
                    permissive: true,
                    using: Some(SecurityExpr::True),
                    with_check: None,
                },
            },
            CatalogCommand::SetColumnMask {
                mask: ColumnMask {
                    name: "m".into(),
                    table: "t".into(),
                    column: 2,
                    strategy: MaskStrategy::Sha256,
                    exempt_subjects: vec![],
                },
            },
            CatalogCommand::CreateRole {
                name: "r".into(),
                created_epoch: 1,
            },
            CatalogCommand::GrantPermission {
                role: "r".into(),
                permission: Permission::Select { table: "t".into() },
            },
        ] {
            let record = CatalogCommandRecord::next(&catalog, command);
            catalog.apply_command(&record).unwrap();
        }
        let security_before = catalog.security.clone();
        assert!(security_before.rls_enabled("t"));
        let record = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::DropTable {
                name: "t".into(),
                at_epoch: 7,
            },
        );
        catalog.apply_command(&record).unwrap();
        assert!(catalog.live("t").is_none());
        assert!(catalog.security.policies.is_empty());
        assert!(catalog.security.masks.is_empty());
        assert!(!catalog.security.rls_enabled("t"));
        assert!(catalog.roles[0].permissions.is_empty());
        assert!(matches!(
            catalog.tables[0].state,
            TableState::Dropped { at_epoch: 7 }
        ));
    }

    #[test]
    fn grant_and_revoke_permission_merge_columns() {
        let mut catalog = catalog_with_table();
        let commands = [
            CatalogCommand::CreateRole {
                name: "r".into(),
                created_epoch: 1,
            },
            CatalogCommand::GrantPermission {
                role: "r".into(),
                permission: Permission::SelectColumns {
                    table: "t".into(),
                    columns: vec!["id".into()],
                },
            },
            CatalogCommand::GrantPermission {
                role: "r".into(),
                permission: Permission::SelectColumns {
                    table: "t".into(),
                    columns: vec!["secret".into(), "id".into()],
                },
            },
        ];
        for command in commands {
            let record = CatalogCommandRecord::next(&catalog, command);
            catalog.apply_command(&record).unwrap();
        }
        assert_eq!(
            catalog.roles[0].permissions,
            vec![Permission::SelectColumns {
                table: "t".into(),
                columns: vec!["id".into(), "secret".into()],
            }]
        );
        let record = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::RevokePermission {
                role: "r".into(),
                permission: Permission::SelectColumns {
                    table: "t".into(),
                    columns: vec!["id".into()],
                },
            },
        );
        catalog.apply_command(&record).unwrap();
        assert_eq!(
            catalog.roles[0].permissions,
            vec![Permission::SelectColumns {
                table: "t".into(),
                columns: vec!["secret".into()],
            }]
        );
    }

    #[test]
    fn job_state_terminal_guard() {
        let mut catalog = Catalog::empty();
        let submit = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::SubmitJob {
                job: JobDefinition {
                    job_id: 1,
                    kind: JobKind::IndexBuild,
                    state: JobState::Pending,
                    target: Some("t".into()),
                    created_epoch: 1,
                    updated_epoch: 1,
                },
            },
        );
        catalog.apply_command(&submit).unwrap();
        let run = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::SetJobState {
                job_id: 1,
                state: JobState::Succeeded,
                at_epoch: 2,
            },
        );
        catalog.apply_command(&run).unwrap();
        let late = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::SetJobState {
                job_id: 1,
                state: JobState::Running,
                at_epoch: 3,
            },
        );
        assert!(matches!(
            catalog.apply_command(&late),
            Err(MongrelError::InvalidArgument(_))
        ));
    }

    #[test]
    fn history_is_bounded() {
        let mut catalog = Catalog::empty();
        for index in 0..(COMMAND_HISTORY_LIMIT + 10) {
            let record = CatalogCommandRecord::next(
                &catalog,
                CatalogCommand::SubmitJob {
                    job: JobDefinition {
                        job_id: index as u64 + 1,
                        kind: JobKind::SchemaValidation,
                        state: JobState::Pending,
                        target: None,
                        created_epoch: 0,
                        updated_epoch: 0,
                    },
                },
            );
            catalog.apply_command(&record).unwrap();
        }
        assert_eq!(catalog.command_log.len(), COMMAND_HISTORY_LIMIT);
        let retained = catalog.commands_since(0);
        assert_eq!(retained.len(), COMMAND_HISTORY_LIMIT);
        // The compacted prefix is gone; the tail is contiguous.
        let oldest = retained[0].catalog_version;
        assert_eq!(
            oldest,
            catalog.catalog_version() - COMMAND_HISTORY_LIMIT as u64 + 1
        );
        // Replaying a compacted command is treated as already applied.
        let compacted = CatalogCommandRecord {
            version: CATALOG_COMMAND_FORMAT_VERSION,
            catalog_version: 1,
            command: CatalogCommand::SubmitJob {
                job: JobDefinition {
                    job_id: 999_999,
                    kind: JobKind::KeyRotation,
                    state: JobState::Pending,
                    target: None,
                    created_epoch: 0,
                    updated_epoch: 0,
                },
            },
        };
        let delta = catalog.apply_command(&compacted).unwrap();
        assert!(matches!(delta, CatalogDelta::NoOp));
        assert!(catalog
            .job_definitions
            .iter()
            .all(|job| job.job_id != 999_999));
    }

    #[test]
    fn required_permission_matches_legacy_gates() {
        assert_eq!(
            required_permission(&CatalogCommand::CreateTable {
                name: "t".into(),
                schema: test_schema(),
                created_epoch: 0,
            }),
            Permission::Ddl
        );
        assert_eq!(
            required_permission(&CatalogCommand::EnableRls { table: "t".into() }),
            Permission::Admin
        );
        assert_eq!(
            required_permission(&CatalogCommand::DropUser {
                username: "u".into()
            }),
            Permission::Admin
        );
        assert_eq!(
            required_permission(&CatalogCommand::DropTrigger { name: "t".into() }),
            Permission::Ddl
        );
    }

    fn replacement_trigger(name: &str) -> StoredTrigger {
        StoredTrigger::new(
            name,
            crate::trigger::TriggerDefinition {
                target: TriggerTarget::Table("t".into()),
                timing: crate::trigger::TriggerTiming::After,
                event: crate::trigger::TriggerEvent::Insert,
                update_of: Vec::new(),
                target_columns: Vec::new(),
                when: None,
                program: crate::trigger::TriggerProgram { steps: Vec::new() },
            },
            0,
        )
        .unwrap()
    }

    fn replacement_procedure(name: &str) -> StoredProcedure {
        StoredProcedure::new(
            name,
            crate::procedure::ProcedureMode::ReadOnly,
            Vec::new(),
            crate::procedure::ProcedureBody {
                steps: Vec::new(),
                return_value: crate::procedure::ProcedureValue::Literal(
                    crate::memtable::Value::Null,
                ),
            },
            0,
        )
        .unwrap()
    }

    #[test]
    fn alter_column_delta_bumps_schema_id_like_the_engine() {
        let mut catalog = catalog_with_table();
        let mut column = catalog.live("t").unwrap().schema.columns[1].clone();
        column.flags = column.flags.with(crate::schema::ColumnFlags::NULLABLE);
        let record = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::AlterColumn {
                table: "t".into(),
                column: column.clone(),
            },
        );
        let delta = catalog.apply_command(&record).unwrap();
        // The engine bumps `schema_id` on every applied alteration; the
        // resolved delta reproduces that image so replay stays deterministic.
        let CatalogDelta::SchemaReplaced { table_id, schema } = delta else {
            panic!("expected SchemaReplaced");
        };
        assert_eq!(table_id, 0);
        assert_eq!(schema.schema_id, 1);
        assert_eq!(schema.columns[1], column);
        assert_eq!(catalog.live("t").unwrap().schema.schema_id, 1);
        // A column id the table does not have fails closed.
        let mut unknown = column;
        unknown.id = 99;
        assert!(matches!(
            apply(
                &catalog,
                &CatalogCommand::AlterColumn {
                    table: "t".into(),
                    column: unknown,
                },
            ),
            Err(MongrelError::ColumnNotFound(_))
        ));
    }

    #[test]
    fn replace_trigger_upserts_and_validates_references() {
        let mut catalog = catalog_with_table();
        // Create-or-replace on an absent name inserts...
        let record = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::ReplaceTrigger {
                trigger: replacement_trigger("trg"),
            },
        );
        let delta = catalog.apply_command(&record).unwrap();
        assert!(matches!(delta, CatalogDelta::TriggerUpserted(_)));
        assert_eq!(catalog.triggers.len(), 1);
        // ...and on a present name replaces in place (no duplicate, no
        // must-not-exist rejection like CreateTrigger).
        let mut resolved = replacement_trigger("trg");
        resolved.version = 2;
        resolved.updated_epoch = 7;
        let record = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::ReplaceTrigger {
                trigger: resolved.clone(),
            },
        );
        catalog.apply_command(&record).unwrap();
        assert_eq!(catalog.triggers.len(), 1);
        assert_eq!(catalog.triggers[0].trigger.version, 2);
        assert_eq!(catalog.triggers[0].trigger.updated_epoch, 7);
        // A trigger targeting an unknown table fails closed.
        let mut dangling = replacement_trigger("trg2");
        dangling.target = TriggerTarget::Table("missing".into());
        assert!(matches!(
            apply(
                &catalog,
                &CatalogCommand::ReplaceTrigger { trigger: dangling }
            ),
            Err(MongrelError::InvalidArgument(_))
        ));
        assert_eq!(
            required_permission(&CatalogCommand::ReplaceTrigger {
                trigger: replacement_trigger("trg"),
            }),
            Permission::Ddl
        );
    }

    #[test]
    fn replace_procedure_upserts_and_validates_references() {
        let mut catalog = catalog_with_table();
        let record = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::ReplaceProcedure {
                procedure: replacement_procedure("proc"),
            },
        );
        let delta = catalog.apply_command(&record).unwrap();
        assert!(matches!(delta, CatalogDelta::ProcedureUpserted(_)));
        assert_eq!(catalog.procedures.len(), 1);
        // Replacement is keyed by name and replays the resolved image
        // verbatim (no must-not-exist rejection like CreateProcedure).
        let mut resolved = replacement_procedure("proc");
        resolved.version = 3;
        resolved.updated_epoch = 9;
        let record = CatalogCommandRecord::next(
            &catalog,
            CatalogCommand::ReplaceProcedure {
                procedure: resolved.clone(),
            },
        );
        catalog.apply_command(&record).unwrap();
        assert_eq!(catalog.procedures.len(), 1);
        assert_eq!(catalog.procedures[0].procedure.version, 3);
        assert_eq!(catalog.procedures[0].procedure.updated_epoch, 9);
        assert_eq!(
            required_permission(&CatalogCommand::ReplaceProcedure {
                procedure: replacement_procedure("proc"),
            }),
            Permission::Ddl
        );
    }

    #[test]
    fn serde_appended_variants_decode_older_records() {
        // Records encoded before ReplaceTrigger/ReplaceProcedure existed
        // decode unchanged (the new variants are appended, never renumbered).
        let legacy = br#"{"version":1,"catalog_version":1,"command":{"DropTrigger":{"name":"t"}}}"#;
        let record = decode_command(legacy).unwrap();
        assert!(matches!(
            record.command,
            CatalogCommand::DropTrigger { ref name } if name == "t"
        ));
        // The new variants round-trip through the versioned envelope.
        let record = CatalogCommandRecord {
            version: CATALOG_COMMAND_FORMAT_VERSION,
            catalog_version: 2,
            command: CatalogCommand::ReplaceTrigger {
                trigger: replacement_trigger("trg"),
            },
        };
        let bytes = encode_command(&record).unwrap();
        let decoded = decode_command(&bytes).unwrap();
        assert_eq!(encode_command(&decoded).unwrap(), bytes);
    }
}
