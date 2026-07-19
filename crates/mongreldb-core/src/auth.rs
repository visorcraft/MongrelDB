//! User, role, and credential management for MongrelDB's catalog-level auth.
//!
//! Users and roles are stored in the engine's `Catalog` struct (alongside
//! procedures and triggers), persisted via `catalog::write_atomic`. This
//! module provides the types and the Argon2id password hashing layer.
//!
//! The daemon (`mongreldb-server`) authenticates HTTP requests via HTTP Basic
//! auth (username:password) when `--auth-users` is enabled. Each authenticated
//! request carries a [`Principal`] in its extensions; permission checks use
//! [`Database::check_permission`].

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

/// Stored verifier for MySQL 8's `caching_sha2_password` fast-auth exchange.
///
/// This is SHA256(SHA256(password)), never the plaintext password.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MysqlCachingSha2Verifier {
    stage2: [u8; 32],
}

impl MysqlCachingSha2Verifier {
    pub fn from_password(password: &str) -> Self {
        let stage1 = Sha256::digest(password.as_bytes());
        Self {
            stage2: Sha256::digest(stage1).into(),
        }
    }

    pub fn verify(&self, nonce: &[u8], proof: &[u8]) -> bool {
        if proof.is_empty() {
            return bool::from(self.stage2.ct_eq(&Self::from_password("").stage2));
        }
        let Ok(proof) = <&[u8; 32]>::try_from(proof) else {
            return false;
        };
        let mask = Sha256::new()
            .chain_update(self.stage2)
            .chain_update(nonce)
            .finalize();
        let mut stage1 = [0_u8; 32];
        for (output, (proof, mask)) in stage1.iter_mut().zip(proof.iter().zip(mask)) {
            *output = proof ^ mask;
        }
        let candidate: [u8; 32] = Sha256::digest(stage1).into();
        bool::from(candidate.ct_eq(&self.stage2))
    }
}

/// A stored user with Argon2id-hashed credentials.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UserEntry {
    /// Stable, monotonic user id.
    pub id: u64,
    /// Unique username (case-sensitive).
    pub username: String,
    /// Argon2id PHC string (includes salt + params; verifiable via
    /// `PasswordVerifier::verify`).
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub password_hash: String,
    /// SCRAM-SHA-256 verifier for native challenge/response authentication.
    /// Older catalogs and callers that supplied only a PHC hash have none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scram_sha_256: Option<crate::security_hardening::ScramVerifier>,
    /// MySQL wire compatibility verifier. Authentication still produces the
    /// same live catalog principal and requires TLS.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mysql_caching_sha2: Option<MysqlCachingSha2Verifier>,
    /// Role names granted to this user.
    #[serde(default)]
    pub roles: Vec<String>,
    /// Bypasses all permission checks (full admin).
    #[serde(default)]
    pub is_admin: bool,
    /// Epoch at which the user was created.
    pub created_epoch: u64,
}

/// A named collection of permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoleEntry {
    /// Unique role name (case-sensitive).
    pub name: String,
    /// Permissions granted to this role.
    #[serde(default)]
    pub permissions: Vec<Permission>,
    /// Epoch at which the role was created.
    pub created_epoch: u64,
}

/// A permission granted to a role. Mirrors SQL `GRANT` / `REVOKE`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Permission {
    /// All permissions on all tables (`GRANT ALL`).
    All,
    /// `SELECT` on a specific table.
    Select { table: String },
    /// `INSERT` on a specific table.
    Insert { table: String },
    /// `UPDATE` on a specific table.
    Update { table: String },
    /// `DELETE` on a specific table.
    Delete { table: String },
    /// `SELECT` limited to named columns.
    SelectColumns { table: String, columns: Vec<String> },
    /// `INSERT` limited to named columns.
    InsertColumns { table: String, columns: Vec<String> },
    /// `UPDATE` limited to named columns.
    UpdateColumns { table: String, columns: Vec<String> },
    /// DDL: `CREATE` / `DROP` / `ALTER TABLE`.
    Ddl,
    /// Admin: `CREATE USER` / `GRANT` / `REVOKE` / `CREATE ROLE`.
    Admin,
}

impl Permission {
    /// Check whether this permission satisfies a required permission.
    ///
    /// `All` satisfies every non-admin permission (DDL + all table-level
    /// operations) but does **not** satisfy `Admin` — user/role management
    /// is gated behind `is_admin = true` on the principal, not grantable via
    /// `Permission::All` (spec §9 decision 2). `Select { table: "*" }` is
    /// not wildcarded (it matches a table literally named `*`).
    pub fn satisfies(&self, required: &Permission) -> bool {
        match (self, required) {
            // All grants every non-admin permission.
            (Permission::All, Permission::Admin) => false,
            (Permission::All, _) => true,
            (Permission::Admin, Permission::Admin) => true,
            (Permission::Ddl, Permission::Ddl) => true,
            (Permission::Select { table: a }, Permission::Select { table: b }) => a == b,
            (Permission::Insert { table: a }, Permission::Insert { table: b }) => a == b,
            (Permission::Update { table: a }, Permission::Update { table: b }) => a == b,
            (Permission::Delete { table: a }, Permission::Delete { table: b }) => a == b,
            (
                Permission::SelectColumns {
                    table: a,
                    columns: granted,
                },
                Permission::SelectColumns {
                    table: b,
                    columns: required,
                },
            )
            | (
                Permission::InsertColumns {
                    table: a,
                    columns: granted,
                },
                Permission::InsertColumns {
                    table: b,
                    columns: required,
                },
            )
            | (
                Permission::UpdateColumns {
                    table: a,
                    columns: granted,
                },
                Permission::UpdateColumns {
                    table: b,
                    columns: required,
                },
            ) => a == b && required.iter().all(|column| granted.contains(column)),
            _ => false,
        }
    }
}

impl std::fmt::Display for Permission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Permission::All => write!(f, "ALL"),
            Permission::Admin => write!(f, "ADMIN"),
            Permission::Ddl => write!(f, "DDL"),
            Permission::Select { table } => write!(f, "SELECT ON {table}"),
            Permission::Insert { table } => write!(f, "INSERT ON {table}"),
            Permission::Update { table } => write!(f, "UPDATE ON {table}"),
            Permission::Delete { table } => write!(f, "DELETE ON {table}"),
            Permission::SelectColumns { table, columns } => {
                write!(f, "SELECT ({}) ON {table}", columns.join(", "))
            }
            Permission::InsertColumns { table, columns } => {
                write!(f, "INSERT ({}) ON {table}", columns.join(", "))
            }
            Permission::UpdateColumns { table, columns } => {
                write!(f, "UPDATE ({}) ON {table}", columns.join(", "))
            }
        }
    }
}

/// The authenticated identity for a single HTTP request. Injected by the
/// auth middleware into request extensions.
#[derive(Debug, Clone)]
pub struct Principal {
    /// Immutable catalog identity. Username reuse must not revive a principal.
    pub user_id: u64,
    /// User generation paired with `user_id`.
    pub created_epoch: u64,
    pub username: String,
    pub is_admin: bool,
    pub roles: Vec<String>,
    /// All permissions from all roles the user belongs to, pre-resolved.
    pub permissions: Vec<Permission>,
}

impl Principal {
    /// Check whether this principal has the required permission.
    pub fn has_permission(&self, required: &Permission) -> bool {
        if self.is_admin {
            return true;
        }
        self.permissions.iter().any(|p| p.satisfies(required))
    }

    pub fn column_access(&self, table: &str, operation: ColumnOperation) -> ColumnAccess {
        if self.is_admin
            || self
                .permissions
                .iter()
                .any(|permission| matches!(permission, Permission::All))
        {
            return ColumnAccess::All;
        }
        let full = self
            .permissions
            .iter()
            .any(|permission| match (operation, permission) {
                (ColumnOperation::Select, Permission::Select { table: granted })
                | (ColumnOperation::Insert, Permission::Insert { table: granted })
                | (ColumnOperation::Update, Permission::Update { table: granted }) => {
                    granted == table
                }
                _ => false,
            });
        if full {
            return ColumnAccess::All;
        }
        let mut columns = Vec::new();
        for permission in &self.permissions {
            let grant = match (operation, permission) {
                (
                    ColumnOperation::Select,
                    Permission::SelectColumns {
                        table: granted,
                        columns,
                    },
                )
                | (
                    ColumnOperation::Insert,
                    Permission::InsertColumns {
                        table: granted,
                        columns,
                    },
                )
                | (
                    ColumnOperation::Update,
                    Permission::UpdateColumns {
                        table: granted,
                        columns,
                    },
                ) if granted == table => Some(columns),
                _ => None,
            };
            if let Some(grant) = grant {
                for column in grant {
                    if !columns.contains(column) {
                        columns.push(column.clone());
                    }
                }
            }
        }
        if columns.is_empty() {
            ColumnAccess::Denied
        } else {
            ColumnAccess::Columns(columns)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnOperation {
    Select,
    Insert,
    Update,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColumnAccess {
    All,
    Columns(Vec<String>),
    Denied,
}

// ── Password hashing (Argon2id) ──────────────────────────────────────────

/// Hash a password using Argon2id with a fresh random salt.
///
/// Returns a PHC string that encodes the algorithm, version, parameters, salt,
/// and hash — suitable for storage as `UserEntry::password_hash` and verifiable
/// via [`verify_password`].
pub fn hash_password(password: &str) -> Result<String, String> {
    use argon2::{
        password_hash::{PasswordHasher, SaltString},
        Algorithm, Argon2, Version,
    };
    use getrandom::getrandom;
    // Reuse the same OWASP-minimum parameters as the encryption KEK derivation.
    let params = argon2::Params::new(19 * 1024, 2, 1, None).map_err(|e| e.to_string())?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    // Generate salt via getrandom (always available in core, no feature gate).
    let mut salt_bytes = [0u8; 32];
    getrandom(&mut salt_bytes).map_err(|e| e.to_string())?;
    let salt = SaltString::encode_b64(&salt_bytes).map_err(|e| e.to_string())?;
    let hash = argon2
        .hash_password(password.as_bytes(), &salt)
        .map_err(|e| e.to_string())?;
    Ok(hash.to_string())
}

/// Build independently salted SCRAM-SHA-256 verifier material.
pub fn scram_verifier(password: &str) -> Result<crate::security_hardening::ScramVerifier, String> {
    let mut salt = [0_u8; 16];
    getrandom::getrandom(&mut salt).map_err(|error| error.to_string())?;
    crate::security_hardening::ScramVerifier::from_password(
        password,
        &salt,
        crate::security_hardening::SCRAM_SHA_256_MIN_ITERATIONS,
    )
    .map_err(|error| error.to_string())
}

/// Verify a password against a stored PHC hash. Returns `Ok(true)` on match,
/// `Ok(false)` on mismatch, `Err` on malformed hash.
pub fn verify_password(password: &str, phc_hash: &str) -> Result<bool, String> {
    use argon2::{password_hash::PasswordVerifier, Argon2};
    let parsed_hash =
        argon2::PasswordHash::new(phc_hash).map_err(|e| format!("malformed hash: {e}"))?;
    Ok(Argon2::default()
        .verify_password(password.as_bytes(), &parsed_hash)
        .is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_hash_round_trip() {
        let password = "correct horse battery staple";
        let hash = hash_password(password).unwrap();
        assert!(verify_password(password, &hash).unwrap());
        assert!(!verify_password("wrong password", &hash).unwrap());
    }

    #[test]
    fn caching_sha2_verifier_accepts_only_matching_proof() {
        let verifier = MysqlCachingSha2Verifier::from_password("password");
        let nonce = b"12345678901234567890";
        let stage1 = Sha256::digest(b"password");
        let stage2 = Sha256::digest(stage1);
        let mask = Sha256::new()
            .chain_update(stage2)
            .chain_update(nonce)
            .finalize();
        let proof = stage1
            .iter()
            .zip(mask)
            .map(|(left, right)| left ^ right)
            .collect::<Vec<_>>();
        assert!(verifier.verify(nonce, &proof));
        assert!(!verifier.verify(nonce, &[0; 32]));
    }

    #[test]
    fn permission_satisfies() {
        // All satisfies DDL and table-level permissions...
        assert!(Permission::All.satisfies(&Permission::Ddl));
        assert!(Permission::All.satisfies(&Permission::Select { table: "t".into() }));
        assert!(Permission::All.satisfies(&Permission::Insert { table: "t".into() }));
        // ...but NOT Admin (spec §9 decision 2 — only is_admin grants admin).
        assert!(!Permission::All.satisfies(&Permission::Admin));
        // Exact table match.
        assert!(Permission::Select { table: "t".into() }
            .satisfies(&Permission::Select { table: "t".into() }));
        assert!(
            !Permission::Select { table: "t".into() }.satisfies(&Permission::Select {
                table: "other".into()
            })
        );
        // Cross-kind never satisfies.
        assert!(!Permission::Select { table: "t".into() }
            .satisfies(&Permission::Insert { table: "t".into() }));
        // Ddl does not satisfy Admin either.
        assert!(!Permission::Ddl.satisfies(&Permission::Admin));
    }

    #[test]
    fn principal_admin_bypasses_checks() {
        let principal = Principal {
            user_id: 0,
            created_epoch: 0,
            username: "admin".into(),
            is_admin: true,
            roles: vec![],
            permissions: vec![],
        };
        assert!(principal.has_permission(&Permission::Admin));
        assert!(principal.has_permission(&Permission::Select {
            table: "anything".into()
        }));
    }
}
