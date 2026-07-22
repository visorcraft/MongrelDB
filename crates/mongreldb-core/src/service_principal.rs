//! Authenticated service-principal definitions for shared-handle open (P0.1).

use std::collections::BTreeMap;
use std::time::{SystemTime, UNIX_EPOCH};

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use parking_lot::RwLock;

use crate::auth::Permission;
use crate::error::{MongrelError, Result};

#[derive(Debug, Clone)]
pub struct ServicePrincipalDefinition {
    pub token_id: String,
    pub principal_id: [u8; 16],
    pub creation_version: u64,
    pub permissions: Vec<Permission>,
    secret_hash_phc: String,
    pub expires_unix: u64,
    pub revoked: bool,
    pub disabled: bool,
}

impl ServicePrincipalDefinition {
    pub fn mint(
        token_id: impl Into<String>,
        principal_id: [u8; 16],
        creation_version: u64,
        permissions: Vec<Permission>,
        raw_secret: &str,
        expires_unix: u64,
    ) -> Result<Self> {
        let mut salt_bytes = [0_u8; 16];
        getrandom::getrandom(&mut salt_bytes)
            .map_err(|e| MongrelError::InvalidArgument(format!("service principal salt: {e}")))?;
        let salt = SaltString::encode_b64(&salt_bytes).map_err(|e| {
            MongrelError::InvalidArgument(format!("service principal salt encode: {e}"))
        })?;
        let secret_hash_phc = Argon2::default()
            .hash_password(raw_secret.as_bytes(), &salt)
            .map_err(|e| MongrelError::InvalidArgument(format!("service principal hash: {e}")))?
            .to_string();
        Ok(Self {
            token_id: token_id.into(),
            principal_id,
            creation_version,
            permissions,
            secret_hash_phc,
            expires_unix,
            revoked: false,
            disabled: false,
        })
    }

    pub fn verify_secret(&self, raw_secret: &str, now_unix: u64) -> bool {
        if self.revoked || self.disabled {
            return false;
        }
        if self.expires_unix != 0 && now_unix > self.expires_unix {
            return false;
        }
        let Ok(hash) = PasswordHash::new(&self.secret_hash_phc) else {
            return false;
        };
        Argon2::default()
            .verify_password(raw_secret.as_bytes(), &hash)
            .is_ok()
    }
}

#[derive(Debug, Default)]
pub struct ServicePrincipalStore {
    next_version: std::sync::atomic::AtomicU64,
    entries: RwLock<BTreeMap<String, ServicePrincipalDefinition>>,
}

impl ServicePrincipalStore {
    pub fn new() -> Self {
        Self::default()
    }

    fn now_unix() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    pub fn register(
        &self,
        token_id: impl Into<String>,
        principal_id: [u8; 16],
        permissions: Vec<Permission>,
        raw_secret: &str,
        expires_unix: u64,
    ) -> Result<ServicePrincipalDefinition> {
        let token_id = token_id.into();
        let creation_version = self
            .next_version
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
            + 1;
        let def = ServicePrincipalDefinition::mint(
            token_id.clone(),
            principal_id,
            creation_version,
            permissions,
            raw_secret,
            expires_unix,
        )?;
        self.entries.write().insert(token_id, def.clone());
        Ok(def)
    }

    pub fn revoke(&self, token_id: &str) -> Result<()> {
        let mut entries = self.entries.write();
        let entry = entries
            .get_mut(token_id)
            .ok_or_else(|| MongrelError::NotFound(format!("service token {token_id}")))?;
        entry.revoked = true;
        Ok(())
    }

    pub fn set_permissions(&self, token_id: &str, permissions: Vec<Permission>) -> Result<()> {
        let mut entries = self.entries.write();
        let entry = entries
            .get_mut(token_id)
            .ok_or_else(|| MongrelError::NotFound(format!("service token {token_id}")))?;
        if entry.revoked || entry.disabled {
            return Err(MongrelError::InvalidCredentials {
                username: format!("service:{token_id}"),
            });
        }
        entry.permissions = permissions;
        Ok(())
    }

    pub fn authenticate(
        &self,
        token_id: &str,
        raw_secret: &str,
    ) -> Result<ServicePrincipalDefinition> {
        let entries = self.entries.read();
        let entry = entries
            .get(token_id)
            .ok_or_else(|| MongrelError::InvalidCredentials {
                username: format!("service:{token_id}"),
            })?;
        if !entry.verify_secret(raw_secret, Self::now_unix()) {
            return Err(MongrelError::InvalidCredentials {
                username: format!("service:{token_id}"),
            });
        }
        Ok(entry.clone())
    }

    pub fn resolve_live(
        &self,
        token_id: &str,
        principal_id: [u8; 16],
        creation_version: u64,
    ) -> Result<ServicePrincipalDefinition> {
        let entries = self.entries.read();
        let entry = entries
            .get(token_id)
            .ok_or_else(|| MongrelError::InvalidCredentials {
                username: format!("service:{token_id}"),
            })?;
        if entry.principal_id != principal_id || entry.creation_version != creation_version {
            return Err(MongrelError::InvalidCredentials {
                username: format!("service:{token_id}"),
            });
        }
        if entry.revoked || entry.disabled {
            return Err(MongrelError::InvalidCredentials {
                username: format!("service:{token_id}"),
            });
        }
        if entry.expires_unix != 0 && Self::now_unix() > entry.expires_unix {
            return Err(MongrelError::InvalidCredentials {
                username: format!("service:{token_id}"),
            });
        }
        Ok(entry.clone())
    }
}
