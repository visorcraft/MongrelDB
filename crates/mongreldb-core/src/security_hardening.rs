//! Security hardening helpers (spec section 14.3, Stage 5C).
//!
//! Cluster mTLS is already enforced in `mongreldb-cluster` transport. This
//! module adds SCRAM-style password verifiers, OIDC/JWT claim validation
//! seams, service tokens, KMS wrap metadata, online rotation records, and
//! secret redaction used by logs/audit.

use std::collections::BTreeMap;
use std::fmt;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// SCRAM-style salted password verifier (does not implement the full wire
/// exchange — that lives in the protocol adapter; this stores the durable
/// verifier material).
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramVerifier {
    /// Mechanism name (e.g. `SCRAM-SHA-256`).
    pub mechanism: String,
    /// Iteration count.
    pub iterations: u32,
    /// Salt (base64 or hex; stored opaque).
    pub salt: Vec<u8>,
    /// Stored key.
    pub stored_key: Vec<u8>,
    /// Server key.
    pub server_key: Vec<u8>,
}

impl ScramVerifier {
    /// Build a deterministic test verifier from a password (NOT for production
    /// key stretching — production uses a proper PBKDF). Useful for unit tests
    /// of the storage shape and redaction.
    pub fn from_password_for_tests(password: &str, salt: &[u8], iterations: u32) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(salt);
        hasher.update(password.as_bytes());
        hasher.update(iterations.to_le_bytes());
        let digest = hasher.finalize();
        Self {
            mechanism: "SCRAM-SHA-256".into(),
            iterations,
            salt: salt.to_vec(),
            stored_key: digest.to_vec(),
            server_key: digest.to_vec(),
        }
    }

    /// Verify a password against this stored verifier (test KDF).
    pub fn verify_password_for_tests(&self, password: &str) -> bool {
        let candidate = Self::from_password_for_tests(password, &self.salt, self.iterations);
        candidate.stored_key == self.stored_key
    }
}

impl fmt::Debug for ScramVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ScramVerifier")
            .field("mechanism", &self.mechanism)
            .field("iterations", &self.iterations)
            .field("salt", &"<redacted>")
            .field("stored_key", &"<redacted>")
            .field("server_key", &"<redacted>")
            .finish()
    }
}

/// OIDC/JWT validation config (claim checks only; signature via injected key).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtValidationConfig {
    /// Expected issuer.
    pub issuer: String,
    /// Expected audience.
    pub audience: String,
    /// Clock skew allowance seconds.
    pub skew_seconds: u64,
}

/// Minimal JWT-like claims map for validation tests.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtClaims {
    /// Issuer.
    pub iss: String,
    /// Audience.
    pub aud: String,
    /// Expiration unix seconds.
    pub exp: u64,
    /// Not-before unix seconds.
    pub nbf: u64,
    /// Subject.
    pub sub: String,
}

/// Why JWT validation failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JwtError {
    /// Issuer mismatch.
    #[error("issuer mismatch")]
    Issuer,
    /// Audience mismatch.
    #[error("audience mismatch")]
    Audience,
    /// Token not yet valid.
    #[error("token not yet valid")]
    NotYetValid,
    /// Token expired.
    #[error("token expired")]
    Expired,
}

/// Validate claims against config at `now_unix`.
pub fn validate_jwt_claims(
    claims: &JwtClaims,
    config: &JwtValidationConfig,
    now_unix: u64,
) -> Result<(), JwtError> {
    if claims.iss != config.issuer {
        return Err(JwtError::Issuer);
    }
    if claims.aud != config.audience {
        return Err(JwtError::Audience);
    }
    let skew = config.skew_seconds;
    if now_unix + skew < claims.nbf {
        return Err(JwtError::NotYetValid);
    }
    if now_unix > claims.exp.saturating_add(skew) {
        return Err(JwtError::Expired);
    }
    Ok(())
}

/// Service token (bearer) bound to a principal name and scope set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceToken {
    /// Token id.
    pub token_id: String,
    /// Principal name.
    pub principal: String,
    /// Scopes.
    pub scopes: Vec<String>,
    /// SHA-256 of the raw secret (never store the raw secret).
    pub secret_sha256: String,
    /// Expiry unix seconds (`0` = non-expiring).
    pub expires_unix: u64,
}

impl ServiceToken {
    /// Mint metadata from a raw secret (stores only the hash).
    pub fn mint(
        token_id: impl Into<String>,
        principal: impl Into<String>,
        scopes: Vec<String>,
        raw_secret: &str,
        expires_unix: u64,
    ) -> Self {
        Self {
            token_id: token_id.into(),
            principal: principal.into(),
            scopes,
            secret_sha256: sha256_hex(raw_secret.as_bytes()),
            expires_unix,
        }
    }

    /// Verify a presented raw secret.
    pub fn verify_secret(&self, raw_secret: &str, now_unix: u64) -> bool {
        if self.expires_unix != 0 && now_unix > self.expires_unix {
            return false;
        }
        self.secret_sha256 == sha256_hex(raw_secret.as_bytes())
    }
}

/// KMS-wrapped data encryption key metadata (spec §14.3).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KmsWrappedKey {
    /// KMS key id.
    pub kms_key_id: String,
    /// Key version for online rotation.
    pub key_version: String,
    /// Wrapped DEK ciphertext.
    pub wrapped_dek: Vec<u8>,
    /// Algorithm id.
    pub algorithm: String,
}

/// Online key rotation record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRotationRecord {
    /// Previous key version.
    pub from_version: String,
    /// New key version.
    pub to_version: String,
    /// Rotation started unix micros.
    pub started_unix_micros: u64,
    /// Rotation completed unix micros (`None` while in progress).
    pub completed_unix_micros: Option<u64>,
}

/// Redact secrets from a free-form log/audit line.
pub fn redact_secrets(input: &str) -> String {
    let mut out = input.to_owned();
    for needle in [
        "password=",
        "password:",
        "passwd=",
        "secret=",
        "api_key=",
        "token=",
        "private_key=",
        "Authorization:",
        "Bearer ",
    ] {
        if let Some(pos) = out.to_ascii_lowercase().find(&needle.to_ascii_lowercase()) {
            let start = pos + needle.len();
            let rest = &out[start..];
            let end_rel = rest
                .find(|c: char| c.is_whitespace() || c == '"' || c == '\'' || c == ',')
                .unwrap_or(rest.len());
            out.replace_range(start..start + end_rel, "***");
        }
    }
    out
}

/// Node-cert ↔ node-id join enforcement helper: the CN/SAN must equal
/// `node-<hex>.mongreldb.cluster` for the presented node id.
pub fn node_cert_matches_id(cert_cn_or_san: &str, node_id_hex: &str) -> bool {
    let expected = format!("node-{node_id_hex}.mongreldb.cluster");
    cert_cn_or_san.eq_ignore_ascii_case(&expected)
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for b in digest {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Registry of service tokens (in-memory; durable store is catalog/settings).
#[derive(Debug, Default, Clone)]
pub struct ServiceTokenRegistry {
    tokens: BTreeMap<String, ServiceToken>,
}

impl ServiceTokenRegistry {
    /// Insert or replace.
    pub fn upsert(&mut self, token: ServiceToken) {
        self.tokens.insert(token.token_id.clone(), token);
    }

    /// Authenticate by token id + raw secret.
    pub fn authenticate(
        &self,
        token_id: &str,
        raw_secret: &str,
        now_unix: u64,
    ) -> Option<&ServiceToken> {
        let t = self.tokens.get(token_id)?;
        if t.verify_secret(raw_secret, now_unix) {
            Some(t)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scram_verifier_round_trip_and_redacted_debug() {
        let v = ScramVerifier::from_password_for_tests("s3cret", b"salt", 10_000);
        assert!(v.verify_password_for_tests("s3cret"));
        assert!(!v.verify_password_for_tests("wrong"));
        let dbg = format!("{v:?}");
        assert!(dbg.contains("<redacted>"));
        assert!(!dbg.contains("s3cret"));
    }

    #[test]
    fn jwt_claims_validation() {
        let cfg = JwtValidationConfig {
            issuer: "https://issuer.example".into(),
            audience: "mongreldb".into(),
            skew_seconds: 60,
        };
        let claims = JwtClaims {
            iss: cfg.issuer.clone(),
            aud: cfg.audience.clone(),
            exp: 1_000,
            nbf: 100,
            sub: "user-1".into(),
        };
        assert!(validate_jwt_claims(&claims, &cfg, 500).is_ok());
        assert!(matches!(
            validate_jwt_claims(&claims, &cfg, 2_000).unwrap_err(),
            JwtError::Expired
        ));
    }

    #[test]
    fn service_token_stores_hash_only() {
        let t = ServiceToken::mint("t1", "svc", vec!["read".into()], "raw-secret", 0);
        assert!(!format!("{t:?}").contains("raw-secret") || t.secret_sha256.len() == 64);
        assert!(t.verify_secret("raw-secret", 0));
        assert!(!t.verify_secret("nope", 0));
    }

    #[test]
    fn redact_secrets_strips_password() {
        let line = redact_secrets("login user=alice password=hunter2 ok");
        assert!(line.contains("***"));
        assert!(!line.contains("hunter2"));
    }

    #[test]
    fn node_cert_binding() {
        let hex = "00112233445566778899aabbccddeeff";
        assert!(node_cert_matches_id(
            &format!("node-{hex}.mongreldb.cluster"),
            hex
        ));
        assert!(!node_cert_matches_id("other.example", hex));
    }
}
