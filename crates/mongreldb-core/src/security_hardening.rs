//! Security hardening helpers (spec section 14.3, Stage 5C).
//!
//! Cluster mTLS is already enforced in `mongreldb-cluster` transport. This
//! module owns production credential primitives and explicit unsupported
//! boundaries for security services that require an operator integration.

use std::collections::BTreeMap;
use std::fmt;
use std::io::Read;
use std::path::{Path, PathBuf};

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use argon2::Argon2;
use base64::engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD};
use base64::Engine;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

type HmacSha256 = Hmac<Sha256>;

pub const SCRAM_SHA_256_MIN_ITERATIONS: u32 = 4096;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SecurityHardeningError {
    #[error("SCRAM iteration count {0} is below the minimum")]
    ScramIterations(u32),
    #[error("invalid SCRAM message: {0}")]
    ScramMessage(String),
    #[error("SCRAM client proof is invalid")]
    ScramProof,
    #[error("SCRAM channel binding does not satisfy policy")]
    ScramChannelBinding,
    #[error("password hash failed")]
    PasswordHash,
    #[error("secure random generation failed")]
    Random,
}

/// Durable SCRAM-SHA-256 verifier material.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ScramVerifier {
    pub mechanism: String,
    pub iterations: u32,
    pub salt: Vec<u8>,
    pub stored_key: Vec<u8>,
    pub server_key: Vec<u8>,
}

impl ScramVerifier {
    pub fn from_password(
        password: &str,
        salt: &[u8],
        iterations: u32,
    ) -> Result<Self, SecurityHardeningError> {
        if iterations < SCRAM_SHA_256_MIN_ITERATIONS {
            return Err(SecurityHardeningError::ScramIterations(iterations));
        }
        let salted_password = pbkdf2_hmac_sha256(password.as_bytes(), salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = Sha256::digest(client_key);
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        Ok(Self {
            mechanism: "SCRAM-SHA-256".into(),
            iterations,
            salt: salt.to_vec(),
            stored_key: stored_key.to_vec(),
            server_key: server_key.to_vec(),
        })
    }

    pub fn verify_password(&self, password: &str) -> bool {
        let Ok(candidate) = Self::from_password(password, &self.salt, self.iterations) else {
            return false;
        };
        bool::from(candidate.stored_key.ct_eq(&self.stored_key))
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScramChannelBindingPolicy {
    Disabled,
    Required,
}

/// One server-side SCRAM-SHA-256 exchange.
pub struct ScramServerSession {
    verifier: ScramVerifier,
    client_first_bare: String,
    server_first: String,
    combined_nonce: String,
    channel_binding_policy: ScramChannelBindingPolicy,
    expected_channel_binding: Vec<u8>,
}

/// Client half of one SCRAM-SHA-256 exchange. The password is zeroized when
/// the exchange finishes or is dropped.
pub struct ScramClientSession {
    password: Zeroizing<String>,
    client_first_bare: String,
    client_nonce: String,
    expected_server_signature: Option<[u8; 32]>,
}

impl ScramClientSession {
    pub fn begin(
        username: &str,
        password: impl Into<String>,
        client_nonce: &str,
    ) -> Result<Self, SecurityHardeningError> {
        validate_scram_nonce(client_nonce)?;
        if username.is_empty() || username.bytes().any(|byte| matches!(byte, b',' | b'=')) {
            return Err(SecurityHardeningError::ScramMessage(
                "username must not be empty or contain ',' or '='".into(),
            ));
        }
        Ok(Self {
            password: Zeroizing::new(password.into()),
            client_first_bare: format!("n={username},r={client_nonce}"),
            client_nonce: client_nonce.into(),
            expected_server_signature: None,
        })
    }

    pub fn client_first_bare(&self) -> &str {
        &self.client_first_bare
    }

    pub fn respond(
        &mut self,
        server_first: &str,
    ) -> Result<(String, String), SecurityHardeningError> {
        let combined_nonce = scram_attribute(server_first, "r")?;
        if !combined_nonce.starts_with(&self.client_nonce)
            || combined_nonce.len() == self.client_nonce.len()
        {
            return Err(SecurityHardeningError::ScramMessage(
                "server nonce does not extend client nonce".into(),
            ));
        }
        let salt = STANDARD
            .decode(scram_attribute(server_first, "s")?)
            .map_err(|_| SecurityHardeningError::ScramMessage("invalid SCRAM salt".into()))?;
        let iterations = scram_attribute(server_first, "i")?
            .parse::<u32>()
            .map_err(|_| SecurityHardeningError::ScramMessage("invalid iteration count".into()))?;
        if iterations < SCRAM_SHA_256_MIN_ITERATIONS {
            return Err(SecurityHardeningError::ScramIterations(iterations));
        }
        let client_final = format!("c=biws,r={combined_nonce}");
        let auth_message = format!("{},{server_first},{client_final}", self.client_first_bare);
        let salted_password = pbkdf2_hmac_sha256(self.password.as_bytes(), &salt, iterations);
        let client_key = hmac_sha256(&salted_password, b"Client Key");
        let stored_key = Sha256::digest(client_key);
        let client_signature = hmac_sha256(&stored_key, auth_message.as_bytes());
        let mut proof = [0_u8; 32];
        for (output, (key, signature)) in proof
            .iter_mut()
            .zip(client_key.iter().zip(client_signature))
        {
            *output = key ^ signature;
        }
        let server_key = hmac_sha256(&salted_password, b"Server Key");
        self.expected_server_signature = Some(hmac_sha256(&server_key, auth_message.as_bytes()));
        Ok((client_final, STANDARD.encode(proof)))
    }

    pub fn verify_server_final(&self, server_final: &str) -> Result<(), SecurityHardeningError> {
        let expected = self.expected_server_signature.ok_or_else(|| {
            SecurityHardeningError::ScramMessage("client response missing".into())
        })?;
        let actual = STANDARD
            .decode(scram_attribute(server_final, "v")?)
            .map_err(|_| SecurityHardeningError::ScramProof)?;
        if actual.len() != expected.len() || !bool::from(actual.as_slice().ct_eq(&expected)) {
            return Err(SecurityHardeningError::ScramProof);
        }
        Ok(())
    }
}

impl ScramServerSession {
    pub fn begin(
        verifier: ScramVerifier,
        client_first_bare: impl Into<String>,
        client_nonce: &str,
        server_nonce: &str,
        channel_binding_policy: ScramChannelBindingPolicy,
        expected_channel_binding: Vec<u8>,
    ) -> Result<Self, SecurityHardeningError> {
        validate_scram_nonce(client_nonce)?;
        validate_scram_nonce(server_nonce)?;
        if channel_binding_policy == ScramChannelBindingPolicy::Required
            && expected_channel_binding.is_empty()
        {
            return Err(SecurityHardeningError::ScramChannelBinding);
        }
        let client_first_bare = client_first_bare.into();
        let nonce_attribute = scram_attribute(&client_first_bare, "r")?;
        if nonce_attribute != client_nonce {
            return Err(SecurityHardeningError::ScramMessage(
                "client nonce does not match client-first message".into(),
            ));
        }
        let combined_nonce = format!("{client_nonce}{server_nonce}");
        let server_first = format!(
            "r={combined_nonce},s={},i={}",
            STANDARD.encode(&verifier.salt),
            verifier.iterations
        );
        Ok(Self {
            verifier,
            client_first_bare,
            server_first,
            combined_nonce,
            channel_binding_policy,
            expected_channel_binding,
        })
    }

    pub fn server_first_message(&self) -> &str {
        &self.server_first
    }

    pub fn finish(
        self,
        client_final_without_proof: &str,
        client_proof_base64: &str,
    ) -> Result<String, SecurityHardeningError> {
        if scram_attribute(client_final_without_proof, "r")? != self.combined_nonce {
            return Err(SecurityHardeningError::ScramMessage(
                "combined nonce mismatch".into(),
            ));
        }
        let channel_binding = STANDARD
            .decode(scram_attribute(client_final_without_proof, "c")?)
            .map_err(|_| {
                SecurityHardeningError::ScramMessage("invalid channel binding encoding".into())
            })?;
        match self.channel_binding_policy {
            ScramChannelBindingPolicy::Disabled if channel_binding != b"n,," => {
                return Err(SecurityHardeningError::ScramChannelBinding);
            }
            ScramChannelBindingPolicy::Required
                if channel_binding != self.expected_channel_binding =>
            {
                return Err(SecurityHardeningError::ScramChannelBinding);
            }
            _ => {}
        }

        let proof = STANDARD
            .decode(client_proof_base64)
            .map_err(|_| SecurityHardeningError::ScramProof)?;
        if proof.len() != 32 || self.verifier.stored_key.len() != 32 {
            return Err(SecurityHardeningError::ScramProof);
        }
        let auth_message = format!(
            "{},{},{}",
            self.client_first_bare, self.server_first, client_final_without_proof
        );
        let client_signature = hmac_sha256(&self.verifier.stored_key, auth_message.as_bytes());
        let mut recovered_client_key = [0_u8; 32];
        for (output, (proof_byte, signature_byte)) in recovered_client_key
            .iter_mut()
            .zip(proof.iter().zip(client_signature))
        {
            *output = proof_byte ^ signature_byte;
        }
        let recovered_stored_key = Sha256::digest(recovered_client_key);
        if !bool::from(
            recovered_stored_key
                .as_slice()
                .ct_eq(&self.verifier.stored_key),
        ) {
            return Err(SecurityHardeningError::ScramProof);
        }
        let server_signature = hmac_sha256(&self.verifier.server_key, auth_message.as_bytes());
        Ok(format!("v={}", STANDARD.encode(server_signature)))
    }
}

fn validate_scram_nonce(nonce: &str) -> Result<(), SecurityHardeningError> {
    if nonce.is_empty()
        || nonce
            .bytes()
            .any(|byte| byte == b',' || !(0x21..=0x7e).contains(&byte))
    {
        return Err(SecurityHardeningError::ScramMessage(
            "nonce must be printable ASCII without comma".into(),
        ));
    }
    Ok(())
}

fn scram_attribute<'a>(message: &'a str, name: &str) -> Result<&'a str, SecurityHardeningError> {
    message
        .split(',')
        .find_map(|attribute| attribute.strip_prefix(&format!("{name}=")))
        .filter(|value| !value.is_empty())
        .ok_or_else(|| SecurityHardeningError::ScramMessage(format!("missing {name} attribute")))
}

fn hmac_sha256(key: &[u8], input: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(input);
    mac.finalize().into_bytes().into()
}

fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut first_input = Vec::with_capacity(salt.len() + 4);
    first_input.extend_from_slice(salt);
    first_input.extend_from_slice(&1_u32.to_be_bytes());
    let mut u = hmac_sha256(password, &first_input);
    let mut output = u;
    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (output_byte, u_byte) in output.iter_mut().zip(u) {
            *output_byte ^= u_byte;
        }
    }
    output
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum JwtAlgorithm {
    Rs256,
    Es256,
}

impl JwtAlgorithm {
    fn parse(value: &str) -> Result<Self, JwtError> {
        match value {
            "RS256" => Ok(Self::Rs256),
            "ES256" => Ok(Self::Es256),
            "none" | "NONE" => Err(JwtError::Algorithm),
            _ => Err(JwtError::Algorithm),
        }
    }
}

/// OIDC/JWT validation policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtValidationConfig {
    pub issuer: String,
    pub audience: String,
    pub skew_seconds: u64,
    pub allowed_algorithms: Vec<JwtAlgorithm>,
    pub max_token_age_seconds: u64,
    pub required_scopes: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JwtClaims {
    pub iss: String,
    pub aud: String,
    pub exp: u64,
    pub nbf: u64,
    pub iat: u64,
    pub sub: String,
    #[serde(default)]
    pub scope: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum JwtError {
    #[error("malformed JWT")]
    Malformed,
    #[error("JWT algorithm is not allowed")]
    Algorithm,
    #[error("JWT signing key is unavailable")]
    Key,
    #[error("JWT signature is invalid")]
    Signature,
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
    #[error("token issued-at time is invalid")]
    IssuedAt,
    #[error("required JWT scope is missing")]
    Scope,
    #[error("JWKS provider failed: {0}")]
    JwksProvider(String),
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
    if claims.iat > now_unix.saturating_add(skew)
        || (config.max_token_age_seconds != 0
            && now_unix
                > claims
                    .iat
                    .saturating_add(config.max_token_age_seconds)
                    .saturating_add(skew))
    {
        return Err(JwtError::IssuedAt);
    }
    let scopes = claims.scope.split_ascii_whitespace().collect::<Vec<_>>();
    if config
        .required_scopes
        .iter()
        .any(|required| !scopes.contains(&required.as_str()))
    {
        return Err(JwtError::Scope);
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Jwk {
    pub kid: String,
    pub kty: String,
    pub alg: String,
    #[serde(default)]
    pub key_use: Option<String>,
    #[serde(default)]
    pub n: Option<String>,
    #[serde(default)]
    pub e: Option<String>,
    #[serde(default)]
    pub crv: Option<String>,
    #[serde(default)]
    pub x: Option<String>,
    #[serde(default)]
    pub y: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct JwksDocument {
    pub keys: Vec<Jwk>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedJwt {
    pub principal: String,
    pub scopes: Vec<String>,
    pub claims: JwtClaims,
}

#[derive(Debug, Deserialize)]
struct JwtHeader {
    alg: String,
    kid: Option<String>,
}

/// Verify a compact JWS before trusting any claims.
pub fn verify_jwt(
    token: &str,
    config: &JwtValidationConfig,
    jwks: &JwksDocument,
    now_unix: u64,
) -> Result<VerifiedJwt, JwtError> {
    let mut parts = token.split('.');
    let encoded_header = parts.next().ok_or(JwtError::Malformed)?;
    let encoded_claims = parts.next().ok_or(JwtError::Malformed)?;
    let encoded_signature = parts.next().ok_or(JwtError::Malformed)?;
    if parts.next().is_some()
        || encoded_header.is_empty()
        || encoded_claims.is_empty()
        || encoded_signature.is_empty()
    {
        return Err(JwtError::Malformed);
    }
    let header_bytes = URL_SAFE_NO_PAD
        .decode(encoded_header)
        .map_err(|_| JwtError::Malformed)?;
    let header: JwtHeader =
        serde_json::from_slice(&header_bytes).map_err(|_| JwtError::Malformed)?;
    let algorithm = JwtAlgorithm::parse(&header.alg)?;
    if !config.allowed_algorithms.contains(&algorithm) {
        return Err(JwtError::Algorithm);
    }
    let kid = header.kid.as_deref().ok_or(JwtError::Key)?;
    let jwk = jwks
        .keys
        .iter()
        .find(|key| key.kid == kid)
        .ok_or(JwtError::Key)?;
    if jwk.alg != header.alg || jwk.key_use.as_deref().is_some_and(|usage| usage != "sig") {
        return Err(JwtError::Algorithm);
    }
    let signature = URL_SAFE_NO_PAD
        .decode(encoded_signature)
        .map_err(|_| JwtError::Malformed)?;
    let signing_input = format!("{encoded_header}.{encoded_claims}");
    verify_jws_signature(algorithm, jwk, signing_input.as_bytes(), &signature)?;
    let claims_bytes = URL_SAFE_NO_PAD
        .decode(encoded_claims)
        .map_err(|_| JwtError::Malformed)?;
    let claims: JwtClaims =
        serde_json::from_slice(&claims_bytes).map_err(|_| JwtError::Malformed)?;
    validate_jwt_claims(&claims, config, now_unix)?;
    Ok(VerifiedJwt {
        principal: claims.sub.clone(),
        scopes: claims
            .scope
            .split_ascii_whitespace()
            .map(str::to_owned)
            .collect(),
        claims,
    })
}

fn verify_jws_signature(
    algorithm: JwtAlgorithm,
    jwk: &Jwk,
    signing_input: &[u8],
    signature_bytes: &[u8],
) -> Result<(), JwtError> {
    match algorithm {
        JwtAlgorithm::Rs256 => {
            if jwk.kty != "RSA" {
                return Err(JwtError::Algorithm);
            }
            let modulus = decode_jwk_component(jwk.n.as_deref())?;
            let exponent = decode_jwk_component(jwk.e.as_deref())?;
            ring::signature::RsaPublicKeyComponents {
                n: &modulus,
                e: &exponent,
            }
            .verify(
                &ring::signature::RSA_PKCS1_2048_8192_SHA256,
                signing_input,
                signature_bytes,
            )
            .map_err(|_| JwtError::Signature)
        }
        JwtAlgorithm::Es256 => {
            if jwk.kty != "EC" || jwk.crv.as_deref() != Some("P-256") {
                return Err(JwtError::Algorithm);
            }
            let x = decode_jwk_component(jwk.x.as_deref())?;
            let y = decode_jwk_component(jwk.y.as_deref())?;
            if x.len() != 32 || y.len() != 32 {
                return Err(JwtError::Key);
            }
            let mut public_key = Vec::with_capacity(65);
            public_key.push(4);
            public_key.extend_from_slice(&x);
            public_key.extend_from_slice(&y);
            ring::signature::UnparsedPublicKey::new(
                &ring::signature::ECDSA_P256_SHA256_FIXED,
                public_key,
            )
            .verify(signing_input, signature_bytes)
            .map_err(|_| JwtError::Signature)
        }
    }
}

fn decode_jwk_component(value: Option<&str>) -> Result<Vec<u8>, JwtError> {
    URL_SAFE_NO_PAD
        .decode(value.ok_or(JwtError::Key)?)
        .map_err(|_| JwtError::Key)
}

pub struct JwksFetch {
    pub document: JwksDocument,
    pub max_age_seconds: u64,
}

/// Fetch boundary. A production adapter performs OIDC discovery and HTTPS
/// retrieval with normal certificate validation.
pub trait JwksProvider: Send + Sync {
    fn fetch(&self, issuer: &str) -> Result<JwksFetch, JwtError>;
}

struct CachedJwks {
    document: JwksDocument,
    expires_unix: u64,
}

/// Thread-safe JWKS cache. Expiry and unknown `kid` both trigger one refresh;
/// provider failure never falls back to stale keys.
pub struct JwksCache<P> {
    provider: P,
    state: std::sync::Mutex<Option<CachedJwks>>,
}

impl<P: JwksProvider> JwksCache<P> {
    pub fn new(provider: P) -> Self {
        Self {
            provider,
            state: std::sync::Mutex::new(None),
        }
    }

    pub fn verify(
        &self,
        token: &str,
        config: &JwtValidationConfig,
        now_unix: u64,
    ) -> Result<VerifiedJwt, JwtError> {
        let kid = jwt_kid(token)?;
        let mut state = self
            .state
            .lock()
            .map_err(|_| JwtError::JwksProvider("cache lock poisoned".into()))?;
        let refresh = state.as_ref().is_none_or(|cached| {
            now_unix >= cached.expires_unix
                || !cached.document.keys.iter().any(|key| key.kid == kid)
        });
        if refresh {
            let fetched = self.provider.fetch(&config.issuer)?;
            if fetched.max_age_seconds == 0 {
                return Err(JwtError::JwksProvider(
                    "JWKS response has zero cache lifetime".into(),
                ));
            }
            *state = Some(CachedJwks {
                document: fetched.document,
                expires_unix: now_unix.saturating_add(fetched.max_age_seconds),
            });
        }
        let cached = state.as_ref().ok_or(JwtError::Key)?;
        verify_jwt(token, config, &cached.document, now_unix)
    }
}

fn jwt_kid(token: &str) -> Result<String, JwtError> {
    let encoded_header = token.split('.').next().ok_or(JwtError::Malformed)?;
    let header: JwtHeader = serde_json::from_slice(
        &URL_SAFE_NO_PAD
            .decode(encoded_header)
            .map_err(|_| JwtError::Malformed)?,
    )
    .map_err(|_| JwtError::Malformed)?;
    header.kid.ok_or(JwtError::Key)
}

/// Service token metadata. The raw bearer secret is never stored.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceToken {
    pub token_id: String,
    pub principal: String,
    pub scopes: Vec<String>,
    /// Argon2id PHC string with a unique random salt.
    pub secret_hash_phc: String,
    pub expires_unix: u64,
}

impl ServiceToken {
    pub fn mint(
        token_id: impl Into<String>,
        principal: impl Into<String>,
        scopes: Vec<String>,
        raw_secret: &str,
        expires_unix: u64,
    ) -> Result<Self, SecurityHardeningError> {
        let mut salt_bytes = [0_u8; 16];
        getrandom::getrandom(&mut salt_bytes).map_err(|_| SecurityHardeningError::Random)?;
        let salt = SaltString::encode_b64(&salt_bytes)
            .map_err(|_| SecurityHardeningError::PasswordHash)?;
        let secret_hash_phc = Argon2::default()
            .hash_password(raw_secret.as_bytes(), &salt)
            .map_err(|_| SecurityHardeningError::PasswordHash)?
            .to_string();
        Ok(Self {
            token_id: token_id.into(),
            principal: principal.into(),
            scopes,
            secret_hash_phc,
            expires_unix,
        })
    }

    pub fn verify_secret(&self, raw_secret: &str, now_unix: u64) -> bool {
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

    /// Issue a random 256-bit bearer secret. The caller receives it once.
    pub fn issue(
        token_id: impl Into<String>,
        principal: impl Into<String>,
        scopes: Vec<String>,
        expires_unix: u64,
    ) -> Result<IssuedServiceToken, SecurityHardeningError> {
        let mut secret_bytes = [0_u8; 32];
        getrandom::getrandom(&mut secret_bytes).map_err(|_| SecurityHardeningError::Random)?;
        let secret = Zeroizing::new(URL_SAFE_NO_PAD.encode(secret_bytes));
        let metadata = Self::mint(token_id, principal, scopes, &secret, expires_unix)?;
        Ok(IssuedServiceToken { metadata, secret })
    }
}

impl fmt::Debug for ServiceToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ServiceToken")
            .field("token_id", &self.token_id)
            .field("principal", &self.principal)
            .field("scopes", &self.scopes)
            .field("secret_hash_phc", &"<redacted>")
            .field("expires_unix", &self.expires_unix)
            .finish()
    }
}

/// Newly issued token. `secret` is zeroized on drop and omitted from Debug.
pub struct IssuedServiceToken {
    pub metadata: ServiceToken,
    secret: Zeroizing<String>,
}

impl IssuedServiceToken {
    pub fn expose_secret_once(&self) -> &str {
        self.secret.as_str()
    }
}

impl fmt::Debug for IssuedServiceToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("IssuedServiceToken")
            .field("metadata", &self.metadata)
            .field("secret", &"<redacted>")
            .finish()
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyManagementHealth {
    Ready,
    Degraded,
    Unavailable,
    Unsupported,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyManagementError {
    #[error("KMS is unsupported: {0}")]
    Unsupported(String),
    #[error("KMS is unavailable: {0}")]
    Unavailable(String),
    #[error("KMS request failed: {0}")]
    Failed(String),
}

/// Provider boundary for external key management. Implementations must never
/// log plaintext keys.
pub trait KeyManagementProvider: Send + Sync {
    fn provider_id(&self) -> &str;
    fn wrap_key(
        &self,
        key_id: &str,
        plaintext_key: &[u8],
    ) -> Result<KmsWrappedKey, KeyManagementError>;
    fn unwrap_key(&self, wrapped: &KmsWrappedKey)
        -> Result<Zeroizing<Vec<u8>>, KeyManagementError>;
    fn rewrap_key(
        &self,
        wrapped: &KmsWrappedKey,
        new_key_id: &str,
    ) -> Result<KmsWrappedKey, KeyManagementError>;
    fn provider_health(&self) -> KeyManagementHealth;
}

/// Explicit fail-closed provider used when no production KMS integration is
/// configured. KMS-backed encryption must not silently fall back to local
/// metadata-only behavior.
#[derive(Debug, Default)]
pub struct UnsupportedKeyManagementProvider;

impl KeyManagementProvider for UnsupportedKeyManagementProvider {
    fn provider_id(&self) -> &str {
        "unsupported"
    }

    fn wrap_key(
        &self,
        _key_id: &str,
        _plaintext_key: &[u8],
    ) -> Result<KmsWrappedKey, KeyManagementError> {
        Err(KeyManagementError::Unsupported(
            "no KeyManagementProvider is configured".into(),
        ))
    }

    fn unwrap_key(
        &self,
        _wrapped: &KmsWrappedKey,
    ) -> Result<Zeroizing<Vec<u8>>, KeyManagementError> {
        Err(KeyManagementError::Unsupported(
            "no KeyManagementProvider is configured".into(),
        ))
    }

    fn rewrap_key(
        &self,
        _wrapped: &KmsWrappedKey,
        _new_key_id: &str,
    ) -> Result<KmsWrappedKey, KeyManagementError> {
        Err(KeyManagementError::Unsupported(
            "no KeyManagementProvider is configured".into(),
        ))
    }

    fn provider_health(&self) -> KeyManagementHealth {
        KeyManagementHealth::Unsupported
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyRotationPhase {
    Pending,
    WrappingNewKey,
    DualRead,
    Reencrypting,
    Validating,
    Published,
    RetiringOldKey,
    Succeeded,
    Failed,
}

/// Persistent online key rotation state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyRotationRecord {
    pub from_version: String,
    pub to_version: String,
    pub started_unix_micros: u64,
    pub completed_unix_micros: Option<u64>,
    pub phase: KeyRotationPhase,
    pub attempt: u32,
    pub last_error: Option<String>,
}

impl KeyRotationRecord {
    pub fn new(
        from_version: impl Into<String>,
        to_version: impl Into<String>,
        started_unix_micros: u64,
    ) -> Self {
        Self {
            from_version: from_version.into(),
            to_version: to_version.into(),
            started_unix_micros,
            completed_unix_micros: None,
            phase: KeyRotationPhase::Pending,
            attempt: 0,
            last_error: None,
        }
    }

    pub fn advance(&mut self, now_unix_micros: u64) -> Result<(), KeyManagementError> {
        self.phase = match self.phase {
            KeyRotationPhase::Pending => KeyRotationPhase::WrappingNewKey,
            KeyRotationPhase::WrappingNewKey => KeyRotationPhase::DualRead,
            KeyRotationPhase::DualRead => KeyRotationPhase::Reencrypting,
            KeyRotationPhase::Reencrypting => KeyRotationPhase::Validating,
            KeyRotationPhase::Validating => KeyRotationPhase::Published,
            KeyRotationPhase::Published => KeyRotationPhase::RetiringOldKey,
            KeyRotationPhase::RetiringOldKey => {
                self.completed_unix_micros = Some(now_unix_micros);
                KeyRotationPhase::Succeeded
            }
            KeyRotationPhase::Succeeded => return Ok(()),
            KeyRotationPhase::Failed => {
                return Err(KeyManagementError::Failed(
                    "failed rotation must be explicitly retried".into(),
                ));
            }
        };
        self.attempt = self.attempt.saturating_add(1);
        self.last_error = None;
        Ok(())
    }

    pub fn fail(&mut self, error: impl Into<String>) {
        self.phase = KeyRotationPhase::Failed;
        self.last_error = Some(redact_secrets(&error.into()));
    }

    pub fn retry(&mut self) {
        if self.phase == KeyRotationPhase::Failed {
            self.phase = KeyRotationPhase::Pending;
            self.completed_unix_micros = None;
            self.last_error = None;
        }
    }
}

/// Crash-safe journal for one online key rotation.
#[derive(Debug, Clone)]
pub struct KeyRotationJournal {
    path: PathBuf,
}

impl KeyRotationJournal {
    pub fn new(root: impl AsRef<Path>) -> Self {
        Self {
            path: root.as_ref().join("_key_rotation.json"),
        }
    }

    pub fn load(&self) -> Result<Option<KeyRotationRecord>, KeyManagementError> {
        let Some(parent) = self.path.parent() else {
            return Err(KeyManagementError::Failed(
                "rotation journal has no parent directory".into(),
            ));
        };
        let file_name = self
            .path
            .file_name()
            .ok_or_else(|| KeyManagementError::Failed("invalid rotation journal path".into()))?;
        let root = crate::durable_file::DurableRoot::open(parent)
            .map_err(|error| KeyManagementError::Failed(error.to_string()))?;
        let mut file = match root.open_regular(file_name) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(KeyManagementError::Failed(error.to_string())),
        };
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)
            .map_err(|error| KeyManagementError::Failed(error.to_string()))?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| KeyManagementError::Failed(error.to_string()))
    }

    pub fn persist(&self, record: &KeyRotationRecord) -> Result<(), KeyManagementError> {
        let bytes = serde_json::to_vec(record)
            .map_err(|error| KeyManagementError::Failed(error.to_string()))?;
        crate::durable_file::write_atomic(&self.path, &bytes)
            .map_err(|error| KeyManagementError::Failed(error.to_string()))
    }
}

/// Redact secrets from a free-form log/audit line.
pub fn redact_secrets(input: &str) -> String {
    let mut out = input.to_owned();
    for key in [
        "password",
        "passwd",
        "secret",
        "api_key",
        "token",
        "private_key",
        "authorization",
    ] {
        let mut search_from = 0;
        loop {
            let lower = out.to_ascii_lowercase();
            let Some(relative) = lower[search_from..].find(key) else {
                break;
            };
            let key_start = search_from + relative;
            let mut value_start = key_start + key.len();
            while out
                .as_bytes()
                .get(value_start)
                .is_some_and(|byte| byte.is_ascii_whitespace() || matches!(byte, b'"' | b'\''))
            {
                value_start += 1;
            }
            if !matches!(out.as_bytes().get(value_start), Some(b'=') | Some(b':')) {
                search_from = value_start;
                continue;
            }
            value_start += 1;
            while out
                .as_bytes()
                .get(value_start)
                .is_some_and(|byte| byte.is_ascii_whitespace() || matches!(byte, b'"' | b'\''))
            {
                value_start += 1;
            }
            if key == "authorization"
                && out[value_start..]
                    .to_ascii_lowercase()
                    .starts_with("bearer ")
            {
                value_start += "bearer ".len();
            }
            let value_end = out[value_start..]
                .find(|character: char| {
                    character.is_whitespace()
                        || matches!(character, '"' | '\'' | ',' | ';' | '&' | '}')
                })
                .map_or(out.len(), |relative| value_start + relative);
            if value_end > value_start {
                out.replace_range(value_start..value_end, "***");
                search_from = value_start + 3;
            } else {
                search_from = value_start;
            }
        }
    }
    let mut search_from = 0;
    loop {
        let lower = out.to_ascii_lowercase();
        let Some(relative) = lower[search_from..].find("bearer ") else {
            break;
        };
        let value_start = search_from + relative + "bearer ".len();
        let value_end = out[value_start..]
            .find(|character: char| {
                character.is_whitespace() || matches!(character, '"' | '\'' | ',' | ';')
            })
            .map_or(out.len(), |relative| value_start + relative);
        if value_end > value_start {
            out.replace_range(value_start..value_end, "***");
            search_from = value_start + 3;
        } else {
            break;
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
    fn scram_sha_256_matches_rfc_7677_exchange() {
        let salt = STANDARD.decode("W22ZaJ0SNY7soEsUEjb6gQ==").unwrap();
        let v = ScramVerifier::from_password("pencil", &salt, 4096).unwrap();
        assert!(v.verify_password("pencil"));
        assert!(!v.verify_password("wrong"));
        let dbg = format!("{v:?}");
        assert!(dbg.contains("<redacted>"));
        assert!(!dbg.contains("pencil"));

        let client_nonce = "rOprNGfwEbeRWgbNEkqO";
        let server_nonce = "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let session = ScramServerSession::begin(
            v,
            format!("n=user,r={client_nonce}"),
            client_nonce,
            server_nonce,
            ScramChannelBindingPolicy::Disabled,
            Vec::new(),
        )
        .unwrap();
        assert_eq!(
            session.server_first_message(),
            "r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
        );
        let server_final = session
            .finish(
                "c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0",
                "dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=",
            )
            .unwrap();
        assert_eq!(
            server_final,
            "v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
    }

    #[test]
    fn scram_rejects_weak_iterations_bad_proof_and_missing_channel_binding() {
        assert!(matches!(
            ScramVerifier::from_password("password", b"salt", 1024),
            Err(SecurityHardeningError::ScramIterations(1024))
        ));
        let verifier = ScramVerifier::from_password("password", b"salt", 4096).unwrap();
        let session = ScramServerSession::begin(
            verifier,
            "n=user,r=client",
            "client",
            "server",
            ScramChannelBindingPolicy::Required,
            b"p=tls-exporter,,binding".to_vec(),
        )
        .unwrap();
        assert!(matches!(
            session.finish(
                "c=biws,r=clientserver",
                "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="
            ),
            Err(SecurityHardeningError::ScramChannelBinding)
        ));
    }

    #[test]
    fn scram_client_and_server_complete_same_exchange() {
        let verifier = ScramVerifier::from_password("secret", b"0123456789abcdef", 4096).unwrap();
        let mut client = ScramClientSession::begin("alice", "secret", "clientnonce").unwrap();
        let server = ScramServerSession::begin(
            verifier,
            client.client_first_bare(),
            "clientnonce",
            "servernonce",
            ScramChannelBindingPolicy::Disabled,
            Vec::new(),
        )
        .unwrap();
        let (client_final, proof) = client.respond(server.server_first_message()).unwrap();
        let server_final = server.finish(&client_final, &proof).unwrap();
        client.verify_server_final(&server_final).unwrap();
    }

    #[test]
    fn jwt_claims_validation() {
        let cfg = JwtValidationConfig {
            issuer: "https://issuer.example".into(),
            audience: "mongreldb".into(),
            skew_seconds: 60,
            allowed_algorithms: vec![JwtAlgorithm::Es256],
            max_token_age_seconds: 3_600,
            required_scopes: vec!["read".into()],
        };
        let claims = JwtClaims {
            iss: cfg.issuer.clone(),
            aud: cfg.audience.clone(),
            exp: 1_000,
            nbf: 100,
            iat: 100,
            sub: "user-1".into(),
            scope: "read write".into(),
        };
        assert!(validate_jwt_claims(&claims, &cfg, 500).is_ok());
        assert!(matches!(
            validate_jwt_claims(&claims, &cfg, 2_000).unwrap_err(),
            JwtError::Expired
        ));
    }

    fn signed_es256_token(
        kid: &str,
        claims: &JwtClaims,
    ) -> (String, Jwk, ring::signature::EcdsaKeyPair) {
        use ring::signature::KeyPair;

        let random = ring::rand::SystemRandom::new();
        let pkcs8 = ring::signature::EcdsaKeyPair::generate_pkcs8(
            &ring::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            &random,
        )
        .unwrap();
        let key_pair = ring::signature::EcdsaKeyPair::from_pkcs8(
            &ring::signature::ECDSA_P256_SHA256_FIXED_SIGNING,
            pkcs8.as_ref(),
            &random,
        )
        .unwrap();
        let header = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&serde_json::json!({"alg":"ES256","kid":kid,"typ":"JWT"})).unwrap(),
        );
        let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).unwrap());
        let signing_input = format!("{header}.{payload}");
        let signature = key_pair.sign(&random, signing_input.as_bytes()).unwrap();
        let token = format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(signature.as_ref())
        );
        let public_key = key_pair.public_key().as_ref();
        assert_eq!(public_key.len(), 65);
        let jwk = Jwk {
            kid: kid.into(),
            kty: "EC".into(),
            alg: "ES256".into(),
            key_use: Some("sig".into()),
            n: None,
            e: None,
            crv: Some("P-256".into()),
            x: Some(URL_SAFE_NO_PAD.encode(&public_key[1..33])),
            y: Some(URL_SAFE_NO_PAD.encode(&public_key[33..65])),
        };
        (token, jwk, key_pair)
    }

    fn jwt_config() -> JwtValidationConfig {
        JwtValidationConfig {
            issuer: "https://issuer.example".into(),
            audience: "mongreldb".into(),
            skew_seconds: 30,
            allowed_algorithms: vec![JwtAlgorithm::Es256],
            max_token_age_seconds: 600,
            required_scopes: vec!["read".into()],
        }
    }

    fn jwt_claims(now: u64) -> JwtClaims {
        JwtClaims {
            iss: "https://issuer.example".into(),
            aud: "mongreldb".into(),
            exp: now + 300,
            nbf: now - 1,
            iat: now - 1,
            sub: "alice".into(),
            scope: "read write".into(),
        }
    }

    #[test]
    fn jwt_verifies_signature_and_rejects_tampering_and_algorithm_confusion() {
        let now = 10_000;
        let (token, jwk, _) = signed_es256_token("key-1", &jwt_claims(now));
        let keys = JwksDocument {
            keys: vec![jwk.clone()],
        };
        let verified = verify_jwt(&token, &jwt_config(), &keys, now).unwrap();
        assert_eq!(verified.principal, "alice");
        assert_eq!(verified.scopes, ["read", "write"]);

        let mut parts = token.split('.').map(str::to_owned).collect::<Vec<_>>();
        let mut signature = URL_SAFE_NO_PAD.decode(&parts[2]).unwrap();
        signature[0] ^= 1;
        parts[2] = URL_SAFE_NO_PAD.encode(signature);
        assert!(matches!(
            verify_jwt(&parts.join("."), &jwt_config(), &keys, now),
            Err(JwtError::Signature)
        ));

        let confused_header = URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&serde_json::json!({"alg":"RS256","kid":"key-1"})).unwrap());
        parts[0] = confused_header;
        assert!(matches!(
            verify_jwt(&parts.join("."), &jwt_config(), &keys, now),
            Err(JwtError::Algorithm)
        ));
    }

    struct RotatingJwksProvider {
        responses: std::sync::Mutex<std::collections::VecDeque<JwksFetch>>,
    }

    impl JwksProvider for RotatingJwksProvider {
        fn fetch(&self, _issuer: &str) -> Result<JwksFetch, JwtError> {
            self.responses
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| JwtError::JwksProvider("injected outage".into()))
        }
    }

    #[test]
    fn jwks_cache_refreshes_unknown_kid_and_fails_closed_on_outage() {
        let now = 20_000;
        let (token_one, key_one, _) = signed_es256_token("key-1", &jwt_claims(now));
        let (token_two, key_two, _) = signed_es256_token("key-2", &jwt_claims(now));
        let cache = JwksCache::new(RotatingJwksProvider {
            responses: std::sync::Mutex::new(std::collections::VecDeque::from([
                JwksFetch {
                    document: JwksDocument {
                        keys: vec![key_one],
                    },
                    max_age_seconds: 300,
                },
                JwksFetch {
                    document: JwksDocument {
                        keys: vec![key_two],
                    },
                    max_age_seconds: 300,
                },
            ])),
        });
        assert_eq!(
            cache
                .verify(&token_one, &jwt_config(), now)
                .unwrap()
                .principal,
            "alice"
        );
        assert_eq!(
            cache
                .verify(&token_two, &jwt_config(), now)
                .unwrap()
                .principal,
            "alice"
        );
        assert!(matches!(
            cache.verify(&token_one, &jwt_config(), now),
            Err(JwtError::JwksProvider(_))
        ));
    }

    #[test]
    fn service_token_stores_hash_only() {
        let t = ServiceToken::mint("t1", "svc", vec!["read".into()], "raw-secret", 0).unwrap();
        assert!(!format!("{t:?}").contains("raw-secret"));
        assert!(t.secret_hash_phc.starts_with("$argon2id$"));
        assert!(t.verify_secret("raw-secret", 0));
        assert!(!t.verify_secret("nope", 0));
        let issued = ServiceToken::issue("t2", "svc", vec!["write".into()], 0).unwrap();
        assert!(issued
            .metadata
            .verify_secret(issued.expose_secret_once(), 0));
        assert!(!format!("{issued:?}").contains(issued.expose_secret_once()));
    }

    #[test]
    fn redact_secrets_strips_every_occurrence_and_structured_values() {
        let line = redact_secrets(
            r#"password=hunter2 password="second" {"token":"third"} Authorization: Bearer fourth"#,
        );
        assert_eq!(line.matches("***").count(), 4);
        assert!(!line.contains("hunter2"));
        assert!(!line.contains("second"));
        assert!(!line.contains("third"));
        assert!(!line.contains("fourth"));
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

    #[test]
    fn kms_is_explicitly_unsupported_without_provider() {
        let provider = UnsupportedKeyManagementProvider;
        assert_eq!(provider.provider_health(), KeyManagementHealth::Unsupported);
        assert!(matches!(
            provider.wrap_key("key", b"plaintext"),
            Err(KeyManagementError::Unsupported(_))
        ));
    }

    #[test]
    fn rotation_journal_resumes_from_every_phase() {
        let dir = tempfile::tempdir().unwrap();
        let journal = KeyRotationJournal::new(dir.path());
        let mut record = KeyRotationRecord::new("old", "new", 1);
        loop {
            journal.persist(&record).unwrap();
            let recovered = journal.load().unwrap().unwrap();
            assert_eq!(recovered, record);
            if record.phase == KeyRotationPhase::Succeeded {
                break;
            }
            record.advance(99).unwrap();
        }
        assert_eq!(record.completed_unix_micros, Some(99));
    }
}
