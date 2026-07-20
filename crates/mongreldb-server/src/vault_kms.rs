//! HashiCorp Vault Transit implementation of the database KMS boundary.

use std::io::Read;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use mongreldb_core::{
    KeyManagementError, KeyManagementHealth, KeyManagementProvider, KmsWrappedKey,
};
use reqwest::blocking::{Client, RequestBuilder};
use reqwest::header::HeaderValue;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;
use zeroize::Zeroizing;

const ALGORITHM: &str = "vault-transit";
const MAX_VAULT_RESPONSE_BYTES: usize = 64 * 1024;

pub struct VaultTransitConfig {
    pub endpoint: String,
    pub mount: String,
    pub token: Zeroizing<String>,
    pub namespace: Option<String>,
    pub timeout: Duration,
    pub ca_certificate_pem: Option<Vec<u8>>,
}

impl std::fmt::Debug for VaultTransitConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VaultTransitConfig")
            .field("endpoint", &self.endpoint)
            .field("mount", &self.mount)
            .field("token", &"<redacted>")
            .field("namespace", &self.namespace)
            .field(
                "ca_certificate_pem",
                &self.ca_certificate_pem.as_ref().map(|_| "<configured>"),
            )
            .field("timeout", &self.timeout)
            .finish()
    }
}

pub struct VaultTransitKeyManagementProvider {
    client: Client,
    endpoint: reqwest::Url,
    mount: String,
    token: Zeroizing<String>,
    namespace: Option<String>,
    provider_id: String,
}

impl std::fmt::Debug for VaultTransitKeyManagementProvider {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("VaultTransitKeyManagementProvider")
            .field("endpoint", &self.endpoint)
            .field("mount", &self.mount)
            .field("token", &"<redacted>")
            .field("namespace", &self.namespace)
            .field("provider_id", &self.provider_id)
            .finish()
    }
}

impl VaultTransitKeyManagementProvider {
    pub fn new(config: VaultTransitConfig) -> Result<Self, KeyManagementError> {
        let mut endpoint = reqwest::Url::parse(&config.endpoint)
            .map_err(|error| KeyManagementError::Failed(format!("invalid Vault URL: {error}")))?;
        if endpoint.scheme() != "https" {
            return Err(KeyManagementError::Failed(
                "Vault endpoint must use HTTPS".into(),
            ));
        }
        if !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(KeyManagementError::Failed(
                "Vault endpoint must not contain credentials, query, or fragment".into(),
            ));
        }
        if config.token.is_empty() {
            return Err(KeyManagementError::Failed(
                "Vault token must not be empty".into(),
            ));
        }
        validate_path_segment("Vault mount", &config.mount)?;
        let endpoint_path = endpoint.path().trim_end_matches('/').to_owned();
        endpoint.set_path(&endpoint_path);

        let mut client = Client::builder().timeout(config.timeout);
        if let Some(pem) = config.ca_certificate_pem {
            let certificate = reqwest::Certificate::from_pem(&pem).map_err(|error| {
                KeyManagementError::Failed(format!("invalid Vault CA certificate: {error}"))
            })?;
            client = client.add_root_certificate(certificate);
        }
        let client = client
            .build()
            .map_err(|error| KeyManagementError::Failed(format!("Vault client: {error}")))?;
        let provider_hash = sha2::Sha256::digest(format!(
            "{endpoint}|{}|{}",
            config.mount,
            config.namespace.as_deref().unwrap_or_default()
        ));
        let provider_id = format!(
            "vault-transit:{}",
            provider_hash
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        Ok(Self {
            client,
            endpoint,
            mount: config.mount,
            token: config.token,
            namespace: config.namespace,
            provider_id,
        })
    }

    fn url(&self, operation: &str, key_id: &str) -> Result<reqwest::Url, KeyManagementError> {
        validate_path_segment("Vault operation", operation)?;
        validate_path_segment("Vault key id", key_id)?;
        let mut url = self.endpoint.clone();
        {
            let mut segments = url.path_segments_mut().map_err(|_| {
                KeyManagementError::Failed("Vault endpoint cannot be a base URL".into())
            })?;
            segments.pop_if_empty();
            segments.extend(["v1", &self.mount, operation, key_id]);
        }
        Ok(url)
    }

    fn authorize(&self, request: RequestBuilder) -> Result<RequestBuilder, KeyManagementError> {
        let mut token = HeaderValue::from_str(self.token.as_str())
            .map_err(|_| KeyManagementError::Failed("invalid Vault token header".into()))?;
        token.set_sensitive(true);
        let mut request = request.header("X-Vault-Token", token);
        if let Some(namespace) = &self.namespace {
            let mut namespace = HeaderValue::from_str(namespace)
                .map_err(|_| KeyManagementError::Failed("invalid Vault namespace".into()))?;
            namespace.set_sensitive(true);
            request = request.header("X-Vault-Namespace", namespace);
        }
        Ok(request)
    }

    fn post<T: Serialize, R: for<'de> Deserialize<'de>>(
        &self,
        operation: &str,
        key_id: &str,
        body: &T,
    ) -> Result<R, KeyManagementError> {
        let request = self.client.post(self.url(operation, key_id)?).json(body);
        let response = self
            .authorize(request)?
            .send()
            .map_err(|error| KeyManagementError::Unavailable(format!("Vault request: {error}")))?;
        let status = response.status();
        if !status.is_success() {
            return Err(KeyManagementError::Failed(format!(
                "Vault {operation} returned HTTP {status}"
            )));
        }
        decode_vault_response(response)
    }
}

impl KeyManagementProvider for VaultTransitKeyManagementProvider {
    fn provider_id(&self) -> &str {
        &self.provider_id
    }

    fn wrap_key(
        &self,
        key_id: &str,
        plaintext_key: &[u8],
    ) -> Result<KmsWrappedKey, KeyManagementError> {
        let response: EncryptResponse = self.post(
            "encrypt",
            key_id,
            &EncryptRequest {
                plaintext: STANDARD.encode(plaintext_key),
            },
        )?;
        let key_version = vault_ciphertext_version(&response.data.ciphertext)?;
        Ok(KmsWrappedKey {
            kms_key_id: key_id.into(),
            key_version,
            wrapped_dek: response.data.ciphertext.into_bytes(),
            algorithm: ALGORITHM.into(),
        })
    }

    fn unwrap_key(
        &self,
        wrapped: &KmsWrappedKey,
    ) -> Result<Zeroizing<Vec<u8>>, KeyManagementError> {
        if wrapped.algorithm != ALGORITHM {
            return Err(KeyManagementError::Failed(format!(
                "unsupported wrapped-key algorithm {:?}",
                wrapped.algorithm
            )));
        }
        let ciphertext = std::str::from_utf8(&wrapped.wrapped_dek)
            .map_err(|_| KeyManagementError::Failed("Vault ciphertext is not UTF-8".into()))?;
        let response: DecryptResponse = self.post(
            "decrypt",
            &wrapped.kms_key_id,
            &DecryptRequest { ciphertext },
        )?;
        let plaintext = STANDARD
            .decode(response.data.plaintext)
            .map_err(|_| KeyManagementError::Failed("Vault plaintext is not base64".into()))?;
        Ok(Zeroizing::new(plaintext))
    }

    fn rewrap_key(
        &self,
        wrapped: &KmsWrappedKey,
        new_key_id: &str,
    ) -> Result<KmsWrappedKey, KeyManagementError> {
        let plaintext = self.unwrap_key(wrapped)?;
        self.wrap_key(new_key_id, plaintext.as_ref())
    }

    fn provider_health(&self) -> KeyManagementHealth {
        let mut url = self.endpoint.clone();
        let Ok(mut segments) = url.path_segments_mut() else {
            return KeyManagementHealth::Unavailable;
        };
        segments.pop_if_empty();
        segments.extend(["v1", "sys", "health"]);
        drop(segments);
        match self
            .client
            .get(url)
            .send()
            .map(|response| response.status())
        {
            Ok(status) if status.is_success() => KeyManagementHealth::Ready,
            Ok(status) if matches!(status.as_u16(), 429 | 472 | 473) => {
                KeyManagementHealth::Degraded
            }
            _ => KeyManagementHealth::Unavailable,
        }
    }
}

fn decode_vault_response<R, T>(reader: R) -> Result<T, KeyManagementError>
where
    R: Read,
    T: for<'de> Deserialize<'de>,
{
    let mut bytes = Vec::new();
    reader
        .take((MAX_VAULT_RESPONSE_BYTES + 1) as u64)
        .read_to_end(&mut bytes)
        .map_err(|error| KeyManagementError::Failed(format!("Vault response: {error}")))?;
    if bytes.len() > MAX_VAULT_RESPONSE_BYTES {
        return Err(KeyManagementError::Failed(
            "Vault response exceeds 64 KiB".into(),
        ));
    }
    serde_json::from_slice(&bytes)
        .map_err(|error| KeyManagementError::Failed(format!("Vault response: {error}")))
}

fn validate_path_segment(name: &str, value: &str) -> Result<(), KeyManagementError> {
    if value.is_empty()
        || value == "."
        || value == ".."
        || value.contains('/')
        || value.contains('\\')
    {
        return Err(KeyManagementError::Failed(format!(
            "{name} must be one non-empty path segment"
        )));
    }
    Ok(())
}

fn vault_ciphertext_version(ciphertext: &str) -> Result<String, KeyManagementError> {
    let mut parts = ciphertext.splitn(3, ':');
    if parts.next() != Some("vault") {
        return Err(KeyManagementError::Failed(
            "Vault ciphertext has invalid prefix".into(),
        ));
    }
    let version = parts
        .next()
        .filter(|value| {
            value
                .strip_prefix('v')
                .is_some_and(|number| number.parse::<u64>().is_ok())
        })
        .ok_or_else(|| KeyManagementError::Failed("Vault ciphertext has invalid version".into()))?;
    if parts.next().is_none() {
        return Err(KeyManagementError::Failed(
            "Vault ciphertext is truncated".into(),
        ));
    }
    Ok(version.into())
}

#[derive(Serialize)]
struct EncryptRequest {
    plaintext: String,
}

#[derive(Deserialize)]
struct EncryptResponse {
    data: EncryptData,
}

#[derive(Deserialize)]
struct EncryptData {
    ciphertext: String,
}

#[derive(Serialize)]
struct DecryptRequest<'a> {
    ciphertext: &'a str,
}

#[derive(Deserialize)]
struct DecryptResponse {
    data: DecryptData,
}

#[derive(Deserialize)]
struct DecryptData {
    plaintext: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_requires_https_and_safe_mount() {
        let config = |endpoint: &str, mount: &str| VaultTransitConfig {
            endpoint: endpoint.into(),
            mount: mount.into(),
            token: Zeroizing::new("token".into()),
            namespace: None,
            timeout: Duration::from_secs(1),
            ca_certificate_pem: None,
        };
        assert!(
            VaultTransitKeyManagementProvider::new(config("http://vault.example", "transit"))
                .is_err()
        );
        assert!(VaultTransitKeyManagementProvider::new(config(
            "https://vault.example",
            "../transit"
        ))
        .is_err());
        assert!(
            VaultTransitKeyManagementProvider::new(config("https://vault.example", "transit"))
                .is_ok()
        );
    }

    #[test]
    fn parses_vault_ciphertext_versions() {
        assert_eq!(
            vault_ciphertext_version("vault:v12:ciphertext").unwrap(),
            "v12"
        );
        assert!(vault_ciphertext_version("invalid:v1:ciphertext").is_err());
        assert!(vault_ciphertext_version("vault:latest:ciphertext").is_err());
    }

    #[test]
    fn response_size_is_bounded_and_namespace_changes_identity() {
        let oversized = vec![b' '; MAX_VAULT_RESPONSE_BYTES + 1];
        assert!(decode_vault_response::<_, serde_json::Value>(oversized.as_slice()).is_err());

        let provider = |namespace| {
            VaultTransitKeyManagementProvider::new(VaultTransitConfig {
                endpoint: "https://vault.example".into(),
                mount: "transit".into(),
                token: Zeroizing::new("token".into()),
                namespace,
                timeout: Duration::from_secs(1),
                ca_certificate_pem: None,
            })
            .unwrap()
        };
        assert_ne!(
            provider(Some("team-a".into())).provider_id(),
            provider(Some("team-b".into())).provider_id()
        );
    }
}
