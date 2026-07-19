//! HTTPS embedding provider with bounded I/O and referenced secrets.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use mongreldb_core::{
    EmbeddingError, EmbeddingFuture, EmbeddingNormalization, EmbeddingProvider, EmbeddingRequest,
    EmbeddingResponse, ProviderExecutionMode, ProviderHealth,
};
use serde::{Deserialize, Serialize};
use zeroize::Zeroizing;

const HEALTH_READY: u8 = 0;
const HEALTH_DEGRADED: u8 = 1;
const HEALTH_UNAVAILABLE: u8 = 2;

pub trait EmbeddingSecretResolver: Send + Sync {
    fn resolve(&self, reference: &str) -> Result<Zeroizing<String>, EmbeddingError>;
}

/// Environment-backed secret references. Configuration stores only the
/// variable name. Values never enter Debug output or replicated schema.
#[derive(Debug, Default)]
pub struct EnvironmentSecretResolver;

impl EmbeddingSecretResolver for EnvironmentSecretResolver {
    fn resolve(&self, reference: &str) -> Result<Zeroizing<String>, EmbeddingError> {
        if reference.is_empty()
            || !reference
                .bytes()
                .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        {
            return Err(provider_error("invalid environment secret reference"));
        }
        std::env::var(reference)
            .map(Zeroizing::new)
            .map_err(|_| provider_error("embedding secret reference is unavailable"))
    }
}

#[derive(Clone)]
pub struct RemoteEmbeddingConfig {
    pub provider_id: String,
    pub model_id: String,
    pub model_version: String,
    pub preprocessing_version: String,
    pub dimension: u32,
    pub normalization: EmbeddingNormalization,
    pub endpoint: reqwest::Url,
    pub allowed_hosts: BTreeSet<String>,
    pub secret_reference: String,
    pub tenant: String,
    pub timeout: Duration,
    pub max_retries: usize,
    pub max_response_bytes: usize,
}

impl std::fmt::Debug for RemoteEmbeddingConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RemoteEmbeddingConfig")
            .field("provider_id", &self.provider_id)
            .field("model_id", &self.model_id)
            .field("model_version", &self.model_version)
            .field("endpoint", &self.endpoint)
            .field("secret_reference", &"<redacted>")
            .field("tenant", &self.tenant)
            .finish_non_exhaustive()
    }
}

#[derive(Clone)]
pub struct RemoteEmbeddingProvider {
    config: RemoteEmbeddingConfig,
    secrets: Arc<dyn EmbeddingSecretResolver>,
    client: reqwest::Client,
    health: Arc<AtomicU8>,
}

impl RemoteEmbeddingProvider {
    pub fn new(
        config: RemoteEmbeddingConfig,
        secrets: Arc<dyn EmbeddingSecretResolver>,
    ) -> Result<Self, EmbeddingError> {
        let host = config
            .endpoint
            .host_str()
            .ok_or_else(|| provider_error("embedding endpoint has no host"))?;
        if config.endpoint.scheme() != "https" {
            return Err(provider_error("embedding endpoint must use HTTPS"));
        }
        if !config.allowed_hosts.contains(host) {
            return Err(provider_error("embedding endpoint host is not allowlisted"));
        }
        if !config.endpoint.username().is_empty() || config.endpoint.password().is_some() {
            return Err(provider_error(
                "embedding endpoint must not contain credentials",
            ));
        }
        if config.tenant.is_empty()
            || config.timeout.is_zero()
            || config.max_response_bytes == 0
            || config.dimension == 0
        {
            return Err(provider_error("invalid remote embedding limits"));
        }
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(config.timeout)
            .build()
            .map_err(|error| provider_error(error.to_string()))?;
        Ok(Self {
            config,
            secrets,
            client,
            health: Arc::new(AtomicU8::new(HEALTH_READY)),
        })
    }

    async fn request(
        &self,
        texts: &[&str],
        trace_id: &str,
    ) -> Result<EmbeddingResponse, EmbeddingError> {
        let secret = self.secrets.resolve(&self.config.secret_reference)?;
        let request = RemoteRequest {
            model: &self.config.model_id,
            input: texts,
        };
        let mut attempt = 0;
        loop {
            let result = self
                .client
                .post(self.config.endpoint.clone())
                .bearer_auth(secret.as_str())
                .header("x-mongreldb-tenant", &self.config.tenant)
                .header("x-mongreldb-trace-id", trace_id)
                .json(&request)
                .send()
                .await;
            match result {
                Ok(response)
                    if response.status().is_server_error() && attempt < self.config.max_retries =>
                {
                    attempt += 1;
                    continue;
                }
                Err(error)
                    if (error.is_connect() || error.is_timeout())
                        && attempt < self.config.max_retries =>
                {
                    attempt += 1;
                    continue;
                }
                Ok(response) => return self.decode(response).await,
                Err(error) => {
                    self.health.store(HEALTH_UNAVAILABLE, Ordering::Release);
                    return Err(provider_error(error.to_string()));
                }
            }
        }
    }

    async fn decode(
        &self,
        response: reqwest::Response,
    ) -> Result<EmbeddingResponse, EmbeddingError> {
        if !response.status().is_success() {
            self.health.store(HEALTH_DEGRADED, Ordering::Release);
            return Err(provider_error(format!(
                "remote provider returned HTTP {}",
                response.status()
            )));
        }
        if response
            .content_length()
            .is_some_and(|length| length > self.config.max_response_bytes as u64)
        {
            return Err(provider_error("remote embedding response is too large"));
        }
        let mut bytes = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| provider_error(error.to_string()))?;
            if bytes.len().saturating_add(chunk.len()) > self.config.max_response_bytes {
                return Err(provider_error("remote embedding response is too large"));
            }
            bytes.extend_from_slice(&chunk);
        }
        let response: RemoteResponse =
            serde_json::from_slice(&bytes).map_err(|error| provider_error(error.to_string()))?;
        self.health.store(HEALTH_READY, Ordering::Release);
        Ok(EmbeddingResponse {
            vectors: match response {
                RemoteResponse::Vectors { vectors } => vectors,
                RemoteResponse::Data { data } => {
                    data.into_iter().map(|item| item.embedding).collect()
                }
            },
        })
    }
}

impl EmbeddingProvider for RemoteEmbeddingProvider {
    fn provider_id(&self) -> &str {
        &self.config.provider_id
    }

    fn model_id(&self) -> &str {
        &self.config.model_id
    }

    fn model_version(&self) -> &str {
        &self.config.model_version
    }

    fn dimension(&self) -> u32 {
        self.config.dimension
    }

    fn normalization(&self) -> EmbeddingNormalization {
        self.config.normalization
    }

    fn preprocessing_version(&self) -> &str {
        &self.config.preprocessing_version
    }

    fn execution_mode(&self) -> ProviderExecutionMode {
        ProviderExecutionMode::Remote
    }

    fn health(&self) -> ProviderHealth {
        match self.health.load(Ordering::Acquire) {
            HEALTH_READY => ProviderHealth::Ready,
            HEALTH_DEGRADED => ProviderHealth::Degraded,
            _ => ProviderHealth::Unavailable,
        }
    }

    fn embed(&self, _request: EmbeddingRequest<'_>) -> Result<EmbeddingResponse, EmbeddingError> {
        Err(provider_error(
            "remote embedding provider requires asynchronous execution",
        ))
    }

    fn embed_async<'a>(&'a self, request: EmbeddingRequest<'a>) -> EmbeddingFuture<'a> {
        Box::pin(async move { self.request(request.texts, request.trace_id).await })
    }
}

#[derive(Serialize)]
struct RemoteRequest<'a> {
    model: &'a str,
    input: &'a [&'a str],
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RemoteResponse {
    Vectors { vectors: Vec<Vec<f32>> },
    Data { data: Vec<RemoteEmbedding> },
}

#[derive(Deserialize)]
struct RemoteEmbedding {
    embedding: Vec<f32>,
}

fn provider_error(message: impl Into<String>) -> EmbeddingError {
    EmbeddingError::ProviderFailed {
        provider: "remote".into(),
        message: message.into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_must_be_https_and_allowlisted() {
        let config = |endpoint: &str, allowed_hosts: &[&str]| RemoteEmbeddingConfig {
            provider_id: "remote".into(),
            model_id: "model".into(),
            model_version: "1".into(),
            preprocessing_version: "1".into(),
            dimension: 2,
            normalization: EmbeddingNormalization::None,
            endpoint: endpoint.parse().unwrap(),
            allowed_hosts: allowed_hosts.iter().map(|host| (*host).into()).collect(),
            secret_reference: "EMBEDDING_TOKEN".into(),
            tenant: "tenant-a".into(),
            timeout: Duration::from_secs(1),
            max_retries: 1,
            max_response_bytes: 1024,
        };
        let secrets: Arc<dyn EmbeddingSecretResolver> = Arc::new(EnvironmentSecretResolver);
        assert!(RemoteEmbeddingProvider::new(
            config("http://provider.example/embed", &["provider.example"]),
            Arc::clone(&secrets),
        )
        .is_err());
        assert!(RemoteEmbeddingProvider::new(
            config("https://provider.example/embed", &["other.example"]),
            secrets,
        )
        .is_err());
    }
}
