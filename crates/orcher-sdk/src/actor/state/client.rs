//! Low-level client for reading and writing actor state.
//!
//! [`ActorStateClient`] talks to the Orcher server over gRPC, or to an in-memory
//! [`MockStateBackend`] in tests. The server is the source of truth; the worker only
//! keeps an optional read cache.
//!
//! ## Architecture
//!
//! ```text
//! Worker (SDK)                    Server
//! ┌─────────────────┐            ┌──────────────────┐
//! │ ActorContext    │            │  ActorService    │
//! │   ↓             │            │                  │
//! │ StateManager    │   gRPC     │  State storage   │
//! │   ↓             │  ──────>   │                  │
//! │ ActorStateClient│            │                  │
//! └─────────────────┘            └──────────────────┘
//! ```
//!
//! State calls use the `GetState`, `SetState`, `DeleteState`, and `ListStateKeys`
//! RPCs of the `orcher.v1.ActorService` gRPC service.

use crate::error::{Error, Result};
use moka::future::Cache;
use orcher_proto::orcher::v1::{
    actor_service_client::ActorServiceClient, DeleteStateRequest, GetStateRequest,
    ListStateKeysRequest, SetStateRequest,
};
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

use super::mock::MockStateBackend;

/// Configuration for [`ActorStateClient`].
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ActorStateClientConfig {
    /// Server URL, such as `"http://localhost:8080"`.
    pub server_url: String,

    /// Per-request timeout.
    pub timeout: Duration,

    /// Not used by this client: failed requests are not retried.
    pub max_retries: u32,

    /// Cache state values in the worker.
    ///
    /// Only this client's own writes and deletes update the cache, so a cached
    /// value can be stale if another worker writes the same key.
    pub enable_cache: bool,

    /// How long a cached value is kept.
    pub cache_ttl: Duration,

    /// Maximum number of cached entries; the least recently used are evicted first.
    pub max_cache_size: u64,

    /// Not sent by this client.
    pub auth_token: Option<String>,

    /// TLS settings (default: None, which follows the URL scheme: `https://`
    /// uses TLS verified against the system trust store).
    pub tls: Option<crate::client::ClientTlsConfig>,
}

impl Default for ActorStateClientConfig {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:8080".to_string(),
            timeout: Duration::from_secs(10),
            max_retries: 3,
            enable_cache: true,
            cache_ttl: Duration::from_secs(60),
            max_cache_size: 1_000,
            auth_token: None,
            tls: None,
        }
    }
}

/// Reads and writes actor state on the server, with an optional local cache.
///
/// Values are JSON-encoded. Each call carries the execution ID of the running
/// operation. Cloning is cheap; clones share the connection and the cache.
///
/// # Examples
///
/// ```rust,no_run
/// use orcher_sdk::actor::{ActorStateClient, ActorStateClientConfig};
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> orcher_sdk::Result<()> {
/// let mut config = ActorStateClientConfig::default();
/// config.server_url = "http://localhost:50051".to_string();
/// let client = ActorStateClient::new(config).await?;
///
/// let items: Vec<String> = client
///     .get_state("ShoppingCart", "user-123", "cart", "exec-456")
///     .await?
///     .unwrap_or_default();
///
/// client
///     .set_state("ShoppingCart", "user-123", "cart", &items, "exec-456", None)
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct ActorStateClient {
    backend: StateBackend,

    config: ActorStateClientConfig,

    /// Keyed by `{actor_name}:{key}:{state_key}`; `None` when caching is disabled.
    cache: Option<Arc<Cache<String, Vec<u8>>>>,
}

/// Where state is actually stored.
enum StateBackend {
    /// The Orcher server.
    Grpc(ActorServiceClient<Channel>),

    /// In-memory storage for tests.
    Mock(MockStateBackend),
}

impl ActorStateClient {
    /// Connects to the server described by `config`.
    ///
    /// The connection attempt itself times out after 5 seconds.
    ///
    /// # Errors
    ///
    /// Returns an error if the URL is invalid or the connection fails.
    pub async fn new(config: ActorStateClientConfig) -> Result<Self> {
        let endpoint = crate::tls::endpoint(&config.server_url, config.tls.as_ref())
            .map_err(Error::Network)?
            .timeout(config.timeout)
            .connect_timeout(Duration::from_secs(5));

        let channel = endpoint.connect().await.map_err(|e| {
            Error::Network(format!(
                "Failed to connect to server: {}",
                crate::tls::with_causes(&e)
            ))
        })?;

        // State up to the configured message limit, not tonic's 4 MiB.
        let max = orcher_sdk_core::limits::default_max_message_bytes();
        let grpc_client = ActorServiceClient::new(channel)
            .max_decoding_message_size(max)
            .max_encoding_message_size(max);

        let cache = if config.enable_cache {
            Some(Arc::new(
                Cache::builder()
                    .max_capacity(config.max_cache_size)
                    .time_to_live(config.cache_ttl)
                    .build(),
            ))
        } else {
            None
        };

        Ok(Self {
            backend: StateBackend::Grpc(grpc_client),
            config,
            cache,
        })
    }

    /// Creates a client backed by fresh in-memory storage, for tests.
    ///
    /// No server is needed and caching is disabled.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk::actor::ActorStateClient;
    ///
    /// # #[tokio::main(flavor = "current_thread")]
    /// # async fn main() -> orcher_sdk::Result<()> {
    /// let client = ActorStateClient::new_mock();
    ///
    /// // Behaves like a server-backed client.
    /// client.set_state("Counter", "key1", "count", &3i64, "exec-1", None).await?;
    /// assert_eq!(client.get_state::<i64>("Counter", "key1", "count", "exec-1").await?, Some(3));
    /// # Ok(())
    /// # }
    /// ```
    pub fn new_mock() -> Self {
        Self {
            backend: StateBackend::Mock(MockStateBackend::new()),
            config: ActorStateClientConfig::default(),
            cache: None, // Cache disabled so tests always observe the backend.
        }
    }

    /// Creates a test client over an existing in-memory backend.
    ///
    /// Clients built from clones of the same backend share storage, so a test
    /// can inspect or seed state that handlers see.
    pub fn new_mock_with_backend(backend: MockStateBackend) -> Self {
        Self {
            backend: StateBackend::Mock(backend),
            config: ActorStateClientConfig::default(),
            cache: None,
        }
    }

    /// Reads `state_key` of the instance `key` of `actor_name`.
    ///
    /// Returns `None` if the key does not exist or holds an empty value. Served from
    /// the cache when possible; a value fetched from the server is cached.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or the value cannot be decoded as `T`.
    pub async fn get_state<T>(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
        execution_id: &str,
    ) -> Result<Option<T>>
    where
        T: DeserializeOwned,
    {
        let cache_key = format!("{}:{}:{}", actor_name, key, state_key);

        if let Some(cache) = &self.cache {
            if let Some(cached_bytes) = cache.get(&cache_key).await {
                return match serde_json::from_slice(&cached_bytes) {
                    Ok(value) => Ok(Some(value)),
                    Err(e) => Err(Error::Serialization(format!(
                        "Failed to deserialize cached state: {}",
                        e
                    ))),
                };
            }
        }

        let value_bytes = match &self.backend {
            StateBackend::Grpc(client) => {
                let request = tonic::Request::new(GetStateRequest {
                    actor_name: actor_name.to_string(),
                    key: key.to_string(),
                    state_key: state_key.to_string(),
                    execution_id: execution_id.to_string(),
                });

                let mut grpc_client = client.clone();
                let response = grpc_client
                    .get_state(request)
                    .await
                    .map_err(|e| Error::Network(format!("gRPC GetState failed: {}", e)))?;

                let state_response = response.into_inner();

                // An empty value is treated as absent.
                if !state_response.exists || state_response.value.is_empty() {
                    return Ok(None);
                }

                state_response.value
            }
            StateBackend::Mock(mock) => {
                let storage_key = format!("actor:{}:{}:{}", actor_name, key, state_key);
                match mock.get(&storage_key).await {
                    Some(bytes) => bytes,
                    None => return Ok(None),
                }
            }
        };

        if let Some(cache) = &self.cache {
            cache.insert(cache_key, value_bytes.clone()).await;
        }

        serde_json::from_slice(&value_bytes)
            .map(Some)
            .map_err(|e| Error::Serialization(format!("Failed to deserialize state: {}", e)))
    }

    /// Reads a state value together with its version.
    ///
    /// Pass the version to [`set_state`](Self::set_state) as `expected_version` for an
    /// optimistic-concurrency write. Always reads from the backend, bypassing the cache.
    /// If the key does not exist, the value is `None` and the version is whatever the
    /// server reports (empty with the mock backend, which has no versions).
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails or the value cannot be decoded as `T`.
    pub async fn get_state_with_version<T>(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
        execution_id: &str,
    ) -> Result<(Option<T>, String)>
    where
        T: DeserializeOwned,
    {
        match &self.backend {
            StateBackend::Grpc(client) => {
                let request = tonic::Request::new(GetStateRequest {
                    actor_name: actor_name.to_string(),
                    key: key.to_string(),
                    state_key: state_key.to_string(),
                    execution_id: execution_id.to_string(),
                });

                let mut grpc_client = client.clone();
                let response = grpc_client
                    .get_state(request)
                    .await
                    .map_err(|e| Error::Network(format!("gRPC GetState failed: {}", e)))?;

                let state_response = response.into_inner();
                let version = state_response.version.clone();

                if !state_response.exists || state_response.value.is_empty() {
                    return Ok((None, version));
                }

                let value = serde_json::from_slice(&state_response.value).map_err(|e| {
                    Error::Serialization(format!("Failed to deserialize state: {}", e))
                })?;

                Ok((Some(value), version))
            }
            StateBackend::Mock(mock) => {
                let storage_key = format!("actor:{}:{}:{}", actor_name, key, state_key);
                match mock.get(&storage_key).await {
                    Some(bytes) => {
                        let value = serde_json::from_slice(&bytes).map_err(|e| {
                            Error::Serialization(format!("Failed to deserialize state: {}", e))
                        })?;
                        Ok((Some(value), String::new()))
                    }
                    None => Ok((None, String::new())),
                }
            }
        }
    }

    /// Writes `value` to `state_key` of the instance `key` of `actor_name`.
    ///
    /// With `expected_version`, the server only applies the write if the stored
    /// version matches. The mock backend ignores it. On success the cache is updated.
    ///
    /// # Errors
    ///
    /// Returns an error if `value` cannot be encoded, the RPC fails, or the server
    /// rejects the write (including a version mismatch).
    pub async fn set_state<T>(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
        value: &T,
        execution_id: &str,
        expected_version: Option<&str>,
    ) -> Result<()>
    where
        T: Serialize,
    {
        let cache_key = format!("{}:{}:{}", actor_name, key, state_key);

        let value_bytes = serde_json::to_vec(value)
            .map_err(|e| Error::Serialization(format!("Failed to serialize state: {}", e)))?;

        match &self.backend {
            StateBackend::Grpc(client) => {
                let request = tonic::Request::new(SetStateRequest {
                    actor_name: actor_name.to_string(),
                    key: key.to_string(),
                    state_key: state_key.to_string(),
                    value: value_bytes.clone(),
                    execution_id: execution_id.to_string(),
                    expected_version: expected_version.unwrap_or("").to_string(),
                });

                let mut grpc_client = client.clone();
                let response = grpc_client
                    .set_state(request)
                    .await
                    .map_err(|e| Error::Network(format!("gRPC SetState failed: {}", e)))?;

                let set_response = response.into_inner();
                if !set_response.success {
                    return Err(Error::Actor(format!(
                        "Failed to set state: {}",
                        set_response.error_message
                    )));
                }
            }
            StateBackend::Mock(mock) => {
                let storage_key = format!("actor:{}:{}:{}", actor_name, key, state_key);
                mock.set(&storage_key, value_bytes.clone()).await;
            }
        }

        if let Some(cache) = &self.cache {
            cache.insert(cache_key, value_bytes).await;
        }

        Ok(())
    }

    /// Deletes `state_key` of the instance `key` of `actor_name`.
    ///
    /// Returns whether the key existed. The cache entry is dropped before the
    /// RPC, so it is gone even if the RPC fails.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn delete_state(
        &self,
        actor_name: &str,
        key: &str,
        state_key: &str,
        execution_id: &str,
    ) -> Result<bool> {
        let cache_key = format!("{}:{}:{}", actor_name, key, state_key);

        if let Some(cache) = &self.cache {
            cache.remove(&cache_key).await;
        }

        match &self.backend {
            StateBackend::Grpc(client) => {
                let request = tonic::Request::new(DeleteStateRequest {
                    actor_name: actor_name.to_string(),
                    key: key.to_string(),
                    state_key: state_key.to_string(),
                    execution_id: execution_id.to_string(),
                });

                let mut grpc_client = client.clone();
                let response = grpc_client
                    .delete_state(request)
                    .await
                    .map_err(|e| Error::Network(format!("gRPC DeleteState failed: {}", e)))?;

                let delete_response = response.into_inner();
                Ok(delete_response.existed)
            }
            StateBackend::Mock(mock) => {
                let storage_key = format!("actor:{}:{}:{}", actor_name, key, state_key);
                Ok(mock.delete(&storage_key).await)
            }
        }
    }

    /// Lists the state keys of the instance `key` of `actor_name`.
    ///
    /// With `prefix`, only keys starting with it are returned. Keys are returned
    /// without the `actor:{actor_name}:{key}:` storage prefix. Always reads from the
    /// backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the RPC fails.
    pub async fn list_state_keys(
        &self,
        actor_name: &str,
        key: &str,
        execution_id: &str,
        prefix: Option<&str>,
    ) -> Result<Vec<String>> {
        match &self.backend {
            StateBackend::Grpc(client) => {
                let request = tonic::Request::new(ListStateKeysRequest {
                    actor_name: actor_name.to_string(),
                    key: key.to_string(),
                    execution_id: execution_id.to_string(),
                    prefix: prefix.unwrap_or_default().to_string(),
                });

                let mut grpc_client = client.clone();
                let response = grpc_client
                    .list_state_keys(request)
                    .await
                    .map_err(|e| Error::Network(format!("gRPC ListStateKeys failed: {}", e)))?;

                let list_response = response.into_inner();

                Ok(list_response.keys)
            }
            StateBackend::Mock(mock) => {
                let prefix_str = format!("actor:{}:{}:{}", actor_name, key, prefix.unwrap_or(""));
                let keys = mock.list_keys(&prefix_str).await;

                // Return bare state keys, matching what the server returns.
                let state_keys = keys
                    .into_iter()
                    .filter_map(|k| {
                        k.strip_prefix(&format!("actor:{}:{}:", actor_name, key))
                            .map(|s| s.to_string())
                    })
                    .collect();

                Ok(state_keys)
            }
        }
    }

    /// Drops every cached value.
    pub async fn clear_cache(&self) {
        if let Some(cache) = &self.cache {
            cache.invalidate_all();
        }
    }

    /// Drops the cached value of one state key, so the next read goes to the backend.
    pub async fn invalidate_cache(&self, actor_name: &str, key: &str, state_key: &str) {
        if let Some(cache) = &self.cache {
            let cache_key = format!("{}:{}:{}", actor_name, key, state_key);
            cache.remove(&cache_key).await;
        }
    }
}

impl Clone for ActorStateClient {
    fn clone(&self) -> Self {
        Self {
            backend: match &self.backend {
                StateBackend::Grpc(client) => StateBackend::Grpc(client.clone()),
                StateBackend::Mock(mock) => StateBackend::Mock(mock.clone()),
            },
            config: self.config.clone(),
            cache: self.cache.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = ActorStateClientConfig::default();
        assert_eq!(config.server_url, "http://localhost:8080");
        assert!(config.enable_cache);
    }

    #[test]
    fn test_cache_key_format() {
        let cache_key = format!("{}:{}:{}", "ShoppingCart", "user-123", "cart");
        assert_eq!(cache_key, "ShoppingCart:user-123:cart");
    }

    #[tokio::test]
    async fn test_client_config() {
        let config = ActorStateClientConfig {
            server_url: "http://localhost:9999".to_string(),
            timeout: Duration::from_secs(5),
            enable_cache: false,
            ..Default::default()
        };

        assert_eq!(config.server_url, "http://localhost:9999");
        assert!(!config.enable_cache);
    }
}
