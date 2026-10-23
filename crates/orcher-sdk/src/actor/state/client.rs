//! Low-level client for reading and writing actor state.
//!
//! [`ActorStateClient`] reads and writes through the worker host, on the
//! worker's connection and credentials, or an in-memory [`MockStateBackend`]
//! in tests. The server is the source of truth; the worker only keeps an
//! optional read cache.
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
//! The host makes the `GetState`, `SetState`, `DeleteState`, and
//! `ListStateKeys` calls, scoped to the operation that is running.

use crate::error::{Error, Result};
use moka::future::Cache;
use serde::{de::DeserializeOwned, Serialize};
use std::sync::Arc;
use std::time::Duration;

use super::mock::MockStateBackend;

/// How an [`ActorStateClient`] caches state.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ActorStateClientConfig {
    /// Cache state values in the worker.
    ///
    /// Only this client's own writes and deletes update the cache, so a cached
    /// value can be stale if another worker writes the same key.
    pub enable_cache: bool,

    /// How long a cached value is kept.
    pub cache_ttl: Duration,

    /// Maximum number of cached entries; the least recently used are evicted first.
    pub max_cache_size: u64,
}

impl Default for ActorStateClientConfig {
    fn default() -> Self {
        Self {
            enable_cache: true,
            cache_ttl: Duration::from_secs(60),
            max_cache_size: 1_000,
        }
    }
}

/// Reads and writes actor state on the server, with an optional local cache.
///
/// Values are JSON-encoded. Each call carries the execution ID of the running
/// operation. Cloning is cheap; clones share the backend and the cache.
///
/// # Examples
///
/// A worker hands one to each actor operation it runs, through its
/// [`ActorContext`](crate::actor::ActorContext); a test builds one with
/// [`ActorStateClient::new_mock`].
///
/// ```rust
/// use orcher_sdk::actor::ActorStateClient;
///
/// # #[tokio::main(flavor = "current_thread")]
/// # async fn main() -> orcher_sdk::Result<()> {
/// let client = ActorStateClient::new_mock();
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
    /// In-memory storage for tests.
    Mock(MockStateBackend),

    /// The worker host, on its connection, with the worker's credentials.
    Host(orcher_sdk_core::Host),
}

impl ActorStateClient {
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

    /// A client that reads and writes state through the worker host, on its
    /// connection and with the worker's TLS and credentials. One is made for
    /// each actor operation the worker runs, so its cache lives as long as
    /// the operation.
    pub(crate) fn from_host(host: orcher_sdk_core::Host) -> Self {
        let config = ActorStateClientConfig::default();
        let cache = config.enable_cache.then(|| {
            Arc::new(
                Cache::builder()
                    .max_capacity(config.max_cache_size)
                    .time_to_live(config.cache_ttl)
                    .build(),
            )
        });
        Self {
            backend: StateBackend::Host(host),
            config,
            cache,
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
            StateBackend::Mock(mock) => {
                let storage_key = format!("actor:{}:{}:{}", actor_name, key, state_key);
                match mock.get(&storage_key).await {
                    Some(bytes) => bytes,
                    None => return Ok(None),
                }
            }
            StateBackend::Host(host) => {
                let scope = orcher_sdk_core::StateScope::new(actor_name, key, execution_id);
                match host
                    .get_actor_state(&scope, state_key)
                    .await
                    .map_err(|e| Error::Network(format!("GetState failed: {e}")))?
                {
                    // An empty value is treated as absent.
                    Some(stored) if !stored.value.is_empty() => stored.value,
                    _ => return Ok(None),
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
            StateBackend::Host(host) => {
                let scope = orcher_sdk_core::StateScope::new(actor_name, key, execution_id);
                match host
                    .get_actor_state(&scope, state_key)
                    .await
                    .map_err(|e| Error::Network(format!("GetState failed: {e}")))?
                {
                    Some(stored) if !stored.value.is_empty() => {
                        let value = serde_json::from_slice(&stored.value).map_err(|e| {
                            Error::Serialization(format!("Failed to deserialize state: {}", e))
                        })?;
                        Ok((Some(value), stored.version))
                    }
                    Some(stored) => Ok((None, stored.version)),
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
            StateBackend::Mock(mock) => {
                let storage_key = format!("actor:{}:{}:{}", actor_name, key, state_key);
                mock.set(&storage_key, value_bytes.clone()).await;
            }
            StateBackend::Host(host) => {
                let scope = orcher_sdk_core::StateScope::new(actor_name, key, execution_id);
                host.set_actor_state(&scope, state_key, value_bytes.clone(), expected_version)
                    .await
                    .map_err(|e| Error::Actor(format!("Failed to set state: {e}")))?;
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
            StateBackend::Mock(mock) => {
                let storage_key = format!("actor:{}:{}:{}", actor_name, key, state_key);
                Ok(mock.delete(&storage_key).await)
            }
            StateBackend::Host(host) => {
                let scope = orcher_sdk_core::StateScope::new(actor_name, key, execution_id);
                host.delete_actor_state(&scope, state_key)
                    .await
                    .map_err(|e| Error::Network(format!("DeleteState failed: {e}")))
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
            StateBackend::Host(host) => {
                let scope = orcher_sdk_core::StateScope::new(actor_name, key, execution_id);
                host.actor_state_keys(&scope, prefix.unwrap_or_default())
                    .await
                    .map_err(|e| Error::Network(format!("ListStateKeys failed: {e}")))
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
                StateBackend::Mock(mock) => StateBackend::Mock(mock.clone()),
                StateBackend::Host(host) => StateBackend::Host(host.clone()),
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
            enable_cache: false,
            cache_ttl: Duration::from_secs(5),
            ..Default::default()
        };

        assert_eq!(config.cache_ttl, Duration::from_secs(5));
        assert!(!config.enable_cache);
    }
}
